# ingestion/run_weather_ingestion.py
"""
Daily weather ingestion pipeline (Open-Meteo -> BigQuery Bronze).

What this script does:
1) Reads all locations from BigQuery table:  <PROJECT>.<DATASET>.bronze_locations
2) For each location (lat/lon), calls Open-Meteo daily endpoint for the last N days
3) Loads the result into a temporary staging table (WRITE_TRUNCATE)
4) MERGEs (upserts) into the target Bronze table: bronze_weather_daily
   - Key: (date, location_key)
   - This prevents duplicates and allows updates if the API values change

Expected tables:
- bronze_locations(location_key, city, state, region, latitude, longitude, ...)
- bronze_weather_daily(date, location_key, latitude, longitude, temp_max, temp_min, precip_sum, ...)

Env vars (can be loaded from .env):
- GCP_PROJECT_ID (required)
- BQ_DATASET (required)
- DAYS_BACK (optional, default 30)
"""

import os
from datetime import date, timedelta
from typing import List

import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv


# Load environment variables from a .env file (if present)
load_dotenv()


def get_bq_client() -> bigquery.Client:
    """
    Create a BigQuery client using Application Default Credentials (ADC)
    or GOOGLE_APPLICATION_CREDENTIALS if configured.
    """
    project_id = os.environ["GCP_PROJECT_ID"]
    return bigquery.Client(project=project_id)


def fetch_locations(client: bigquery.Client, dataset: str) -> pd.DataFrame:
    """
    Read locations with valid latitude/longitude from bronze_locations.
    We only ingest weather for locations that have coordinates.
    """
    query = f"""
        SELECT
          location_key,
          city,
          state,
          region,
          latitude,
          longitude
        FROM `{client.project}.{dataset}.bronze_locations`
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    return client.query(query).to_dataframe()


def fetch_open_meteo_daily(lat: float, lon: float, days_back: int) -> pd.DataFrame:
    """
    Fetch daily weather for the last `days_back` days from Open-Meteo API.

    API notes:
    - This endpoint returns a 'daily' object containing arrays for each variable.
    - We request: max temperature, min temperature, and precipitation sum.
    """
    end = date.today()
    start = end - timedelta(days=days_back)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "UTC",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }

    # Make the HTTP request with a reasonable timeout.
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    # Parse the JSON response and extract the daily arrays.
    payload = resp.json()
    daily = payload["daily"]

    # Build a tidy dataframe: one row per date.
    df = pd.DataFrame(
        {
            "date": daily["time"],
            "temp_max": daily["temperature_2m_max"],
            "temp_min": daily["temperature_2m_min"],
            "precip_sum": daily["precipitation_sum"],
            "latitude": lat,
            "longitude": lon,
        }
    )

    # Ensure "date" is a Python date (BigQuery DATE)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def load_to_staging_table(
    client: bigquery.Client, df: pd.DataFrame, dataset: str, staging_table: str
) -> None:
    """
    Load the dataframe into a staging table (truncate/recreate the contents).
    This is used to perform a MERGE into the final bronze table.
    """
    table_id = f"{client.project}.{dataset}.{staging_table}"

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        ),
    )
    job.result()  # Wait for the load job to finish


def merge_into_bronze(
    client: bigquery.Client, dataset: str, staging_table: str, target_table: str
) -> None:
    """
    Upsert rows from staging_table into target_table based on (date, location_key).

    Why MERGE?
    - Prevent duplicates if you run the pipeline multiple times
    - Allow updating values if the upstream API changes historical values
    """
    query = f"""
    MERGE `{client.project}.{dataset}.{target_table}` T
    USING `{client.project}.{dataset}.{staging_table}` S
    ON T.date = S.date AND T.location_key = S.location_key

    WHEN MATCHED THEN UPDATE SET
      temp_max   = S.temp_max,
      temp_min   = S.temp_min,
      precip_sum = S.precip_sum,
      latitude   = S.latitude,
      longitude  = S.longitude,
      source     = S.source,
      ingested_at = S.ingested_at

    WHEN NOT MATCHED THEN INSERT (
      date, location_key, latitude, longitude,
      temp_max, temp_min, precip_sum,
      source, ingested_at
    )
    VALUES (
      S.date, S.location_key, S.latitude, S.longitude,
      S.temp_max, S.temp_min, S.precip_sum,
      S.source, S.ingested_at
    )
    """
    client.query(query).result()


def main() -> None:
    """
    Main pipeline entrypoint.
    - Validates env vars
    - Pulls locations
    - Pulls Open-Meteo data per location
    - Loads to staging
    - MERGEs into Bronze
    """
    # Required environment variables
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]

    # Optional: number of days to fetch
    days_back = int(os.environ.get("DAYS_BACK", "30"))

    client = get_bq_client()

    # 1) Fetch locations from BigQuery
    locations = fetch_locations(client, dataset)
    if locations.empty:
        raise RuntimeError(
            "bronze_locations is empty or has no valid lat/lon. "
            "Insert at least one location with latitude and longitude."
        )

    # 2) Fetch weather data for each location and build a single dataframe
    frames: List[pd.DataFrame] = []
    for _, loc in locations.iterrows():
        lat = float(loc["latitude"])
        lon = float(loc["longitude"])
        location_key = loc["location_key"]

        # Fetch Open-Meteo daily weather
        df = fetch_open_meteo_daily(lat, lon, days_back)

        # Add location and metadata columns
        df["location_key"] = location_key
        df["source"] = "open-meteo"
        df["ingested_at"] = pd.Timestamp.utcnow()

        frames.append(df)

    final_df = pd.concat(frames, ignore_index=True)

    # 3) Load into a staging table (truncate each run)
    staging_table = "_stg_weather_daily_load"
    load_to_staging_table(client, final_df, dataset, staging_table)

    # 4) Merge (upsert) into the final Bronze table
    merge_into_bronze(client, dataset, staging_table, "bronze_weather_daily")

    print(
        f"✅ Done. Processed {len(final_df)} rows into "
        f"{project_id}.{dataset}.bronze_weather_daily"
    )


if __name__ == "__main__":
    main()
