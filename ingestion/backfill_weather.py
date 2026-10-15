"""
Historical weather backfill (Open-Meteo Archive API -> BigQuery Bronze).

The daily job (run_weather_ingestions.py) uses the forecast endpoint and only keeps
the last DAYS_BACK days. This script loads the full history once, using the same
staging + MERGE pattern, so it is safe to re-run.

Env vars:
- GCP_PROJECT_ID (required)
- BQ_DATASET (required)
- BACKFILL_START (optional, default 2022-01-01)
- BACKFILL_END   (optional, default today - 7 days; the archive lags a few days)
"""

import os
from datetime import date, timedelta

import pandas as pd
import requests

from run_weather_ingestions import (
    get_bq_client,
    fetch_locations,
    load_to_staging_table,
    merge_into_bronze,
)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEZONE = "America/Sao_Paulo"


def fetch_archive(lat: float, lon: float, start: date, end: date) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": TIMEZONE,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    resp = requests.get(ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    daily = resp.json()["daily"]

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
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def main() -> None:
    dataset = os.environ["BQ_DATASET"]
    start = date.fromisoformat(os.environ.get("BACKFILL_START", "2022-01-01"))
    end_env = os.environ.get("BACKFILL_END")
    end = date.fromisoformat(end_env) if end_env else date.today() - timedelta(days=7)

    client = get_bq_client()
    locations = fetch_locations(client, dataset)
    if locations.empty:
        raise RuntimeError("bronze_locations is empty. Run `dbt seed` first.")

    frames = []
    for _, loc in locations.iterrows():
        df = fetch_archive(float(loc["latitude"]), float(loc["longitude"]), start, end)
        df["location_key"] = loc["location_key"]
        df["source"] = "open-meteo-archive"
        df["ingested_at"] = pd.Timestamp.now(tz="UTC")
        frames.append(df)
        print(f"{loc['location_key']}: {len(df)} days ({start} -> {end})")

    final_df = pd.concat(frames, ignore_index=True)

    staging = "_stg_weather_backfill"
    load_to_staging_table(client, final_df, dataset, staging)
    merge_into_bronze(client, dataset, staging, "bronze_weather_daily")

    print(f"Done. {len(final_df)} rows merged into {client.project}.{dataset}.bronze_weather_daily")


if __name__ == "__main__":
    main()