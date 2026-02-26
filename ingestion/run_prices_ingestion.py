"""
Prices ingestion pipeline (Stooq + ANP) -> BigQuery Bronze.

Goals:
- Avoid "downloading everything every time" by using an incremental watermark table (ingestion_state)
- Upsert into bronze_prices_daily using MERGE (idempotent runs)
- Keep sources separated by location granularity:
  - Stooq futures (global proxy): location_key = global_market_na (daily)
  - ANP ethanol (Brazil, typically weekly): location_key = rio_de_janeiro_rj_sudeste

Env vars:
- GCP_PROJECT_ID (required)
- BQ_DATASET (required)
- STQ_LOOKBACK_DAYS (optional, default 90)  # safety window
- ANP_LOOKBACK_WEEKS (optional, default 8)  # safety window
"""

import os
import io
import re
from datetime import date, timedelta
from typing import Optional, Dict, List

import pandas as pd
import requests
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Helpers: BigQuery
# -----------------------------

def get_client() -> bigquery.Client:
    return bigquery.Client(project=os.environ["GCP_PROJECT_ID"])


def get_last_loaded_date(
    client: bigquery.Client,
    dataset: str,
    source: str,
    entity: str
) -> Optional[date]:
    """
    Reads the incremental watermark (last_loaded_date) from ingestion_state.
    Returns None if no record exists yet.
    """
    q = f"""
      SELECT last_loaded_date
      FROM `{client.project}.{dataset}.ingestion_state`
      WHERE source = @source AND entity = @entity
      LIMIT 1
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("entity", "STRING", entity),
            ]
        ),
    )
    rows = list(job.result())
    if not rows or rows[0]["last_loaded_date"] is None:
        return None
    return rows[0]["last_loaded_date"]


def upsert_last_loaded_date(
    client: bigquery.Client,
    dataset: str,
    source: str,
    entity: str,
    last_loaded_date: date
) -> None:
    """
    Upserts the watermark into ingestion_state.
    """
    q = f"""
    MERGE `{client.project}.{dataset}.ingestion_state` T
    USING (SELECT @source AS source, @entity AS entity, @d AS last_loaded_date) S
    ON T.source = S.source AND T.entity = S.entity
    WHEN MATCHED THEN UPDATE SET
      last_loaded_date = S.last_loaded_date,
      updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (source, entity, last_loaded_date, updated_at)
      VALUES (S.source, S.entity, S.last_loaded_date, CURRENT_TIMESTAMP())
    """
    client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("entity", "STRING", entity),
                bigquery.ScalarQueryParameter("d", "DATE", last_loaded_date),
            ]
        ),
    ).result()


def load_to_staging(
    client: bigquery.Client,
    dataset: str,
    staging_table: str,
    df: pd.DataFrame
) -> None:
    """
    Loads df into a staging table (truncate). Used for MERGE into final bronze table.
    """
    table_id = f"{client.project}.{dataset}.{staging_table}"
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()


def merge_prices_into_bronze(
    client: bigquery.Client,
    dataset: str,
    staging_table: str,
    target_table: str = "bronze_prices_daily"
) -> None:
    """
    Upsert from staging -> bronze_prices_daily using (date, location_key, product).
    """
    q = f"""
    MERGE `{client.project}.{dataset}.{target_table}` T
    USING `{client.project}.{dataset}.{staging_table}` S
    ON  T.date = S.date
    AND T.location_key = S.location_key
    AND T.product = S.product

    WHEN MATCHED THEN UPDATE SET
      price = S.price,
      currency = S.currency,
      unit = S.unit,
      source = S.source,
      ingested_at = S.ingested_at

    WHEN NOT MATCHED THEN
      INSERT (date, location_key, product, price, currency, unit, source, ingested_at)
      VALUES (S.date, S.location_key, S.product, S.price, S.currency, S.unit, S.source, S.ingested_at)
    """
    client.query(q).result()


# -----------------------------
# Source A: Stooq (CSV)
# -----------------------------

def fetch_stooq_csv(symbol: str) -> pd.DataFrame:
    """
    Fetch daily OHLC from Stooq as CSV.
    Example symbols:
      - zc.f (corn futures)
      - le.f (live cattle futures)
      - (soy symbol depends on Stooq listing)
    """
    url = f"https://stooq.com/q/d/l/?s={symbol.lower()}&i=d"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    # Expect columns: Date, Open, High, Low, Close, Volume
    df.columns = [c.strip().lower() for c in df.columns]
    df.rename(columns={"date": "date", "close": "close"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def stooq_to_bronze(
    df: pd.DataFrame,
    product: str,
    location_key: str,
    source_name: str
) -> pd.DataFrame:
    """
    Normalize Stooq close price to bronze schema.
    We store Close as 'price' in USD (proxy). Unit is 'index' (or contract close).
    """
    out = pd.DataFrame({
        "date": df["date"],
        "location_key": location_key,
        "product": product,
        "price": pd.to_numeric(df["close"], errors="coerce"),
        "currency": "USD",
        "unit": "close",
        "source": source_name,
        "ingested_at": pd.Timestamp.utcnow()
    })
    out = out.dropna(subset=["date", "price"])
    return out


# -----------------------------
# Source B: ANP ethanol (Brazil)
# -----------------------------
"""
ANP data access varies (sometimes XLS/CSV per week, sometimes open data portal).

To keep this robust and CI-friendly:
- We start with a placeholder function that expects a direct CSV/XLS URL.
- You will provide the official ANP URL you want to use.
- The code reads the file, filters RJ (state), and aggregates to a daily/weekly date.

Next step after you send the ANP link:
- We finalize the parser mapping columns -> (date, price, unit, city/state).
"""

def download_anp_file(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def parse_anp_ethanol_rj(file_bytes: bytes, file_type: str = "xlsx") -> pd.DataFrame:
    """
    Parse ANP ethanol dataset and return standardized rows for RJ only.
    This function is a template because ANP column names differ by file.
    You will likely need to adjust column names once we confirm the file structure.

    Expected output columns:
      date, location_key, product, price, currency, unit, source, ingested_at
    """
    if file_type == "csv":
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        df = pd.read_excel(io.BytesIO(file_bytes))

    # ---- TODO: adjust these column names to the ANP file you choose ----
    # Common patterns:
    # - UF / Estado column
    # - Produto / Product column
    # - Data / Week column
    # - Preço médio revenda (R$/L)
    # -------------------------------------------------------------------
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Example guesses (will adjust after you share the ANP file columns):
    # df = df[df["uf"] == "RJ"]
    # df = df[df["produto"].str.contains("etanol", case=False, na=False)]

    raise NotImplementedError(
        "ANP parser is a template. Share the ANP dataset URL + column names (or a sample file) "
        "and I will finalize the parser for RJ ethanol."
    )


# -----------------------------
# Orchestration
# -----------------------------

def main():
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]

    # Incremental safety windows (even with watermarks)
    stq_lookback_days = int(os.environ.get("STQ_LOOKBACK_DAYS", "90"))
    anp_lookback_weeks = int(os.environ.get("ANP_LOOKBACK_WEEKS", "8"))

    client = get_client()

    # Define products and symbols for Stooq
    # NOTE: Soy symbol depends on Stooq availability; we'll confirm.
    stooq_map = {
        "milho": {"symbol": "zc.f", "source": "stooq_zc_f"},
        "boi_gordo": {"symbol": "le.f", "source": "stooq_le_f"},
        # TODO: confirm soybean symbol on Stooq; placeholder below:
        "soja": {"symbol": "zs.f", "source": "stooq_zs_f"},
    }

    location_global = "global_market_na"

    # Build a combined dataframe for all Stooq products
    stooq_frames: List[pd.DataFrame] = []

    for product, meta in stooq_map.items():
        last_loaded = get_last_loaded_date(client, dataset, "stooq", product)
        # Use watermark if exists; otherwise fallback to lookback window
        cutoff = (date.today() - timedelta(days=stq_lookback_days)) if last_loaded is None else last_loaded
        raw = fetch_stooq_csv(meta["symbol"])
        raw = raw[raw["date"] > cutoff]  # strict greater to avoid reloading last day
        bronze = stooq_to_bronze(raw, product, location_global, meta["source"])
        if not bronze.empty:
            stooq_frames.append(bronze)

    # If we have any Stooq rows, upsert them
    if stooq_frames:
        stooq_all = pd.concat(stooq_frames, ignore_index=True)
        staging = "_stg_prices_stooq_load"
        load_to_staging(client, dataset, staging, stooq_all)
        merge_prices_into_bronze(client, dataset, staging)

        # Update watermarks per product
        for product in stooq_map.keys():
            max_date = stooq_all.loc[stooq_all["product"] == product, "date"].max()
            if pd.notna(max_date):
                upsert_last_loaded_date(client, dataset, "stooq", product, max_date)

        print(f"✅ Stooq upsert done: {len(stooq_all)} rows -> {project}.{dataset}.bronze_prices_daily")
    else:
        print("ℹ️ No new Stooq rows to load (watermarks up to date).")

    # --- ANP ethanol (template) ---
    # Once you provide the official ANP file URL, we finalize parsing and enable this section.
    # Example:
    # anp_url = os.environ.get("ANP_URL")
    # if anp_url:
    #     bytes_ = download_anp_file(anp_url)
    #     ethanol_df = parse_anp_ethanol_rj(bytes_, file_type="xlsx")
    #     staging = "_stg_prices_anp_load"
    #     load_to_staging(client, dataset, staging, ethanol_df)
    #     merge_prices_into_bronze(client, dataset, staging)
    #     upsert_last_loaded_date(...)

if __name__ == "__main__":
    main()