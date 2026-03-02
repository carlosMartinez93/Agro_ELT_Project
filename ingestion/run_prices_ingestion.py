# ingestion/run_prices_ingestion.py
"""
Prices ingestion pipeline:
- Stooq (global futures proxy) -> global_market_na (daily close)
- ANP weekly XLSX (official) -> rio_de_janeiro_rj_sudeste (weekly avg resale price)

Idempotent:
- Loads into staging tables then MERGEs into bronze_prices_daily
- Uses ingestion_state for incremental watermarks (Stooq + ANP)

Env vars:
- GCP_PROJECT_ID (required)
- BQ_DATASET (required)
- STQ_LOOKBACK_DAYS (optional, default 365)
- ANP_LOOKBACK_WEEKS (optional, default 12)
"""

import os
import io
import re
from datetime import date, timedelta
from typing import Optional, List, Dict

import pandas as pd
import requests
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

ANP_LATEST_WEEKS_PAGE = (
    "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/"
    "levantamento-de-precos-de-combustiveis-ultimas-semanas-pesquisadas"
)

# -----------------------------
# BigQuery helpers
# -----------------------------
def get_client() -> bigquery.Client:
    return bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

def get_last_loaded_date(client: bigquery.Client, dataset: str, source: str, entity: str) -> Optional[date]:
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

def upsert_last_loaded_date(client: bigquery.Client, dataset: str, source: str, entity: str, last_loaded_date: date) -> None:
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

def load_to_staging(client: bigquery.Client, dataset: str, staging_table: str, df: pd.DataFrame) -> None:
    table_id = f"{client.project}.{dataset}.{staging_table}"
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()

def merge_prices_into_bronze(client: bigquery.Client, dataset: str, staging_table: str, target_table: str = "bronze_prices_daily") -> None:
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
# Stooq (CSV)
# -----------------------------
def fetch_stooq_csv(symbol: str) -> pd.DataFrame:
    url = f"https://stooq.com/q/d/l/?s={symbol.lower()}&i=d"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def stooq_to_bronze(df: pd.DataFrame, product: str, location_key: str, source_name: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "date": df["date"],
        "location_key": location_key,
        "product": product,
        "price": pd.to_numeric(df["close"], errors="coerce"),
        "currency": "USD",
        "unit": "close",
        "source": source_name,
        "ingested_at": pd.Timestamp.utcnow(),
    }).dropna(subset=["date", "price"])
    return out

# -----------------------------
# ANP (weekly XLSX): auto-discover latest file links
# -----------------------------
def discover_latest_anp_weekly_xlsx_links(html: str) -> List[str]:
    """
    Extracts XLSX links for 'resumo_semanal_lpc_YYYY-MM-DD_YYYY-MM-DD.xlsx'
    from the ANP "latest weeks" page HTML.
    """
    links = re.findall(r'href="([^"]*resumo_semanal_lpc_[^"]*\.xlsx)"', html, flags=re.IGNORECASE)
    # Normalize relative links
    norm = []
    for l in links:
        if l.startswith("http"):
            norm.append(l)
        else:
            norm.append("https://www.gov.br" + l)
    # Deduplicate preserving order
    seen = set()
    out = []
    for l in norm:
        if l not in seen:
            out.append(l)
            seen.add(l)
    return out

def download_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def normalize_columns(cols: List[str]) -> Dict[str, str]:
    """
    Returns a mapping for detected columns in a robust way (case/accents/spacing).
    """
    def norm(s: str) -> str:
        s = s.lower().strip()
        s = re.sub(r"\s+", " ", s)
        return s

    col_map = {norm(c): c for c in cols}

    def pick_contains(*tokens: str) -> Optional[str]:
        for k, orig in col_map.items():
            if all(t in k for t in tokens):
                return orig
        return None

    # Common columns in ANP weekly summaries
    estado = pick_contains("estado") or pick_contains("uf")
    municipio = pick_contains("municipio")
    produto = pick_contains("produto")
    preco_revenda = (
        pick_contains("preço", "médio", "revenda") or
        pick_contains("preco", "medio", "revenda") or
        pick_contains("preco", "médio", "revenda") or
        pick_contains("preço", "medio", "revenda")
    )
    data_ini = pick_contains("data", "inicial")
    data_fim = pick_contains("data", "final")

    return {
        "estado": estado,
        "municipio": municipio,
        "produto": produto,
        "preco_revenda": preco_revenda,
        "data_ini": data_ini,
        "data_fim": data_fim,
    }

def parse_anp_weekly_rj_ethanol_gasoline(file_bytes: bytes) -> pd.DataFrame:
    """
    Parses the ANP weekly 'resumo_semanal_lpc' workbook and returns RJ (Rio de Janeiro city)
    for products ETANOL and GASOLINA.

    Output schema matches bronze_prices_daily.
    """
    xls = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0)
    cols = normalize_columns(list(xls.columns))

    missing = [k for k, v in cols.items() if k in ("estado","municipio","produto","preco_revenda") and v is None]
    if missing:
        raise ValueError(f"ANP parser couldn't detect required columns: {missing}. Columns seen: {list(xls.columns)}")

    df = xls.copy()
    df["__estado__"] = df[cols["estado"]].astype(str).str.strip().str.upper()
    df["__municipio__"] = df[cols["municipio"]].astype(str).str.strip().str.upper()
    df["__produto__"] = df[cols["produto"]].astype(str).str.strip().str.upper()

    # Filter RJ + Rio de Janeiro city
    df = df[(df["__estado__"] == "RJ") & (df["__municipio__"] == "RIO DE JANEIRO")]

    # Filter products of interest
    # Some files use "GASOLINA C" or "GASOLINA"
    is_gas = df["__produto__"].str.contains("GASOLINA", na=False)
    is_eth = df["__produto__"].str.contains("ETANOL", na=False)
    df = df[is_gas | is_eth]

    # Choose a representative date for the week: use "data final" if available, else today's date
    if cols["data_fim"] is not None:
        df["week_date"] = pd.to_datetime(df[cols["data_fim"]], errors="coerce").dt.date
    else:
        df["week_date"] = date.today()

    price = pd.to_numeric(df[cols["preco_revenda"]], errors="coerce")
    df = df.assign(__price__=price).dropna(subset=["week_date", "__price__"])

    def map_product(p: str) -> str:
        return "gasolina" if "GASOLINA" in p else "etanol"

    out = pd.DataFrame({
        "date": df["week_date"],
        "location_key": "rio_de_janeiro_rj_sudeste",
        "product": df["__produto__"].apply(map_product),
        "price": df["__price__"],
        "currency": "BRL",
        "unit": "R$/litro",
        "source": "anp_resumo_semanal_lpc",
        "ingested_at": pd.Timestamp.utcnow(),
    })

    # If multiple rows exist, take mean for the city/week/product (safety)
    out = (
        out.groupby(["date","location_key","product","currency","unit","source"], as_index=False)
           .agg(price=("price","mean"), ingested_at=("ingested_at","max"))
    )
    return out

# -----------------------------
# Main
# -----------------------------
def main():
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]

    stq_lookback_days = int(os.environ.get("STQ_LOOKBACK_DAYS", "365"))
    anp_lookback_weeks = int(os.environ.get("ANP_LOOKBACK_WEEKS", "12"))

    client = get_client()

    # ---- Stooq products ----
    stooq_map = {
        "milho": {"symbol": "zc.f", "source": "stooq_zc_f"},
        "soja": {"symbol": "zs.f", "source": "stooq_zs_f"},
        "boi_gordo": {"symbol": "le.f", "source": "stooq_le_f"},
    }
    location_global = "global_market_na"

    stooq_frames: List[pd.DataFrame] = []
    for product, meta in stooq_map.items():
        last_loaded = get_last_loaded_date(client, dataset, "stooq", product)
        cutoff = (date.today() - timedelta(days=stq_lookback_days)) if last_loaded is None else last_loaded
        raw = fetch_stooq_csv(meta["symbol"])
        raw = raw[raw["date"] > cutoff]
        bronze = stooq_to_bronze(raw, product, location_global, meta["source"])
        if not bronze.empty:
            stooq_frames.append(bronze)

    if stooq_frames:
        stooq_all = pd.concat(stooq_frames, ignore_index=True)
        staging = "_stg_prices_stooq_load"
        load_to_staging(client, dataset, staging, stooq_all)
        merge_prices_into_bronze(client, dataset, staging)

        for product in stooq_map.keys():
            max_date = stooq_all.loc[stooq_all["product"] == product, "date"].max()
            if pd.notna(max_date):
                upsert_last_loaded_date(client, dataset, "stooq", product, max_date)

        print(f"✅ Stooq upsert done: {len(stooq_all)} rows -> {project}.{dataset}.bronze_prices_daily")
    else:
        print("ℹ️ No new Stooq rows to load (watermarks up to date).")

    # ---- ANP weekly (ethanol + gasoline, RJ city) ----
    # We discover the latest weekly XLSX links from the ANP page itself.
    last_anp = get_last_loaded_date(client, dataset, "anp", "combustiveis_rj")
    cutoff_week = (date.today() - timedelta(days=7 * anp_lookback_weeks)) if last_anp is None else last_anp

    page = requests.get(ANP_LATEST_WEEKS_PAGE, timeout=60)
    page.raise_for_status()
    links = discover_latest_anp_weekly_xlsx_links(page.text)

    if not links:
        raise RuntimeError("Could not find ANP weekly XLSX links on the ANP latest weeks page.")

    # Try newest links first; load only weeks after cutoff_week
    loaded_any = False
    for url in links[:12]:  # limit attempts
        # Extract week end date from filename if possible
        m = re.search(r"resumo_semanal_lpc_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.xlsx", url)
        week_end = None
        if m:
            week_end = pd.to_datetime(m.group(2)).date()
            if week_end <= cutoff_week:
                continue

        try:
            content = download_bytes(url)
            anp_df = parse_anp_weekly_rj_ethanol_gasoline(content)

            if anp_df.empty:
                continue

            staging = "_stg_prices_anp_load"
            load_to_staging(client, dataset, staging, anp_df)
            merge_prices_into_bronze(client, dataset, staging)

            # Update watermark using max date loaded
            max_date = anp_df["date"].max()
            upsert_last_loaded_date(client, dataset, "anp", "combustiveis_rj", max_date)

            print(f"✅ ANP upsert done: {len(anp_df)} rows from {url}")
            loaded_any = True
            break  # load latest successful week and stop
        except requests.HTTPError:
            # Some weeks may be unavailable (404). Try next.
            continue

    if not loaded_any:
        print("ℹ️ No new ANP week loaded (either up to date or files unavailable).")

if __name__ == "__main__":
    main()