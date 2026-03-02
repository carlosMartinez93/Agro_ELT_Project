"""
Option A (ANP only): Ethanol + Gasoline (RJ) -> BigQuery bronze_prices_daily

Why this version:
- Yahoo/Stooq may rate-limit/block in CI
- ANP weekly XLSX often contains a cover/title section before the real header row
- This script auto-detects the header row and parses robustly

What it does:
1) Discover latest weekly ANP XLSX links from the official ANP page
2) Download the newest file(s)
3) Parse RJ + Rio de Janeiro city rows for ETANOL and GASOLINA
4) Normalize to bronze_prices_daily schema
5) Load to staging then MERGE into bronze_prices_daily (idempotent)
6) Update ingestion_state watermark for ANP

Env vars:
- GCP_PROJECT_ID (required)
- BQ_DATASET (required)
- ANP_LOOKBACK_WEEKS (optional, default 12)
"""

import os
import io
import re
from datetime import date, timedelta
from typing import Optional, List

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


def get_last_loaded_date(
    client: bigquery.Client,
    dataset: str,
    source: str,
    entity: str
) -> Optional[date]:
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


def merge_prices_into_bronze(
    client: bigquery.Client,
    dataset: str,
    staging_table: str,
    target_table: str = "bronze_prices_daily"
) -> None:
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
# ANP helpers
# -----------------------------

def discover_latest_anp_weekly_xlsx_links(html: str) -> List[str]:
    """
    Extract XLSX links that look like:
      resumo_semanal_lpc_YYYY-MM-DD_YYYY-MM-DD.xlsx
    from the ANP 'latest weeks' page HTML.
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
    r = requests.get(url, timeout=90, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.content


def parse_anp_weekly_rj_ethanol_gasoline(file_bytes: bytes) -> pd.DataFrame:
    """
    Robust parser for ANP weekly XLSX:
    - Detects the real header row automatically (ANP files often have a cover/title first)
    - Filters to RJ + Rio de Janeiro city
    - Keeps only ETANOL and GASOLINA rows
    - Uses 'Data Final' if present as the week date (else 'Data Inicial', else today)
    - Returns standardized bronze rows for two products: etanol, gasolina
    """

    # Read first sheet WITHOUT headers to locate the real header row
    preview = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0, header=None)

    def norm(x: str) -> str:
        x = str(x).strip().lower()
        x = re.sub(r"\s+", " ", x)
        return x

    header_row = None
    for i in range(min(len(preview), 80)):  # scan first ~80 rows
        row_vals = [norm(v) for v in preview.iloc[i].tolist() if pd.notna(v)]
        row_text = " | ".join(row_vals)

        # Typical header row contains these concepts
        if ("estado" in row_text or "uf" in row_text) and "produto" in row_text and ("municip" in row_text):
            header_row = i
            break

    if header_row is None:
        raise ValueError("ANP parser: could not find header row (estado/municipio/produto). File format changed.")

    # Re-read with the detected header row
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0, header=header_row)
    df.columns = [norm(c) for c in df.columns]

    def find_col_contains(*tokens):
        for c in df.columns:
            if all(t in c for t in tokens):
                return c
        return None

    col_estado = find_col_contains("estado") or find_col_contains("uf")
    col_mun = find_col_contains("municip")
    col_prod = find_col_contains("produto")
    col_preco = (
        find_col_contains("preço", "médio", "revenda") or
        find_col_contains("preco", "medio", "revenda") or
        find_col_contains("preço", "medio", "revenda") or
        find_col_contains("preco", "médio", "revenda")
    )
    col_data_fim = find_col_contains("data", "final")
    col_data_ini = find_col_contains("data", "inicial")

    missing = [k for k, v in {
        "estado": col_estado,
        "municipio": col_mun,
        "produto": col_prod,
        "preco_revenda": col_preco
    }.items() if v is None]

    if missing:
        raise ValueError(f"ANP parser: missing columns {missing}. Columns seen: {list(df.columns)}")

    dfx = df.copy()
    dfx["estado"] = dfx[col_estado].astype(str).str.strip().str.upper()
    dfx["municipio"] = dfx[col_mun].astype(str).str.strip().str.upper()
    dfx["produto"] = dfx[col_prod].astype(str).str.strip().str.upper()

    # RJ + Rio de Janeiro city
    dfx = dfx[(dfx["estado"] == "RJ") & (dfx["municipio"] == "RIO DE JANEIRO")]

    # Keep only gasoline and ethanol
    is_gas = dfx["produto"].str.contains("GASOLINA", na=False)
    is_eth = dfx["produto"].str.contains("ETANOL", na=False)
    dfx = dfx[is_gas | is_eth]

    # Pick week date
    if col_data_fim is not None:
        dfx["week_date"] = pd.to_datetime(dfx[col_data_fim], errors="coerce").dt.date
    elif col_data_ini is not None:
        dfx["week_date"] = pd.to_datetime(dfx[col_data_ini], errors="coerce").dt.date
    else:
        dfx["week_date"] = date.today()

    dfx["price"] = pd.to_numeric(dfx[col_preco], errors="coerce")
    dfx = dfx.dropna(subset=["week_date", "price"])

    def map_product(p: str) -> str:
        return "gasolina" if "GASOLINA" in p else "etanol"

    out = pd.DataFrame({
        "date": dfx["week_date"],
        "location_key": "rio_de_janeiro_rj_sudeste",
        "product": dfx["produto"].apply(map_product),
        "price": dfx["price"],
        "currency": "BRL",
        "unit": "R$/litro",
        "source": "anp_resumo_semanal_lpc",
        "ingested_at": pd.Timestamp.utcnow(),
    })

    # Aggregate safety: 1 row per (week_date, product, location)
    out = (
        out.groupby(["date", "location_key", "product", "currency", "unit", "source"], as_index=False)
           .agg(price=("price", "mean"), ingested_at=("ingested_at", "max"))
    )
    return out

# -----------------------------
# Main
# -----------------------------

def main():
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]
    lookback_weeks = int(os.environ.get("ANP_LOOKBACK_WEEKS", "12"))

    client = get_client()

    last_anp = get_last_loaded_date(client, dataset, "anp", "combustiveis_rj")
    cutoff_week = (date.today() - timedelta(days=7 * lookback_weeks)) if last_anp is None else last_anp

    page = requests.get(ANP_LATEST_WEEKS_PAGE, timeout=90, headers={"User-Agent": "Mozilla/5.0"})
    page.raise_for_status()

    links = discover_latest_anp_weekly_xlsx_links(page.text)
    if not links:
        raise RuntimeError("Could not find ANP weekly XLSX links on the ANP 'latest weeks' page.")

    print(f"Found {len(links)} ANP XLSX link(s). Watermark cutoff={cutoff_week}")

    loaded_any = False

    # Try newest links first (limit attempts)
    for url in links[:15]:
        # Extract week end date from filename if possible
        m = re.search(r"resumo_semanal_lpc_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.xlsx", url)
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

            max_date = anp_df["date"].max()
            upsert_last_loaded_date(client, dataset, "anp", "combustiveis_rj", max_date)

            print(f"✅ ANP upsert done: {len(anp_df)} rows from {url}")
            loaded_any = True
            break

        except requests.HTTPError as e:
            # Some links may be stale/unavailable
            print(f"HTTP error for {url}: {e}. Trying next link...")
            continue

    if not loaded_any:
        print("ℹ️ No new ANP week loaded (either up to date or files unavailable).")

    print(f"Done. Target table: {project}.{dataset}.bronze_prices_daily")


if __name__ == "__main__":
    main()