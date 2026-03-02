"""
ANP Prices ingestion (Option A - robust):
- Ingests ANP weekly XLSX (official) for RJ / Rio de Janeiro city
- Products: Ethanol + Gasoline
- Writes to BigQuery bronze_prices_daily (idempotent MERGE)
- Maintains incremental watermark in ingestion_state

Env vars:
- GCP_PROJECT_ID (required)
- BQ_DATASET (required)
- ANP_LOOKBACK_WEEKS (optional, default 12)
"""

import os
import io
import re
import unicodedata
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
# Text normalization helpers
# -----------------------------

def norm(x: str) -> str:
    """Lowercase, trim, remove accents, and collapse whitespace."""
    x = str(x).strip().lower()
    x = "".join(
        ch for ch in unicodedata.normalize("NFKD", x)
        if not unicodedata.combining(ch)
    )
    x = re.sub(r"\s+", " ", x)
    return x

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
    Extract XLSX links like:
      resumo_semanal_lpc_YYYY-MM-DD_YYYY-MM-DD.xlsx
    from the ANP page HTML.
    """
    links = re.findall(r'href="([^"]*resumo_semanal_lpc_[^"]*\.xlsx)"', html, flags=re.IGNORECASE)

    norm_links: List[str] = []
    for l in links:
        if l.startswith("http"):
            norm_links.append(l)
        else:
            norm_links.append("https://www.gov.br" + l)

    # Deduplicate while preserving order
    seen = set()
    out = []
    for l in norm_links:
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
    Robust ANP weekly XLSX parser:
    - Tries multiple sheets (ANP layout changes)
    - Auto-detects header row (skips cover/title sections)
    - Normalizes accents and spacing
    - Filters RJ + Rio de Janeiro city
    - Keeps only ETANOL and GASOLINA
    - Uses 'Data Final' as week date when available
    """

    xls_file = pd.ExcelFile(io.BytesIO(file_bytes))

    def find_header_row(preview_df: pd.DataFrame) -> Optional[int]:
        for i in range(min(len(preview_df), 120)):
            row_vals = [norm(v) for v in preview_df.iloc[i].tolist() if pd.notna(v)]
            row_text = " | ".join(row_vals)
            if (("estado" in row_text or "uf" in row_text)
                and ("produto" in row_text)
                and ("municip" in row_text)):
                return i
        return None

    def find_col_contains(columns: List[str], *tokens: str) -> Optional[str]:
        for c in columns:
            if all(t in c for t in tokens):
                return c
        return None

    # Try up to first N sheets
    for sheet_name in xls_file.sheet_names[:8]:
        preview = pd.read_excel(xls_file, sheet_name=sheet_name, header=None)
        header_row = find_header_row(preview)
        if header_row is None:
            continue

        df = pd.read_excel(xls_file, sheet_name=sheet_name, header=header_row)
        df.columns = [norm(c) for c in df.columns]

        col_estado = find_col_contains(df.columns, "estado") or find_col_contains(df.columns, "uf")
        col_mun = find_col_contains(df.columns, "municip")
        col_prod = find_col_contains(df.columns, "produto")
        col_preco = (
            find_col_contains(df.columns, "preco", "medio", "revenda") or
            find_col_contains(df.columns, "preco", "medio", "venda") or
            find_col_contains(df.columns, "preco", "revenda")
        )
        col_data_fim = find_col_contains(df.columns, "data", "final")
        col_data_ini = find_col_contains(df.columns, "data", "inicial")

        if any(v is None for v in [col_estado, col_mun, col_prod, col_preco]):
            # Not the expected table even if header-like row exists
            continue

        dfx = df.copy()
        dfx["estado"] = dfx[col_estado].astype(str).str.strip().str.upper()
        dfx["municipio"] = dfx[col_mun].astype(str).str.strip().str.upper()
        dfx["produto"] = dfx[col_prod].astype(str).str.strip().str.upper()

        # RJ + Rio de Janeiro
        dfx = dfx[(dfx["estado"] == "RJ") & (dfx["municipio"] == "RIO DE JANEIRO")]

        is_gas = dfx["produto"].str.contains("GASOLINA", na=False)
        is_eth = dfx["produto"].str.contains("ETANOL", na=False)
        dfx = dfx[is_gas | is_eth]

        if dfx.empty:
            # This file/sheet may not contain RJ rows
            continue

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

        out = (
            out.groupby(["date","location_key","product","currency","unit","source"], as_index=False)
               .agg(price=("price","mean"), ingested_at=("ingested_at","max"))
        )

        if not out.empty:
            return out

    raise ValueError(
        "ANP parser: could not find a valid sheet/header with (estado/uf, municipio, produto, preco). "
        "ANP file format may have changed."
    )

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

    for url in links[:40]:  # try first 40 (newest first, usually enough)
        m = re.search(r"resumo_semanal_lpc_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.xlsx", url)
        if m:
            week_end = pd.to_datetime(m.group(2)).date()
            if week_end <= cutoff_week:
                continue

        try:
            content = download_bytes(url)
            anp_df = parse_anp_weekly_rj_ethanol_gasoline(content)
        except ValueError as e:
            print(f"Parser failed for {url}: {e}. Trying next link...")
            continue
        except requests.HTTPError as e:
            print(f"HTTP error for {url}: {e}. Trying next link...")
            continue

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

    if not loaded_any:
        print("ℹ️ No new ANP week loaded (either up to date, RJ not present, or format changed).")

    print(f"Done. Target table: {project}.{dataset}.bronze_prices_daily")


if __name__ == "__main__":
    main()