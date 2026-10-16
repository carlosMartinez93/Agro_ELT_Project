"""Validate silver_prices_weekly (Base dos Dados mirror) against an official ANP semester CSV.

Usage:
  python scripts/validate_prices_vs_anp.py "<path to ANP csv>" [tolerance]
Needs GCP_PROJECT_ID (and optionally BQ_DATASET) plus Application Default Credentials.
"""
import csv
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from google.cloud import bigquery

STATES = {"SP", "GO", "MG", "MS", "PR"}
PRODUCTS = {"GASOLINA": "gasoline", "ETANOL": "ethanol"}


def week_start(d):
    return d - timedelta(days=d.weekday())  # Monday, same as date_trunc(week(monday))


def load_anp(path):
    sums, counts, seen = defaultdict(float), defaultdict(int), Counter()
    dates = []
    with open(path, encoding="latin-1", newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            if row["Estado - Sigla"].strip() not in STATES:
                continue
            prod = row["Produto"].strip().upper()
            seen[prod] += 1
            if prod not in PRODUCTS:
                continue
            raw = row["Valor de Venda"].strip().replace(",", ".")
            if not raw:
                continue
            price = float(raw)
            if not 1 <= price <= 20:
                continue
            d = datetime.strptime(row["Data da Coleta"].strip(), "%d/%m/%Y").date()
            dates.append(d)
            key = (week_start(d), row["Estado - Sigla"].strip(), PRODUCTS[prod])
            sums[key] += price
            counts[key] += 1
    anp = {k: (sums[k] / counts[k], counts[k]) for k in sums}
    return anp, seen, min(dates), max(dates)


def load_silver(wmin, wmax):
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    dataset = os.environ.get("BQ_DATASET", "analytics_engineering")
    sql = (
        f"select week_start, state, product, avg_price, n_obs "
        f"from `{dataset}.silver_prices_weekly` "
        f"where week_start between @a and @b"
    )
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("a", "DATE", wmin),
        bigquery.ScalarQueryParameter("b", "DATE", wmax),
    ])
    return {
        (r.week_start, r.state, r.product): (r.avg_price, r.n_obs)
        for r in client.query(sql, job_config=cfg).result()
    }


def main():
    path = sys.argv[1]
    tol = float(sys.argv[2]) if len(sys.argv) > 2 else 0.005
    anp, seen, first, last = load_anp(path)
    print(f"ANP file: {first} -> {last}")
    print("Products seen in the 5 states:", dict(seen))
    silver = load_silver(week_start(first), week_start(last))

    only_anp = [k for k in anp if k not in silver]
    only_silver = [k for k in silver if k not in anp]
    both = [k for k in anp if k in silver]
    diffs = sorted(((abs(anp[k][0] - silver[k][0]), k) for k in both), reverse=True)
    n_ok = sum(1 for d, _ in diffs if d <= tol)
    n_same_obs = sum(1 for k in both if anp[k][1] == silver[k][1])

    print(f"\nCells (week x state x product): ANP={len(anp)} silver={len(silver)} compared={len(both)}")
    print(f"Only in ANP: {len(only_anp)} | only in silver: {len(only_silver)}")
    print(f"Avg price within R$ {tol}: {n_ok}/{len(both)}")
    print(f"Same number of observations: {n_same_obs}/{len(both)}")
    if diffs:
        print(f"Max abs price difference: R$ {diffs[0][0]:.4f}")
        print("\nWorst 10 cells:")
        for d, k in diffs[:10]:
            print(f"  {k}  anp={anp[k][0]:.4f} (n={anp[k][1]})  silver={silver[k][0]:.4f} (n={silver[k][1]})  diff={d:.4f}")


if __name__ == "__main__":
    main()