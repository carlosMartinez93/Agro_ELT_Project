# Agro ELT Project

> A modern data pipeline for weather and fuel price analysis in the Brazilian agricultural sector.

![CI](https://github.com/carlosMartinez93/Agro_ELT_Project/actions/workflows/ci.yml/badge.svg)
![CD](https://github.com/carlosMartinez93/Agro_ELT_Project/actions/workflows/cd.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![dbt](https://img.shields.io/badge/dbt-1.8.7-orange?logo=dbt)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?logo=google-cloud)

---

## Overview

The **Agro ELT Project** is a production-ready data pipeline that ingests daily weather data and fuel prices (ethanol and gasoline) from ANP, transforms them into structured analytical layers via dbt, and serves the results in Google BigQuery for agribusiness analysis.

The pipeline runs automatically every day via GitHub Actions, ensuring fresh data with no manual intervention.

---

## Architecture

```
+------------------------------------------------------------------+
|                         Data Sources                             |
|                                                                  |
|   Open-Meteo API          ANP (Gov BR)          Seed Data        |
|   (Daily weather)      (Weekly prices)         (Locations)       |
+----------------+-----------------+------------------+-----------+
                 |                 |                  |
                 v                 v                  v
+------------------------------------------------------------------+
|                       Bronze Layer (Raw)                         |
|                          BigQuery                                |
|                                                                  |
|  bronze_weather_daily   bronze_prices_daily   bronze_locations   |
|  ingestion_state (watermark)                                     |
+---------------------------+--------------------------------------+
                            |  dbt models
                            v
+------------------------------------------------------------------+
|                      Silver Layer (Cleaned)                      |
|                     BigQuery Views (dbt)                         |
|                                                                  |
|              silver_weather_daily (deduped, typed)               |
+---------------------------+--------------------------------------+
                            |  dbt models
                            v
+------------------------------------------------------------------+
|                       Gold Layer (Business)                      |
|                     BigQuery Tables (dbt)                        |
|                                                                  |
|  gold_fct_weather_daily    gold_dim_location    gold_dim_date    |
+------------------------------------------------------------------+
```

---

## Data Sources

| Source | Type | Data | Frequency |
|--------|------|------|-----------|
| [Open-Meteo](https://open-meteo.com/) | REST API (free) | Max/min temperature, precipitation | Daily |
| [ANP](https://www.gov.br/anp) | XLSX (gov portal) | Ethanol and gasoline prices (BRL/liter) | Weekly |
| Seed | BigQuery table | Cities, states, regions, coordinates | Static |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Data Warehouse | Google BigQuery |
| Transformation | dbt-core 1.8.7 + dbt-bigquery |
| GCP Authentication | Workload Identity Federation |
| Orchestration | GitHub Actions |
| Excel Parsing | openpyxl |
| HTTP | requests |

---

## Project Structure

```
Agro_ELT_Project/
+-- .github/
|   +-- workflows/
|   |   +-- ci.yml              # Runs dbt build on PRs
|   |   +-- cd.yml              # Ingestion + dbt every day at 10:00 UTC
|   |   +-- pages.yml           # Publishes dbt docs to GitHub Pages
|   +-- CODEOWNERS.txt
|   +-- pull_request_template.md
|
+-- dbt/agro_elt/
|   +-- models/
|   |   +-- silver/             # Cleaned and typed views
|   |   +-- gold/               # Facts and dimensions for analysis
|   |   +-- sources.yml         # Bronze source definitions
|   +-- tests/                  # Referential integrity tests
|   +-- dbt_project.yml
|   +-- profiles.yml
|
+-- ingestion/
|   +-- run_weather_ingestions.py   # Open-Meteo -> BigQuery ingestion
|   +-- run_prices_ingestion.py     # ANP XLSX -> BigQuery ingestion
|
+-- requirements.txt
+-- README.md
```

---

## dbt Models

### Silver
| Model | Type | Description |
|-------|------|-------------|
| `silver_weather_daily` | View | Cleaned and deduped daily weather by (date, location_key) |

### Gold
| Model | Type | Description |
|-------|------|-------------|
| `gold_fct_weather_daily` | Table | Daily weather fact table (max/min temp, precipitation) |
| `gold_dim_location` | Table | Location dimension (city, state, region, coords) |
| `gold_dim_date` | Table | Date dimension (year, month, day, year_month) |

---

## Ingestion Patterns

All ingestion scripts follow the **Staging + MERGE** pattern to guarantee idempotency:

```
1. Fetch data from external source
2. Load into staging table (_stg_*) with WRITE_TRUNCATE
3. MERGE into bronze table (insert or update)
4. Update watermark in ingestion_state (for incremental loading)
```

This means the pipeline can run multiple times on the same day **without duplicating data**.

---

## CI/CD

### CI - Pull Requests
- Installs dependencies and authenticates to GCP via WIF
- Runs `dbt build` (silver + gold models + all tests)
- Generates and uploads dbt documentation as a build artifact

### CD - Daily Deploy
- **Schedule**: every day at 10:00 UTC (~07:00 Brasilia time)
- **Trigger**: also fires on push to `main`
- Steps:
  1. Weather ingestion (last 30 days, configurable via `DAYS_BACK`)
  2. ANP price ingestion (last 12 weeks, configurable via `ANP_LOOKBACK_WEEKS`)
  3. dbt model execution

### Docs - GitHub Pages
- Automatically publishes the dbt documentation site on every push to `main`

---

## Configuration

### Environment Variables

```bash
# Required
GCP_PROJECT_ID=your-gcp-project
BQ_DATASET=analytics_engineering

# Optional
DAYS_BACK=30                # Days of weather history to fetch (default: 30)
ANP_LOOKBACK_WEEKS=12       # ANP price lookback in weeks (default: 12)
```

### GitHub Secrets (Workload Identity Federation)

```
WIF_PROVIDER          # Workload Identity Provider resource
WIF_SERVICE_ACCOUNT   # Service account email
```

### GitHub Variables (Actions)

```
GCP_PROJECT_ID
BQ_DATASET
```

---

## Running Locally

```bash
# Clone the repository
git clone https://github.com/carlosMartinez93/Agro_ELT_Project.git
cd Agro_ELT_Project

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env  # fill in your GCP credentials

# Run ingestion
python ingestion/run_weather_ingestions.py
python ingestion/run_prices_ingestion.py

# Run dbt transformations
cd dbt/agro_elt
dbt build
```

---

## Data Quality

dbt models include automated tests:

- **not_null**: critical columns can never be null
- **unique**: primary and composite keys are unique
- **relationships**: foreign keys validated against dimensions
- **Deduplication**: silver layer enforces 1 row per (date, location_key)

All tests run automatically on every PR via GitHub Actions.

---

## dbt Documentation

The full model documentation (data lineage, column descriptions, and test results) is automatically published to GitHub Pages on every deploy.

---

## Contributing

1. Fork the repository
2. Create a branch: `git checkout -b feat/my-feature`
3. Follow the [pull request template](.github/pull_request_template.md)
4. Open a PR - CI will run `dbt build` automatically

> **Code Owner**: [@carlosMartinez93](https://github.com/carlosMartinez93)