# Silesia Housing Data Platform

End-to-end data engineering pipeline for the residential real estate market in the Silesian Voivodeship, Poland. Built as a production-style portfolio project — not a tutorial — covering ingestion, orchestration, storage, transformation, data quality, infrastructure-as-code, and analytics.

**Status: raw ingestion pipeline complete and running end-to-end.** Scraper → Great Expectations validation → GCS raw landing zone → BigQuery raw table are all built, deployed via Docker/Terraform, and orchestrated through a daily Airflow DAG. Transformation (dbt), CI/CD, and the dashboard layer are next — see [Roadmap](#roadmap).

## Motivation

Most portfolio ETL projects stop at "scrape + dump to CSV." This one is built the way a real internal analytics platform would be: validated ingestion, a proper star schema, orchestrated daily runs, infrastructure defined as code, and CI enforcing data quality on every push. The target market — Silesia's largest cities — is scoped down deliberately (8 cities for MVP, full list later) so the pipeline ships end-to-end before scope creep sets in.

## Current Progress

| Layer | Status |
|---|---|
| Scraper (OLX GraphQL, paginated, retry/backoff) | ✅ Done |
| Parser (typed field extraction, PL-language normalization) | ✅ Done |
| Local raw storage (partitioned by city/date) | ✅ Done |
| Unit tests (parser) | ✅ Done |
| Terraform (GCS bucket, BigQuery datasets, service accounts, IAM) | ✅ Done |
| Docker Compose (Airflow webserver/scheduler + scraper environment) | ✅ Done |
| Great Expectations checkpoints (critical + warning suites, unit tested) | ✅ Done |
| GCS raw landing zone | ✅ Done |
| Airflow DAG (scrape → validate → upload → load, daily, per-city selective run) | ✅ Done |
| BigQuery raw table (`raw_apartment_listings`, partitioned + clustered) | ✅ Done |
| dbt models (staging → star schema → marts) | ⬜ Not started |
| dbt snapshots (price history) | ⬜ Not started |
| GitHub Actions CI | ⬜ Not started |
| Power BI dashboard | ⬜ Not started |

## Tech Stack

- **Language:** Python 3.12+
- **Warehouse:** BigQuery
- **Storage:** Google Cloud Storage
- **Orchestration:** Apache Airflow 2.10 (Docker Compose)
- **Data Quality:** Great Expectations
- **Infra as Code:** Terraform
- **Containerization:** Docker / Docker Compose
- **Transformation (planned):** dbt
- **CI/CD (planned):** GitHub Actions
- **Visualization (planned):** Power BI

## Architecture

```
OLX listings
   ↓
Python scraper (GraphQL, rate-limited, retry/backoff)
   ↓
Local raw JSON (data/raw/{city}/{date}/listings.json)
   ↓
Great Expectations validation gate — blocks pipeline on critical failures
   ↓
GCS raw landing zone (gs://.../raw/{city}/{date}/listings.json)
   ↓
BigQuery raw table (raw_apartment_listings — partitioned on date_collected, clustered on source_city)
   ↓
dbt (staging → star schema → marts)   ← next up
   ↓
Power BI dashboard   ← planned
```

Orchestrated daily via Airflow, one task chain per city: `scrape → validate → upload → load_bq`. All infra (GCS bucket, BigQuery datasets, service accounts, IAM bindings) is provisioned via Terraform — nothing is created manually in the GCP console.

## What's Built So Far

### Scraper (`scraper/scrapper.py`)
Queries OLX's GraphQL search endpoint per city, paginating until results run out. Includes exponential-backoff retries on request failure and a randomized delay between requests/cities to keep load light.

### Parser (`scraper/parser.py`)
Flattens OLX's nested `params` array into a typed record. Handles Polish-language values directly at the source (`"Parter"` → `0`, `"Tak"/"Nie"` → bool, `"Wtórny"` → `"secondary"`), extracts numeric values from labeled strings (`"48,5 m²"` → `48.5`), and stamps each record with a `date_collected` UTC timestamp at parse time.

### Loader (`scraper/loader.py`)
Writes parsed listings to a locally partitioned directory (`data/raw/{city}/{date}/listings.json`) — the shared handoff point that Great Expectations, the GCS uploader, and local debugging all read from.

### Great Expectations gate (`great_expectations/validate_batch.py`)
Sits between the local raw write and the GCS upload. Runs two suites against each city's batch:
- **Critical suite** (blocks the pipeline on failure): ID uniqueness/not-null, price and area sanity bounds, room count bounds, boolean-field honesty on top-coded fields (`rooms_capped`, `floor_capped`), valid `market_type`, and a cross-field check that OLX's listed price/m² agrees with `price / area_sqm` (catches parsing bugs a plain range check would miss).
- **Warning suite** (logged, never blocks): `district` not-null rate — deliberately informational, since a real run showed ~40% missing for smaller cities, which is a property of the source data, not a bug.

Code-first (no persisted GX project), since it runs inside a scheduled Airflow container.

### GCS uploader (`scraper/gcs_uploader.py`)
Uploads a validated city's local raw file to the GCS landing zone, mirroring the local partition scheme (`raw/{city}/{date}/listings.json`). Only runs after the GE gate passes.

### BigQuery loader (`scraper/bq_loader.py`)
Reads the just-uploaded GCS blob (not the local file, so BigQuery mirrors what's actually in the landing zone) and appends it into `raw_apartment_listings`. Table is created on first run with daily partitioning on `date_collected` and clustering on `source_city`; explicit schema keeps the raw layer typed without transforming any values.

### Airflow DAG (`airflow/dags/housing_pipeline_dag.py`)
One `scrape → validate → upload → load_bq` chain per city, run daily. Exposes a `cities` param on the Trigger UI form so a subset (e.g. just `["katowice"]`) can be run on demand for testing — the rest of the cities' chains skip cleanly rather than being removed from the graph.

### Infrastructure (`terraform/`)
Provisions the GCS raw bucket (90-day lifecycle rule — BigQuery is the source of truth post-load), three BigQuery datasets (`raw_housing`, `staging_housing`, `marts_housing`), and two service accounts with least-privilege IAM: an ingestion SA (`storage.objectAdmin` on the raw bucket, `bigquery.dataEditor` on raw + `bigquery.jobUser`) and a dbt SA (`dataViewer` on raw, `dataEditor` on staging/marts).

### Tests
- `scraper/tests/` — unit tests for both rental and sale listing shapes, individual field parsers, and the price/m² cross-check.
- `great_expectations/tests/test_validate_batch.py` — unit tests for the GE suites themselves.

Run them:
```bash
cd scraper
pip install -r requirements.txt
pytest tests/ -v
```

## Target Cities (MVP)

Katowice, Gliwice, Zabrze, Bytom, Chorzów, Tychy, Sosnowiec, Bielsko-Biała.

Full Silesian city list is deferred to the roadmap until the pipeline is stable end-to-end.

**Known data quality note:** OLX's search API matches on free text, not a strict location filter, so the raw `city` field includes bleed from neighboring metro areas (Ruda Śląska, Mysłowice, etc. — legitimately on the roadmap's future city list) and occasional unrelated noise (a handful of Warszawa/Kraków results). This is expected, not a scraper bug — `source_city` (the actual scrape target) is the reliable field for filtering to the 8 MVP cities, and `stg_listings` will handle city-name standardization for anything that needs the fuzzy `city` field itself.

## Scraping Ethics

- Only publicly visible listing metadata is collected — no authenticated endpoints.
- Requests are rate-limited with randomized delays (1.5–3.5s between pages, 5–10s between cities).
- Failed requests retry with exponential backoff rather than hammering the endpoint.
- No attempt is made to bypass access controls, CAPTCHAs, or ToS restrictions.

## Project Structure

```
housing-data-platform/
├── scraper/
│   ├── scrapper.py          # fetch + orchestrate per-city scraping
│   ├── parser.py             # flatten + type raw GraphQL items
│   ├── loader.py              # write partitioned local raw JSON
│   ├── gcs_uploader.py         # push validated batch to GCS
│   ├── bq_loader.py             # load GCS batch into BigQuery raw
│   ├── requirements.txt
│   └── tests/
│       ├── test_parser.py
│       └── sample_raw_listing*.json
├── great_expectations/
│   ├── validate_batch.py     # critical + warning expectation suites
│   └── tests/
│       └── test_validate_batch.py
├── airflow/
│   └── dags/
│       └── housing_pipeline_dag.py
├── docker/
│   └── airflow/
│       ├── Dockerfile
│       └── requirements.txt
├── docker-compose.yml
├── terraform/
│   ├── providers.tf / versions.tf
│   ├── variables.tf / outputs.tf
│   ├── storage.tf            # GCS raw bucket
│   ├── bigquery.tf           # raw / staging / marts datasets
│   ├── apis.tf
│   └── iam.tf                 # ingestion + dbt service accounts
└── keys/                     # gitignored — service account key files
```

`dbt/`, `.github/workflows/`, and `dashboards/` will be added as those layers are built.

## Running Locally

```bash
# 1. Provision GCP infra
cd terraform
terraform init && terraform apply

# 2. Bring up Airflow + scraper environment
cd ..
docker compose up -d

# 3. Open the Airflow UI at localhost:8080, trigger `housing_pipeline`
#    (optionally trim the `cities` param to run a subset)
```

## Roadmap

- dbt: `stg_listings` (dedup, city-name standardization, impossible price/area filtering) → dimension + fact tables → marts
- dbt snapshots for price-change history
- GitHub Actions CI (lint, test, dbt build/test, GE checkpoint)
- Power BI dashboard
- Expansion to full Silesian city list
- Stretch: ML price prediction, geospatial features, OSM integration

## Disclaimer

This project scrapes only publicly available data for educational/portfolio purposes. It is not affiliated with OLX or Otodom.