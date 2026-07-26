# Silesia Housing Data Platform

End-to-end data engineering pipeline for the residential real estate market in the Silesian Voivodeship, Poland. Built as a production-style portfolio project — not a tutorial — covering ingestion, orchestration, storage, transformation, data quality, infrastructure-as-code, and analytics.

**Status: raw ingestion pipeline complete and running end-to-end; dbt staging layer built and tested.** Scraper → Great Expectations validation → GCS raw landing zone → BigQuery raw table → dbt `stg_listings` are all built and verified against live data. Dimensional modeling (star schema), CI/CD, and the dashboard layer are next — see [Roadmap](#roadmap).

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
| dbt staging layer (`stg_listings` — dedup, city standardization, sanity filters) | ✅ Done |
| dbt dimensional model (dims + `fact_apartments`) | ⬜ Not started |
| dbt marts (price stats, city/district summaries, market trends) | ⬜ Not started |
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
dbt staging (stg_listings — done) → star schema → marts   ← star schema next up
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

### dbt staging layer (`dbt/`)
`stg_listings` sits on top of the `raw_apartment_listings` source and produces one row per listing, deduplicated to its most recent scrape (`qualify row_number() over (partition by listing_id order by date_collected desc) = 1`) — same listing can recur across days since raw is append-only. Standardizes city names via a `city_lookup` seed keyed on `source_city` (the reliable scrape-target field, not OLX's noisy free-text `city`), and filters out-of-bounds price/area as a second, defensive gate on top of the GE checks already applied upstream. Backed by 11 dbt tests (`unique`/`not_null` on the surrogate + native keys, `accepted_values` on `city`/`market_type`, source freshness checks) — all passing against live data (2,610 listings across the 8 MVP cities as of last run).

Custom `generate_schema_name` macro makes each layer's `+schema` config map to its Terraform-provisioned dataset exactly (`staging_housing`, `marts_housing`) instead of dbt's default behavior of appending it as a suffix to the target dataset.

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
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml           # connection config only — values from env_var(), no secrets committed
│   ├── packages.yml           # dbt_utils (surrogate keys)
│   ├── macros/
│   │   └── get_custom_schema.sql
│   ├── seeds/
│   │   └── city_lookup.csv    # source_city slug → standardized display name
│   └── models/staging/
│       ├── _staging__sources.yml
│       ├── _staging__models.yml
│       └── stg_listings.sql
└── keys/                     # gitignored — service account key files
```

`dbt/models/marts/`, `.github/workflows/`, and `dashboards/` will be added as those layers are built.

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

# 4. Run dbt (separate Python env — dbt isn't in the Airflow container yet)
python3 -m venv ~/venvs/housing-dbt && source ~/venvs/housing-dbt/bin/activate
pip install dbt-core dbt-bigquery
cd dbt
export DBT_PROFILES_DIR=$(pwd)
export GCP_PROJECT_ID=<your-project-id>
export BQ_DATASET_STAGING=staging_housing
export GCP_REGION=europe-central2
export DBT_KEYFILE_PATH=../keys/dbt-key.json   # housing-dbt-sa key — see terraform/iam.tf
dbt deps && dbt seed && dbt run && dbt test
```

## Roadmap

- ~~dbt: `stg_listings` (dedup, city-name standardization, impossible price/area filtering)~~ ✅ done
- dbt dimensional model: `dim_city`, `dim_district`, `dim_building_type`, `dim_market`, `dim_date` → `fact_apartments`
- dbt marts: price statistics, city/district summaries, market trends
- dbt snapshots for price-change history
- GitHub Actions CI (lint, test, dbt build/test, GE checkpoint)
- Power BI dashboard
- Expansion to full Silesian city list
- Stretch: ML price prediction, geospatial features, OSM integration

## Disclaimer

This project scrapes only publicly available data for educational/portfolio purposes. It is not affiliated with OLX or Otodom.