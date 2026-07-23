# Silesia Housing Data Platform

End-to-end data engineering pipeline for the residential real estate market in the Silesian Voivodeship, Poland. Built as a production-style portfolio project — not a tutorial — covering ingestion, orchestration, storage, transformation, data quality, infrastructure-as-code, and analytics.

**Status: early build.** The scraper/parser/loader layer is done and tested. Orchestration, warehouse, transformation, and dashboard layers are planned — see [Roadmap](#roadmap).

## Motivation

Most portfolio ETL projects stop at "scrape + dump to CSV." This one is built the way a real internal analytics platform would be: validated ingestion, a proper star schema, orchestrated daily runs, infrastructure defined as code, and CI enforcing data quality on every push. The target market — Silesia's largest cities — is scoped down deliberately (8 cities for MVP, full list later) so the pipeline ships end-to-end before scope creep sets in.

## Current Progress

| Layer | Status |
|---|---|
| Scraper (OLX GraphQL, paginated, retry/backoff) | ✅ Done |
| Parser (typed field extraction, PL-language normalization) | ✅ Done |
| Local raw storage (partitioned by city/date) | ✅ Done |
| Unit tests (parser) | ✅ Done |
| Great Expectations checkpoints | ⬜ Not started |
| GCS raw landing zone | ⬜ Not started |
| Terraform (GCS, BigQuery, IAM) | ⬜ Not started |
| Airflow DAG | ⬜ Not started |
| BigQuery raw + staging tables | ⬜ Not started |
| dbt models (staging → star schema → marts) | ⬜ Not started |
| dbt snapshots (price history) | ⬜ Not started |
| GitHub Actions CI | ⬜ Not started |
| Power BI dashboard | ⬜ Not started |

## Tech Stack (planned end state)

- **Language:** Python 3.12+
- **Warehouse:** BigQuery
- **Storage:** Google Cloud Storage
- **Orchestration:** Apache Airflow
- **Transformation:** dbt
- **Data Quality:** Great Expectations
- **Infra as Code:** Terraform
- **Containerization:** Docker / Docker Compose
- **CI/CD:** GitHub Actions
- **Visualization:** Power BI

## Architecture (planned)

```
OLX listings
   ↓
Python scraper (GraphQL, rate-limited)
   ↓
Raw JSON → local / GCS landing zone
   ↓
Great Expectations validation
   ↓
BigQuery (raw)
   ↓
dbt (staging → star schema → marts)
   ↓
Power BI dashboard
```

All orchestrated daily via Airflow once the DAG is built.

## What's Built So Far

### Scraper (`scraper/scrapper.py`)
Queries OLX's GraphQL search endpoint per city, paginating until results run out. Includes exponential-backoff retries on request failure and a randomized delay between requests/cities to keep load light.

### Parser (`scraper/parser.py`)
Flattens OLX's nested `params` array into a typed record. Handles Polish-language values directly at the source (`"Parter"` → `0`, `"Tak"/"Nie"` → bool, `"Wtórny"` → `"secondary"`), extracts numeric values from labeled strings (`"48,5 m²"` → `48.5`), and stamps each record with a `date_collected` UTC timestamp at parse time.

### Loader (`scraper/loader.py`)
Writes parsed listings to a locally partitioned directory (`data/raw/{city}/{date}/listings.json`), mirroring the eventual GCS layout so the migration to cloud storage is a drop-in swap.

### Tests (`scraper/tests/`)
Unit tests for both the rental and sale listing shapes, individual field parsers (rooms, floor, market type, boolean flags), and a data-quality cross-check (OLX's listed price/m² should match `price / area_sqm`).

Run them:
```bash
cd scraper
pip install -r requirements.txt
pytest tests/ -v
```

## Target Cities (MVP)

Katowice, Gliwice, Zabrze, Bytom, Chorzów, Tychy, Sosnowiec, Bielsko-Biała.

Full Silesian city list is deferred to the roadmap until the pipeline is stable end-to-end.

## Scraping Ethics

- Only publicly visible listing metadata is collected — no authenticated endpoints.
- Requests are rate-limited with randomized delays (1.5–3.5s between pages, 5–10s between cities).
- Failed requests retry with exponential backoff rather than hammering the endpoint.
- No attempt is made to bypass access controls, CAPTCHAs, or ToS restrictions.

## Project Structure

```
scraper/
├── scrapper.py       # fetch + orchestrate per-city scraping
├── parser.py          # flatten + type raw GraphQL items
├── loader.py           # write partitioned raw JSON
├── requirements.txt
└── tests/
    ├── test_parser.py
    └── sample_raw_listing*.json
```

The rest of the structure (`airflow/`, `dbt/`, `terraform/`, `great_expectations/`, `.github/workflows/`) will be added as those layers are built.

## Roadmap

- Great Expectations validation checkpoints
- GCS raw landing zone + Terraform-provisioned infra
- Airflow DAG for daily orchestration
- BigQuery raw → staging → star schema via dbt
- dbt snapshots for price-change history
- GitHub Actions CI (lint, test, dbt build/test, GE checkpoint)
- Power BI dashboard
- Expansion to full Silesian city list
- Stretch: ML price prediction, geospatial features, OSM integration

## Disclaimer

This project scrapes only publicly available data for educational/portfolio purposes. It is not affiliated with OLX or Otodom.
