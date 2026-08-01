"""
Daily batch entrypoint for the Cloud Run Job.

Runs the same scrape -> validate -> upload -> load chain the Airflow DAG
defines (housing_pipeline_dag.py), for all cities in TARGET_CITIES (34 as
of Stage 2 — see dbt/seeds/city_lookup.csv), then triggers `dbt build` to
refresh staging/marts.

Why this exists: catchup=False + a non-continuous local Airflow scheduler
means daily runs get silently missed instead of backfilled (backfilling
isn't possible anyway — OLX only returns currently-live listings, so a
"past" run would just relabel today's data with yesterday's date). This
script is the guaranteed-to-run daily path: Cloud Scheduler triggers this
Cloud Run Job once a day with zero idle cost and no host to keep alive.

The Airflow DAG is unchanged and still the local/dev orchestrator — full
task-level UI, retries, and manual subset runs via the `cities` Param.
"""

import asyncio
import logging
import subprocess
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app/scraper")
sys.path.insert(0, "/app/great_expectations")

from bq_loader import load_city_to_bigquery
from gcs_uploader import upload_city_listings_to_gcs
from loader import save_to_local_raw
from scrapper import scrape_cities_async
from validate_batch import validate_city_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Full Stage 2 list — must stay in sync with dbt/seeds/city_lookup.csv
# (source_city column). Order matches that seed (population-descending).
TARGET_CITIES = [
    "warszawa",
    "krakow",
    "lodz",
    "wroclaw",
    "poznan",
    "gdansk",
    "szczecin",
    "bydgoszcz",
    "lublin",
    "bialystok",
    "katowice",
    "gdynia",
    "czestochowa",
    "radom",
    "rzeszow",
    "sosnowiec",
    "torun",
    "kielce",
    "gliwice",
    "zabrze",
    "olsztyn",
    "bielsko-biala",
    "bytom",
    "zielona-gora",
    "rybnik",
    "ruda-slaska",
    "opole",
    "tychy",
    "gorzow-wielkopolski",
    "dabrowa-gornicza",
    "plock",
    "elblag",
    "walbrzych",
    "chorzow",
]


# Global cap across ALL cities combined, not per city — see scraper/rate_limiter.py.
# Keeps OLX's actual request rate roughly flat as TARGET_CITIES grows in Stage 2.
SCRAPE_MAX_CONCURRENT = 4


def load_city(city: str, listings: list, date_str: str) -> bool:
    """Runs one city's post-scrape chain (save -> validate -> upload -> load).

    Deliberately catches and logs rather than raising: one bad city
    (validation failure, transient GCS/BQ error) shouldn't take down the
    rest of the batch's daily data.
    """
    try:
        save_to_local_raw(city, listings, date_str=date_str)
        validate_city_batch(city, date_str)
        upload_city_listings_to_gcs(city, date_str=date_str)
        load_city_to_bigquery(city, date_str=date_str)
        logger.info(f"[{city}] daily chain OK")
        return True
    except Exception:
        logger.exception(f"[{city}] daily chain FAILED — continuing with remaining cities")
        return False


def main() -> int:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"Starting daily batch run for {date_str}")

    # Phase 1: scrape every city concurrently, paced by one shared rate
    # limiter. This is the part that used to scale linearly with city count
    # (sequential, one city fully finished before the next started).
    logger.info(f"Scraping {len(TARGET_CITIES)} cities (max_concurrent={SCRAPE_MAX_CONCURRENT})")
    scraped = asyncio.run(
        scrape_cities_async(TARGET_CITIES, max_pages=25, max_concurrent=SCRAPE_MAX_CONCURRENT)
    )

    # Phase 2: validate/upload/load stays sequential per city. GE + GCS + BQ
    # client calls are sync and cheap relative to network-bound scraping, so
    # there's no real wall-clock win in parallelizing this phase too — and
    # keeping it sequential keeps BQ load ordering predictable and logs easy
    # to follow.
    results = {city: load_city(city, listings, date_str) for city, listings in scraped.items()}
    failed = [city for city, ok in results.items() if not ok]
    if failed:
        logger.error(f"Cities failed this run: {failed}")

    logger.info("Running dbt build")
    dbt_result = subprocess.run(
        ["dbt", "build", "--target", "cloud_run"],
        cwd="/app/dbt",
        capture_output=True,
        text=True,
        check=False,
    )
    if dbt_result.stdout:
        logger.info(dbt_result.stdout)
    if dbt_result.returncode != 0:
        logger.error(dbt_result.stderr)
        return 1

    # Only fail the whole job (non-zero exit -> shows as a failed execution
    # in Cloud Run / Cloud Logging) if every city failed. A partial run
    # still lands useful data and shouldn't need paging over one bad city.
    return 1 if len(failed) == len(TARGET_CITIES) else 0


if __name__ == "__main__":
    sys.exit(main())
