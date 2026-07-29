"""
Daily batch entrypoint for the Cloud Run Job.

Runs the same scrape -> validate -> upload -> load chain the Airflow DAG
defines (housing_pipeline_dag.py), for all 8 MVP cities, then triggers
`dbt build` to refresh staging/marts.

Why this exists: catchup=False + a non-continuous local Airflow scheduler
means daily runs get silently missed instead of backfilled (backfilling
isn't possible anyway — OLX only returns currently-live listings, so a
"past" run would just relabel today's data with yesterday's date). This
script is the guaranteed-to-run daily path: Cloud Scheduler triggers this
Cloud Run Job once a day with zero idle cost and no host to keep alive.

The Airflow DAG is unchanged and still the local/dev orchestrator — full
task-level UI, retries, and manual subset runs via the `cities` Param.
"""

import logging
import subprocess
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app/scraper")
sys.path.insert(0, "/app/great_expectations")

from bq_loader import load_city_to_bigquery
from gcs_uploader import upload_city_listings_to_gcs
from loader import save_to_local_raw
from scrapper import scrape_city
from validate_batch import validate_city_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TARGET_CITIES = [
    "katowice",
    "gliwice",
    "zabrze",
    "bytom",
    "chorzow",
    "tychy",
    "sosnowiec",
    "bielsko-biala",
]


def run_city(city: str, date_str: str) -> bool:
    """Runs one city's full chain. Returns True on success, False on failure.

    Deliberately catches and logs rather than raising: one bad city
    (OLX hiccup, validation failure, transient GCS/BQ error) shouldn't
    take down the other 7 cities' daily data.
    """
    try:
        data = scrape_city(city, max_pages=25)
        save_to_local_raw(city, data, date_str=date_str)
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

    results = {city: run_city(city, date_str) for city in TARGET_CITIES}
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
