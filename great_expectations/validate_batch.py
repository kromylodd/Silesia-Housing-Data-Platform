"""
Great Expectations validation gate for raw scraped listing batches.

Sits between the scraper and the GCS upload in the Airflow DAG:

    scrape_task >> validate_task >> upload_task

Validates a single city's local raw JSON file (data/raw/{city}/{date}/listings.json)
before it's trusted enough to land in GCS. This is deliberately a lightweight,
code-first suite (no persisted GX project / Data Docs) because it runs inside
an Airflow container on a schedule — see README section on generating Data
Docs locally if you want an HTML report for the portfolio writeup.
"""

import json
import logging
import os
import sys

import great_expectations as gx
import pandas as pd
from great_expectations import expectations as gxe

logger = logging.getLogger(__name__)

# District is legitimately missing from OLX for a meaningful share of listings
# (smaller cities in particular) — this is not a scraper bug. Treat it as a
# tolerance threshold rather than a hard not-null check: if less than 75% of
# rows have a district, that's a real problem (e.g. OLX changed its response
# shape) and should fail the batch; some missing districts should not.
DISTRICT_NOT_NULL_THRESHOLD = 0.75

# Sanity bounds — wide enough to never reject a real Silesian apartment
# listing, tight enough to catch a scraping/parsing bug (e.g. picking up
# rent instead of price, or a unit mismatch).
PRICE_MIN = 1
PRICE_MAX = 20_000_000
AREA_MIN = 10
AREA_MAX = 500
ROOMS_MIN = 1
ROOMS_MAX = 10

# OLX's own listed price/m² vs price/area_sqm computed from our own fields
# should agree to within this fraction — a bigger gap usually means a parsing
# bug in one of the two source fields, not real-world rounding.
PRICE_PER_SQM_TOLERANCE = 0.05


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adds columns computed for validation purposes only (not part of the stored schema)."""
    df = df.copy()

    has_both = df["price_per_sqm_listed"].notna() & df["area_sqm"].notna() & (df["area_sqm"] != 0)
    computed = df["price"] / df["area_sqm"].replace(0, pd.NA)
    diff_pct = (computed - df["price_per_sqm_listed"]).abs() / df["price_per_sqm_listed"]
    df["price_per_sqm_diff_pct"] = diff_pct.where(has_both)

    return df


def build_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="raw_listings_suite")

    # --- identity / structural ---
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="url"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="city"))

    # --- location: soft threshold, not a hard requirement ---
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(column="district", mostly=DISTRICT_NOT_NULL_THRESHOLD)
    )

    # --- core numeric sanity ---
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="price", min_value=PRICE_MIN, max_value=PRICE_MAX)
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="area_sqm", min_value=AREA_MIN, max_value=AREA_MAX)
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="num_rooms", min_value=ROOMS_MIN, max_value=ROOMS_MAX, mostly=0.95
        )
    )

    # --- topcoded-field honesty: these must be real booleans, not silently missing ---
    # (ExpectColumnValuesToBeInSet alone would let a null slip through — GX treats
    # null as vacuously in-set by default — so a not-null check is required too.)
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="rooms_capped"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="rooms_capped", value_set=[True, False]))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="floor_capped"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="floor_capped", value_set=[True, False]))

    # --- categorical ---
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="market_type", value_set=["primary", "secondary"])
    )

    # --- cross-field consistency: catches parsing bugs a plain range check can't ---
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="price_per_sqm_diff_pct", min_value=0, max_value=PRICE_PER_SQM_TOLERANCE
        )
    )

    return suite


def validate_dataframe(df: pd.DataFrame) -> tuple[bool, list[dict]]:
    """Runs the suite against a dataframe. Returns (success, list of per-expectation result dicts)."""
    df = _add_derived_columns(df)

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("raw_listings_pandas")
    asset = data_source.add_dataframe_asset(name="raw_listings")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = build_suite()
    result = batch.validate(suite)

    summary = []
    for r in result.results:
        summary.append(
            {
                "expectation": r.expectation_config.type,
                "column": r.expectation_config.kwargs.get("column"),
                "success": r.success,
                "unexpected_count": r.result.get("unexpected_count"),
                "unexpected_percent": r.result.get("unexpected_percent"),
                "partial_unexpected_list": r.result.get("partial_unexpected_list"),
            }
        )

    return result.success, summary


def _local_raw_path(city: str, date_str: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(base_dir)
    return os.path.join(repo_root, "data", "raw", city, date_str, "listings.json")


def validate_city_batch(city: str, date_str: str) -> None:
    """
    Airflow entry point. Loads a city's local raw JSON, validates it, and
    raises if the batch fails — which fails the Airflow task and blocks the
    downstream GCS upload task from running on bad data.
    """
    path = _local_raw_path(city, date_str)
    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)

    if not records:
        logger.warning(f"[{city}] 0 listings in batch — nothing to validate, skipping.")
        return

    df = pd.DataFrame(records)
    success, results = validate_dataframe(df)

    failed = [r for r in results if not r["success"]]
    for r in results:
        level = logging.INFO if r["success"] else logging.ERROR
        logger.log(
            level,
            f"[{city}] {r['expectation']} ({r['column']}): "
            f"{'PASS' if r['success'] else 'FAIL'} "
            f"unexpected={r['unexpected_count']} ({r['unexpected_percent']}%) "
            f"sample={r['partial_unexpected_list']}",
        )

    if not success:
        raise ValueError(
            f"[{city}] Great Expectations validation FAILED — {len(failed)} expectation(s) violated: "
            + ", ".join(f"{r['expectation']}({r['column']})" for r in failed)
        )

    logger.info(f"[{city}] Validation passed — {len(records)} listings, all expectations satisfied.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    target_city = sys.argv[1] if len(sys.argv) > 1 else "katowice"
    from datetime import datetime

    target_date = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime("%Y-%m-%d")
    validate_city_batch(target_city, target_date)