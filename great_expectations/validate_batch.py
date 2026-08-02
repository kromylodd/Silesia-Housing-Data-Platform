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

import functools
import json
import logging
import os
import sys

import pandas as pd

import great_expectations as gx
from great_expectations import expectations as gxe

logger = logging.getLogger(__name__)

# city_lookup.csv is the single source of truth for per-city geo bounding
# boxes (bbox_lat_min/max, bbox_lon_min/max) — shared with the dbt seed
# used by dim_city and the assert_geo_within_city_bounds.sql defense-in-depth
# test, so the bounds never drift between the pre-load and post-load checks.
_CITY_LOOKUP_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt", "seeds", "city_lookup.csv"
)


@functools.lru_cache(maxsize=1)
def _load_city_bounds() -> dict:
    """
    Loads {source_city: {lat_min, lat_max, lon_min, lon_max}} from the
    city_lookup seed. Cached (module-level, one process = one Airflow task /
    one Cloud Run execution) since it's read at most once per batch anyway.
    Missing file or missing bbox columns degrades to an empty dict rather
    than crashing — a city with no known bbox just skips geo validation
    (see build_critical_suite), it doesn't fail the pipeline.
    """
    try:
        lookup = pd.read_csv(_CITY_LOOKUP_PATH)
    except FileNotFoundError:
        logger.warning(
            f"city_lookup.csv not found at {_CITY_LOOKUP_PATH} — geo validation disabled."
        )
        return {}

    bbox_cols = {"bbox_lat_min", "bbox_lat_max", "bbox_lon_min", "bbox_lon_max"}
    if not bbox_cols.issubset(lookup.columns):
        logger.warning("city_lookup.csv missing bbox_* columns — geo validation disabled.")
        return {}

    bounds = {}
    for _, row in lookup.iterrows():
        if row[list(bbox_cols)].isna().any():
            continue
        bounds[row["source_city"]] = {
            "lat_min": float(row["bbox_lat_min"]),
            "lat_max": float(row["bbox_lat_max"]),
            "lon_min": float(row["bbox_lon_min"]),
            "lon_max": float(row["bbox_lon_max"]),
        }
    return bounds


def _city_bounds(city: str) -> dict | None:
    return _load_city_bounds().get(city)


# District is legitimately missing from OLX for a meaningful share of listings
# (smaller cities in particular) — this is not a scraper bug. A real Katowice
# run showed ~40% missing, well above what a small manual sample suggested,
# so this is NOT used as a hard-fail threshold — see build_warning_suite().
# Kept here only as a reference value for the informational check.
DISTRICT_NOT_NULL_REFERENCE = 0.6

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

# A handful of malformed rows (bad m² parse, a stray commercial/plot listing
# bleeding into search results, etc.) is expected noise in any given day's
# batch and shouldn't cost the whole city its data — those rows are
# quarantined individually instead. But if a *large* share of a batch fails,
# that's no longer "a few outliers", it's almost certainly a systemic parsing
# bug (e.g. a field mapping broke upstream) and should still hard-fail the
# city rather than silently discard a big chunk of real data.
QUARANTINE_MAX_FRACTION = 0.05


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adds columns computed for validation purposes only (not part of the stored schema)."""
    df = df.copy()

    has_both = df["price_per_sqm_listed"].notna() & df["area_sqm"].notna() & (df["area_sqm"] != 0)
    computed = df["price"] / df["area_sqm"].replace(0, pd.NA)
    diff_pct = (computed - df["price_per_sqm_listed"]).abs() / df["price_per_sqm_listed"]
    df["price_per_sqm_diff_pct"] = diff_pct.where(has_both)

    return df


def build_critical_suite(
    city: str | None = None, columns: frozenset[str] | None = None
) -> gx.ExpectationSuite:
    """
    Expectations that block the pipeline on failure. Only things that
    indicate real corruption/parsing bugs belong here — anything that's a
    known, legitimate property of the source data (e.g. district being
    sometimes absent) does NOT belong here, it belongs in the warning suite.

    `city` (the scrape-target source_city, e.g. "gliwice") scopes the
    latitude/longitude range check to that city's bounding box, if known —
    see the geo bounding-box block below.

    `columns` (the incoming dataframe's column set) guards that same check:
    parser.py's clean_listing_data() always emits latitude/longitude keys
    (null when OLX's `map` field is absent), so a missing *column* rather
    than a null *value* would mean an unexpected raw-file shape (e.g. a
    pre-geo-parser file re-validated by hand). GX raises a hard KeyError
    trying to evaluate a between-check against a column that doesn't exist
    at all — vs. a null value, which it already excludes gracefully — so
    this is checked explicitly to keep that failure mode a skip, not a
    crashed Airflow task.
    """
    suite = gx.ExpectationSuite(name="raw_listings_critical")

    # --- identity / structural ---
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="url"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="city"))

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
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="rooms_capped", value_set=[True, False])
    )
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="floor_capped"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="floor_capped", value_set=[True, False])
    )

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

    # --- geo bounding box: catches OLX fuzzy-search noise (a listing that
    # comes back for city X's search query but is actually located nowhere
    # near X). Scoped per-city rather than one national box because a
    # national box would never catch a same-region mismatch (e.g. a
    # Warszawa listing bleeding into a Radom batch — both nationally
    # in-bounds, only a per-city box catches it). Null lat/lon is common
    # (OLX's `map` field is often absent) and passes vacuously — GX only
    # evaluates non-null values for a between check, it doesn't need
    # `mostly=` for that. Skipped entirely if `city` has no known bbox
    # (unmapped source_city) rather than failing the batch.
    has_geo_columns = columns is None or {"latitude", "longitude"}.issubset(columns)
    if city is not None and has_geo_columns:
        bounds = _city_bounds(city)
        if bounds is not None:
            suite.add_expectation(
                gxe.ExpectColumnValuesToBeBetween(
                    column="latitude", min_value=bounds["lat_min"], max_value=bounds["lat_max"]
                )
            )
            suite.add_expectation(
                gxe.ExpectColumnValuesToBeBetween(
                    column="longitude", min_value=bounds["lon_min"], max_value=bounds["lon_max"]
                )
            )
        else:
            logger.warning(
                f"[{city}] No geo bounding box in city_lookup — skipping geo validation for this batch."
            )
    elif city is not None:
        logger.warning(
            f"[{city}] Batch has no latitude/longitude columns — skipping geo validation."
        )

    return suite


def build_warning_suite() -> gx.ExpectationSuite:
    """
    Expectations that are logged but never block the pipeline. district is
    here rather than in the critical suite because its missing-rate varies
    by city and isn't reliably boundable from a small sample — treating it
    as informational avoids the suite failing on legitimate source-data
    behavior while still surfacing the number every run for visibility.
    """
    suite = gx.ExpectationSuite(name="raw_listings_warning")
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(column="district", mostly=DISTRICT_NOT_NULL_REFERENCE)
    )
    return suite


def _run_suite(batch, suite) -> tuple[list[dict], set]:
    """
    Runs a suite with result_format="COMPLETE" so failed expectations report
    the exact row indices responsible (unexpected_index_list), not just a
    sample. Returns (summary, bad_indices) — bad_indices is the union of row
    indices behind every *failed* expectation in this suite (rows within a
    passing `mostly=` tolerance are left alone; they were already acceptable).
    """
    result = batch.validate(suite, result_format="COMPLETE")
    summary = []
    bad_indices: set = set()
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
        if not r.success:
            bad_indices.update(r.result.get("unexpected_index_list") or [])
    return summary, bad_indices


def validate_dataframe(
    df: pd.DataFrame,
    city: str | None = None,
) -> tuple[bool, list[dict], list[dict], pd.DataFrame, pd.DataFrame]:
    """
    Runs both suites against a dataframe, quarantining rows that violate the
    critical suite instead of failing the whole batch on their account.

    `city` (source_city slug) scopes the geo bounding-box check — see
    build_critical_suite(). Optional and defaults to None (geo check
    skipped) so existing callers/tests that validate a bare dataframe
    without city context keep working unchanged.

    Returns (systemic_failure, critical_results, warning_results, clean_df, quarantined_df):
      - systemic_failure: True only if the quarantined share exceeds
        QUARANTINE_MAX_FRACTION — this is the one thing that should still
        gate the pipeline. A handful of quarantined rows is expected and
        does NOT count as failure.
      - clean_df: original df minus quarantined rows (what should actually
        get uploaded/loaded).
      - quarantined_df: the rows that were dropped, for logging/inspection.
    """
    df = _add_derived_columns(df)

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("raw_listings_pandas")
    asset = data_source.add_dataframe_asset(name="raw_listings")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    critical_results, bad_indices = _run_suite(
        batch, build_critical_suite(city=city, columns=frozenset(df.columns))
    )
    warning_results, _ = _run_suite(batch, build_warning_suite())

    quarantine_fraction = len(bad_indices) / len(df) if len(df) else 0.0
    systemic_failure = quarantine_fraction > QUARANTINE_MAX_FRACTION

    if bad_indices and not systemic_failure:
        clean_df = df.drop(index=list(bad_indices)).reset_index(drop=True)
        quarantined_df = df.loc[sorted(bad_indices)]
    else:
        clean_df = df
        quarantined_df = df.iloc[0:0]

    return systemic_failure, critical_results, warning_results, clean_df, quarantined_df


def _local_raw_path(city: str, date_str: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(base_dir)
    return os.path.join(repo_root, "data", "raw", city, date_str, "listings.json")


def _local_quarantine_path(city: str, date_str: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(base_dir)
    return os.path.join(repo_root, "data", "quarantine", city, date_str, "rejected.json")


def validate_city_batch(city: str, date_str: str) -> None:
    """
    Airflow entry point. Loads a city's local raw JSON, validates it, and
    rewrites that same file to contain only the rows that pass the critical
    suite — a handful of malformed listings (bad m² parse, a stray
    commercial/plot listing bleeding into search results) get quarantined
    individually instead of costing the whole city its data for the day.

    Only raises — failing the Airflow task and blocking the downstream GCS
    upload task — if the quarantined share exceeds QUARANTINE_MAX_FRACTION,
    which signals a systemic parsing bug rather than a few outliers.
    """
    path = _local_raw_path(city, date_str)
    try:
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        # save_to_local_raw() deliberately doesn't write a file when there's
        # nothing to save — a city with zero listings this run (genuinely no
        # results, or its scrape task failed entirely and was mapped to an
        # empty list upstream) looks identical to "file never created" from
        # here. Treat it the same as the empty-records case below rather
        # than crashing: nothing to validate, nothing to upload.
        logger.warning(
            f"[{city}] No raw file found for {date_str} — scrape produced 0 listings "
            f"(or its scrape task failed entirely), nothing to validate."
        )
        return

    if not records:
        logger.warning(f"[{city}] 0 listings in batch — nothing to validate, skipping.")
        return

    df = pd.DataFrame(records)
    systemic_failure, critical_results, warning_results, clean_df, quarantined_df = (
        validate_dataframe(df, city=city)
    )

    for r in critical_results:
        level = logging.INFO if r["success"] else logging.ERROR
        logger.log(
            level,
            f"[{city}] {r['expectation']} ({r['column']}): "
            f"{'PASS' if r['success'] else 'FAIL'} "
            f"unexpected={r['unexpected_count']} ({r['unexpected_percent']}%) "
            f"sample={r['partial_unexpected_list']}",
        )

    for r in warning_results:
        # Warning suite never fails the task — just surfaces the number for visibility.
        logger.log(
            logging.INFO if r["success"] else logging.WARNING,
            f"[{city}] [INFO-ONLY] {r['expectation']} ({r['column']}): "
            f"{'within expected range' if r['success'] else 'outside expected range'} "
            f"unexpected={r['unexpected_count']} ({r['unexpected_percent']}%)",
        )

    if systemic_failure:
        failed = [r for r in critical_results if not r["success"]]
        raise ValueError(
            f"[{city}] Great Expectations validation FAILED — quarantine share exceeded "
            f"{QUARANTINE_MAX_FRACTION:.0%}, treating as a systemic bug: "
            + ", ".join(f"{r['expectation']}({r['column']})" for r in failed)
        )

    if len(quarantined_df) > 0:
        # quarantined_df.index still holds the ORIGINAL row positions (clean_df's
        # index was reset in validate_dataframe, quarantined_df's wasn't) — use
        # those to filter the pristine `records` list straight from the JSON we
        # loaded, rather than reconstructing rows via pandas. A DataFrame
        # round-trip silently upcasts any nullable-int column (e.g. `floor`,
        # which is often null on search-result listings) to float64 the moment
        # a single None appears alongside real integers — so a rewritten file
        # would write "floor": 1.0 instead of 1, which BigQuery's strict
        # INTEGER schema then rejects at load time. Filtering the original
        # dicts by index sidesteps that entirely: native types are untouched.
        bad_positions = set(quarantined_df.index.tolist())
        rejected_records = [records[i] for i in sorted(bad_positions)]
        clean_records = [r for i, r in enumerate(records) if i not in bad_positions]

        for rec in rejected_records:
            logger.warning(
                f"[{city}] Quarantined listing id={rec.get('id')} url={rec.get('url')} "
                f"— failed critical expectation(s)."
            )

        # Best-effort: persist rejected rows for later inspection. Never let
        # this fail the pipeline — it's a nice-to-have audit trail, not a
        # dependency of the upload/load steps below.
        try:
            qpath = _local_quarantine_path(city, date_str)
            os.makedirs(os.path.dirname(qpath), exist_ok=True)
            with open(qpath, "w", encoding="utf-8") as f:
                json.dump(rejected_records, f, ensure_ascii=False, indent=2, default=str)
        except Exception:
            logger.warning(f"[{city}] Failed to write quarantine file — continuing anyway.")

        # Overwrite the raw file with only the clean rows so gcs_uploader /
        # bq_loader — which both re-read this same path — never see the
        # quarantined listings. No changes needed in either of those.
        # (clean_records, filtered above, is already the pristine JSON shape —
        # no derived columns to strip, since price_per_sqm_diff_pct only ever
        # existed inside validate_dataframe's internal copy of the frame.)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(clean_records, f, ensure_ascii=False, indent=2, default=str)

        logger.warning(
            f"[{city}] Quarantined {len(quarantined_df)}/{len(records)} listings "
            f"({len(quarantined_df) / len(records):.1%}) — "
            f"{len(clean_df)} clean listings proceeding to upload."
        )
    else:
        logger.info(
            f"[{city}] Validation passed — {len(records)} listings, all critical expectations satisfied."
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    target_city = sys.argv[1] if len(sys.argv) > 1 else "katowice"
    from datetime import datetime

    target_date = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime("%Y-%m-%d")
    validate_city_batch(target_city, target_date)
