import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from validate_batch import QUARANTINE_MAX_FRACTION, validate_dataframe


def _load_fixture():
    path = os.path.join(os.path.dirname(__file__), "sample_batch.json")
    with open(path, "r", encoding="utf-8") as f:
        return pd.DataFrame(json.load(f))


def _padded_fixture(min_rows=200):
    """
    Duplicates the 18-row fixture (with fresh unique ids) up to at least
    min_rows. A single corrupted row in an 18-row batch is already ~5.6% —
    right at QUARANTINE_MAX_FRACTION — so tests that mean to exercise
    per-row quarantine (as opposed to the systemic-failure ceiling) need a
    batch large enough that one bad row is unambiguously a small fraction,
    matching what a real city's batch (tens to low thousands of rows) looks
    like rather than the unit-test fixture size.
    """
    base = _load_fixture()
    reps = -(-min_rows // len(base))  # ceil
    frames = []
    next_id = int(base["id"].max()) + 1
    for r in range(reps):
        chunk = base.copy()
        if r > 0:
            chunk["id"] = range(next_id, next_id + len(chunk))
            next_id += len(chunk)
        frames.append(chunk)
    return pd.concat(frames, ignore_index=True)


def _fails_on(results, expectation_type, column):
    return any(
        r["expectation"] == expectation_type and r["column"] == column and not r["success"]
        for r in results
    )


def test_real_sample_batch_passes():
    """The actual transformed sample batch (18 real listings, ~17% missing district) should pass critical checks."""
    df = _load_fixture()
    systemic_failure, critical_results, _warning_results, clean_df, quarantined_df = (
        validate_dataframe(df)
    )
    failed = [r for r in critical_results if not r["success"]]
    assert not systemic_failure, f"Unexpected critical failures on clean data: {failed}"
    assert len(quarantined_df) == 0
    assert len(clean_df) == len(df)


def test_single_bad_row_is_quarantined_not_a_systemic_failure():
    """
    One malformed row out of a real-sized batch (e.g. the 2026-08-01 katowice
    incident: one listing with area_sqm=495000) should be dropped, not cost
    the whole city its data for the day.
    """
    df = _padded_fixture()
    df.loc[0, "area_sqm"] = 495000.0
    df.loc[0, "price_per_sqm_listed"] = df.loc[0, "price"] / 87.0  # matches the ORIGINAL area,
    # so it's inconsistent with the new bad area_sqm too — same shape as the real incident,
    # where both the range check and the cross-field consistency check caught the same row.
    systemic_failure, critical_results, _warning_results, clean_df, quarantined_df = (
        validate_dataframe(df)
    )
    assert not systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_be_between", "area_sqm")
    assert len(quarantined_df) == 1
    assert quarantined_df.iloc[0]["area_sqm"] == 495000.0
    assert len(clean_df) == len(df) - 1
    assert 495000.0 not in clean_df["area_sqm"].values


def test_widespread_corruption_is_a_systemic_failure():
    """
    If a large share of the batch violates a critical expectation, that's a
    parser/schema bug, not a few outliers — should still hard-fail rather
    than silently discard most of a day's data for that city.
    """
    df = _padded_fixture()
    n_bad = int(len(df) * (QUARANTINE_MAX_FRACTION + 0.05))  # comfortably over the ceiling
    df.loc[df.index[:n_bad], "area_sqm"] = 3.0  # below AREA_MIN
    systemic_failure, critical_results, _warning_results, clean_df, quarantined_df = (
        validate_dataframe(df)
    )
    assert systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_be_between", "area_sqm")
    # On systemic failure, nothing should be silently dropped — the caller
    # (validate_city_batch) is expected to raise and leave the raw data untouched.
    assert len(clean_df) == len(df)
    assert len(quarantined_df) == 0


def test_negative_price_is_quarantined():
    df = _padded_fixture()
    df.loc[0, "price"] = -100
    systemic_failure, critical_results, _, clean_df, quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_be_between", "price")
    assert len(quarantined_df) == 1
    assert len(clean_df) == len(df) - 1


def test_duplicate_id_is_quarantined():
    df = _padded_fixture()
    df.loc[1, "id"] = df.loc[0, "id"]
    systemic_failure, critical_results, _, clean_df, _quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_be_unique", "id")
    assert len(clean_df) < len(df)


def test_missing_url_is_quarantined():
    df = _padded_fixture()
    df.loc[0, "url"] = None
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_not_be_null", "url")
    assert len(quarantined_df) == 1


def test_price_per_sqm_mismatch_is_quarantined():
    """If the listed price/m² diverges wildly from price/area_sqm, that's a parsing bug, not noise."""
    df = _padded_fixture()
    df.loc[0, "price_per_sqm_listed"] = 500.0  # nowhere near price/area_sqm for that row
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert _fails_on(
        critical_results, "expect_column_values_to_be_between", "price_per_sqm_diff_pct"
    )
    assert len(quarantined_df) == 1


def test_rooms_capped_null_is_quarantined():
    df = _padded_fixture()
    df["rooms_capped"] = df["rooms_capped"].astype(object)
    df.loc[0, "rooms_capped"] = None
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_not_be_null", "rooms_capped")
    assert len(quarantined_df) == 1


def test_rooms_capped_invalid_value_is_quarantined():
    df = _padded_fixture()
    df["rooms_capped"] = df["rooms_capped"].astype(object)
    df.loc[0, "rooms_capped"] = "yes"  # wrong type entirely, e.g. a future parser bug
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_be_in_set", "rooms_capped")
    assert len(quarantined_df) == 1


def test_invalid_market_type_is_quarantined():
    df = _padded_fixture()
    df.loc[0, "market_type"] = "unknown"
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert _fails_on(critical_results, "expect_column_values_to_be_in_set", "market_type")
    assert len(quarantined_df) == 1


# --- district: warning-only, must NEVER affect critical success, at any missing rate ---


def test_district_moderately_missing_does_not_block_pipeline():
    df = _load_fixture()
    non_null_idx = df[df["district"].notna()].index[0]
    df.loc[non_null_idx, "district"] = None  # ~22% missing
    systemic_failure, critical_results, _, _clean_df, _quarantined_df = validate_dataframe(df)
    assert (
        not systemic_failure
    ), f"District missingness must never fail critical checks: {critical_results}"


def test_district_almost_entirely_missing_still_does_not_block_pipeline():
    """Even a real scenario like the live Katowice run (~40% missing) must not fail the task."""
    df = _load_fixture()
    df["district"] = None
    systemic_failure, critical_results, _, _clean_df, _quarantined_df = validate_dataframe(df)
    assert (
        not systemic_failure
    ), f"District missingness must never fail critical checks: {critical_results}"


def test_district_missing_rate_is_still_surfaced_in_warning_results():
    """Even though it can't block the pipeline, the number should still show up for visibility."""
    df = _load_fixture()
    df["district"] = None
    _, _, warning_results, _, _ = validate_dataframe(df)
    assert _fails_on(warning_results, "expect_column_values_to_not_be_null", "district")


# --- geo bounding box: catches OLX fuzzy-search noise ---


def test_no_city_passed_skips_geo_check():
    """Existing callers that don't pass `city` (default None) get unchanged behavior."""
    df = _padded_fixture()
    df.loc[0, "latitude"] = 54.3520  # Gdańsk — nowhere near Katowice
    df.loc[0, "longitude"] = 18.6466
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(df)
    assert not systemic_failure
    assert len(quarantined_df) == 0
    assert not _fails_on(critical_results, "expect_column_values_to_be_between", "latitude")


def test_null_lat_lon_passes_geo_check_vacuously():
    """Real batches routinely have entirely null lat/lon (OLX's `map` field is often absent) —
    that must never be treated as an out-of-bounds failure."""
    df = _load_fixture()
    assert df["latitude"].isna().all()  # sanity-check the fixture's actual shape
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(
        df, city="katowice"
    )
    assert not systemic_failure
    assert len(quarantined_df) == 0
    assert not _fails_on(critical_results, "expect_column_values_to_be_between", "latitude")


def test_listing_outside_city_bbox_is_quarantined():
    """A listing whose coordinates land in a completely different city (Gdańsk) despite being
    scraped under a Katowice batch — the OLX fuzzy-search-noise scenario this check exists for."""
    df = _padded_fixture()
    df.loc[0, "latitude"] = 54.3520
    df.loc[0, "longitude"] = 18.6466
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(
        df, city="katowice"
    )
    assert not systemic_failure
    assert _fails_on(
        critical_results, "expect_column_values_to_be_between", "latitude"
    ) or _fails_on(critical_results, "expect_column_values_to_be_between", "longitude")
    assert len(quarantined_df) == 1


def test_listing_inside_city_bbox_passes():
    """Real Katowice coordinates should never get quarantined by the check meant to catch
    the opposite problem."""
    df = _padded_fixture()
    df.loc[0, "latitude"] = 50.2649
    df.loc[0, "longitude"] = 19.0238
    systemic_failure, _critical_results, _, _clean_df, quarantined_df = validate_dataframe(
        df, city="katowice"
    )
    assert not systemic_failure
    assert len(quarantined_df) == 0


def test_missing_geo_columns_entirely_skips_check_without_crashing():
    """A raw file shaped without latitude/longitude keys at all (not just null values) must
    degrade to a skip, not a KeyError crash — GX can't evaluate a between-check against a
    column that doesn't exist, unlike a null value which it excludes gracefully."""
    df = _load_fixture().drop(columns=["latitude", "longitude"])
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(
        df, city="katowice"
    )
    assert not systemic_failure
    assert len(quarantined_df) == 0
    assert not _fails_on(critical_results, "expect_column_values_to_be_between", "latitude")


def test_unknown_city_skips_geo_check_without_failing():
    """A source_city not present in city_lookup (or a typo) shouldn't crash the batch or
    silently quarantine everything — it just means no geo check runs for that batch."""
    df = _padded_fixture()
    df.loc[0, "latitude"] = 54.3520
    df.loc[0, "longitude"] = 18.6466
    systemic_failure, critical_results, _, _clean_df, quarantined_df = validate_dataframe(
        df, city="not-a-real-city"
    )
    assert not systemic_failure
    assert len(quarantined_df) == 0
    assert not _fails_on(critical_results, "expect_column_values_to_be_between", "latitude")
