import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from validate_batch import validate_dataframe


def _load_fixture():
    path = os.path.join(os.path.dirname(__file__), "sample_batch.json")
    with open(path, "r", encoding="utf-8") as f:
        return pd.DataFrame(json.load(f))


def _fails_on(results, expectation_type, column):
    return any(
        r["expectation"] == expectation_type and r["column"] == column and not r["success"]
        for r in results
    )


def test_real_sample_batch_passes():
    """The actual transformed sample batch (18 real listings, ~17% missing district) should pass critical checks."""
    df = _load_fixture()
    success, critical_results, warning_results = validate_dataframe(df)
    failed = [r for r in critical_results if not r["success"]]
    assert success, f"Unexpected critical failures on clean data: {failed}"


def test_negative_price_fails():
    df = _load_fixture()
    df.loc[0, "price"] = -100
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_be_between", "price")


def test_area_too_small_fails():
    df = _load_fixture()
    df.loc[0, "area_sqm"] = 3.0  # below AREA_MIN=10, e.g. a mis-parsed storage unit
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_be_between", "area_sqm")


def test_duplicate_id_fails():
    df = _load_fixture()
    df.loc[1, "id"] = df.loc[0, "id"]
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_be_unique", "id")


def test_missing_url_fails():
    df = _load_fixture()
    df.loc[0, "url"] = None
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_not_be_null", "url")


def test_price_per_sqm_mismatch_fails():
    """If the listed price/m² diverges wildly from price/area_sqm, that's a parsing bug, not noise."""
    df = _load_fixture()
    df.loc[0, "price_per_sqm_listed"] = 500.0  # nowhere near price/area_sqm for that row
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_be_between", "price_per_sqm_diff_pct")


def test_rooms_capped_null_fails():
    df = _load_fixture()
    df["rooms_capped"] = df["rooms_capped"].astype(object)
    df.loc[0, "rooms_capped"] = None
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_not_be_null", "rooms_capped")


def test_rooms_capped_invalid_value_fails():
    df = _load_fixture()
    df["rooms_capped"] = df["rooms_capped"].astype(object)
    df.loc[0, "rooms_capped"] = "yes"  # wrong type entirely, e.g. a future parser bug
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_be_in_set", "rooms_capped")


def test_invalid_market_type_fails():
    df = _load_fixture()
    df.loc[0, "market_type"] = "unknown"
    success, critical_results, _ = validate_dataframe(df)
    assert not success
    assert _fails_on(critical_results, "expect_column_values_to_be_in_set", "market_type")


# --- district: warning-only, must NEVER affect critical success, at any missing rate ---

def test_district_moderately_missing_does_not_block_pipeline():
    df = _load_fixture()
    non_null_idx = df[df["district"].notna()].index[0]
    df.loc[non_null_idx, "district"] = None  # ~22% missing
    success, critical_results, _ = validate_dataframe(df)
    assert success, f"District missingness must never fail critical checks: {critical_results}"


def test_district_almost_entirely_missing_still_does_not_block_pipeline():
    """Even a real scenario like the live Katowice run (~40% missing) must not fail the task."""
    df = _load_fixture()
    df["district"] = None
    success, critical_results, _ = validate_dataframe(df)
    assert success, f"District missingness must never fail critical checks: {critical_results}"


def test_district_missing_rate_is_still_surfaced_in_warning_results():
    """Even though it can't block the pipeline, the number should still show up for visibility."""
    df = _load_fixture()
    df["district"] = None
    _, _, warning_results = validate_dataframe(df)
    assert _fails_on(warning_results, "expect_column_values_to_not_be_null", "district")