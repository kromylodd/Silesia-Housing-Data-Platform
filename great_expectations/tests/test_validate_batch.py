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
    """The actual transformed sample batch (18 real listings, ~17% missing district) should pass as-is."""
    df = _load_fixture()
    success, results = validate_dataframe(df)
    failed = [r for r in results if not r["success"]]
    assert success, f"Unexpected failures on clean data: {failed}"


def test_negative_price_fails():
    df = _load_fixture()
    df.loc[0, "price"] = -100
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_be_between", "price")


def test_area_too_small_fails():
    df = _load_fixture()
    df.loc[0, "area_sqm"] = 3.0  # below AREA_MIN=10, e.g. a mis-parsed storage unit
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_be_between", "area_sqm")


def test_duplicate_id_fails():
    df = _load_fixture()
    df.loc[1, "id"] = df.loc[0, "id"]
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_be_unique", "id")


def test_missing_url_fails():
    df = _load_fixture()
    df.loc[0, "url"] = None
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_not_be_null", "url")


def test_price_per_sqm_mismatch_fails():
    """If the listed price/m² diverges wildly from price/area_sqm, that's a parsing bug, not noise."""
    df = _load_fixture()
    df.loc[0, "price_per_sqm_listed"] = 500.0  # nowhere near price/area_sqm for that row
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_be_between", "price_per_sqm_diff_pct")


def test_district_missing_below_threshold_tolerated():
    """~22% missing district (real-world OLX behavior) should NOT fail the batch — under the 25% cap."""
    df = _load_fixture()
    # fixture already has 3/18 (~17%) null; bump one more non-null row to null -> 4/18 (~22%), still under 25%
    non_null_idx = df[df["district"].notna()].index[0]
    df.loc[non_null_idx, "district"] = None
    success, results = validate_dataframe(df)
    assert success, f"Batch should tolerate ~22% missing district: {[r for r in results if not r['success']]}"


def test_district_mostly_missing_fails():
    """If district is missing on nearly every row, that signals a real scraping/schema problem."""
    df = _load_fixture()
    df["district"] = None
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_not_be_null", "district")


def test_rooms_capped_null_fails():
    df = _load_fixture()
    df["rooms_capped"] = df["rooms_capped"].astype(object)
    df.loc[0, "rooms_capped"] = None
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_not_be_null", "rooms_capped")


def test_rooms_capped_invalid_value_fails():
    df = _load_fixture()
    df["rooms_capped"] = df["rooms_capped"].astype(object)
    df.loc[0, "rooms_capped"] = "yes"  # wrong type entirely, e.g. a future parser bug
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_be_in_set", "rooms_capped")


def test_invalid_market_type_fails():
    df = _load_fixture()
    df.loc[0, "market_type"] = "unknown"
    success, results = validate_dataframe(df)
    assert not success
    assert _fails_on(results, "expect_column_values_to_be_in_set", "market_type")