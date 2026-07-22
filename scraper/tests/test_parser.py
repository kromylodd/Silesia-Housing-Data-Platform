import json
import os
import pytest

from parser import (
    clean_listing_data,
    extract_params,
    parse_floor,
    parse_bool_pl,
    parse_market,
    parse_price_per_m,
    parse_rooms,
)


def _load_fixture(filename):
    fixture_path = os.path.join(os.path.dirname(__file__), filename)
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def rental_listing():
    """Raw fixture from the rental category (category_id=15)."""
    return _load_fixture("sample_raw_listing.json")


@pytest.fixture
def sale_listing():
    """Raw fixture from the sale category (category_id=14) — the one MVP targets."""
    return _load_fixture("sample_raw_listing_sale.json")


# --- extract_params ---

def test_extract_params(rental_listing):
    params = extract_params(rental_listing.get("params"))
    assert params.get("price") == 2500
    assert params.get("currency") == "PLN"
    assert params.get("m") == "48,5 m²"


# --- rental fixture, end-to-end ---

def test_clean_listing_data_rental(rental_listing):
    cleaned = clean_listing_data(rental_listing)

    assert cleaned["id"] == 987654321
    assert cleaned["city"] == "Katowice"
    assert cleaned["district"] == "Koszutka"

    assert cleaned["area_sqm"] == 48.5
    assert isinstance(cleaned["area_sqm"], float)

    assert cleaned["extra_rent_pln"] == 650.0
    assert isinstance(cleaned["extra_rent_pln"], float)

    assert cleaned["num_rooms"] == 2
    assert isinstance(cleaned["num_rooms"], int)

    # Sale-only fields must be None here — this fixture has no market/price_per_m
    assert cleaned["market_type"] is None
    assert cleaned["price_per_sqm_listed"] is None


def test_clean_listing_missing_fields():
    """Ensures parser gracefully handles missing optional fields without crashing."""
    empty_item = {"id": 1111, "title": "Empty Test"}
    cleaned = clean_listing_data(empty_item)

    assert cleaned["id"] == 1111
    assert cleaned["city"] is None
    assert cleaned.get("area_sqm") is None
    assert cleaned.get("num_rooms") is None
    assert cleaned.get("floor") is None
    assert cleaned.get("market_type") is None


# --- sale fixture, end-to-end (this is the MVP target category) ---

def test_clean_listing_data_sale(sale_listing):
    cleaned = clean_listing_data(sale_listing)

    assert cleaned["id"] == 1078049895
    assert cleaned["district"] == "Janów-Nikiszowiec"
    assert cleaned["area_sqm"] == 58.0
    assert cleaned["num_rooms"] == 2

    assert cleaned["market_type"] == "secondary"
    assert cleaned["building_type"] == "Kamienica"
    assert cleaned["is_furnished"] is False
    assert cleaned["floor"] == 0  # "Parter" -> ground floor

    assert cleaned["price_per_sqm_listed"] == 6750.0


def test_sale_price_per_sqm_matches_computed_price(sale_listing):
    """Data quality check: OLX's own listed price/m² should match price / area_sqm."""
    cleaned = clean_listing_data(sale_listing)
    computed = cleaned["price"] / cleaned["area_sqm"]
    assert abs(computed - cleaned["price_per_sqm_listed"]) < 1.0


# --- individual parser unit tests ---

def test_parse_floor_ground_floor():
    assert parse_floor("Parter") == 0


def test_parse_floor_numeric():
    assert parse_floor("9") == 9


def test_parse_floor_missing():
    assert parse_floor(None) is None


def test_parse_bool_pl():
    assert parse_bool_pl("Tak") is True
    assert parse_bool_pl("Nie") is False
    assert parse_bool_pl(None) is None


def test_parse_market():
    assert parse_market("Wtórny") == "secondary"
    assert parse_market("Pierwotny") == "primary"
    assert parse_market(None) is None
    assert parse_market("Unknown") is None


def test_parse_price_per_m():
    assert parse_price_per_m("6750 zł/m²") == 6750.0
    assert parse_price_per_m(None) is None


def test_parse_rooms_double_digit():
    assert parse_rooms("10 pokoi i więcej") == 10


def test_parse_rooms_kawalerka():
    assert parse_rooms("Kawalerka") == 1