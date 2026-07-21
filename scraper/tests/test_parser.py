import json
import pytest
import sys
import os

# Add parent directory (scraper/) to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Now imports will resolve cleanly
from parser import clean_listing_data, extract_params
from parser import clean_listing_data, extract_params

@pytest.fixture
def raw_listing():
    """Loads the offline raw JSON fixture."""
    fixture_path = os.path.join(os.path.dirname(__file__), "sample_raw_listing.json")
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)

def test_extract_params(raw_listing):
    params = extract_params(raw_listing.get("params"))
    assert params.get("price") == 2500
    assert params.get("currency") == "PLN"
    assert params.get("m") == "48,5 m²"

def test_clean_listing_data_parsing(raw_listing):
    cleaned = clean_listing_data(raw_listing)

    # Check core schema
    assert cleaned["id"] == 987654321
    assert cleaned["city"] == "Katowice"
    assert cleaned["district"] == "Koszutka"

    # Check type coercions and regex extractions
    assert cleaned["area_sqm"] == 48.5
    assert isinstance(cleaned["area_sqm"], float)

    assert cleaned["extra_rent_pln"] == 650.0
    assert isinstance(cleaned["extra_rent_pln"], float)

    assert cleaned["num_rooms"] == 2
    assert isinstance(cleaned["num_rooms"], int)

def test_clean_listing_missing_fields():
    """Ensures parser gracefully handles missing optional fields without crashing."""
    empty_item = {"id": 1111, "title": "Empty Test"}
    cleaned = clean_listing_data(empty_item)

    assert cleaned["id"] == 1111
    assert cleaned["city"] is None
    assert cleaned.get("area_sqm") is None
    assert cleaned.get("num_rooms") is None