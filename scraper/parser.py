import re
from datetime import datetime, timezone


def _parse_leading_number(value_str):
    """Extracts and converts a leading numeric value like '48,5 m²' -> 48.5"""
    if not value_str:
        return None
    match = re.search(r"([\d\s,.]+)", value_str)
    if not match:
        return None
    cleaned = match.group(1).replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_rooms(rooms_str):
    """
    Extracts room count from OLX's bucketed labels: 'kawalerka', '1 pokój',
    '2 pokoje', '3 pokoje', '4 i więcej'.

    OLX's own category system tops out at "4 and more" — there is no
    separate bucket for 5, 6, etc, so an exact count above 4 is not
    recoverable from this field. Returns (num_rooms, is_capped): when
    is_capped is True, num_rooms is a lower bound ("at least this many"),
    not an exact value.
    """
    if not rooms_str:
        return None, False
    s = rooms_str.lower()
    if "kawalerka" in s:
        return 1, False
    match = re.search(r"\d+", s)
    if not match:
        return None, False
    is_capped = "więcej" in s or "wiecej" in s
    return int(match.group()), is_capped


def parse_floor(floor_str):
    """
    'Parter' (ground floor) -> 0, 'Powyżej 10' (above 10) -> capped bucket,
    otherwise numeric string -> int.

    OLX exposes floors 1-10 individually, then buckets everything above
    that into a single "Powyżej 10" option. Returns (floor, is_capped):
    when is_capped is True, floor is a lower bound ("above this floor"),
    not an exact value.
    """
    if not floor_str:
        return None, False
    s = floor_str.strip().lower()
    if s == "parter":
        return 0, False
    is_capped = "powyżej" in s or "powyzej" in s
    match = re.search(r"\d+", s)
    return (int(match.group()) if match else None), is_capped


def parse_bool_pl(value_str):
    """Polish 'Tak'/'Nie' -> True/False."""
    if value_str is None:
        return None
    return value_str.strip().lower() == "tak"


MARKET_MAP = {
    "pierwotny": "primary",
    "wtórny": "secondary",
}


def parse_market(market_str):
    """'Pierwotny'/'Wtórny' -> 'primary'/'secondary'"""
    if not market_str:
        return None
    return MARKET_MAP.get(market_str.strip().lower())


def parse_price_per_m(value_str):
    """'6750 zł/m²' -> 6750.0"""
    return _parse_leading_number(value_str) if value_str else None


def extract_params(params_list):
    """Flattens the nested OLX params array into a flat dictionary."""
    parsed = {}
    if not params_list:
        return parsed

    for p in params_list:
        key = p.get("key")
        val_data = p.get("value") or {}

        if val_data.get("__typename") == "PriceParam":
            parsed["price"] = val_data.get("value")
            parsed["currency"] = val_data.get("currency")
        elif val_data.get("__typename") == "GenericParam":
            parsed[key] = val_data.get("label") or val_data.get("key")

    return parsed


def clean_listing_data(item):
    """Takes a raw GraphQL item, flattens it, and parses clean typed fields."""
    loc = item.get("location") or {}
    coords = item.get("map") or {}

    data = {
        "id": item.get("id"),
        "url": item.get("url"),
        "title": item.get("title"),
        "created_time": item.get("created_time"),
        "date_collected": datetime.now(timezone.utc).isoformat(),
        "city": loc.get("city", {}).get("name") if loc.get("city") else None,
        "district": loc.get("district", {}).get("name") if loc.get("district") else None,
        "latitude": coords.get("lat"),
        "longitude": coords.get("lon"),
        **extract_params(item.get("params")),
    }

    if isinstance(data.get("m"), str):
        data["area_sqm"] = _parse_leading_number(data["m"])

    if isinstance(data.get("rent"), str):
        data["extra_rent_pln"] = _parse_leading_number(data["rent"])

    data["num_rooms"], data["rooms_capped"] = parse_rooms(data.get("rooms"))

    # Independent of whether "rooms" was present on this listing
    data["floor"], data["floor_capped"] = parse_floor(data.get("floor_select"))
    data["is_furnished"] = parse_bool_pl(data.get("furniture"))
    data["building_type"] = data.get("builttype")
    data["market_type"] = parse_market(data.get("market"))
    data["price_per_sqm_listed"] = parse_price_per_m(data.get("price_per_m"))

    # Drop raw/intermediate label fields now that typed equivalents exist —
    # keeping both is redundant and pushes messy Polish strings downstream.
    for raw_key in (
        "m",
        "rent",
        "rooms",
        "floor_select",
        "furniture",
        "market",
        "builttype",
        "price_per_m",
    ):
        data.pop(raw_key, None)

    return data
