import re

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
    """Extracts room count from labels like '3 pokoje', '10 pokoi i więcej', 'kawalerka'."""
    if not rooms_str:
        return None
    if "kawalerka" in rooms_str.lower():
        return 1
    match = re.search(r"\d+", rooms_str)
    return int(match.group()) if match else None


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
    """Takes a raw GraphQL item, flattens it, and parses clean numeric fields."""
    loc = item.get("location") or {}
    coords = item.get("map") or {}

    data = {
        "id": item.get("id"),
        "url": item.get("url"),
        "title": item.get("title"),
        "created_time": item.get("created_time"),
        "city": loc.get("city", {}).get("name") if loc.get("city") else None,
        "district": loc.get("district", {}).get("name") if loc.get("district") else None,
        "latitude": coords.get("lat"),
        "longitude": coords.get("lon"),
        **extract_params(item.get("params"))
    }

    if isinstance(data.get("m"), str):
        data["area_sqm"] = _parse_leading_number(data["m"])

    if isinstance(data.get("rent"), str):
        data["extra_rent_pln"] = _parse_leading_number(data["rent"])

    if isinstance(data.get("rooms"), str):
        data["num_rooms"] = parse_rooms(data["rooms"])

    return data