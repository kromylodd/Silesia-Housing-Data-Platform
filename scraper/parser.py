import re


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

    # Base dictionary mapping to your plan's schema
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

    # Parse square meters ("47 m²" -> 47.0)
    if "m" in data and isinstance(data["m"], str):
        match = re.search(r"([\d\s,.]+)", data["m"])
        if match:
            val_str = match.group(1).replace(" ", "").replace(",", ".")
            try:
                data["area_sqm"] = float(val_str)
            except ValueError:
                data["area_sqm"] = None

    # Parse additional rent ("860 zł" -> 860.0)
    if "rent" in data and isinstance(data["rent"], str):
        match = re.search(r"([\d\s,.]+)", data["rent"])
        if match:
            val_str = match.group(1).replace(" ", "").replace(",", ".")
            try:
                data["extra_rent_pln"] = float(val_str)
            except ValueError:
                data["extra_rent_pln"] = None

    # Parse number of rooms ("3 pokoje" -> 3)
    if "rooms" in data and isinstance(data["rooms"], str):
        rooms_str = data["rooms"].lower()
        if "1" in rooms_str or "kawalerka" in rooms_str:
            data["num_rooms"] = 1
        elif "2" in rooms_str:
            data["num_rooms"] = 2
        elif "3" in rooms_str:
            data["num_rooms"] = 3
        elif "4" in rooms_str:
            data["num_rooms"] = 4

    return data