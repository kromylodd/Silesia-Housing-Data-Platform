"""
Builds olx_location_ids.csv from a folder of captured city-search JSON
responses (the shape you already pulled for "Wa" — a list of {city, region,
municipality, county} objects). Works whether those captures came from a
real browser session or a script; this only cares about the JSON shape,
not how it was obtained.

WORKFLOW
  1. For each of the 34 target cities (or fewer per capture — one query can
     cover several targets, like "Wa" covered both Warszawa and Wałbrzych),
     search the full city name in OLX's location field and save the raw
     JSON response into scraper/captures/, one file per capture, any
     filename. Multiple targets landing in the same file is fine and
     expected — this script dedupes across files automatically.
  2. Run this script. It reads every *.json in scraper/captures/, builds a
     lookup keyed by normalized city name, and matches each row in
     dbt/seeds/city_lookup.csv against it — preferring a match in the
     row's own voivodeship, falling back to a cross-region match (flagged)
     if the region-scoped lookup misses.
  3. Anything printed as MISS has no candidate in any capture file yet —
     go capture that specific city and rerun. This script is idempotent;
     rerunning after adding more captures just picks up the new file.

NORMALIZATION FIX
  The previous _normalize() used NFKD decomposition alone, which handles
  ą/ć/ę/ł's Latin cousins fine but NOT ł/Ł specifically — that codepoint
  has no combining-mark decomposition in Unicode, so NFKD leaves it as-is.
  OLX's own normalized_name field confirms the correct behavior (Łódzkie
  -> lodzkie), so this version strips ł/Ł explicitly before the NFKD pass.
  This matters for 2 of your 34 target cities (Łódź, Wałbrzych) and 2
  voivodeships (łódzkie, małopolskie doesn't have an ł but łódzkie does —
  double check any city in łódzkie specifically).

Run from repo root: python scraper/build_location_ids_from_captures.py
"""

import csv
import glob
import json
import os
import unicodedata

_POLISH_L = str.maketrans({"ł": "l", "Ł": "L"})


def _normalize(text: str) -> str:
    """Strip diacritics (incl. ł/Ł, which NFKD alone misses) + lowercase."""
    text = text.translate(_POLISH_L)
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def load_city_lookup(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_captures(captures_dir: str) -> dict:
    """
    Returns normalized_city_name -> list of unique candidates:
      {city_id, city_label, region_id, region_label, region_norm}
    A list (not a single value) because ambiguous common names are real —
    keep every candidate so mismatches are visible instead of silently
    overwritten.
    """
    candidates = {}
    seen_ids = set()  # (city_id, region_id) dedupe across overlapping capture files
    paths = sorted(glob.glob(os.path.join(captures_dir, "*.json")))
    if not paths:
        raise FileNotFoundError(
            f"No .json files in {captures_dir} — capture at least one city search response first."
        )

    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
        except json.JSONDecodeError as e:
            print(f"  SKIP {os.path.basename(path)}: not valid JSON ({e})")
            continue

        # Response body can be copied either as the wrapping {"data": [...]}
        # object or as the bare list itself, depending on which DevTools
        # view you copied from — accept either.
        if isinstance(payload, list):
            entries = payload
        elif isinstance(payload, dict):
            if "data" in payload:
                entries = payload["data"]
            elif "city" in payload:
                # bare single-entry object (exact-match lookup response),
                # not wrapped in a list — treat it as a 1-item list
                entries = [payload]
            else:
                print(f"  SKIP {os.path.basename(path)}: dict has neither 'data' nor 'city' key")
                continue
        else:
            print(
                f"  SKIP {os.path.basename(path)}: unexpected top-level type {type(payload).__name__}"
            )
            continue

        if not isinstance(entries, list):
            print(
                f"  SKIP {os.path.basename(path)}: 'data' is not a list ({type(entries).__name__})"
            )
            continue

        for entry in entries:
            city = entry.get("city") or {}
            region = entry.get("region") or {}
            city_id = city.get("id")
            region_id = region.get("id")
            if city_id is None or region_id is None:
                continue
            key = (city_id, region_id)
            if key in seen_ids:
                continue
            seen_ids.add(key)

            norm = _normalize(city.get("name", ""))
            candidates.setdefault(norm, []).append(
                {
                    "city_id": city_id,
                    "city_label": city.get("name"),
                    "region_id": region_id,
                    "region_label": region.get("name"),
                    "region_norm": _normalize(region.get("name", "")),
                }
            )

    print(
        f"Loaded {len(paths)} capture file(s), {sum(len(v) for v in candidates.values())} unique city candidates"
    )
    return candidates


def main():
    repo_root = os.path.dirname(os.path.abspath(__file__))
    # if this script lives in scraper/, repo_root is scraper/'s parent;
    # adjust if you place it elsewhere
    if os.path.basename(repo_root) == "scraper":
        repo_root = os.path.dirname(repo_root)

    city_lookup_path = os.path.join(repo_root, "dbt", "seeds", "city_lookup.csv")
    captures_dir = os.path.join(repo_root, "scraper", "captures")
    out_path = os.path.join(repo_root, "scraper", "olx_location_ids.csv")

    cities = load_city_lookup(city_lookup_path)
    candidates = load_captures(captures_dir)

    print("\n--- Matching target cities ---")
    rows_out = []
    unmatched = []
    fallback_matches = []
    ambiguous = []

    for row in cities:
        source_city = row["source_city"]
        city_name_norm = _normalize(row["city_name"])
        voivodeship_norm = _normalize(row["voivodeship"])

        options = candidates.get(city_name_norm, [])
        if not options:
            unmatched.append(source_city)
            print(f"  MISS      {source_city:25} -> no capture contains '{row['city_name']}'")
            continue

        direct = [c for c in options if c["region_norm"] == voivodeship_norm]

        if len(direct) == 1:
            chosen = direct[0]
            print(
                f"  OK        {source_city:25} -> city_id={chosen['city_id']} "
                f"region_id={chosen['region_id']} ({chosen['city_label']}, {chosen['region_label']})"
            )
        elif len(direct) > 1:
            ambiguous.append(source_city)
            chosen = direct[0]
            print(
                f"  AMBIGUOUS {source_city:25} -> {len(direct)} candidates in expected region, taking first:"
            )
            for c in direct:
                print(
                    f"              city_id={c['city_id']} region_id={c['region_id']} ({c['city_label']})"
                )
        elif len(options) == 1:
            chosen = options[0]
            fallback_matches.append(source_city)
            print(
                f"  FALLBACK  {source_city:25} -> city_id={chosen['city_id']} "
                f"region_id={chosen['region_id']} ({chosen['city_label']}, {chosen['region_label']}) "
                f"[expected {row['voivodeship']}, verify]"
            )
        else:
            ambiguous.append(source_city)
            chosen = options[0]
            print(
                f"  AMBIGUOUS {source_city:25} -> {len(options)} candidates, none in expected region, taking first:"
            )
            for c in options:
                print(
                    f"              city_id={c['city_id']} region_id={c['region_id']} ({c['city_label']}, {c['region_label']})"
                )

        rows_out.append(
            {
                "source_city": source_city,
                "city_id": chosen["city_id"],
                "region_id": chosen["region_id"],
                "matched_label": f"{chosen['city_label']}, {chosen['region_label']}",
            }
        )

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["source_city", "city_id", "region_id", "matched_label"]
        )
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"\nWrote {len(rows_out)}/{len(cities)} matches to {out_path}")
    if fallback_matches:
        print(
            f"FALLBACK (only candidate found, but in an unexpected region — verify): {fallback_matches}"
        )
    if ambiguous:
        print(f"AMBIGUOUS (multiple candidates, picked first — verify manually): {ambiguous}")
    if unmatched:
        print(f"MISS (capture these next): {unmatched}")


if __name__ == "__main__":
    main()
