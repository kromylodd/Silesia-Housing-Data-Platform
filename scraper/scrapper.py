import time
import random
import requests
import json

from loader import save_to_local_raw
from parser import clean_listing_data

GRAPHQL_URL = "https://www.olx.pl/apigateway/graphql"  # Note: OLX sometimes switches between /api/graphql and /apigateway/graphql

HEADERS = {
    "accept": "application/json",
    "accept-language": "pl",
    "content-type": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "x-client": "DESKTOP",
    "origin": "https://www.olx.pl",
}

# The brand new query you extracted! I trimmed the extra metadata fields
# to keep it fast, but kept all the listing data we need.
QUERY = """
query ListingSearchQuery(
  $searchParameters: [SearchParameter!] = []
  $searchOptions: SearchOptions
) {
  clientCompatibleListings(searchParameters: $searchParameters, searchOptions: $searchOptions) {
    __typename
    ... on ListingSuccess {
      __typename
      data {
        id
        url
        title
        created_time
        location {
          city { name }
          district { name }
        }
        map {
          lat
          lon
        }
        params {
          key
          value {
            __typename
            ... on GenericParam {
              key
              label
            }
            ... on PriceParam {
              value
              currency
            }
          }
        }
      }
    }
    ... on ListingError {
      error {
        code
        detail
      }
    }
  }
}
"""


def fetch_olx_page(city, offset=0, limit=40, max_retries=3):
    payload = {
        "operationName": "ListingSearchQuery",
        "query": QUERY,
        "variables": {
            "searchParameters": [
                {"key": "offset", "value": str(offset)},
                {"key": "limit", "value": str(limit)},
                {"key": "query", "value": city},
                {"key": "category_id", "value": "14"},
                {"key": "suggest_filters", "value": "true"}
            ],
            "searchOptions": None
            # fetchPayAndShip removed — unused, caused GRAPHQL_VALIDATION_FAILED
        },
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Print any hidden GraphQL validation or runtime errors for debugging
            if "errors" in data:
                print(f"[{city.upper()}] GraphQL Error response: {data['errors']}")

            return data
        except requests.RequestException as err:
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(
                f"[{city.upper()}] Request failed (attempt {attempt + 1}/{max_retries}): {err}. Retrying in {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError(f"[{city.upper()}] Failed offset={offset} after {max_retries} attempts")


def scrape_city(city, max_pages=25):
    """Scrapes and parses all sale listings for a single city, paginating through results."""
    all_listings = []
    limit = 40

    for page in range(max_pages):
        offset = page * limit
        print(f"[{city.upper()}] Fetching page {page + 1} (offset: {offset})...")

        try:
            res_json = fetch_olx_page(city=city, offset=offset, limit=limit)
        except RuntimeError as err:
            print(f"[{city.upper()}] Giving up: {err}")
            break

        if "errors" in res_json:
            print(f"[{city.upper()}] GraphQL Error response: {res_json['errors']}")
            break

        listings_data = res_json.get("data", {}).get("clientCompatibleListings", {})

        if listings_data.get("__typename") != "ListingSuccess":
            print(f"[{city.upper()}] Non-success response: {listings_data}")
            break

        raw_items = listings_data.get("data", [])
        if not raw_items:
            break

        for item in raw_items:
            try:
                all_listings.append(clean_listing_data(item))
            except Exception as err:
                print(f"[{city.upper()}] Skipping ad due to error: {err}")

        if len(raw_items) < limit:
            break  # last page reached

        time.sleep(random.uniform(1.5, 3.5))

    unique_listings = list({item["id"]: item for item in all_listings}.values())
    print(f"[{city.upper()}] Finished. Unique listings: {len(unique_listings)}")
    return unique_listings


if __name__ == "__main__":
    TARGET_CITIES = [
        "katowice", "gliwice", "zabrze", "bytom",
        "chorzow", "tychy", "sosnowiec", "bielsko-biala"
    ]

    for target_city in TARGET_CITIES:
        print(f"\n{'=' * 40}\n STARTING TARGET: {target_city.upper()}\n{'=' * 40}")
        city_listings = scrape_city(target_city, max_pages=25)
        save_to_local_raw(target_city, city_listings)

        time.sleep(random.uniform(5.0, 10.0))

    raw_response = fetch_olx_page(city=target_city, offset=0, limit=40)

    # --- Debug block just in case ---
    if "errors" in raw_response:
        print("\n--- GRAPHQL ERROR ---")
        print(json.dumps(raw_response["errors"], indent=2))
        print("---------------------\n")

    # The NEW path to extract ads based on your network sniffing
    raw_ads = raw_response.get("data", {}).get("clientCompatibleListings", {}).get("data", [])

    print(f"Found {len(raw_ads)} raw ads!")

    cleaned_listings = []

    for ad in raw_ads:
        try:
            cleaned_listings.append(clean_listing_data(ad))
        except Exception as err:
            print(f"Skipping ad due to error: {err}")

    if cleaned_listings:
        save_to_local_raw(target_city, cleaned_listings)