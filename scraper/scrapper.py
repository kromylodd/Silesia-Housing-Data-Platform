import time
import random
import requests
import json

from loader import save_to_local_raw
from parser import clean_listing_data

GRAPHQL_URL = "https://www.olx.pl/api/graphql"  # Note: OLX sometimes switches between /api/graphql and /apigateway/graphql

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}

# The brand new query you extracted! I trimmed the extra metadata fields
# to keep it fast, but kept all the listing data we need.
QUERY = """
query ListingSearchQuery(
  $searchParameters: [SearchParameter!] = []
  $fetchPayAndShip: Boolean = false
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
                {"key": "category_id", "value": "1307"},
                {"key": "suggest_filters", "value": "true"}
            ],
            "fetchPayAndShip": True,
            "searchOptions": None
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

if __name__ == "__main__":
    city_to_scrape = "katowice"
    print(f"Fetching data for {city_to_scrape}...")

    raw_response = fetch_olx_page(city=city_to_scrape, offset=0, limit=40)

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
        save_to_local_raw(city_to_scrape, cleaned_listings)