import requests
import time
import random
from parser import clean_listing_data
from loader import save_to_local_raw

GRAPHQL_URL = "https://www.olx.pl/apigateway/graphql"

HEADERS = {
    "accept": "application/json",
    "accept-language": "pl",
    "content-type": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "x-client": "DESKTOP",
    "origin": "https://www.olx.pl",
}

QUERY = """query ListingSearchQuery($searchParameters: [SearchParameter!] = [], $searchOptions: SearchOptions) {
  clientCompatibleListings(searchParameters: $searchParameters, searchOptions: $searchOptions) {
    __typename
    ... on ListingSuccess {
      data { id title url created_time location { city { name } district { name } } params { key name value { __typename ... on PriceParam { value currency } ... on GenericParam { key label } } } map { lat lon } }
      metadata { total_elements }
    }
    ... on ListingError { error { code detail } }
  }
}"""


def fetch_olx_page(city, offset=0, limit=40):
    payload = {
        "query": QUERY,
        "variables": {
            "searchParameters": [
                {"key": "offset", "value": str(offset)},
                {"key": "limit", "value": str(limit)},
                {"key": "query", "value": city},
                {"key": "category_id", "value": "15"},
                {"key": "suggest_filters", "value": "true"},
            ],
            "searchOptions": None,
        },
    }
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=10)
    response.raise_for_status()
    return response.json()


def scrape_city(city, max_pages=25):
    """Scrapes, parses, and deduplicates listings for a single city."""
    all_listings = []
    limit = 40

    for page in range(max_pages):
        offset = page * limit
        print(f"[{city.upper()}] Fetching page {page + 1} (offset: {offset})...")

        try:
            res_json = fetch_olx_page(city=city, offset=offset, limit=limit)
            listings_data = res_json.get("data", {}).get("clientCompatibleListings", {})

            if listings_data.get("__typename") != "ListingSuccess":
                print(f"[{city.upper()}] GraphQL Error: {listings_data}")
                break

            raw_items = listings_data.get("data", [])
            total_items = listings_data.get("metadata", {}).get("total_elements", 0)

            if not raw_items:
                break

            for item in raw_items:
                all_listings.append(clean_listing_data(item))

            if offset + limit >= total_items:
                break

            time.sleep(random.uniform(1.5, 3.5))

        except Exception as e:
            print(f"[{city.upper()}] Failed on page {page + 1}: {e}")
            break

    # Deduplicate by ID
    unique_listings = list({item['id']: item for item in all_listings}.values())
    print(f"[{city.upper()}] Finished. Unique listings: {len(unique_listings)}")
    return unique_listings


if __name__ == "__main__":
    # Target cities defined in your MVP scope
    TARGET_CITIES = [
        "katowice", "gliwice", "zabrze", "bytom",
        "chorzow", "tychy", "sosnowiec", "bielsko-biala"
    ]

    for target_city in TARGET_CITIES:
        print(f"\n{'=' * 40}\n STARTING TARGET: {target_city.upper()}\n{'=' * 40}")
        city_data = scrape_city(target_city, max_pages=25)
        save_to_local_raw(target_city, city_data)

        # Extended pause between completely different city requests
        time.sleep(random.uniform(5.0, 10.0))