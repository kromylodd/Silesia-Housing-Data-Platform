import asyncio
import csv
import logging
import os
import random
import time

import httpx
from loader import save_to_local_raw
from parser import clean_listing_data
from rate_limiter import GlobalRateLimiter

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://www.olx.pl/apigateway/graphql"  # Note: OLX sometimes switches between /api/graphql and /apigateway/graphql

LOCATION_IDS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "olx_location_ids.csv")


def _load_location_ids(path: str = LOCATION_IDS_PATH) -> dict:
    """source_city -> (city_id, region_id), as produced by location_id_builder.py.

    Using the resolved location IDs instead of a free-text "query" search
    parameter avoids OLX's fuzzy text matching, which is the main source of
    noise flagged for the 34-city expansion. A city missing from the CSV
    (or a missing/unreadable CSV) falls back to free-text search rather
    than raising, so a stale mapping degrades one city instead of killing
    the whole batch.
    """
    ids = {}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ids[row["source_city"]] = (row["city_id"], row["region_id"])
    except FileNotFoundError:
        logger.warning(f"{path} not found — falling back to free-text city search for all cities")
    return ids


LOCATION_IDS = _load_location_ids()

HEADERS = {
    "accept": "application/json",
    "accept-language": "pl",
    "content-type": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "x-client": "DESKTOP",
    "origin": "https://www.olx.pl",
}

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
      metadata {
        total_elements
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


async def fetch_olx_page_async(
    client: httpx.AsyncClient,
    limiter: GlobalRateLimiter,
    city: str,
    offset: int = 0,
    limit: int = 40,
    max_retries: int = 3,
) -> dict:
    """Fetches one page of listings for `city`, paced by the shared limiter.

    The limiter is acquired fresh on every attempt (including retries), so a
    retry after a transient failure still respects the global pacing budget
    instead of firing immediately.
    """
    location = LOCATION_IDS.get(city)
    if location:
        city_id, region_id = location
        location_params = [
            {"key": "city_id", "value": city_id},
            {"key": "region_id", "value": region_id},
        ]
    else:
        logger.warning(
            f"[{city.upper()}] No location_id mapping found — falling back to free-text query"
        )
        location_params = [{"key": "query", "value": city}]

    payload = {
        "operationName": "ListingSearchQuery",
        "query": QUERY,
        "variables": {
            "searchParameters": [
                {"key": "offset", "value": str(offset)},
                {"key": "limit", "value": str(limit)},
                *location_params,
                {"key": "category_id", "value": "14"},
                {"key": "suggest_filters", "value": "true"},
            ],
            "searchOptions": None,
        },
    }

    for attempt in range(max_retries):
        try:
            async with limiter:
                response = await client.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                logger.error(f"[{city.upper()}] GraphQL error response: {data['errors']}")

            return data
        except httpx.HTTPError as err:
            wait = (2**attempt) + random.uniform(0, 1)
            logger.warning(
                f"[{city.upper()}] Request failed (attempt {attempt + 1}/{max_retries}): {err}. Retrying in {wait:.1f}s"
            )
            await asyncio.sleep(wait)

    raise RuntimeError(f"[{city.upper()}] Failed offset={offset} after {max_retries} attempts")


async def scrape_city_async(
    client: httpx.AsyncClient,
    limiter: GlobalRateLimiter,
    city: str,
    max_pages: int = 25,
) -> list:
    """Scrapes and parses all sale listings for a single city, paginating through results.

    Pagination within a city stays sequential (page N+1 needs to know whether
    page N was full) — concurrency happens *across* cities in
    scrape_cities_async, not within one city's page loop.
    """
    all_listings = []
    limit = 40
    total_elements = None

    for page in range(max_pages):
        offset = page * limit
        logger.info(f"[{city.upper()}] Fetching page {page + 1} (offset: {offset})...")

        try:
            res_json = await fetch_olx_page_async(client, limiter, city, offset, limit)
        except RuntimeError as err:
            logger.error(f"[{city.upper()}] Giving up: {err}")
            break

        if "errors" in res_json:
            logger.error(f"[{city.upper()}] GraphQL error response: {res_json['errors']}")
            break

        listings_data = res_json.get("data", {}).get("clientCompatibleListings", {})

        if listings_data.get("__typename") != "ListingSuccess":
            logger.error(f"[{city.upper()}] Non-success response: {listings_data}")
            break

        raw_items = listings_data.get("data", [])
        total_elements = listings_data.get("metadata", {}).get("total_elements")
        if page == 0:
            logger.info(f"[{city.upper()}] API reports {total_elements} total listings")

        if not raw_items:
            break

        for item in raw_items:
            try:
                all_listings.append(clean_listing_data(item))
            except Exception as err:
                logger.warning(f"[{city.upper()}] Skipping ad due to error: {err}")

        # Prefer the API's own count as the stop condition — a short page
        # (promoted-ad reshuffling, a dropped invalid item) doesn't
        # necessarily mean we've reached the end of the result set.
        # `len(raw_items) < limit` stays as a fallback for the rare case
        # total_elements comes back null/zero.
        if isinstance(total_elements, int) and total_elements > 0:
            if offset + limit >= total_elements:
                break
        elif len(raw_items) < limit:
            break  # last page reached (no usable total_elements to check against)
    else:
        # Loop exhausted max_pages without hitting a break above — i.e. we
        # never satisfied total_elements. Silent truncation for big cities
        # is worse than a noisy log line, so flag it explicitly.
        if isinstance(total_elements, int) and total_elements > max_pages * limit:
            logger.warning(
                f"[{city.upper()}] Hit max_pages={max_pages} limit but API reports "
                f"{total_elements} total listings — results are truncated. "
                f"Increase max_pages for this city."
            )

    unique_listings = list({item["id"]: item for item in all_listings}.values())
    logger.info(f"[{city.upper()}] Finished. Unique listings: {len(unique_listings)}")
    return unique_listings


async def scrape_cities_async(
    cities: list,
    max_pages: int = 25,
    max_concurrent: int = 4,
    min_interval: tuple = (1.5, 3.0),
) -> dict:
    """Scrapes multiple cities concurrently under one shared rate limiter.

    `max_concurrent` and `min_interval` bound the *global* request rate to
    OLX across every city combined — this is what keeps the source-facing
    hit rate roughly flat as the target city list grows (8 -> 30-40+ cities
    in Stage 2), instead of scaling with city count.

    Returns {city: [listings]}. A city whose scrape task raises is logged
    and mapped to an empty list rather than failing the whole batch — one
    bad city shouldn't take down the other 33.
    """
    limiter = GlobalRateLimiter(max_concurrent=max_concurrent, min_interval=min_interval)
    results = {}

    async with httpx.AsyncClient() as client:
        tasks = {
            city: asyncio.create_task(scrape_city_async(client, limiter, city, max_pages))
            for city in cities
        }
        for city, task in tasks.items():
            try:
                results[city] = await task
            except Exception:
                logger.exception(f"[{city}] scrape task failed entirely")
                results[city] = []

    return results


def scrape_city(city: str, max_pages: int = 25) -> list:
    """Sync wrapper around the async scraper for a single city.

    Kept for callers that don't need cross-city concurrency: the Airflow
    DAG's per-task PythonOperator callables (Airflow parallelizes at the
    task level, not within a task), and tests. Runs the same async path
    with concurrency effectively capped at 1.
    """
    return asyncio.run(scrape_cities_async([city], max_pages=max_pages, max_concurrent=1))[city]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Full Stage 2 list — must stay in sync with dbt/seeds/city_lookup.csv
    # (source_city column). Order matches that seed (population-descending).
    TARGET_CITIES = [
        "warszawa",
        "krakow",
        "lodz",
        "wroclaw",
        "poznan",
        "gdansk",
        "szczecin",
        "bydgoszcz",
        "lublin",
        "bialystok",
        "katowice",
        "gdynia",
        "czestochowa",
        "radom",
        "rzeszow",
        "sosnowiec",
        "torun",
        "kielce",
        "gliwice",
        "zabrze",
        "olsztyn",
        "bielsko-biala",
        "bytom",
        "zielona-gora",
        "rybnik",
        "ruda-slaska",
        "opole",
        "tychy",
        "gorzow-wielkopolski",
        "dabrowa-gornicza",
        "plock",
        "elblag",
        "walbrzych",
        "chorzow",
    ]

    date_str = time.strftime("%Y-%m-%d")
    all_results = asyncio.run(scrape_cities_async(TARGET_CITIES, max_pages=25, max_concurrent=4))

    for target_city, city_listings in all_results.items():
        save_to_local_raw(target_city, city_listings, date_str=date_str)
