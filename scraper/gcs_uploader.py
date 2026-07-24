import os
import json
import logging
from datetime import datetime
from google.cloud import storage

logger = logging.getLogger(__name__)


def upload_city_listings_to_gcs(city, date_str=None, bucket_name=None):
    """
    Uploads a city's local raw listings.json to the GCS raw landing zone.
    Mirrors local partition scheme: raw/{city}/{date}/listings.json
    """
    bucket_name = bucket_name or os.environ["GCS_RAW_BUCKET"]
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    local_path = os.path.join(base_dir, "data", "raw", city, date_str, "listings.json")

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"No local file found for {city} at {local_path}")

    with open(local_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        logger.warning(f"[{city}] File exists but has 0 listings — skipping upload.")
        return None

    blob_path = f"raw/{city}/{date_str}/listings.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path, content_type="application/json")

    logger.info(f"[{city}] Uploaded {len(data)} listings to gs://{bucket_name}/{blob_path}")
    return blob_path


if __name__ == "__main__":
    import sys
    city_arg = sys.argv[1] if len(sys.argv) > 1 else "katowice"
    upload_city_listings_to_gcs(city_arg)