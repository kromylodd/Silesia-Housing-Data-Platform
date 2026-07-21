import os
import json
from datetime import datetime


def save_to_local_raw(city, data):
    """
    Saves parsed listings to a partitioned local directory simulating GCS.
    Path format: data/raw/{city}/{date}/listings.json
    """
    if not data:
        print(f"No data to save for {city}.")
        return

    # Use today's date for partitioning
    today = datetime.now().strftime("%Y-%m-%d")

    # Step out of the scraper folder and into data/raw
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    folder_path = os.path.join(base_dir, "data", "raw", city, today)

    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, "listings.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(data)} listings to {file_path}")