"""
Loads a validated city batch from the GCS raw landing zone into BigQuery's
raw_apartment_listings table.

Sits after the GCS upload in the Airflow DAG:

    scrape_task >> validate_task >> upload_task >> load_task

Reads the same GCS blob gcs_uploader.py just wrote (not the local file) so
BigQuery's raw layer mirrors what's actually sitting in the landing zone,
not a local copy that could drift or get cleaned up independently.
"""

import json
import logging
import os
from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

# Mirrors clean_listing_data()'s output in parser.py. No transformations here
# beyond typing — that's what the raw layer is for.
RAW_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("created_time", "STRING"),
    bigquery.SchemaField("date_collected", "TIMESTAMP"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("district", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT"),
    bigquery.SchemaField("longitude", "FLOAT"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("area_sqm", "FLOAT"),
    bigquery.SchemaField("extra_rent_pln", "FLOAT"),
    bigquery.SchemaField("num_rooms", "INTEGER"),
    bigquery.SchemaField("rooms_capped", "BOOLEAN"),
    bigquery.SchemaField("floor", "INTEGER"),
    bigquery.SchemaField("floor_capped", "BOOLEAN"),
    bigquery.SchemaField("is_furnished", "BOOLEAN"),
    bigquery.SchemaField("building_type", "STRING"),
    bigquery.SchemaField("market_type", "STRING"),
    bigquery.SchemaField("price_per_sqm_listed", "FLOAT"),
    bigquery.SchemaField("source_city", "STRING"),  # the scrape target, not the parsed `city` field
]


def _ensure_raw_table(bq_client, table_id):
    """
    Creates raw_apartment_listings on first run, partitioned on
    date_collected and clustered on source_city. Deliberately done as an
    explicit create rather than via load-job config, so later append loads
    never risk a partition-spec mismatch error against an existing table.
    """
    try:
        bq_client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=RAW_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date_collected"
        )
        table.clustering_fields = ["source_city"]
        bq_client.create_table(table)
        logger.info(f"Created {table_id} (partitioned on date_collected, clustered on source_city)")


def load_city_to_bigquery(
    city,
    date_str=None,
    bucket_name=None,
    project_id=None,
    dataset_id=None,
    table_name="raw_apartment_listings",
):
    """
    Downloads a city's validated batch from GCS and appends it into
    raw_apartment_listings. Returns the number of rows loaded (0 if the
    batch was empty).
    """
    bucket_name = bucket_name or os.environ["GCS_RAW_BUCKET"]
    project_id = project_id or os.environ["GCP_PROJECT_ID"]
    dataset_id = dataset_id or os.environ["BQ_DATASET_RAW"]
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")

    blob_path = f"raw/{city}/{date_str}/listings.json"
    storage_client = storage.Client()
    blob = storage_client.bucket(bucket_name).blob(blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"No GCS blob at gs://{bucket_name}/{blob_path}")

    records = json.loads(blob.download_as_text())
    if not records:
        logger.warning(f"[{city}] GCS batch has 0 listings — skipping BQ load.")
        return 0

    for record in records:
        record["source_city"] = city

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    bq_client = bigquery.Client(project=project_id)
    _ensure_raw_table(bq_client, table_id)

    job_config = bigquery.LoadJobConfig(
        schema=RAW_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )

    load_job = bq_client.load_table_from_json(records, table_id, job_config=job_config)
    load_job.result()  # blocks until done, raises on failure

    table = bq_client.get_table(table_id)
    logger.info(
        f"[{city}] Loaded {load_job.output_rows} rows into {table_id}. Total rows now: {table.num_rows}"
    )
    return load_job.output_rows


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    city_arg = sys.argv[1] if len(sys.argv) > 1 else "katowice"
    load_city_to_bigquery(city_arg)
