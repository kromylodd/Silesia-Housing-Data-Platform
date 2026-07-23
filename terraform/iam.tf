resource "google_service_account" "ingestion" {
  account_id   = "housing-ingestion-sa"
  display_name = "Airflow/scraper ingestion SA"
  project      = var.project_id
}

resource "google_service_account" "dbt" {
  account_id   = "housing-dbt-sa"
  display_name = "dbt transform SA"
  project      = var.project_id
}

# Ingestion SA: write to raw bucket + load into raw dataset
resource "google_storage_bucket_iam_member" "ingestion_bucket_writer" {
  bucket = google_storage_bucket.raw_landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingestion.email}"
}

resource "google_bigquery_dataset_iam_member" "ingestion_raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.ingestion.email}"
}

resource "google_project_iam_member" "ingestion_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"  # needed to run load jobs
  member  = "serviceAccount:${google_service_account.ingestion.email}"
}

# dbt SA: read raw, write staging + marts
resource "google_bigquery_dataset_iam_member" "dbt_raw_reader" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_staging_editor" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_marts_editor" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_project_iam_member" "dbt_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}