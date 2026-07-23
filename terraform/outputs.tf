output "raw_bucket_name" {
  value = google_storage_bucket.raw_landing.name
}

output "raw_dataset_id" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "staging_dataset_id" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "marts_dataset_id" {
  value = google_bigquery_dataset.marts.dataset_id
}

output "ingestion_sa_email" {
  value = google_service_account.ingestion.email
}

output "dbt_sa_email" {
  value = google_service_account.dbt.email
}