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

output "project_id" {
  value = var.project_id
}

output "batch_sa_email" {
  value = google_service_account.batch.email
}

output "scheduler_sa_email" {
  value = google_service_account.scheduler.email
}

output "ci_deploy_sa_email" {
  value = google_service_account.ci_deploy.email
}

output "batch_job_name" {
  value = google_cloud_run_v2_job.daily_batch.name
}

output "artifact_registry_repo" {
  value = google_artifact_registry_repository.batch_job.repository_id
}