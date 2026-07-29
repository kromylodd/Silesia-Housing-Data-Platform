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

# Batch SA: runs the daily Cloud Run Job (scrape -> validate -> upload -> load -> dbt build).
# Combines ingestion + dbt permissions since one job identity does the whole chain.
resource "google_service_account" "batch" {
  account_id   = "housing-batch-sa"
  display_name = "Cloud Run Job daily batch SA"
  project      = var.project_id
}

resource "google_storage_bucket_iam_member" "batch_bucket_writer" {
  bucket = google_storage_bucket.raw_landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.batch.email}"
}

resource "google_bigquery_dataset_iam_member" "batch_raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.batch.email}"
}

resource "google_bigquery_dataset_iam_member" "batch_staging_editor" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.batch.email}"
}

resource "google_bigquery_dataset_iam_member" "batch_marts_editor" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.batch.email}"
}

resource "google_project_iam_member" "batch_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.batch.email}"
}

# Scheduler SA: only allowed to invoke the batch Cloud Run Job, nothing else.
resource "google_service_account" "scheduler" {
  account_id   = "housing-scheduler-sa"
  display_name = "Cloud Scheduler invoker for the daily batch job"
  project      = var.project_id
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.daily_batch.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

# CI/CD deploy SA: pushes images to Artifact Registry + updates the Cloud Run
# Job's image. Static JSON key for now (secrets.BATCH_DEPLOY_SA_KEY in GitHub
# Actions) — fold into the planned Workload Identity Federation migration
# alongside the existing dbt CI auth, rather than solving it twice.
resource "google_service_account" "ci_deploy" {
  account_id   = "housing-ci-deploy-sa"
  display_name = "GitHub Actions deploy SA for the batch Cloud Run Job"
  project      = var.project_id
}

resource "google_artifact_registry_repository_iam_member" "ci_deploy_writer" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.batch_job.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.ci_deploy.email}"
}

resource "google_cloud_run_v2_job_iam_member" "ci_deploy_developer" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.daily_batch.name
  role     = "roles/run.developer"
  member   = "serviceAccount:${google_service_account.ci_deploy.email}"
}
