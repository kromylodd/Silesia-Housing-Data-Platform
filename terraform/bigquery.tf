resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw_housing"
  project     = var.project_id
  location    = var.region
  description = "Raw ingestion layer, no transformations"
  labels      = var.labels

  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging_housing"
  project     = var.project_id
  location    = var.region
  description = "dbt staging models"
  labels      = var.labels

  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "marts_housing"
  project     = var.project_id
  location    = var.region
  description = "dbt marts — star schema + business marts"
  labels      = var.labels

  depends_on = [google_project_service.required]
}