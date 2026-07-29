resource "google_artifact_registry_repository" "batch_job" {
  project       = var.project_id
  location      = var.region
  repository_id = "housing-batch-job"
  format        = "DOCKER"
  description   = "Container images for the daily batch Cloud Run Job"
  labels        = var.labels

  depends_on = [google_project_service.required]
}
