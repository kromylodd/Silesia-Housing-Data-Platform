resource "google_storage_bucket" "raw_landing" {
  name          = var.raw_bucket_name
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  force_destroy                = true  # dev convenience; drop this before anything resembling prod

  lifecycle_rule {
    condition {
      age = 90  # raw JSON gets deleted after 90 days — BigQuery is the source of truth post-load
    }
    action {
      type = "Delete"
    }
  }

  labels = var.labels

  depends_on = [google_project_service.required]
}