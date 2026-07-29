resource "google_cloud_scheduler_job" "daily_batch_trigger" {
  project     = var.project_id
  region      = var.region
  name        = "housing-daily-batch-trigger"
  description = "Triggers the daily scrape -> validate -> upload -> load -> dbt build batch job"
  schedule    = "0 3 * * *" # 03:00 — after most listings for the day are posted
  time_zone   = var.scheduler_time_zone

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.daily_batch.name}:run"
    body        = base64encode("{}")

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }

  depends_on = [
    google_project_service.required,
    google_cloud_run_v2_job_iam_member.scheduler_invoker,
  ]
}
