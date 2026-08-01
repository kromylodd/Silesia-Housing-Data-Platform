resource "google_cloud_run_v2_job" "daily_batch" {
  project  = var.project_id
  name     = "housing-daily-batch"
  location = var.region

  template {
    template {
      service_account = google_service_account.batch.email
      timeout         = "7200s" # 34 cities (Stage 2) + dbt build — global rate-limited scrape
                                 # alone can approach ~30-40min worst case (see rate_limiter.py);
                                 # was 3600s, sized only for the original 8 MVP cities
      max_retries     = 1

      containers {
        image = var.batch_job_image

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCP_REGION"
          value = var.region
        }
        env {
          name  = "GCS_RAW_BUCKET"
          value = google_storage_bucket.raw_landing.name
        }
        env {
          name  = "BQ_DATASET_RAW"
          value = google_bigquery_dataset.raw.dataset_id
        }
        env {
          name  = "BQ_DATASET_STAGING"
          value = google_bigquery_dataset.staging.dataset_id
        }
        env {
          name  = "BQ_DATASET_MARTS"
          value = google_bigquery_dataset.marts.dataset_id
        }
        # No DBT_KEYFILE_PATH here on purpose — dbt/profiles.yml falls back
        # to `method: oauth` (Application Default Credentials via the
        # attached service_account above) when that var is unset, so no
        # static key needs to exist inside this container at all.
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].template[0].containers[0].image]
    # Image is updated by CI (.github/workflows/deploy-batch-job.yml) via
    # `gcloud run jobs update --image=...`, not by terraform apply —
    # otherwise every apply would fight the latest pushed tag with
    # whatever var.batch_job_image happens to default to.
  }

  depends_on = [google_project_service.required]
}
