variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  default     = "europe-central2"  # Warsaw — closer to Silesia than EU multi-region, cheaper too
  description = "Default region for resources"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "raw_bucket_name" {
  type        = string
  description = "GCS bucket name for raw landing zone (must be globally unique)"
}

variable "labels" {
  type = map(string)
  default = {
    project     = "silesia-housing"
    environment = "dev"
    managed_by  = "terraform"
  }
}

variable "batch_job_image" {
  type        = string
  description = <<-EOT
    Container image URI for the daily batch Cloud Run Job.
    Bootstrap the very first `terraform apply` with a placeholder
    (Artifact Registry repo is empty before CI's first push), e.g.:
      us-docker.pkg.dev/cloudrun/container/job:latest
    CI (.github/workflows/deploy-batch-job.yml) updates the real image
    afterwards via `gcloud run jobs update --image=...` — terraform
    ignores drift on this field after that (see cloud_run_job.tf lifecycle block).
  EOT
}

variable "scheduler_time_zone" {
  type    = string
  default = "Europe/Warsaw"
}