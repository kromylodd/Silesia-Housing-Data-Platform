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