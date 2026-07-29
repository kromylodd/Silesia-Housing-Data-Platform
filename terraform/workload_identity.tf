resource "google_iam_workload_identity_pool" "github_actions" {
  project                   = var.project_id
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions"
}

resource "google_iam_workload_identity_pool_provider" "github_actions" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_actions.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC provider"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  # Hard-restrict to this repo — without this any GitHub repo could mint tokens for your SAs.
  attribute_condition = "assertion.repository == \"kromylodd/Silesia-Housing-Data-Platform\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "dbt_wif_binding" {
  service_account_id = google_service_account.dbt.name
  role                = "roles/iam.workloadIdentityUser"
  member              = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_actions.name}/attribute.repository/kromylodd/Silesia-Housing-Data-Platform"
}

resource "google_service_account_iam_member" "ci_deploy_wif_binding" {
  service_account_id = google_service_account.ci_deploy.name
  role                = "roles/iam.workloadIdentityUser"
  member              = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_actions.name}/attribute.repository/kromylodd/Silesia-Housing-Data-Platform"
}

output "wif_provider_full_name" {
  value = google_iam_workload_identity_pool_provider.github_actions.name
}