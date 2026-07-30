# test_auth.py
import sys

print("Running with interpreter:", sys.executable)

import google.auth
from google.auth import impersonated_credentials
import google.auth.transport.requests

source_credentials, project = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
print("Source credentials OK. Project:", project)

target_credentials = impersonated_credentials.Credentials(
    source_credentials=source_credentials,
    target_principal="housing-dbt-sa@silesia-housing-data-platform.iam.gserviceaccount.com",
    target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
    lifetime=300,
)

request = google.auth.transport.requests.Request()
target_credentials.refresh(request)
print("Impersonation succeeded. Token starts with:", target_credentials.token[:20])
