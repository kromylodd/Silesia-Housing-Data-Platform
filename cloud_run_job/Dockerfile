# Build from repo root:
#   docker build -f cloud_run_job/Dockerfile -t <image-tag> .
FROM python:3.12-slim

WORKDIR /app

COPY cloud_run_job/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scraper/ /app/scraper/
COPY great_expectations/ /app/great_expectations/
COPY dbt/ /app/dbt/

# Placeholder values here only satisfy profiles.yml's env_var() calls so
# dbt can parse the project and resolve packages.yml — dbt deps never
# connects to BigQuery, so these never touch real infra. Actual values
# come from the Cloud Run Job's env vars at execution time.
RUN cd /app/dbt && \
    GCP_PROJECT_ID=placeholder BQ_DATASET_STAGING=placeholder GCP_REGION=europe-central2 \
    dbt deps --profiles-dir /app/dbt

COPY cloud_run_job/run_daily_pipeline.py /app/run_daily_pipeline.py

ENTRYPOINT ["python", "/app/run_daily_pipeline.py"]