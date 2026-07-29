# Build from repo root:
#   docker build -f cloud_run_job/Dockerfile -t <image-tag> .
FROM python:3.12-slim

WORKDIR /app

COPY cloud_run_job/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scraper/ /app/scraper/
COPY great_expectations/ /app/great_expectations/
COPY dbt/ /app/dbt/
COPY cloud_run_job/run_daily_pipeline.py /app/run_daily_pipeline.py

ENTRYPOINT ["python", "/app/run_daily_pipeline.py"]