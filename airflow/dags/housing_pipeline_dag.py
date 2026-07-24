from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
sys.path.insert(0, "/opt/airflow/scraper")

from scrapper import scrape_city
from loader import save_to_local_raw
from gcs_uploader import upload_city_listings_to_gcs

TARGET_CITIES = ["katowice", "gliwice", "zabrze", "bytom", "chorzow", "tychy", "sosnowiec", "bielsko-biala"]

def run_scrape_city(city, **kwargs):
    data = scrape_city(city, max_pages=25)
    save_to_local_raw(city, data)

def run_upload_city(city, **kwargs):
    execution_date = kwargs["ds"]  # Airflow's logical date, format YYYY-MM-DD
    upload_city_listings_to_gcs(city, date_str=execution_date)

with DAG(
    dag_id="housing_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["silesia", "housing"],
) as dag:
    for city in TARGET_CITIES:
        scrape_task = PythonOperator(
            task_id=f"scrape_{city}",
            python_callable=run_scrape_city,
            op_kwargs={"city": city},
        )

        upload_task = PythonOperator(
            task_id=f"upload_{city}",
            python_callable=run_upload_city,
            op_kwargs={"city": city},
        )

        scrape_task >> upload_task