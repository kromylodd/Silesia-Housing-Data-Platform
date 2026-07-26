import sys
from datetime import datetime

from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, "/opt/airflow/scraper")

from loader import save_to_local_raw
from scrapper import scrape_city

TARGET_CITIES = [
    "katowice",
    "gliwice",
    "zabrze",
    "bytom",
    "chorzow",
    "tychy",
    "sosnowiec",
    "bielsko-biala",
]


def run_scrape_city(city, **kwargs):
    data = scrape_city(city, max_pages=25)
    save_to_local_raw(city, data)


with DAG(
    dag_id="housing_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["silesia", "housing"],
) as dag:
    for city in TARGET_CITIES:
        PythonOperator(
            task_id=f"scrape_{city}",
            python_callable=run_scrape_city,
            op_kwargs={"city": city},
        )
