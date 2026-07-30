import sys
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, "/opt/airflow/scraper")
sys.path.insert(0, "/opt/airflow/great_expectations")
sys.path.insert(0, "/opt/airflow/plugins")

from bq_loader import load_city_to_bigquery
from callbacks import notify_discord_on_failure
from gcs_uploader import upload_city_listings_to_gcs
from loader import save_to_local_raw
from scrapper import scrape_city
from validate_batch import validate_city_batch

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
    selected = kwargs["params"].get("cities") or TARGET_CITIES
    if city not in selected:
        raise AirflowSkipException(f"{city} not in requested cities: {selected}")
    execution_date = kwargs["ds"]
    data = scrape_city(city, max_pages=25)
    save_to_local_raw(city, data, date_str=execution_date)


def run_validate_city(city, **kwargs):
    execution_date = kwargs["ds"]
    validate_city_batch(city, date_str=execution_date)


def run_upload_city(city, **kwargs):
    execution_date = kwargs["ds"]  # Airflow's logical date, format YYYY-MM-DD
    upload_city_listings_to_gcs(city, date_str=execution_date)


def run_load_city(city, **kwargs):
    execution_date = kwargs["ds"]
    load_city_to_bigquery(city, date_str=execution_date)


with DAG(
    dag_id="housing_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["silesia", "housing"],
    on_failure_callback=notify_discord_on_failure,
    params={
        "cities": Param(
            default=TARGET_CITIES,
            type="array",
            title="Cities to run",
            description='Trim to a subset, e.g. ["katowice"], to run one city only. '
            "Other cities' task chains still appear in the graph but show as skipped.",
        ),
    },
) as dag:
    load_bq_tasks = []
    for city in TARGET_CITIES:
        scrape_task = PythonOperator(
            task_id=f"scrape_{city}",
            python_callable=run_scrape_city,
            op_kwargs={"city": city},
        )

        validate_task = PythonOperator(
            task_id=f"validate_{city}",
            python_callable=run_validate_city,
            op_kwargs={"city": city},
        )

        upload_task = PythonOperator(
            task_id=f"upload_{city}",
            python_callable=run_upload_city,
            op_kwargs={"city": city},
        )

        load_task = PythonOperator(
            task_id=f"load_bq_{city}",
            python_callable=run_load_city,
            op_kwargs={"city": city},
        )

        scrape_task >> validate_task >> upload_task >> load_task
        load_bq_tasks.append(load_task)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt && dbt build",
    )

    # after building each city's scrape >> validate >> upload >> load_bq chain:
    load_bq_tasks >> dbt_build  # load_bq_tasks = list of each city's final task
