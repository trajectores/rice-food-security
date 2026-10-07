
from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


def test_faostat_connection():
    url = "https://fenixservices.fao.org/faostat/api/v1/en/groupsanddomains"

    response = requests.get(url, timeout=30)

    print("FAOSTAT HTTP Status:", response.status_code)

    response.raise_for_status()

    print("Berhasil terhubung ke FAOSTAT!")


with DAG(
    dag_id="faostat_etl",
    description="ETL project for Rice Food Security using FAOSTAT",
    start_date=datetime(2026, 10, 3),
    schedule=None,
    catchup=False,
    tags=["FAOSTAT", "Rice", "ETL"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_faostat_connection",
        python_callable=test_faostat_connection,
    )
