
from datetime import datetime

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


def extract_transform_load():

    # 1. Ambil data dari World Bank API
    url = (
        "https://api.worldbank.org/v2/country/IDN/"
        "indicator/AG.PRD.CREL.MT"
        "?date=2010:2023&format=json&per_page=100"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    result = response.json()

    if len(result) < 2 or not result[1]:
        raise ValueError("Tidak ada data dari World Bank API")

    observations = result[1]

    # 2. Transformasi data
    rows = []

    for item in observations:
        rows.append((
            item["country"]["id"],
            item["country"]["value"],
            item["indicator"]["id"],
            item["indicator"]["value"],
            int(item["date"]),
            item["value"],
            item.get("unit")
        ))

    # 3. Koneksi ke MySQL
    mysql_hook = MySqlHook(mysql_conn_id="mysql_world_bank")
    connection = mysql_hook.get_conn()

    try:
        cursor = connection.cursor()

        sql = """
            INSERT INTO raw_world_bank_indicators
            (
                country_code,
                country_name,
                indicator_code,
                indicator_name,
                year,
                value,
                unit
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                country_name = VALUES(country_name),
                indicator_name = VALUES(indicator_name),
                value = VALUES(value),
                unit = VALUES(unit)
        """

        cursor.executemany(sql, rows)
        connection.commit()

        print(f"Berhasil menyimpan {len(rows)} observasi ke MySQL.")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


with DAG(
    dag_id="world_bank_etl",
    description="ETL World Bank Agricultural Data",
    start_date=datetime(2026, 10, 5),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["World Bank", "Agriculture", "ETL"],
) as dag:

    etl_task = PythonOperator(
        task_id="extract_transform_load",
        python_callable=extract_transform_load,
    )
