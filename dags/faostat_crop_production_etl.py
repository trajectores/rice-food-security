
from datetime import datetime
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


API_URL = (
    "https://faostatservices.fao.org/api/v1/en/data/QCL"
    "?area=101&element=2510&item=27"
    "&year=2023%2C2022%2C2021%2C2020%2C2019%2C2018%2C"
    "2017%2C2016%2C2015%2C2014%2C2013%2C2012%2C2011%2C2010"
)


def extract_transform_load():

    # 1. Ambil token dari Airflow Variables
    token = Variable.get("FAOSTAT_API_TOKEN")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # 2. Extract: ambil data dari FAOSTAT API
    response = requests.get(
        API_URL,
        headers=headers,
        timeout=60
    )
    response.raise_for_status()

    payload = response.json()
    records = payload.get("data", [])

    if not records:
        raise ValueError("API tidak mengembalikan data.")

    # 3. Transform: ubah JSON menjadi baris MySQL
    rows = []

    for item in records:
        rows.append((
            item.get("Domain Code"),
            item.get("Domain"),
            item.get("Area Code"),
            item.get("Area"),
            item.get("Element Code"),
            item.get("Element"),
            item.get("Item Code"),
            item.get("Item"),
            int(item.get("Year")),
            item.get("Unit"),
            float(item.get("Value")) if item.get("Value") not in (None, "") else None,
            item.get("Flag"),
            item.get("Flag Description"),
            item.get("Note")
        ))

    # 4. Load: simpan ke MySQL
    mysql_hook = MySqlHook(mysql_conn_id="mysql_world_bank")

    sql = """
        INSERT INTO raw_faostat_crop_production (
            domain_code, domain_name,
            area_code, area_name,
            element_code, element_name,
            item_code, item_name,
            year, unit, production_value,
            flag, flag_description, note
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            production_value = VALUES(production_value),
            unit = VALUES(unit),
            flag = VALUES(flag),
            flag_description = VALUES(flag_description),
            note = VALUES(note)
    """

    connection = mysql_hook.get_conn()

    try:
        cursor = connection.cursor()
        cursor.executemany(sql, rows)
        connection.commit()
        print(f"Berhasil menyimpan {len(rows)} data FAOSTAT ke MySQL.")
    finally:
        cursor.close()
        connection.close()


with DAG(
    dag_id="faostat_crop_production_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["faostat", "agriculture", "etl"]
) as dag:

    etl_task = PythonOperator(
        task_id="extract_transform_load",
        python_callable=extract_transform_load
    )

