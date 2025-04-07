import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.extract import extract
from etl.transform import transform
from etl.load import load_data
from etl.profiling_task import profile_transformed_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

# Wrapper for Airflow Extract Task
def airflow_extract():
    extract(data_dir="/opt/airflow/data")

# Wrapper for Airflow Transform Task
def airflow_transform():
    datasets = extract(data_dir="/opt/airflow/data")
    matched = transform(datasets["conversions"], datasets["broker_data"])
    matched.to_csv("/opt/airflow/output/matched_data.csv", index=False)

# Wrapper for Airflow Load Task
def airflow_load():
    load_data()

# Wrapper for Airflow Profiling Task
def airflow_profile():
    profile_transformed_data()

# Define DAG
with DAG(
    dag_id="brokerchooser_etl",
    default_args=default_args,
    description="Run BrokerChooser ETL pipeline daily",
    schedule_interval="@daily",
    catchup=False,
    tags=["brokerchooser", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=airflow_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=airflow_transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=airflow_load,
    )

    profile_task = PythonOperator(
        task_id="profile",
        python_callable=airflow_profile,
    )

    extract_task >> transform_task >> load_task >> profile_task
