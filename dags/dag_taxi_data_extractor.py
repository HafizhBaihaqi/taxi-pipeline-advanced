from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/scripts')
from taxi_data_extractor import main

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='extract_concat_upload_to_gcs',
    default_args=default_args,
    description='Extract Taxi and supporting files and upload to GCS: gs:///jdeol003-bucket/capstone3_hafizh',
    start_date=datetime(2025, 5, 21),
    schedule_interval='@daily',
    catchup=False,
    tags=['local_to_gcs']
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=main,
    )

    run_etl