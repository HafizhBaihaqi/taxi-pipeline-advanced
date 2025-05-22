from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')
from taxi_data_loader import load_csv_to_bq

with DAG(
    dag_id='dag_taxi_data_loader',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='load_to_bq',
        python_callable=load_csv_to_bq
    )