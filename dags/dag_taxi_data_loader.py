from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')
from taxi_data_loader import load_csv_to_bq

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'dag_gcs_to_bq_loader',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gcs_to_bq'],
) as dag:

    filenames = [
        'taxi_data.csv',
        'payment_type.csv',
        'taxi_zone_lookup.csv'
    ]

    for filename in filenames:
        PythonOperator(
            task_id=f'load_{filename.replace(".csv", "")}_to_bq',
            python_callable=load_csv_to_bq(filename),
            op_args=[filename],
        )