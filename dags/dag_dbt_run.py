from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 5, 19),
}

with DAG(
    dag_id='dbt_run',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False,
    tags=['dbt'],
    description='dbt run taxi models',
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command = 'cd /opt/airflow/dbt_modeling && dbt run -m stg_taxi_stream',
    )

    dbt_run