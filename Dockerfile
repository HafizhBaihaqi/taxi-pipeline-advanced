# Dockerfile

FROM apache/airflow:2.11.0

RUN pip install dbt-core dbt-bigquery google-cloud-pubsub