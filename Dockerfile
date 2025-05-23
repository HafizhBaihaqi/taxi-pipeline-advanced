# Dockerfile

FROM apache/airflow:2.11.0-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    gcc \
    libc6-dev \
    python3-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --upgrade pip setuptools wheel

RUN pip install dbt-core dbt-bigquery google-cloud-pubsub apache-beam[gcp]