from google.cloud import bigquery

# Static Configs
gcs_bucket = 'jdeol003-bucket'
gcs_folder = 'capstone3_hafizh'
project_id = 'purwadika'
dataset_id = 'jcdeol3_capstone3_hafizh'
credentials_path = '/opt/airflow/keys/purwadika-key.json'

def load_csv_to_bq():
    client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

    csv_to_table = {
        'taxi_data.csv': 'taxi',
        'payment_type.csv': 'payment_type',
        'taxi_zone_lookup.csv': 'taxi_zone'
    }
    for filename, table_id in csv_to_table.items():
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        uri = f"gs://{gcs_bucket}/{gcs_folder}/{filename}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for job to complete
        print(f"Loaded {uri} into {table_ref}")