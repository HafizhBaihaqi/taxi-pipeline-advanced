from google.cloud import bigquery

gcs_bucket = 'jdeol003-bucket'
gcs_folder = 'capstone3_hafizh'
project_id = 'purwadika'
dataset_id = 'jcdeol3_capstone3_hafizh'
credentials_path = '/opt/airflow/keys/purwadika-key.json'

def load_csv_to_bq():
    # Set the client
    client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

    # Store csv name and table name to dict
    csv_to_table = {
        'taxi_data.csv': 'taxi',
        'payment_type.csv': 'payment_type',
        'taxi_zone_lookup.csv': 'taxi_zone'
    }

    # For loop every csv to its designated table
    for filename, table_id in csv_to_table.items():
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        uri = f"gs://{gcs_bucket}/{gcs_folder}/{filename}"

        # Config
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Load
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()
        print(f"Loaded {uri} into {table_ref}")