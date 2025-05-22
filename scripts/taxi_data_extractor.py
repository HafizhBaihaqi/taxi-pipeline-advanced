import os
import pandas as pd
import re
from google.cloud import storage
    
# CSV Extractor using Pandas
class CSVExtractor:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        # Fetch the base directory of taxi-pipeline path
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(f'Base directory path {base_directory}\n')
        
        # Fetch the CSV directory
        csv_directory = os.path.join(base_directory, self.file_path)
        print(f'CSV folder path: {csv_directory}\n')

        # Fetch the CSV files within the directory
        csv_files = os.listdir(csv_directory)
        print(f'CSV list of files: {csv_files}\n')

        # Reading CSV files
        print(f'Reading CSV files...\n')
        csv_extractor = [pd.read_csv(os.path.join(csv_directory, file)) for file in csv_files] 
        print(f'Concancenate CSV files...\n')
        df_csv = pd.concat(csv_extractor)

        return df_csv

# JSON Extractor using Pandas
class JSONExtractor:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        # Fetch the base directory of taxi-pipeline path
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(f'Base directory path {base_directory}\n')

        # Fetch the JSON directory
        json_directory = os.path.join(base_directory, self.file_path)
        print(f'JSON folder path: {json_directory}\n')

        # Fetch the JSON files within the directory
        json_files = os.listdir(json_directory)
        print(f'JSON list of files: {json_files}\n')

        # Reading JSON files
        print(f'Reading JSON files...\n')
        json_extractor = [pd.read_json(os.path.join(json_directory, file)) for file in json_files] 
        print(f'Concancenate JSON files...\n')
        df_json = pd.concat(json_extractor)

        return df_json
    
# Transformer
class Concater:
    def __init__(self, csv_extractor, json_extractor, concat_path):
        self.csv_extractor = csv_extractor
        self.json_extractor = json_extractor
        self.concat_path = concat_path

    def concancenate(self):
        # Extract CSV and JSON
        df_csv = self.csv_extractor.extract()
        df_json = self.json_extractor.extract()

        # Concancenate data
        print(f'Concancenate CSV and JSON files...\n')
        df_concancenated = pd.concat([df_csv, df_json])

        output_path = os.path.join(self.concat_path, 'taxi_data.csv')
        df_concancenated.to_csv(output_path, index=False)

        return output_path

def upload_to_gcs(local_file_path, gcs_file_path, bucket_name, project_id, credentials_path):
    print(f"Uploading {local_file_path} to gs://{bucket_name}/...\n")
    client = storage.Client.from_service_account_json(credentials_path, project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(local_file_path)
    print("Upload completed.\n")

def main():
    # Path config
    csv_input_path = '/opt/airflow/data/csv'
    json_input_path = '/opt/airflow/data/json'
    output_filename = 'taxi_data.csv'
    local_output_dir = '/opt/airflow/data'
    local_output_path = os.path.join(local_output_dir, output_filename)

    # GCS config
    credentials_path = '/opt/airflow/keys/purwadika-key.json'
    project_id = 'purwadika'
    gcs_bucket = 'jdeol003-bucket' 
    
    # Extract and Concancenate
    csv_extractor = CSVExtractor(csv_input_path)
    json_extractor = JSONExtractor(json_input_path)
    concater = Concater(csv_extractor, json_extractor, local_output_dir)
    
    local_output_path = concater.concancenate()
    print(f'Saved combined CSV to {local_output_path}\n')

    # Upload
    files_to_upload = {
        local_output_path: 'capstone3_hafizh/taxi_data.csv',
        '/opt/airflow/data/payment_type.csv': 'capstone3_hafizh/payment_type.csv',
        '/opt/airflow/data/taxi_zone_lookup.csv': 'capstone3_hafizh/taxi_zone_lookup.csv'
    }

    for local_path, gcs_path in files_to_upload.items():
        upload_to_gcs(local_path, gcs_path, gcs_bucket, project_id, credentials_path)

if __name__ == '__main__':
    main()