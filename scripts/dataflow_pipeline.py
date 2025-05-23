import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions

# Pub/Sub subscription and BigQuery table
INPUT_SUBSCRIPTION = "projects/purwadika/subscriptions/capstone3_hafizh_taxi-sub"
OUTPUT_TABLE = "purwadika:jcdeol3_capstone3_hafizh.taxi_streaming"

# Define Beam pipeline options
beam_options_dict = {
    'project': 'purwadika',
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'temp_location': 'gs://purwadhika-dataflow-bucket/temp',
    'job_name': 'capstone3-hafizh-taxi-streaming-job',
    'streaming': True,
    'service_account_email': 'jdeol-03@purwadika.iam.gserviceaccount.com'
}
beam_options = PipelineOptions.from_dictionary(beam_options_dict)

BQ_SCHEMA = (
    "VendorID:INTEGER,"
    "lpep_pickup_datetime:TIMESTAMP,"
    "lpep_dropoff_datetime:TIMESTAMP,"
    "store_and_fwd_flag:BOOLEAN,"
    "RatecodeID:INTEGER,"
    "PULocationID:INTEGER,"
    "DOLocationID:INTEGER,"
    "passenger_count:INTEGER,"
    "trip_distance:FLOAT,"
    "fare_amount:FLOAT,"
    "extra:FLOAT,"
    "mta_tax:FLOAT,"
    "tip_amount:FLOAT,"
    "tolls_amount:FLOAT,"
    "ehail_fee:STRING,"
    "improvement_surcharge:FLOAT,"
    "total_amount:FLOAT,"
    "payment_type:INTEGER,"
    "trip_type:INTEGER,"
    "congestion_surcharge:STRING"
)

if __name__ == '__main__':
    with beam.Pipeline(options=beam_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
            | "Decode bytes" >> beam.Map(lambda x: x.decode('utf-8'))
            | "Parse JSON" >> beam.Map(json.loads)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=BQ_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )