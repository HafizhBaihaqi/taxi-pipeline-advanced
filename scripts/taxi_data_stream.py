import json
import random
import time
from datetime import datetime, timedelta
from google.cloud import pubsub_v1

# GCP config
project_id = "purwadika"
topic_id = "capstone3_hafizh_taxi"
credentials_path = "/opt/airflow/keys/purwadika-key.json"

publisher = pubsub_v1.PublisherClient.from_service_account_file(credentials_path)
topic_path = publisher.topic_path(project_id, topic_id)

def generate_dummy_taxi_data():
    pickup = datetime.utcnow() - timedelta(minutes=random.randint(1, 60))
    dropoff = pickup + timedelta(minutes=random.randint(1, 30))

    data = {
        "VendorID": random.choice([1, 2]),
        "lpep_pickup_datetime": pickup.isoformat(),
        "lpep_dropoff_datetime": dropoff.isoformat(),
        "store_and_fwd_flag": random.choice([True, False]),
        "RatecodeID": random.randint(1, 6),
        "PULocationID": random.randint(1, 265),
        "DOLocationID": random.randint(1, 265),
        "passenger_count": random.randint(1, 6),
        "trip_distance": round(random.uniform(0.5, 15), 2),
        "fare_amount": round(random.uniform(5, 100), 2),
        "extra": round(random.uniform(0, 5), 2),
        "mta_tax": 0.5,
        "tip_amount": round(random.uniform(0, 25), 2),
        "tolls_amount": round(random.uniform(0, 10), 2),
        "ehail_fee": None,
        "improvement_surcharge": 0.3,
        "total_amount": round(random.uniform(10, 150), 2),
        "payment_type": random.randint(1, 6),
        "trip_type": random.choice([1, 2]),
        "congestion_surcharge": str(round(random.choice([0, 2.5]), 2))
    }
    return data

def publish():
    while True:
        message = json.dumps(generate_dummy_taxi_data()).encode("utf-8")
        future = publisher.publish(topic_path, message)
        print(f"Published message ID: {future.result()}")
        time.sleep(1)

if __name__ == "__main__":
    publish()