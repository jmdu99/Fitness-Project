import os
import json
import random
import time
import boto3
import pandas as pd
import requests
from kafka import KafkaProducer
import socket

# --------------------------------------------------------------------------------
# Environment variables for AWS region, and S3 paths for CSV + mapping.json
# --------------------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
CSV_S3_PATH = os.getenv("CSV_S3_PATH", None)
MAPPING_S3_PATH = os.getenv("MAPPING_S3_PATH", None)

# --------------------------------------------------------------------------------
# API for Calories Burned
# --------------------------------------------------------------------------------
API_NINJAS_API_KEY = os.getenv("API_NINJAS_API_KEY", "")
if not API_NINJAS_API_KEY:
    raise Exception("Error: API_NINJAS_API_KEY is missing from the environment.")
CALORIES_API_URL = "https://api.api-ninjas.com/v1/caloriesburned"
HEADERS = {"X-Api-Key": API_NINJAS_API_KEY}

# --------------------------------------------------------------------------------
# Local filenames for CSV and mapping
# --------------------------------------------------------------------------------
LOCAL_CSV = "megaGymDataset.csv"
LOCAL_MAPPING = "mapping.json"

def wait_for_kafka(bootstrap_server, timeout=60):
    """Wait until a connection to the Kafka broker is available."""
    start_time = time.time()
    host, port = bootstrap_server.split(":")
    port = int(port)
    while time.time() - start_time < timeout:
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
            print("Kafka broker is available.", flush=True)
            return
        except Exception as e:
            print(f"Waiting for Kafka broker {bootstrap_server}... ({e})", flush=True)
            time.sleep(3)
    raise Exception(f"Kafka broker {bootstrap_server} not available after {timeout} seconds.")

def download_from_s3(s3_path, local_path):
    """
    Download a file from S3 to the local container path using Boto3.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3_path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = s3_path_parts[0]
    key = s3_path_parts[1]
    print(f"Downloading from {s3_path} to {local_path} ...", flush=True)
    s3.download_file(bucket, key, local_path)

def call_calories_api(activity_name, weight, duration):
    """
    Call the Ninja's Calories Burned API with the specified activity, weight, and duration.
    Returns a dict containing 'calories_per_hour' and 'total_calories' if successful,
    else returns None.
    """
    params = {
        "activity": activity_name,
        "weight": weight,
        "duration": duration
    }
    try:
        response = requests.get(CALORIES_API_URL, headers=HEADERS, params=params, timeout=10)
    except Exception as e:
        print(f"Error during API request for '{activity_name}': {e}", flush=True)
        return None

    if response.status_code != 200:
        print(f"API call failed with status {response.status_code} for activity '{activity_name}': {response.text}", flush=True)
        return None

    try:
        data = response.json()
    except Exception as e:
        print(f"Failed to parse JSON for activity '{activity_name}': {e}", flush=True)
        return None

    if not data:
        print(f"API returned empty data for activity '{activity_name}'", flush=True)
        return None

    return data[0]

def main():
    """
    Main producer logic:
      1) Download the CSV and mapping from S3.
      2) Read random exercises from the CSV.
      3) Use the mapping to determine an activity for the API call.
      4) Enforce a rate-limit of one API call every 15 seconds.
      5) If the API call is successful, produce an enriched message to Kafka on the
         'exercises_to_enrich' topic.
    """
    # 1) Download CSV and mapping file
    download_from_s3(CSV_S3_PATH, LOCAL_CSV)
    download_from_s3(MAPPING_S3_PATH, LOCAL_MAPPING)

    # 2) Read CSV and extract unique exercise names
    df = pd.read_csv(LOCAL_CSV)
    exercises = df["Title"].unique().tolist()

    # 3) Load mapping dictionary
    with open(LOCAL_MAPPING, "r") as f:
        mapping_dict = json.load(f)

    bootstrap_server = "kafka:9092"
    # Wait for Kafka broker to be ready
    wait_for_kafka(bootstrap_server)

    # Prepare Kafka producer with retry settings
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        retries=5,
        retry_backoff_ms=1000,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Producer started. Publishing to 'exercises_to_enrich'...", flush=True)

    # Rate-limit: 1 request every 15 seconds
    next_call_time = time.time()

    while True:
        # Pick a random exercise
        exercise = random.choice(exercises)
        # Retrieve the corresponding activity for the API
        activity_for_api = mapping_dict.get(exercise, None)
        # Generate random weight and duration
        weight = random.randint(50, 500)
        duration = random.randint(1, 120)

        # Enforce API rate limit: 1 request every 15 seconds
        now = time.time()
        if now < next_call_time:
            time.sleep(next_call_time - now)
        next_call_time = time.time() + 15

        # Call the Calories API
        api_result = call_calories_api(activity_for_api, weight, duration)

        if api_result is None:
            print(f"Error: API call failed for activity '{activity_for_api}'. Skipping message.", flush=True)
            time.sleep(5)
            continue

        # Build the final message
        message = {
            "event_id": f"{time.time()}-{random.randint(1000,9999)}",
            "title": exercise,
            "weight": weight,
            "duration": duration,
            "calories_per_hour_aprox": api_result["calories_per_hour"],
            "total_calories_aprox": api_result["total_calories"],
            "timestamp": int(time.time())
        }

        # Send the message to Kafka
        producer.send("exercises_to_enrich", value=message)
        producer.flush()

        print(f"Produced: {message}", flush=True)

        # Sleep 5 seconds before next iteration
        time.sleep(5)

if __name__ == "__main__":
    main()
