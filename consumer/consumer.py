import json
import socket
import time

from kafka import KafkaConsumer
from pymongo import MongoClient

# MongoDB connection
MONGO_URI = "mongodb://mongo:27017/"
client = MongoClient(MONGO_URI)
db = client["fitness_db"]
collection = db["exercises_calories"]


def wait_for_kafka(bootstrap_server, timeout=60):
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
    raise Exception(
        f"Kafka broker {bootstrap_server} not available after {timeout} seconds."
    )


def on_commit_callback(offsets, exception):
    if exception:
        print(f"Async commit error: {exception}", flush=True)
    else:
        print(f"Offsets successfully committed: {offsets}", flush=True)


def main():
    bootstrap_server = "kafka:9092"
    wait_for_kafka(bootstrap_server)

    # Configure Kafka consumer
    consumer = KafkaConsumer(
        "exercises_to_enrich",
        bootstrap_servers=bootstrap_server,
        group_id="fitness_consumer_group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    print("Consumer started, listening on 'exercises_to_enrich'...", flush=True)

    try:
        for msg in consumer:
            data = msg.value
            collection.insert_one(data)
            print(f"Inserted into MongoDB: {data}", flush=True)
            # perform async commit after processing
            consumer.commit_async(callback=on_commit_callback)
    finally:
        # Final synchronous commit before shutdown
        try:
            consumer.commit()  # final synchronous commit
            print("Final synchronous commit completed.", flush=True)
        except Exception as e:
            print(f"Final commit error: {e}", flush=True)

        consumer.close()  # close the Kafka consumer
        print("Consumer closed.", flush=True)


if __name__ == "__main__":
    main()
