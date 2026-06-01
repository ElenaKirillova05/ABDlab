"""
Kafka Producer for Lab 3
Reads CSV files, converts to JSON, sends to Kafka topic
"""

import csv
import json
import time
import os
import glob
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "pet_shop_sales")
DATA_DIR = os.getenv("DATA_DIR", "/data")
DELAY_MS = float(os.getenv("DELAY_MS", "5"))


def create_producer():
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except Exception as e:
            print(f"Attempt {attempt + 1}/10: Kafka not ready ({e}), waiting 5s...")
            time.sleep(5)
    raise RuntimeError("Failed to connect to Kafka after 10 attempts")


def get_csv_files():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "MOCK_DATA*.csv")))
    if not files:
        raise RuntimeError(f"No CSV files found in {DATA_DIR}")
    return files


def send_file(producer, filepath):
    filename = os.path.basename(filepath)
    count = 0
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(TOPIC, value=row)
            count += 1
            if DELAY_MS > 0:
                time.sleep(DELAY_MS / 1000)
    print(f"  Sent {count} messages from {filename}")
    return count


def main():
    producer = create_producer()
    files = get_csv_files()
    total = 0

    print(f"Found {len(files)} CSV files. Sending to topic '{TOPIC}'...")
    for filepath in files:
        total += send_file(producer, filepath)

    producer.flush()
    producer.close()
    print(f"Done! Total messages sent: {total}")


if __name__ == "__main__":
    main()
