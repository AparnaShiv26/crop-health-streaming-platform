import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = PROJECT_ROOT / "data" / "raw" / "enriched_sensor_data.csv"

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "crop-sensor-data"


def create_producer():
    """Create and return a Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def stream_sensor_data():
    """Read sensor data and publish each row to Kafka."""

    print(f"Reading data from: {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)

    producer = create_producer()

    print("Connected to Kafka.")
    print(f"Streaming {len(df)} records to '{TOPIC_NAME}'...\n")

    for index, row in df.iterrows():

        message = row.to_dict()

        producer.send(
            TOPIC_NAME,
            value=message,
        )

        print(
            f"Sent {index + 1}: "
            f"Farm={message['farm_id']} | "
            f"Crop={message['crop']} | "
            f"NDVI={message['ndvi']}"
        )

        # Simulates incoming sensor events
        time.sleep(1)

    producer.flush()
    producer.close()

    print("\nAll sensor records streamed successfully.")


if __name__ == "__main__":
    stream_sensor_data()