import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor-readings"
SENSOR_ID = "temp-01"


def build_reading():
    temperature = round(random.normalvariate(22.0, 1.2), 2)

    # Occasionally generate a larger jump so the monitor consumer
    # has something unusual to detect during testing.
    if random.random() < 0.05:
        temperature += random.choice([-6.0, 6.0])

    return {
        "sensor_id": SENSOR_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temperature, 2),
        "unit": "C",
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Sending simulated readings to '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        while True:
            reading = build_reading()
            producer.send(TOPIC, value=reading)
            producer.flush()
            print(reading)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
