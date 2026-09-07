import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor-readings"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="sensor-logger",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"Listening to '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        for message in consumer:
            reading = message.value
            print(
                f"{reading['timestamp']} | "
                f"{reading['sensor_id']} | "
                f"{reading['temperature']}{reading['unit']}"
            )
    except KeyboardInterrupt:
        print("\nLogger stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
