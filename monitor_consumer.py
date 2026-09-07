import json
from collections import deque

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor-readings"
WINDOW_SIZE = 10
ANOMALY_THRESHOLD = 4.0


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="sensor-monitor",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    recent_temperatures = deque(maxlen=WINDOW_SIZE)
    print(f"Monitoring '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        for message in consumer:
            reading = message.value
            temperature = float(reading["temperature"])

            if recent_temperatures:
                rolling_average = sum(recent_temperatures) / len(recent_temperatures)
                difference = abs(temperature - rolling_average)

                status = "ANOMALY" if difference >= ANOMALY_THRESHOLD else "OK"
                print(
                    f"{status} | {reading['sensor_id']} | "
                    f"current={temperature:.2f}{reading['unit']} | "
                    f"average={rolling_average:.2f}{reading['unit']} | "
                    f"difference={difference:.2f}"
                )
            else:
                print(
                    f"START | {reading['sensor_id']} | "
                    f"current={temperature:.2f}{reading['unit']}"
                )

            recent_temperatures.append(temperature)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
