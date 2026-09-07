# SensorStream Kafka

SensorStream is a small real-time data project I built to explore how live sensor readings can move through a backend system rather than being processed in one script.

The current version simulates temperature readings in Python and sends them into an Apache Kafka topic. Two separate consumers then use the same stream in different ways: one records the readings and the other keeps a rolling average and flags unusual values.

I chose this project because it connects two parts of my degree that I want to keep developing: software systems and electronics. The sensor is simulated for now, but the structure is intended to make it straightforward to replace the simulator with data coming from a microcontroller later.

## How it works

```text
simulated sensor
      |
      v
Python producer
      |
      v
Kafka topic: sensor-readings
      |
      +--------------------+
      |                    |
      v                    v
logger consumer      monitor consumer
                       |
                       v
              rolling average + alerts
```

Each reading contains a sensor ID, timestamp, temperature and unit. The producer sends JSON messages to Kafka every second.

The logger consumer prints the stream as it arrives. The monitor consumer keeps a moving window of recent temperatures and compares each new reading with the rolling average. If the difference passes the configured threshold, it prints an anomaly warning.

## Run locally

You need Python 3 and a local Kafka broker running on `localhost:9092`.

Install the Python dependency:

```bash
pip install -r requirements.txt
```

Start the producer in one terminal:

```bash
python producer.py
```

Then run either or both consumers in separate terminals:

```bash
python logger_consumer.py
python monitor_consumer.py
```

Kafka will create the `sensor-readings` topic automatically when the producer first publishes if automatic topic creation is enabled on the broker.

## Current version

This first version uses simulated temperature data so I can focus on the streaming architecture and consumer behaviour before adding hardware.

A later version will replace the simulator with a physical sensor connected to a microcontroller. The planned path is sensor -> microcontroller -> serial/USB -> Python producer -> Kafka, without changing the consumer side of the project.

## Built with

Python and Apache Kafka, using the `kafka-python-ng` client.
