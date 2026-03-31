# Benchmark Agent

`benchmark-agent` is a real producer/consumer worker used for Kafka, RabbitMQ, and Artemis benchmark jobs.

## Features

- CloudEvents v1.0 structured JSON envelope generation
- producer role with fixed-rate publish loop
- consumer role with end-to-end latency extraction from event metadata
- broker adapters:
  - Kafka (`confluent-kafka`)
  - RabbitMQ (`pika`)
  - Artemis AMQP 1.0 (`python-qpid-proton`)
- JSON summary output for ingestion by collector/reporting pipeline

## Local Usage

```bash
python services/benchmark-agent/agent.py \
  --role=producer \
  --broker=kafka \
  --bootstrap-servers=localhost:9092 \
  --destination=benchmark.events \
  --run-id=local-kafka-run \
  --scenario-id=spsc-small \
  --message-size-bytes=1024 \
  --message-rate=5000 \
  --message-count=1000
```

```bash
python services/benchmark-agent/agent.py \
  --role=consumer \
  --broker=kafka \
  --bootstrap-servers=localhost:9092 \
  --destination=benchmark.events \
  --run-id=local-kafka-run \
  --scenario-id=spsc-small \
  --message-limit=1000 \
  --consumer-group=bench-group
```
