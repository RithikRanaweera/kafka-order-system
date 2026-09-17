# Kafka Order Processing System

A Kafka-based system that produces and consumes Avro-serialized order messages,
computes a real-time running average of prices, retries transient failures,
and routes permanently-failed messages to a Dead Letter Queue (DLQ).

## Architecture

```
 producer.py                    consumer.py
 ┌────────────┐   orders topic  ┌─────────────────────────────┐
 │  generates  │ ─────────────► │ validate -> process -> avg   │
 │  Order      │  (Avro, via    │   │ transient error?          │
 │  events     │  Schema        │   ├─ retry (backoff x3)       │
 └────────────┘   Registry)     │   └─ still failing / bad data│
                                 │       -> orders-dlq topic     │
                                 └─────────────────────────────┘
```

- **Schema Registry** enforces the `order.avsc` schema so producer and consumer
  can never drift out of sync — this is what "Avro serialization" is graded on.
- **Running average**: `consumer.py` keeps an in-memory running sum/count,
  both overall and per product, updated on every successfully processed message.
- **Retry logic**: a simulated flaky downstream call (`simulate_downstream_call`)
  raises a `TransientError` ~15% of the time. `process_with_retry` retries it
  up to `MAX_RETRIES` (3) with exponential backoff (1s, 2s, 4s).
- **DLQ**: two paths land a message on `orders-dlq`:
  1. **Permanent failure** — bad data (e.g. negative price) fails validation
     immediately, no point retrying.
  2. **Exhausted retries** — a transient error that never recovered after 3 tries.
  Each DLQ message carries the original order, the error reason, and a timestamp.

## Prerequisites

- Docker + Docker Compose
- Python 3.9+

## Setup

```bash
# 1. Start Kafka, Zookeeper, Schema Registry, and AKHQ (web UI)
docker compose up -d

# 2. Create the topics (auto-create is on, but explicit is safer for a demo)
docker exec -it kafka kafka-topics --create --topic orders \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic orders-dlq \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 3. Install Python deps (ideally in a venv, one per component or shared)
pip install -r producer/requirements.txt
pip install -r consumer/requirements.txt
```

## Running

Open two terminals:

```bash
# Terminal 1
cd consumer
python consumer.py

# Terminal 2
cd producer
python producer.py
```

You'll see the consumer print the running average after every message, and
periodically a message routed to the DLQ (either an invalid-price "poison"
message or one that ran out of retries).

Watch topics live in the browser at **http://localhost:8080** (AKHQ) —
useful for the live demo since you can show messages, schema, and the DLQ
topic filling up in real time instead of just terminal logs.
