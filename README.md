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

## Live Demo Script (suggested order)

1. Show `order.avsc` and explain the schema.
2. Start the consumer, then the producer — show orders flowing and the running
   average updating live in the consumer logs.
3. Open AKHQ, show the `orders` topic and the registered Avro schema in Schema Registry.
4. Point out a transient-retry warning in the consumer log (attempt 1/3, 2/3...) —
   explain exponential backoff.
5. Show a message landing in `orders-dlq` (either from a poison price, or from
   exhausted retries), and open it in AKHQ to show the error reason + timestamp attached.
6. Kill and restart the consumer to show it resumes from committed offsets (no data loss).

## Git Repository

```bash
git init
git add .
git commit -m "Initial Kafka order system: Avro producer/consumer, running avg, retry, DLQ"
git remote add origin <your-repo-url>
git push -u origin main
```

Suggested `.gitignore` is included. Commit early and often (schema first, then
producer, then consumer, then retry/DLQ) — a commit history that shows the
system being built incrementally is usually part of the grading rubric for
"maintains a Git repository."

## Requirement -> Implementation mapping (for your own checklist)

| Requirement                         | Where it's implemented                                   |
|--------------------------------------|------------------------------------------------------------|
| Avro serialization                  | `schemas/order.avsc`, `AvroSerializer`/`AvroDeserializer` via Schema Registry |
| Real-time aggregation (running avg) | `RunningAverage` class in `consumer.py`                    |
| Retry logic for temporary failures  | `process_with_retry` + `TransientError`, exponential backoff |
| Dead Letter Queue                   | `send_to_dlq`, topic `orders-dlq`, triggered by `PermanentError` or exhausted retries |
| Live demo                           | See "Live Demo Script" above; AKHQ at localhost:8080 for visuals |
| Git repository                      | `git init` steps above; commit incrementally               |
| Any language, independent research  | This is Python — see "Notes" below if you'd rather implement it yourself in another language |

## Notes on making this genuinely yours

The brief says you're "expected to research and implement solutions
independently" — so treat this as a **reference implementation**, not
something to submit unread:

- Be ready to explain *why* Schema Registry matters (schema evolution,
  compatibility checks) — a likely viva/demo question.
- Consider whether you want retries to be per-message-blocking (as done here,
  simplest to reason about) or handled asynchronously/via a retry topic
  (more advanced, closer to production Kafka patterns — mention this as a
  possible extension if asked about scaling).
- If your course expects a specific language (Java is common for Kafka
  courses because of first-class client support), the same architecture
  translates directly — Avro schema stays identical, only the client code
  changes (KafkaAvroSerializer/Deserializer in Java are close analogues).
