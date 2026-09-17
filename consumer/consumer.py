"""
Order Consumer
--------------
Consumes Avro-serialized order events from the 'orders' topic and:
  1. Maintains a real-time running average of prices (overall + per product)
  2. Retries processing on TRANSIENT failures (simulated flaky downstream call)
     using exponential backoff, up to MAX_RETRIES
  3. Routes messages that fail PERMANENTLY (bad data) or exhaust their retries
     to a Dead Letter Queue topic ('orders-dlq') with the failure reason attached

Run:
    python consumer.py
"""

import json
import random
import time
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CONSUMER] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SOURCE_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-aggregator"

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1  # doubles each retry: 1s, 2s, 4s


class TransientError(Exception):
    """Represents a temporary failure (e.g. downstream service momentarily down)."""


class PermanentError(Exception):
    """Represents unrecoverable bad data -> should go straight to the DLQ."""


class RunningAverage:
    """Tracks a real-time running average of prices, overall and per product."""

    def __init__(self):
        self.total_sum = 0.0
        self.total_count = 0
        self.per_product = {}  # product -> {"sum": float, "count": int}

    def update(self, product: str, price: float):
        self.total_sum += price
        self.total_count += 1

        stats = self.per_product.setdefault(product, {"sum": 0.0, "count": 0})
        stats["sum"] += price
        stats["count"] += 1

    def overall_avg(self) -> float:
        return self.total_sum / self.total_count if self.total_count else 0.0

    def product_avg(self, product: str) -> float:
        stats = self.per_product.get(product)
        return (stats["sum"] / stats["count"]) if stats else 0.0

    def report(self, just_updated_product: str):
        log.info(
            f"[RUNNING AVG] overall={self.overall_avg():.2f} "
            f"({self.total_count} orders) | "
            f"{just_updated_product}={self.product_avg(just_updated_product):.2f}"
        )


def validate_order(order: dict):
    """Business validation. Bad data here is a PERMANENT failure - retrying won't help."""
    if order["price"] is None or order["price"] <= 0:
        raise PermanentError(f"Invalid price {order['price']} for order {order['orderId']}")


def simulate_downstream_call(order: dict):
    """
    Stand-in for a flaky external dependency (DB write, API call, etc).
    ~15% chance of a transient failure, to give the retry logic something to do.
    """
    if random.random() < 0.15:
        raise TransientError(f"Temporary downstream failure processing order {order['orderId']}")


def process_with_retry(order: dict, running_avg: RunningAverage):
    """
    Validates the order, then attempts processing with retry + exponential backoff
    for transient errors. Raises the final exception if all retries are exhausted,
    or immediately if the error is permanent.
    """
    validate_order(order)  # permanent errors bypass retries entirely

    attempt = 0
    while True:
        try:
            simulate_downstream_call(order)
            running_avg.update(order["product"], order["price"])
            running_avg.report(order["product"])
            return
        except TransientError as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise  # exhausted retries -> caller sends to DLQ
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning(
                f"Transient error (attempt {attempt}/{MAX_RETRIES}) on order "
                f"{order['orderId']}: {e}. Retrying in {backoff}s..."
            )
            time.sleep(backoff)


def build_dlq_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def send_to_dlq(dlq_producer: Producer, order: dict, reason: str):
    payload = {
        "order": order,
        "error_reason": reason,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=str(order.get("orderId", "unknown")),
        value=json.dumps(payload).encode("utf-8"),
    )
    dlq_producer.flush()
    log.error(f"Sent order {order.get('orderId')} to DLQ. Reason: {reason}")


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client)
    string_deserializer = StringSerializer("utf_8")

    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([SOURCE_TOPIC])

    dlq_producer = build_dlq_producer()
    running_avg = RunningAverage()

    log.info(f"Starting consumer on topic '{SOURCE_TOPIC}'. Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f"Consumer error: {msg.error()}")
                continue

            order = avro_deserializer(
                msg.value(), SerializationContext(SOURCE_TOPIC, MessageField.VALUE)
            )
            log.info(f"Received order: {order}")

            try:
                process_with_retry(order, running_avg)
            except PermanentError as e:
                send_to_dlq(dlq_producer, order, str(e))
            except TransientError as e:
                send_to_dlq(dlq_producer, order, f"Exhausted retries: {e}")

    except KeyboardInterrupt:
        log.info("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
