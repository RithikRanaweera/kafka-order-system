"""
Order Producer
--------------
Generates random order events and publishes them to the 'orders' Kafka topic,
serialized with Avro against the schema registered in Schema Registry.

Run:
    python producer.py
"""

import json
import random
import time
import uuid
import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
SCHEMA_PATH = "../schemas/order.avsc"

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def load_schema(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


def order_to_dict(order, ctx):
    """Convert our Python dict into the dict shape the Avro serializer expects."""
    return order


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed for order {msg.key()}: {err}")
    else:
        log.info(f"Delivered to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def make_order() -> dict:
    """Create a random order. Occasionally emits an intentionally 'poison' record
    (negative price) so the consumer's retry/DLQ logic has something real to catch."""
    price = round(random.uniform(5.0, 500.0), 2)

    # ~8% of the time, simulate a bad/poison message to exercise the DLQ path
    if random.random() < 0.08:
        price = -1.0  # invalid price -> will permanently fail validation downstream

    return {
        "orderId": str(uuid.uuid4().int)[:10],
        "product": random.choice(PRODUCTS),
        "price": price,
    }


def main():
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client,
        load_schema(SCHEMA_PATH),
        order_to_dict,
    )

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    }

    producer = SerializingProducer(producer_conf)

    log.info(f"Starting producer -> topic '{TOPIC}'. Ctrl+C to stop.")
    try:
        while True:
            order = make_order()
            producer.produce(
                topic=TOPIC,
                key=order["orderId"],
                value=order,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            log.info(f"Produced order: {order}")
            time.sleep(1)  # one order per second -> easy to narrate live in a demo
    except KeyboardInterrupt:
        log.info("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
