import json
import os
import time

from confluent_kafka import Consumer, KafkaException

from traffic_pipeline.control.state import TOPICS


def kafka_options():
    options = {}
    if os.getenv("KAFKA_CLIENT_CONFIG"):
        with open(os.environ["KAFKA_CLIENT_CONFIG"]) as f:
            options = json.load(f)
    return {**options, "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}


def handle_message(state, consumer, message, generation):
    result = state.ingest(message.value(), message.topic(), message.partition(), message.offset(), generation)
    # Never advance an offset before the inbox transaction has committed.
    consumer.commit(message=message, asynchronous=False)
    return result


def ingest(state, once=False, idle_seconds=3):
    generation = os.environ["KAFKA_LOG_GENERATION"]
    consumer = Consumer(
        {
            **kafka_options(),
            "group.id": "traffic-task-ingestor-v1",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
        }
    )
    count, last = 0, time.monotonic()
    try:
        consumer.subscribe([TOPICS["tasks"]])
        while not once or time.monotonic() - last < idle_seconds:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            handle_message(state, consumer, message, generation)
            count += 1
            last = time.monotonic()
    finally:
        consumer.close()
    return {"ingested_messages": count}
