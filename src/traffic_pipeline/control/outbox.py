from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from traffic_pipeline.core.common import canonical
from traffic_pipeline.control.kafka_ingestor import kafka_options
from traffic_pipeline.control.state import TOPICS


def create_topics():
    admin = AdminClient(kafka_options())
    existing = admin.list_topics(timeout=15).topics
    topics = [
        NewTopic(
            topic,
            num_partitions=4 if name == "dlq" else 16,
            replication_factor=1,
            config={
                "cleanup.policy": "delete",
                "retention.ms": str((14 if name == "tasks" else 30) * 86400000),
            },
        )
        for name, topic in TOPICS.items()
        if topic not in existing
    ]
    if topics:
        for future in admin.create_topics(topics).values():
            future.result(timeout=30)


def publish(state, run_id=None, producer=None, limit=1000):
    producer = producer or Producer(
        {
            **kafka_options(),
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 15000,
            "request.timeout.ms": 5000,
        }
    )
    count = 0
    for _ in range(limit):
        with state.tx() as db:
            event = db.execute(
                """SELECT * FROM outbox_events WHERE acknowledged_at IS NULL
                AND (%s::text IS NULL OR run_id=%s) ORDER BY created_at,event_id
                FOR UPDATE SKIP LOCKED LIMIT 1""",
                (run_id, run_id),
            ).fetchone()
            if not event:
                break
            acknowledged = []

            def delivery(error, message):
                acknowledged.append(error)

            try:
                producer.produce(
                    event["topic"],
                    key=event["task_id"] or event["event_id"],
                    value=canonical(event["payload"]).encode(),
                    on_delivery=delivery,
                )
                pending = producer.flush(16)
                if pending or not acknowledged or acknowledged[0] is not None:
                    raise IOError("KAFKA_DELIVERY_NOT_ACKNOWLEDGED")
            except Exception:
                db.execute(
                    "UPDATE outbox_events SET retry_count=retry_count+1 WHERE event_id=%s",
                    (event["event_id"],),
                )
                # Commit retry metadata; leave this event pending for a later publisher.
                return {"published": count, "pending_retry": True}
            db.execute(
                "UPDATE outbox_events SET acknowledged_at=clock_timestamp() WHERE event_id=%s",
                (event["event_id"],),
            )
            count += 1
    return {"published": count, "pending_retry": False}
