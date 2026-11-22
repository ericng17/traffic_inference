from pathlib import Path

import pyarrow.parquet as pq
import pytest
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from conftest import queue_tasks
from traffic_pipeline.outputs.aggregate import aggregate
from traffic_pipeline.core.common import canonical
from traffic_pipeline.inference.coordinator import work
from traffic_pipeline.control.kafka_ingestor import handle_message, ingest, kafka_options
from traffic_pipeline.control.outbox import publish, create_topics
from traffic_pipeline.control.state import State, TOPICS
from traffic_pipeline.outputs.verify import export, verify

pytestmark = pytest.mark.integration


class Message:
    def __init__(self, task):
        self.task = task

    def value(self):
        return canonical(State.requested(self.task)).encode()

    def topic(self):
        return TOPICS["tasks"]

    def partition(self):
        return 0

    def offset(self):
        return 1


def test_db_commit_survives_offset_commit_failure(planned):
    config, state = planned
    with state.tx() as db:
        task = db.execute("SELECT * FROM tasks WHERE run_id=%s LIMIT 1", (config.run.id,)).fetchone()

    class Rebalanced:
        def commit(self, **kwargs):
            raise RuntimeError("consumer lost assignment after DB commit")

    with pytest.raises(RuntimeError):
        handle_message(state, Rebalanced(), Message(task), config.run.id)
    assert state.status(config.run.id)["tasks"]["QUEUED"] == 1

    class Committed:
        def commit(self, **kwargs):
            assert kwargs["asynchronous"] is False

    assert handle_message(state, Committed(), Message(task), config.run.id) == "duplicate_offset"


def test_outbox_replays_after_uncertain_delivery(planned):
    config, state = planned
    sent = []

    class ProducerStub:
        def __init__(self, acknowledged):
            self.acknowledged = acknowledged

        def produce(self, topic, key, value, on_delivery):
            sent.append(value)
            if self.acknowledged:
                on_delivery(None, None)

        def flush(self, timeout):
            return 0 if self.acknowledged else 1

    assert publish(state, config.run.id, ProducerStub(False), limit=1)["pending_retry"]
    assert publish(state, config.run.id, ProducerStub(True), limit=1)["published"] == 1
    assert sent[0] == sent[1]
    with state.tx() as db:
        result = db.execute(
            "SELECT count(*) n FROM outbox_events WHERE run_id=%s AND acknowledged_at IS NOT NULL",
            (config.run.id,),
        ).fetchone()
    assert result["n"] == 1


def test_real_kafka_duplicate_delivery_and_full_run(planned):
    config, state = planned
    create_topics()
    assert not publish(state, config.run.id)["pending_retry"]
    ingest(state, once=True, idle_seconds=5)
    assert state.status(config.run.id)["tasks"]["QUEUED"] == 7
    with state.tx() as db:
        task = db.execute("SELECT * FROM tasks WHERE run_id=%s LIMIT 1", (config.run.id,)).fetchone()
    producer = Producer(kafka_options())
    for _ in range(2):
        producer.produce(TOPICS["tasks"], key=task["task_id"], value=canonical(state.requested(task)))
    assert producer.flush(15) == 0
    ingest(state, once=True, idle_seconds=3)
    assert state.status(config.run.id)["tasks"]["QUEUED"] == 7
    work(state, config.run.id, actors=2, executor="local")
    verified = verify(state, config.run.id)
    assert verified["state"] == "COMPLETED_WITH_ERRORS"
    assert verified["committed_observations"] == verified["unique_source_ids"] == 21
    assert verified["errors"] == 1
    selected = export(state, config.run.id)
    assert selected["final"]
    # A new coordinator process must skip committed tasks even with Kafka result events still pending.
    assert work(State(), config.run.id, actors=2, executor="local")["committed_observations"] == 21
    series = aggregate(state, config.run.id, "1min")
    table = pq.read_table(series["uri"]).to_pylist()
    assert any(row["missing_mask"] and row["total_vehicles"] is None for row in table)
    # Midnight observations belong to the exact midnight label, later snapshots cannot affect them.
    first = table[0]
    assert first["total_vehicles"] == 0 and first["valid_count"] == 1 and not first["missing_mask"]
    assert first["observation_age_seconds"] == 0
    second, missing = table[1], table[2]
    assert second["total_vehicles"] == 2 and second["observation_count"] == 2  # mean, never sum=4
    assert missing["missing_mask"] and missing["valid_count"] == 0
    assert missing["observation_age_seconds"] == 94  # last valid observation was at 00:00:26
    with state.tx() as db:
        output = db.execute(
            "SELECT * FROM committed_outputs WHERE run_id=%s LIMIT 1", (config.run.id,)
        ).fetchone()
    Path(output["uri"]).unlink()
    assert verify(state, config.run.id, repair=True)["state"] == "INCOMPLETE"
    assert state.status(config.run.id)["tasks"]["FAILED"] == 1


@pytest.mark.ray
def test_two_real_ray_actors(planned):
    pytest.importorskip("ray")
    config, state = planned
    queue_tasks(state, config.run.id)
    work(state, config.run.id, actors=2, executor="ray")
    assert verify(state, config.run.id)["unique_source_ids"] == 21


def test_real_consumer_reassignment_redelivers_after_db_commit(planned):
    import time
    import uuid

    config, state = planned
    with state.tx() as db:
        task = db.execute("SELECT * FROM tasks WHERE run_id=%s LIMIT 1", (config.run.id,)).fetchone()
    topic = "test.rebalance." + uuid.uuid4().hex
    admin = AdminClient(kafka_options())
    admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(timeout=20)
    producer = Producer(kafka_options())
    producer.produce(topic, value=canonical(state.requested(task)))
    assert producer.flush(15) == 0
    options = {
        **kafka_options(),
        "group.id": "test-" + topic,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    try:
        for index in range(2):
            consumer = Consumer(options)
            try:
                consumer.subscribe([topic])
                limit, message = time.monotonic() + 20, None
                while time.monotonic() < limit and message is None:
                    message = consumer.poll(0.5)
                assert message is not None and message.error() is None
                result = state.ingest(
                    message.value(), topic, message.partition(), message.offset(), "rebalance"
                )
                assert result == ("accepted" if index == 0 else "duplicate_offset")
                if index:
                    consumer.commit(message=message, asynchronous=False)
            finally:
                # First consumer loses its assignment without committing its offset.
                consumer.close()
        assert state.status(config.run.id)["tasks"]["QUEUED"] == 1
    finally:
        admin.delete_topics([topic])[topic].result(timeout=20)


def test_oom_restarts_actor_with_smaller_batch(planned, monkeypatch):
    from traffic_pipeline.inference import coordinator
    from traffic_pipeline.inference.gpu_actor import GPUActor

    config, state = planned
    queue_tasks(state, config.run.id)

    class SimulatedOOM(MemoryError):
        pass

    class OOMActor(GPUActor):
        def process(self, task):
            if self.batch_size > 1:
                raise SimulatedOOM()
            return super().process(task)

    monkeypatch.setattr(coordinator, "GPUActor", OOMActor)
    monkeypatch.setattr(coordinator, "is_oom", lambda e: isinstance(e, SimulatedOOM))
    coordinator.work(state, config.run.id, actors=1, executor="local")
    assert verify(state, config.run.id)["unique_source_ids"] == 21
    with state.tx() as db:
        attempts = db.execute(
            "SELECT error_code,metrics FROM task_attempts WHERE run_id=%s", (config.run.id,)
        ).fetchall()
    assert any(a["error_code"] == "CUDA_OOM" and a["metrics"]["batch_size"] == 2 for a in attempts)
    assert all(a["metrics"]["batch_size"] == 1 for a in attempts if a["error_code"] is None)
