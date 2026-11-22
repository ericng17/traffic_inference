from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from conftest import queue_tasks
from traffic_pipeline.core.common import ConfigurationError, LeaseLost, canonical
from traffic_pipeline.inference.gpu_actor import GPUActor
from traffic_pipeline.data.planner import plan
from traffic_pipeline.control.state import TOPICS

pytestmark = pytest.mark.integration


def test_planner_idempotency_and_resume_rejects_changes(planned):
    config, state = planned
    with state.tx() as db:
        before = db.execute(
            "SELECT count(*) n FROM outbox_events WHERE run_id=%s", (config.run.id,)
        ).fetchone()["n"]
    plan(config, state)
    with state.tx() as db:
        after = db.execute(
            "SELECT count(*) n FROM outbox_events WHERE run_id=%s", (config.run.id,)
        ).fetchone()["n"]
    assert before == after == 7
    config.detector.confidence = 0.5
    with pytest.raises(ConfigurationError):
        plan(config, state)


def test_duplicate_message_cannot_reset_running_task(planned):
    config, state = planned
    task = queue_tasks(state, config.run.id, only_first=True)[0]
    raw = canonical(state.requested(task)).encode()
    claim = state.claim(config.run.id, "worker", config.execution)
    assert claim["task_id"] == task["task_id"]
    generation = "new-generation-" + config.run.id
    assert state.ingest(raw, TOPICS["tasks"], 0, 100, generation) == "duplicate_event"
    assert state.status(config.run.id)["tasks"]["RUNNING"] == 1
    changed = state.requested(task)
    changed["expected_images"] += 1
    assert state.ingest(canonical(changed).encode(), TOPICS["tasks"], 0, 101, generation) == "rejected"
    assert state.status(config.run.id)["tasks"]["RUNNING"] == 1


@pytest.mark.parametrize(
    "raw", [b"not json", b"null", b"[]", b'{"schema_version":1,"event_type":"task.requested","run_id":{}}']
)
def test_invalid_messages_have_durable_dlq(state, raw):
    import uuid

    generation = uuid.uuid4().hex
    assert state.ingest(raw, TOPICS["tasks"], 1, 0, generation) == "rejected"
    assert state.ingest(raw, TOPICS["tasks"], 1, 0, generation) == "duplicate_offset"
    with state.tx() as db:
        inbox = db.execute(
            "SELECT * FROM inbox_messages WHERE kafka_log_generation=%s", (generation,)
        ).fetchone()
    assert inbox["outcome"] == "rejected"


def expire(state, task):
    with state.tx() as db:
        db.execute(
            "UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE run_id=%s AND task_id=%s",
            (task["run_id"], task["task_id"]),
        )


def test_upload_before_crash_then_old_worker_fenced(planned):
    config, state = planned
    queue_tasks(state, config.run.id, only_first=True)
    old = state.claim(config.run.id, "old", config.execution)
    actor = GPUActor(config.model_dump())
    orphan = actor.process(old)
    assert Path(orphan["candidate"]["uri"]).exists()
    expire(state, old)
    assert not state.heartbeat(old, 180)
    state.reap(config.run.id, config.execution)
    new = state.claim(config.run.id, "replacement", config.execution)
    assert new["task_id"] == old["task_id"] and new["lease_token"] != old["lease_token"]
    with pytest.raises(LeaseLost):
        state.finalize(old, orphan["candidate"], orphan["metrics"], orphan["environment"])
    result = actor.process(new)
    state.finalize(new, result["candidate"], result["metrics"], result["environment"])
    with pytest.raises(LeaseLost):
        state.finalize(new, result["candidate"], result["metrics"], result["environment"])
    with state.tx() as db:
        selected = db.execute("SELECT * FROM committed_outputs WHERE run_id=%s", (config.run.id,)).fetchall()
        events = db.execute(
            "SELECT * FROM outbox_events WHERE run_id=%s AND topic=%s", (config.run.id, TOPICS["results"])
        ).fetchall()
    assert len(selected) == len(events) == 1
    assert selected[0]["attempt_id"] == new["attempt_id"]


def test_concurrent_claims_do_not_overlap(planned):
    config, state = planned
    queue_tasks(state, config.run.id)
    with ThreadPoolExecutor(max_workers=4) as pool:
        claims = list(
            pool.map(lambda i: state.claim(config.run.id, f"worker-{i}", config.execution), range(10))
        )
    claims = [c for c in claims if c]
    assert len(claims) == len({c["task_id"] for c in claims}) == 7


def test_crash_before_output_and_finite_deadline(planned):
    config, state = planned
    queue_tasks(state, config.run.id, only_first=True)
    config.execution.max_attempts = 2
    for _ in range(2):
        task = state.claim(config.run.id, "crashing", config.execution)
        with state.tx() as db:
            db.execute(
                "UPDATE tasks SET deadline_at=clock_timestamp()-interval '1 second' "
                "WHERE run_id=%s AND task_id=%s",
                (task["run_id"], task["task_id"]),
            )
        assert not state.heartbeat(task, 180)
        state.reap(config.run.id, config.execution)
    assert state.status(config.run.id)["tasks"]["FAILED"] == 1
    assert state.retry(config.run.id) == 1
    assert state.claim(config.run.id, "recovered", config.execution)


def test_finalize_transaction_rolls_back_if_result_outbox_fails(planned, monkeypatch):
    config, state = planned
    queue_tasks(state, config.run.id, only_first=True)
    task = state.claim(config.run.id, "worker", config.execution)
    result = GPUActor(config.model_dump()).process(task)
    real_outbox = state.outbox

    def crash(db, topic, payload):
        if topic == TOPICS["results"]:
            raise RuntimeError("crash before transaction commit")
        real_outbox(db, topic, payload)

    monkeypatch.setattr(state, "outbox", crash)
    with pytest.raises(RuntimeError):
        state.finalize(task, result["candidate"], result["metrics"], result["environment"])
    assert state.status(config.run.id)["committed_observations"] == 0
    assert state.status(config.run.id)["tasks"]["RUNNING"] == 1
    monkeypatch.setattr(state, "outbox", real_outbox)
    state.finalize(task, result["candidate"], result["metrics"], result["environment"])
    assert state.status(config.run.id)["tasks"]["SUCCEEDED"] == 1


def test_reconcile_republishes_only_planned_with_same_event_ids(planned):
    config, state = planned
    queued = queue_tasks(state, config.run.id, only_first=True)[0]
    with state.tx() as db:
        db.execute(
            "UPDATE outbox_events SET acknowledged_at=clock_timestamp() WHERE run_id=%s", (config.run.id,)
        )
        before = db.execute(
            "SELECT event_id FROM outbox_events WHERE run_id=%s AND topic=%s ORDER BY event_id",
            (config.run.id, TOPICS["tasks"]),
        ).fetchall()
    assert state.reconcile(config.run.id) == 6
    with state.tx() as db:
        after = db.execute(
            "SELECT event_id FROM outbox_events WHERE run_id=%s AND topic=%s ORDER BY event_id",
            (config.run.id, TOPICS["tasks"]),
        ).fetchall()
        acknowledged = db.execute(
            "SELECT acknowledged_at FROM outbox_events WHERE event_id=%s",
            (state.requested(queued)["event_id"],),
        ).fetchone()
    assert before == after and acknowledged["acknowledged_at"] is not None
