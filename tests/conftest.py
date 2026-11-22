import os
import uuid

import pytest
from dotenv import load_dotenv

from traffic_pipeline.data.discovery import discover
from traffic_pipeline.diagnostics.fixtures import make_fixture
from traffic_pipeline.data.planner import plan
from traffic_pipeline.control.state import State, TOPICS
from traffic_pipeline.core.common import canonical


@pytest.fixture
def config(tmp_path):
    return make_fixture(tmp_path / "fixture", "test-" + uuid.uuid4().hex[:12], images_per_repo=3)[0]


@pytest.fixture
def state():
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("set RUN_INTEGRATION=1 with real PostgreSQL and Kafka")
    load_dotenv()
    instance = State()
    instance.migrate()
    return instance


@pytest.fixture
def planned(config, state):
    discover(config)
    plan(config, state)
    return config, state


def queue_tasks(state, run_id, only_first=False, offset_start=0):
    with state.tx() as db:
        tasks = db.execute("SELECT * FROM tasks WHERE run_id=%s ORDER BY task_id", (run_id,)).fetchall()
    selected = tasks[:1] if only_first else tasks
    for i, task in enumerate(selected):
        state.ingest(
            canonical(state.requested(task)).encode(), TOPICS["tasks"], 0, offset_start + i, "test-" + run_id
        )
    return selected
