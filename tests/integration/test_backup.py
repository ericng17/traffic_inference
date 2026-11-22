import subprocess
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from conftest import queue_tasks
from traffic_pipeline.control.backup import backup, pg_environment
from traffic_pipeline.inference.coordinator import work
from traffic_pipeline.control.state import State
from traffic_pipeline.outputs.verify import verify

pytestmark = pytest.mark.integration


def test_consistent_backup_restore_and_resume(planned, tmp_path, monkeypatch):
    config, state = planned
    queue_tasks(state, config.run.id)
    work(state, config.run.id, actors=2, executor="local")
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.delenv("BACKUP_BASE_URI", raising=False)
    dump = backup(state)
    database = "restore_" + uuid.uuid4().hex[:16]
    target = make_conninfo(state.dsn, dbname=database)
    with psycopg.connect(state.dsn, autocommit=True) as db:
        db.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        subprocess.run(
            [
                "pg_restore",
                "--no-owner",
                "--no-privileges",
                "--exit-on-error",
                "--dbname",
                database,
                dump["database_dump"],
            ],
            env=pg_environment(target),
            check=True,
            capture_output=True,
        )
        restored = State(target)
        assert verify(restored, config.run.id)["unique_source_ids"] == 21
        assert work(restored, config.run.id, actors=2, executor="local")["tasks"]["SUCCEEDED"] == 7
    finally:
        with psycopg.connect(state.dsn, autocommit=True) as db:
            db.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
