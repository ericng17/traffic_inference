import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from importlib.resources import files

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from traffic_pipeline.core.common import ConfigurationError, LeaseLost, digest, utcnow

TOPICS = {name: f"traffic.inference.{name}.v1" for name in ("tasks", "events", "results", "dlq")}


class State:
    def __init__(self, dsn=None):
        self.dsn = dsn or os.environ["DATABASE_URL"]

    @contextmanager
    def tx(self):
        with psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=10) as db:
            yield db

    def migrate(self):
        sql = files("traffic_pipeline.control").joinpath("migrations/001_initial.sql").read_text()
        with self.tx() as db:
            db.execute("SELECT pg_advisory_xact_lock(793109)")
            db.execute(sql, prepare=False)

    @staticmethod
    def outbox(db, topic, payload):
        db.execute(
            """INSERT INTO outbox_events(event_id,run_id,task_id,topic,payload)
                   VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (payload["event_id"], payload.get("run_id"), payload.get("task_id"), topic, Jsonb(payload)),
        )

    @staticmethod
    def requested(task):
        return {
            "schema_version": 1,
            "event_id": digest([task["run_id"], task["task_id"], "requested"]),
            "event_type": "task.requested",
            **{
                k: task[k]
                for k in (
                    "run_id",
                    "task_id",
                    "spec_hash",
                    "manifest_uri",
                    "manifest_sha256",
                    "expected_images",
                )
            },
        }

    def transition(self, db, task, target, **extra):
        updated = db.execute(
            """UPDATE tasks SET state=%s,state_version=state_version+1
            WHERE run_id=%s AND task_id=%s RETURNING *""",
            (target, task["run_id"], task["task_id"]),
        ).fetchone()
        event = {
            "schema_version": 1,
            "event_type": "task.state_changed",
            "run_id": task["run_id"],
            "task_id": task["task_id"],
            "state": target,
            "state_version": updated["state_version"],
            "created_at": utcnow().isoformat(),
            **extra,
        }
        event["event_id"] = digest([task["run_id"], task["task_id"], updated["state_version"]])
        self.outbox(db, TOPICS["events"], event)
        db.execute("UPDATE runs SET state='PENDING' WHERE run_id=%s", (task["run_id"],))
        return updated

    def get_run(self, run_id):
        with self.tx() as db:
            run = db.execute("SELECT * FROM runs WHERE run_id=%s", (run_id,)).fetchone()
        if not run:
            raise ConfigurationError("RUN_NOT_FOUND")
        return run

    def ingest(self, raw, topic, partition, offset, generation):
        checksum = hashlib.sha256(raw or b"").hexdigest()
        with self.tx() as db:
            # Serializes identical deliveries, including two consumers during a rebalance.
            db.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"{generation}:{topic}:{partition}:{offset}",),
            )
            if db.execute(
                """SELECT 1 FROM inbox_messages WHERE kafka_log_generation=%s AND topic=%s
                          AND partition_id=%s AND offset_id=%s""",
                (generation, topic, partition, offset),
            ).fetchone():
                return "duplicate_offset"
            error, event_id = None, None
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict) or len(raw) > 65536:
                    raise ValueError()
                if payload.get("schema_version") != 1 or payload.get("event_type") != "task.requested":
                    raise ValueError()
                if any(
                    not isinstance(payload.get(k), str)
                    for k in ("event_id", "run_id", "task_id", "spec_hash", "manifest_uri", "manifest_sha256")
                ):
                    raise ValueError()
                if type(payload.get("expected_images")) is not int:
                    raise ValueError()
                task = db.execute(
                    "SELECT * FROM tasks WHERE run_id=%s AND task_id=%s FOR UPDATE",
                    (payload.get("run_id"), payload.get("task_id")),
                ).fetchone()
                if not task:
                    raise ValueError()
                expected = self.requested(task)
                if any(payload.get(k) != v for k, v in expected.items()):
                    raise ValueError()
                event_id = expected["event_id"]
                exists = db.execute("SELECT 1 FROM inbox_messages WHERE event_id=%s", (event_id,)).fetchone()
                if exists:
                    outcome, event_id = "duplicate_event", None
                else:
                    if task["state"] == "PLANNED":
                        self.transition(db, task, "QUEUED")
                    outcome = "accepted"
            except (ValueError, TypeError, UnicodeError):
                error, outcome = "INVALID_TASK_MESSAGE", "rejected"
            db.execute(
                """INSERT INTO inbox_messages(kafka_log_generation,topic,partition_id,offset_id,
                event_id,digest,outcome) VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                (generation, topic, partition, offset, event_id, checksum, outcome),
            )
            if error:
                payload = {
                    "schema_version": 1,
                    "event_type": "message.rejected",
                    "error_code": error,
                    "event_id": digest([generation, topic, partition, offset, checksum]),
                    "source": {
                        "generation": generation,
                        "topic": topic,
                        "partition": partition,
                        "offset": offset,
                    },
                    "digest": checksum,
                }
                self.outbox(db, TOPICS["dlq"], payload)
            return outcome

    def _fail_locked(self, db, task, policy, error_code, permanent=False):
        terminal = permanent or task["attempt_count"] >= policy.max_attempts
        target = "FAILED" if terminal else "RETRY_WAIT"
        delays = policy.retry_backoff_seconds
        delay = delays[min(task["attempt_count"] - 1, len(delays) - 1)] if delays else 0
        db.execute(
            """UPDATE task_attempts SET state=%s,error_code=%s,finished_at=clock_timestamp()
            WHERE run_id=%s AND task_id=%s AND attempt_id=%s""",
            (target, error_code, task["run_id"], task["task_id"], task["attempt_id"]),
        )
        db.execute(
            """UPDATE tasks SET available_at=clock_timestamp()+(%s*interval '1 second'),
            lease_token=NULL,lease_expires_at=NULL WHERE run_id=%s AND task_id=%s""",
            (delay, task["run_id"], task["task_id"]),
        )
        updated = self.transition(db, task, target, error_code=error_code)
        if terminal:
            self.outbox(
                db,
                TOPICS["dlq"],
                {
                    "schema_version": 1,
                    "event_type": "task.failed",
                    "event_id": digest([task["task_id"], updated["state_version"], "dlq"]),
                    "run_id": task["run_id"],
                    "task_id": task["task_id"],
                    "error_code": error_code,
                },
            )

    def reap(self, run_id, policy):
        with self.tx() as db:
            expired = db.execute(
                """SELECT * FROM tasks WHERE run_id=%s AND state='RUNNING'
                AND (lease_expires_at<=clock_timestamp() OR deadline_at<=clock_timestamp())
                FOR UPDATE SKIP LOCKED""",
                (run_id,),
            ).fetchall()
            for task in expired:
                self._fail_locked(db, task, policy, "LEASE_EXPIRED")
        return len(expired)

    def claim(self, run_id, worker, policy):
        self.reap(run_id, policy)
        with self.tx() as db:
            task = db.execute(
                """SELECT * FROM tasks WHERE run_id=%s AND state IN ('QUEUED','RETRY_WAIT')
                AND available_at<=clock_timestamp() ORDER BY available_at,task_id
                FOR UPDATE SKIP LOCKED LIMIT 1""",
                (run_id,),
            ).fetchone()
            if not task:
                return None
            attempt, token = uuid.uuid4().hex, uuid.uuid4().hex
            db.execute(
                """UPDATE tasks SET attempt_count=attempt_count+1,attempt_id=%s,lease_token=%s,
                lease_owner=%s,lease_expires_at=clock_timestamp()+(%s*interval '1 second'),
                deadline_at=clock_timestamp()+(%s*interval '1 second') WHERE run_id=%s AND task_id=%s""",
                (
                    attempt,
                    token,
                    worker,
                    policy.lease_ttl_seconds,
                    policy.task_deadline_seconds,
                    run_id,
                    task["task_id"],
                ),
            )
            db.execute(
                """INSERT INTO task_attempts(run_id,task_id,attempt_id,worker,lease_token)
                VALUES(%s,%s,%s,%s,%s)""",
                (run_id, task["task_id"], attempt, worker, token),
            )
            return self.transition(db, task, "RUNNING")

    def heartbeat(self, task, ttl):
        with self.tx() as db:
            result = db.execute(
                """UPDATE tasks SET lease_expires_at=LEAST(deadline_at,
                clock_timestamp()+(%s*interval '1 second')) WHERE run_id=%s AND task_id=%s
                AND state='RUNNING' AND lease_token=%s AND attempt_id=%s
                AND lease_expires_at>clock_timestamp() AND deadline_at>clock_timestamp() RETURNING task_id""",
                (ttl, task["run_id"], task["task_id"], task["lease_token"], task["attempt_id"]),
            ).fetchone()
            return bool(result)

    @staticmethod
    def _locked_lease(db, task):
        current = db.execute(
            """SELECT * FROM tasks WHERE run_id=%s AND task_id=%s AND state='RUNNING'
            AND lease_token=%s AND attempt_id=%s AND lease_expires_at>clock_timestamp()
            AND deadline_at>clock_timestamp() FOR UPDATE""",
            (task["run_id"], task["task_id"], task["lease_token"], task["attempt_id"]),
        ).fetchone()
        if not current:
            raise LeaseLost("LEASE_FENCED")
        return current

    def fail(self, task, policy, code, permanent=False):
        with self.tx() as db:
            current = self._locked_lease(db, task)
            self._fail_locked(db, current, policy, code, permanent)

    def note_attempt(self, task, environment, batch_size):
        with self.tx() as db:
            self._locked_lease(db, task)
            db.execute(
                """UPDATE task_attempts SET environment=%s,metrics=%s
                WHERE run_id=%s AND task_id=%s AND attempt_id=%s""",
                (
                    Jsonb(environment),
                    Jsonb({"batch_size": batch_size}),
                    task["run_id"],
                    task["task_id"],
                    task["attempt_id"],
                ),
            )

    def finalize(self, task, candidate, metrics, environment):
        with self.tx() as db:
            current = self._locked_lease(db, task)
            if candidate["row_count"] != current["expected_images"]:
                raise ConfigurationError("OUTPUT_ROW_COUNT_MISMATCH")
            db.execute(
                """INSERT INTO committed_outputs(run_id,task_id,attempt_id,uri,sha256,row_count,
                success_count,error_count) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                tuple(task[k] for k in ("run_id", "task_id", "attempt_id"))
                + tuple(candidate[k] for k in ("uri", "sha256", "row_count", "success_count", "error_count")),
            )
            db.execute(
                """UPDATE task_attempts SET state='SUCCEEDED',finished_at=clock_timestamp(),
                candidate_output=%s,metrics=%s,environment=%s WHERE run_id=%s AND task_id=%s AND attempt_id=%s""",
                (
                    Jsonb(candidate),
                    Jsonb(metrics),
                    Jsonb(environment),
                    task["run_id"],
                    task["task_id"],
                    task["attempt_id"],
                ),
            )
            updated = self.transition(db, task, "SUCCEEDED")
            result = {
                "schema_version": 1,
                "event_type": "task.result_committed",
                "event_id": digest([task["task_id"], task["attempt_id"], "result"]),
                "run_id": task["run_id"],
                "task_id": task["task_id"],
                "spec_hash": task["spec_hash"],
                "state_version": updated["state_version"],
                **candidate,
            }
            self.outbox(db, TOPICS["results"], result)

    def status(self, run_id):
        with self.tx() as db:
            run = db.execute("SELECT state,expected_images FROM runs WHERE run_id=%s", (run_id,)).fetchone()
            if not run:
                raise ConfigurationError("RUN_NOT_FOUND")
            states = {
                name: 0 for name in ("PLANNED", "QUEUED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED")
            }
            states.update(
                {
                    r["state"]: r["n"]
                    for r in db.execute(
                        "SELECT state,count(*) AS n FROM tasks WHERE run_id=%s GROUP BY state", (run_id,)
                    )
                }
            )
            counts = db.execute(
                """SELECT coalesce(sum(row_count),0)::bigint AS committed_observations,
                coalesce(sum(success_count),0)::bigint AS ok,coalesce(sum(error_count),0)::bigint AS errors
                FROM committed_outputs WHERE run_id=%s""",
                (run_id,),
            ).fetchone()
            return {"run_id": run_id, **run, "tasks": states, **counts}

    def retry(self, run_id):
        with self.tx() as db:
            tasks = db.execute(
                "SELECT * FROM tasks WHERE run_id=%s AND state='FAILED' FOR UPDATE", (run_id,)
            ).fetchall()
            for task in tasks:
                # Manual recovery grants a fresh bounded attempt budget. History is retained.
                db.execute(
                    "UPDATE tasks SET attempt_count=0,available_at=clock_timestamp() "
                    "WHERE run_id=%s AND task_id=%s",
                    (run_id, task["task_id"]),
                )
                self.transition(db, task, "RETRY_WAIT")
            return len(tasks)

    def reconcile(self, run_id):
        with self.tx() as db:
            tasks = db.execute(
                "SELECT * FROM tasks WHERE run_id=%s AND state='PLANNED' FOR UPDATE", (run_id,)
            ).fetchall()
            for task in tasks:
                event = self.requested(task)
                self.outbox(db, TOPICS["tasks"], event)
                db.execute(
                    "UPDATE outbox_events SET acknowledged_at=NULL WHERE event_id=%s", (event["event_id"],)
                )
            return len(tasks)

    def snapshot(self, run_id):
        with self.tx() as db:
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            run = db.execute("SELECT * FROM runs WHERE run_id=%s", (run_id,)).fetchone()
            tasks = db.execute("SELECT * FROM tasks WHERE run_id=%s ORDER BY task_id", (run_id,)).fetchall()
            outputs = db.execute(
                "SELECT * FROM committed_outputs WHERE run_id=%s ORDER BY task_id", (run_id,)
            ).fetchall()
            return run, tasks, outputs

    def invalidate(self, run_id, task_id):
        with self.tx() as db:
            task = db.execute(
                "SELECT * FROM tasks WHERE run_id=%s AND task_id=%s FOR UPDATE", (run_id, task_id)
            ).fetchone()
            if task["state"] == "SUCCEEDED":
                db.execute("DELETE FROM committed_outputs WHERE run_id=%s AND task_id=%s", (run_id, task_id))
                self.transition(db, task, "FAILED", error_code="COMMITTED_OUTPUT_LOST_OR_CORRUPT")
