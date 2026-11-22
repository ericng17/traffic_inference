import os
import signal
import subprocess
import threading
from pathlib import Path

from psycopg.conninfo import conninfo_to_dict

from traffic_pipeline.core.common import ConfigurationError, canonical, sha256_file, utcnow
from traffic_pipeline.data.storage import join_uri, put_file, write_json


def pg_environment(dsn):
    options = conninfo_to_dict(dsn)
    names = {
        "host": "PGHOST",
        "port": "PGPORT",
        "dbname": "PGDATABASE",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
        "options": "PGOPTIONS",
    }
    return {**os.environ, **{names[k]: v for k, v in options.items() if k in names}}


def backup(state):
    destination = Path(os.environ.get("BACKUP_DIR", "var/backups"))
    destination.mkdir(parents=True, exist_ok=True)
    label = utcnow().strftime("%Y%m%dT%H%M%S%fZ")
    dump = destination / (label + ".dump")
    partial = dump.with_suffix(".partial")
    with state.tx() as db:
        db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snapshot = db.execute("SELECT pg_export_snapshot() AS id").fetchone()["id"]
        try:
            subprocess.run(
                [
                    os.getenv("PG_DUMP_BIN", "pg_dump"),
                    "--format=custom",
                    "--no-owner",
                    "--snapshot",
                    snapshot,
                    "--file",
                    str(partial),
                ],
                env=pg_environment(state.dsn),
                check=True,
                capture_output=True,
                timeout=600,
            )
        except (subprocess.SubprocessError, OSError):
            partial.unlink(missing_ok=True)
            raise ConfigurationError("PG_DUMP_FAILED: check pg_dump version/connectivity") from None
        metadata = {
            table: db.execute(f"SELECT * FROM {table}").fetchall()
            for table in ("runs", "tasks", "task_attempts", "committed_outputs")
        }
        metadata["kafka_log_generation"] = os.environ["KAFKA_LOG_GENERATION"]
    os.replace(partial, dump)
    summary = {
        "created_at": utcnow().isoformat(),
        "database_dump": dump.name,
        "sha256": sha256_file(dump),
        "metadata": metadata,
    }
    metadata_path = destination / (label + ".json")
    metadata_path.write_text(canonical(summary))
    if os.getenv("BACKUP_BASE_URI"):
        put_file(dump, join_uri(os.environ["BACKUP_BASE_URI"], dump.name), immutable=True)
        write_json(join_uri(os.environ["BACKUP_BASE_URI"], metadata_path.name), summary, immutable=True)
    return {"database_dump": str(dump), "sha256": summary["sha256"], "metadata": str(metadata_path)}


def watch_backups(state):
    stop = threading.Event()
    previous = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, lambda *_: stop.set())
    try:
        while not stop.is_set():
            backup(state)
            stop.wait(float(os.getenv("BACKUP_INTERVAL_SECONDS", "3600")))
    finally:
        backup(state)  # Controlled shutdown always has a final consistent checkpoint.
        for signum, handler in previous.items():
            signal.signal(signum, handler)
