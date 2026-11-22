"""Restore into an operator-provided empty database; never overwrites the active database."""

import os
import subprocess

from dotenv import load_dotenv
from psycopg import connect

from traffic_pipeline.control.backup import pg_environment

load_dotenv(os.getenv("ENV_FILE", ".env"))
target = os.environ["RESTORE_DATABASE_URL"]
if target == os.environ["DATABASE_URL"]:
    raise SystemExit("RESTORE_DATABASE_URL must identify a separate, empty database")
with connect(target) as db:
    count = db.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'"
    ).fetchone()[0]
    if count:
        raise SystemExit("Restore target must be empty")
subprocess.run(
    [
        "pg_restore",
        "--no-owner",
        "--no-privileges",
        "--exit-on-error",
        "--dbname",
        pg_environment(target)["PGDATABASE"],
        os.environ["RESTORE_DUMP"],
    ],
    env=pg_environment(target),
    check=True,
)
print("Restored. Point DATABASE_URL at the restored database, then verify --repair and reconcile.")
