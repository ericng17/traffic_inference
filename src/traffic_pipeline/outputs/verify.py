import tempfile
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from traffic_pipeline.data.cache import Cache
from traffic_pipeline.core.common import ConfigurationError, digest
from traffic_pipeline.core.config import Config
from traffic_pipeline.outputs.finalize import validate_output
from traffic_pipeline.data.storage import join_uri, write_json, put_file


@contextmanager
def committed_database(state, run_id):
    run, tasks, outputs = state.snapshot(run_id)
    if not run:
        raise ConfigurationError("RUN_NOT_FOUND")
    config = Config.model_validate(run["config"])
    cache = Cache(config.execution.cache_dir, config.execution.cache_max_bytes)
    with tempfile.TemporaryDirectory() as temporary:
        db = duckdb.connect(str(Path(temporary) / "verify.duckdb"))
        db.execute("SET memory_limit='256MB'")
        try:
            for i, output in enumerate(outputs):
                with cache.get(output["uri"], output["sha256"]) as path:
                    db.read_parquet(str(path)).create_view("part")
                    if i == 0:
                        db.execute("CREATE TABLE counts AS SELECT * FROM part")
                    else:
                        db.execute("INSERT INTO counts SELECT * FROM part")
            if not outputs:
                raise ConfigurationError("NO_COMMITTED_OUTPUTS")
            yield db, run, tasks, outputs, config, cache
        finally:
            db.close()


def verify(state, run_id, repair=False):
    run, tasks, outputs = state.snapshot(run_id)
    if not run:
        raise ConfigurationError("RUN_NOT_FOUND")
    cfg = Config.model_validate(run["config"])
    cache = Cache(cfg.execution.cache_dir, cfg.execution.cache_max_bytes)
    by_task = {o["task_id"]: o for o in outputs}
    bad, total, ok, errors = [], 0, 0, 0
    for task in tasks:
        output = by_task.get(task["task_id"])
        if not output or task["state"] != "SUCCEEDED":
            bad.append(task["task_id"])
            continue
        try:
            with cache.get(task["manifest_uri"], task["manifest_sha256"]) as manifest:
                with cache.get(output["uri"], output["sha256"], refresh=True) as path:
                    metrics = validate_output(path, manifest, {**task, "attempt_id": output["attempt_id"]})
                    if any(metrics[k] != output[k] for k in metrics):
                        raise ConfigurationError("COMMITTED_METADATA_MISMATCH")
                    table = pq.read_table(path, columns=["weights_sha256", "roi_sha256"])
                    for column, expected in (
                        ("weights_sha256", cfg.artifacts.weights_sha256),
                        ("roi_sha256", cfg.artifacts.roi_sha256),
                    ):
                        if pa.compute.any(pa.compute.not_equal(table[column], expected)).as_py():
                            raise ConfigurationError("OUTPUT_ARTIFACT_HASH_MISMATCH")
            total += metrics["row_count"]
            ok += metrics["success_count"]
            errors += metrics["error_count"]
        except (OSError, ValueError):
            bad.append(task["task_id"])
            if repair:
                state.invalidate(run_id, task["task_id"])
    complete = not bad and total == run["expected_images"]
    unique = 0
    if outputs:
        try:
            with committed_database(state, run_id) as (db, *_):
                unique = db.execute("SELECT count(DISTINCT source_image_id) FROM counts").fetchone()[0]
        except (OSError, ValueError):
            complete = False
    complete = complete and unique == total
    status = "COMPLETED_WITH_ERRORS" if complete and errors else "COMPLETED" if complete else "INCOMPLETE"
    # Check versions again under task locks, so a simultaneous repair/retry invalidates this verification.
    with state.tx() as db:
        current = db.execute(
            "SELECT task_id,state_version,state FROM tasks WHERE run_id=%s ORDER BY task_id FOR SHARE",
            (run_id,),
        ).fetchall()
        if repair:
            # Repairs intentionally changed versions and marked the run PENDING.
            status = "INCOMPLETE" if bad else status
        elif [(t["task_id"], t["state_version"]) for t in current] != [
            (t["task_id"], t["state_version"]) for t in tasks
        ]:
            raise ConfigurationError("OUTPUT_SELECTION_CHANGED_DURING_VERIFY")
        db.execute("UPDATE runs SET state=%s WHERE run_id=%s", (status, run_id))
    result = {
        "run_id": run_id,
        "state": status,
        "expected_images": run["expected_images"],
        "committed_observations": total,
        "unique_source_ids": unique,
        "ok": ok,
        "errors": errors,
        "missing_or_invalid_tasks": bad,
    }
    write_json(join_uri(cfg.output.base_uri, "runs", run_id, "reports", "summary.json"), result)
    return result


def export(state, run_id):
    verified = verify(state, run_id)
    run, tasks, outputs = state.snapshot(run_id)
    cfg = Config.model_validate(run["config"])
    final = verified["state"] in ("COMPLETED", "COMPLETED_WITH_ERRORS") and all(
        t["state"] == "SUCCEEDED" for t in tasks
    )
    selection = {
        "schema_version": 1,
        "run_id": run_id,
        "spec_hash": run["spec_hash"],
        "final": final,
        "missing_tasks": [t["task_id"] for t in tasks if t["state"] != "SUCCEEDED"],
        "outputs": [
            {
                k: o[k]
                for k in (
                    "task_id",
                    "attempt_id",
                    "uri",
                    "sha256",
                    "row_count",
                    "success_count",
                    "error_count",
                )
            }
            for o in outputs
        ],
    }
    checksum = digest(selection)
    uri = join_uri(cfg.output.base_uri, "runs", run_id, "exports", checksum + ".json")
    write_json(uri, selection, immutable=True)
    write_json(
        join_uri(cfg.output.base_uri, "runs", run_id, "committed_outputs.json"),
        {"uri": uri, "sha256": checksum, "final": final},
    )
    if outputs:
        with committed_database(state, run_id) as (db, *_):
            with tempfile.TemporaryDirectory() as tempdir:
                errors = Path(tempdir) / "errors.parquet"
                db.sql("SELECT * FROM counts WHERE status='error'").write_parquet(
                    str(errors), compression="zstd"
                )
                put_file(errors, join_uri(cfg.output.base_uri, "runs", run_id, "reports", "errors.parquet"))
    return {"manifest_uri": uri, "manifest_sha256": checksum, "final": final, "verification": verified}
