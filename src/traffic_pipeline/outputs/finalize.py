from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from traffic_pipeline.core.common import ConfigurationError, sha256_file
from traffic_pipeline.core.manifest import OUTPUT_SCHEMA
from traffic_pipeline.data.storage import join_uri, put_file


def validate_output(output, manifest, task):
    if not pq.ParquetFile(output).schema_arrow.equals(OUTPUT_SCHEMA):
        raise ConfigurationError("OUTPUT_SCHEMA_MISMATCH")
    with duckdb.connect() as db:
        db.execute("SET memory_limit='256MB'")
        db.read_parquet(str(output)).create_view("output")
        db.read_parquet(str(manifest)).create_view("manifest")
        n, unique = db.execute("SELECT count(*),count(DISTINCT source_image_id) FROM output").fetchone()
        if n != unique or n != task["expected_images"]:
            raise ConfigurationError("OUTPUT_COVERAGE_MISMATCH")
        mismatch = db.execute("""SELECT count(*) FROM (
            (SELECT source_image_id,camera_id,observed_at,repo_id,revision,source_path,row_index FROM output
             EXCEPT ALL SELECT source_image_id,camera_id,observed_at,repo_id,revision,source_path,row_index
             FROM manifest) UNION ALL
            (SELECT source_image_id,camera_id,observed_at,repo_id,revision,source_path,row_index FROM manifest
             EXCEPT ALL SELECT source_image_id,camera_id,observed_at,repo_id,revision,source_path,row_index FROM output)
            )""").fetchone()[0]
        invalid = db.execute(
            """SELECT count(*) FROM output WHERE
            run_id IS DISTINCT FROM ? OR spec_hash IS DISTINCT FROM ? OR task_id IS DISTINCT FROM ?
            OR attempt_id IS DISTINCT FROM ? OR status IS NULL OR status NOT IN ('ok','error')
            OR (status='ok' AND (error_code IS NOT NULL OR count_2wheel IS NULL OR count_bus IS NULL
              OR count_car IS NULL OR count_truck IS NULL OR total_vehicles IS NULL OR count_2wheel<0
              OR count_bus<0 OR count_car<0 OR count_truck<0
              OR total_vehicles<>count_2wheel+count_bus+count_car+count_truck))
            OR (status='error' AND (error_code IS NULL OR count_2wheel IS NOT NULL OR count_bus IS NOT NULL
              OR count_car IS NOT NULL OR count_truck IS NOT NULL OR total_vehicles IS NOT NULL))""",
            [task[k] for k in ("run_id", "spec_hash", "task_id", "attempt_id")],
        ).fetchone()[0]
        if mismatch or invalid:
            raise ConfigurationError("OUTPUT_INVARIANT_VIOLATION")
        ok = db.execute("SELECT count(*) FROM output WHERE status='ok'").fetchone()[0]
        return {"row_count": n, "success_count": ok, "error_count": n - ok}


def upload_candidate(local, manifest, task, config, cache):
    counts = validate_output(local, manifest, task)
    checksum = sha256_file(local)
    uri = join_uri(
        config.output.base_uri,
        "runs",
        task["run_id"],
        "attempts",
        task["task_id"],
        task["attempt_id"],
        "counts.parquet",
    )
    put_file(local, uri, immutable=True)
    # Read back bytes, not an ETag (multipart S3 ETags are not SHA256).
    with cache.get(uri, checksum, Path(local).stat().st_size):
        pass
    return {"uri": uri, "sha256": checksum, **counts}
