import re
import tempfile
from pathlib import Path

from traffic_pipeline.core.common import ConfigurationError, sha256_file
from traffic_pipeline.core.manifest import COUNT_COLUMNS
from traffic_pipeline.data.storage import join_uri, put_file, write_json
from traffic_pipeline.outputs.verify import verify, committed_database


def aggregate(state, run_id, window="1min"):
    match = re.fullmatch(r"([1-9][0-9]*)(min|s|h)", window)
    if not match:
        raise ConfigurationError("window must be a positive integer followed by s, min or h")
    seconds = int(match[1]) * {"s": 1, "min": 60, "h": 3600}[match[2]]
    verification = verify(state, run_id)
    if verification["state"] not in ("COMPLETED", "COMPLETED_WITH_ERRORS"):
        raise ConfigurationError("cannot aggregate an incomplete run")
    with committed_database(state, run_id) as (db, run, tasks, outputs, cfg, cache):
        conflict = db.execute(
            "SELECT 1 FROM counts GROUP BY camera_id,observed_at HAVING count(*)>1 LIMIT 1"
        ).fetchone()
        if conflict:
            raise ConfigurationError("CAMERA_TIMESTAMP_CONFLICT: define a policy before aggregation")
        method = cfg.output.aggregate_method
        # Ceil implements (label-window, label], including observations exactly at the label.
        db.execute(
            f"CREATE VIEW labeled AS SELECT *, to_timestamp(ceil(epoch(observed_at)/{seconds})*{seconds}) "
            "AS label FROM counts"
        )
        aggregates = ",".join(
            f"{method}({c}) FILTER(WHERE status='ok') AS {c}" for c in COUNT_COLUMNS + ["total_vehicles"]
        )
        db.execute(f"""CREATE TABLE series AS WITH grouped AS (
            SELECT camera_id,label,count(*) AS observation_count,count(*) FILTER(WHERE status='ok') AS valid_count,
             max(observed_at) FILTER(WHERE status='ok') AS last_valid_at,{aggregates} FROM labeled GROUP BY camera_id,label
            ), bounds AS (SELECT min(label) lo,max(label) hi FROM labeled), grid AS (
             SELECT camera_id,label FROM (SELECT DISTINCT camera_id FROM counts),bounds,
             LATERAL generate_series(lo,hi,INTERVAL '{seconds} seconds') AS t(label))
            SELECT grid.camera_id,grid.label AS observed_at,coalesce(observation_count,0) AS observation_count,
             coalesce(valid_count,0) AS valid_count,coalesce(valid_count,0)=0 AS missing_mask,
             epoch(grid.label - max(last_valid_at) OVER(PARTITION BY grid.camera_id ORDER BY grid.label
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS observation_age_seconds,
             {",".join(COUNT_COLUMNS + ["total_vehicles"])}
            FROM grid LEFT JOIN grouped USING(camera_id,label) ORDER BY grid.camera_id,grid.label""")
        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "timeseries.parquet"
            db.table("series").write_parquet(str(local), compression="zstd")
            uri = join_uri(
                cfg.output.base_uri, "runs", run_id, "timeseries", window, sha256_file(local) + ".parquet"
            )
            put_file(local, uri, immutable=True)
        metadata = {
            "uri": uri,
            "window": window,
            "method": method,
            "semantics": "snapshot vehicle counts in (label-window, label], never traffic flow",
            "rows": db.execute("SELECT count(*) FROM series").fetchone()[0],
            "split_policy": "split chronologically before constructing overlapping STGCN windows",
        }
        write_json(
            join_uri(cfg.output.base_uri, "runs", run_id, "timeseries", window, "metadata.json"), metadata
        )
        return metadata
