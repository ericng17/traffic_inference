import tempfile
import time
import uuid
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from traffic_pipeline.data.cache import Cache
from traffic_pipeline.core.common import ConfigurationError, digest
from traffic_pipeline.inference.coordinator import work
from traffic_pipeline.control.kafka_ingestor import ingest
from traffic_pipeline.core.manifest import MANIFEST_SCHEMA
from traffic_pipeline.control.outbox import create_topics, publish
from traffic_pipeline.data.planner import plan
from traffic_pipeline.data.storage import join_uri, read_json, write_json
from traffic_pipeline.outputs.verify import verify


def benchmark(config, sample_manifest, state, executor="ray"):
    token = uuid.uuid4().hex[:10]
    cache_dir = str(Path(config.execution.cache_dir) / ("benchmark-" + token))
    cache = Cache(cache_dir, config.execution.cache_max_bytes)
    with cache.get(sample_manifest) as path:
        file = pq.ParquetFile(path)
        if not file.schema_arrow.equals(MANIFEST_SCHEMA) or file.metadata.num_rows > 50000:
            raise ConfigurationError("benchmark needs a valid sample manifest of at most 50000 observations")
        table = file.read()
        from traffic_pipeline.core.common import sha256_file

        manifest_sha256 = sha256_file(path)
    with duckdb.connect() as db:
        db.register("sample", table)
        if db.execute(
            "SELECT 1 FROM sample GROUP BY camera_id,observed_at HAVING count(*)>1 LIMIT 1"
        ).fetchone():
            raise ConfigurationError("BENCHMARK_SAMPLE_CONFLICT")
    repos = set(table["repo_id"].to_pylist())
    if repos != set(config.input.repo_ids):
        raise ConfigurationError("benchmark sample must represent every configured repo")
    source_descriptors = read_json(config.input.sources_uri)
    sources = [
        {"repo_id": repo, "revision": revision, "parser": source_descriptors[repo]}
        for repo, revision in sorted(set(zip(table["repo_id"].to_pylist(), table["revision"].to_pylist())))
    ]
    part = {
        "manifest_uri": sample_manifest,
        "manifest_sha256": manifest_sha256,
        "expected_images": len(table),
        "source_bytes": sum(v or 0 for v in table["expected_bytes"].to_pylist()),
    }
    discovery = {
        "parts": [part],
        "sources": sources,
        "expected_images": len(table),
        "duplicate_source_ids": 0,
        "camera_timestamp_duplicate_groups": 0,
    }
    discovery["manifest_hash"] = digest(
        {"sources": sources, "parts": [{k: v for k, v in part.items() if k != "manifest_uri"}]}
    )
    results = []
    create_topics()
    with tempfile.TemporaryDirectory() as directory:
        report_path = str(Path(directory) / "discovery.json")
        write_json(report_path, discovery)
        for mode in ("cold", "warm"):
            cfg = config.model_copy(deep=True)
            cfg.run.id = f"benchmark-{token}-{mode}"
            cfg.input.discovery_uri = report_path
            cfg.execution.cache_dir = cache_dir
            started = time.monotonic()
            plan(cfg, state)
            publish(state, cfg.run.id)
            ingest(state, once=True, idle_seconds=5)
            work(state, cfg.run.id, executor=executor)
            verified = verify(state, cfg.run.id)
            elapsed = time.monotonic() - started
            if verified["state"] not in ("COMPLETED", "COMPLETED_WITH_ERRORS"):
                raise ConfigurationError("BENCHMARK_INCOMPLETE")
            speed = verified["ok"] / elapsed
            with state.tx() as db:
                attempts = db.execute(
                    "SELECT metrics,environment FROM task_attempts WHERE run_id=%s", (cfg.run.id,)
                ).fetchall()
            results.append(
                {
                    "mode": mode,
                    "run_id": cfg.run.id,
                    "wall_seconds": elapsed,
                    "ok_committed_per_second": speed,
                    "observations": verified["committed_observations"],
                    "errors": verified["errors"],
                    "eta_seconds_2million": 2000000 / speed if speed else None,
                    "cost_per_million_per_hourly_dollar": 1000000 / (3600 * speed) if speed else None,
                    "attempts": attempts,
                }
            )
    report = {
        "schema_version": 1,
        "profiles": results,
        "notes": "Includes startup, Kafka handoff, durable commit and verification; discovery/repack excluded. "
        "Cold mode uses a fresh application cache; OS and upstream caches are not controlled.",
    }
    uri = join_uri(config.output.base_uri, "benchmarks", token + ".json")
    write_json(uri, report)
    return {"report_uri": uri, **report}
