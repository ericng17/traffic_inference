from psycopg.types.json import Jsonb

from traffic_pipeline.data.cache import Cache
from traffic_pipeline.core.common import ConfigurationError, digest
from traffic_pipeline.data.discovery import verify_artifacts
from traffic_pipeline.data.storage import join_uri, read_json, write_json, put_file
from traffic_pipeline.control.state import TOPICS


def plan(config, state):
    config = config.model_copy(deep=True)
    cache = Cache(config.execution.cache_dir, config.execution.cache_max_bytes)
    verify_artifacts(config, cache)
    for name, directory, filename in (
        ("weights", "models", "best.pt"),
        ("roi", "rois", "camera_rois.json"),
        ("camera_mapping", "mappings", "camera_mapping.json"),
    ):
        checksum = getattr(config.artifacts, name + "_sha256")
        target = join_uri(config.output.base_uri, "artifacts", directory, checksum, filename)
        with cache.get(getattr(config.artifacts, name + "_uri"), checksum) as local:
            put_file(local, target, immutable=True)
        setattr(config.artifacts, name + "_uri", target)
    discovery = read_json(config.input.discovery_uri)
    if discovery["duplicate_source_ids"] or discovery["camera_timestamp_duplicate_groups"]:
        raise ConfigurationError("resolve duplicate/conflicting observations before planning")
    if not discovery["expected_images"] or set(config.input.repo_ids) != {
        source["repo_id"] for source in discovery["sources"]
    }:
        raise ConfigurationError("DISCOVERY_SOURCE_MISMATCH")
    expected_hash = digest(
        {
            "sources": discovery["sources"],
            "parts": [{k: v for k, v in p.items() if k != "manifest_uri"} for p in discovery["parts"]],
        }
    )
    if discovery["manifest_hash"] != expected_hash:
        raise ConfigurationError("DISCOVERY_CHECKSUM_MISMATCH")
    spec = config.spec(discovery)
    spec_hash = digest(spec)
    run_id = config.run.id
    write_json(join_uri(config.output.base_uri, "runs", run_id, "spec.json"), spec, immutable=True)
    with state.tx() as db:
        db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (run_id,))
        run = db.execute("SELECT * FROM runs WHERE run_id=%s", (run_id,)).fetchone()
        if run and run["spec_hash"] != spec_hash:
            raise ConfigurationError("RESUME_SPEC_MISMATCH")
        if not run:
            db.execute(
                """INSERT INTO runs(run_id,spec_hash,spec,config,manifest_hash,discovery,expected_images)
                VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                (
                    run_id,
                    spec_hash,
                    Jsonb(spec),
                    Jsonb(config.model_dump()),
                    discovery["manifest_hash"],
                    Jsonb(discovery),
                    discovery["expected_images"],
                ),
            )
        for part in discovery["parts"]:
            task_id = digest(
                [
                    run_id,
                    spec_hash,
                    {
                        "manifest_sha256": part["manifest_sha256"],
                        "start": 0,
                        "count": part["expected_images"],
                    },
                ]
            )
            task = {"run_id": run_id, "task_id": task_id, "spec_hash": spec_hash, **part}
            added = db.execute(
                """INSERT INTO tasks(run_id,task_id,spec_hash,manifest_uri,manifest_sha256,
                expected_images,state) VALUES(%s,%s,%s,%s,%s,%s,'PLANNED') ON CONFLICT DO NOTHING RETURNING task_id""",
                tuple(
                    task[k]
                    for k in (
                        "run_id",
                        "task_id",
                        "spec_hash",
                        "manifest_uri",
                        "manifest_sha256",
                        "expected_images",
                    )
                ),
            ).fetchone()
            if added:
                state.outbox(db, TOPICS["tasks"], state.requested(task))
    return {
        "run_id": run_id,
        "spec_hash": spec_hash,
        "planned_tasks": len(discovery["parts"]),
        "expected_images": discovery["expected_images"],
        "state": "PLANNED",
    }
