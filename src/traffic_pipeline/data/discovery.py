import fnmatch
import hashlib
import io
import os
import tempfile
import time
from collections import Counter
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

from traffic_pipeline.data.cache import Cache
from traffic_pipeline.core.common import ConfigurationError, digest, sha256_file
from traffic_pipeline.core.manifest import MANIFEST_SCHEMA, parse_camera_time, parse_timestamp, source_id
from traffic_pipeline.core.roi import ROI
from traffic_pipeline.data.storage import join_uri, read_json, safe_uri, write_json, put_file


def verify_artifacts(config, cache):
    import json

    values = {}
    for name in ("weights", "roi", "camera_mapping"):
        uri = getattr(config.artifacts, name + "_uri")
        checksum = getattr(config.artifacts, name + "_sha256")
        with cache.get(uri, checksum) as path:
            values[name] = json.loads(path.read_text()) if name != "weights" else None
    roi = ROI(values["roi"])
    mapping = values["camera_mapping"]
    if not isinstance(mapping, dict) or not mapping:
        raise ConfigurationError("CAMERA_MAPPING_INVALID")
    for camera in mapping.values():
        roi.require(camera)
    return roi, mapping


def pin_source(repo_id, source):
    representation = source.get("representation")
    if representation not in ("raw", "parquet"):
        raise ConfigurationError("choose raw or parquet explicitly for every repo")
    pattern = source.get("include")
    if not pattern:
        raise ConfigurationError("source include glob is required")
    if "local_root" in source:
        root = Path(source["local_root"]).resolve()

        def inventory():
            for directory, dirs, names in os.walk(root):
                dirs.sort()
                for name in sorted(names):
                    path = Path(directory) / name
                    relative = path.relative_to(root).as_posix()
                    if fnmatch.fnmatch(relative, pattern):
                        yield {
                            "path": relative,
                            "size": path.stat().st_size,
                            "checksum": sha256_file(path),
                            "uri": str(path),
                        }

        h = hashlib.sha256()
        for item in inventory():
            h.update(digest({k: v for k, v in item.items() if k != "uri"}).encode())
        revision = h.hexdigest()
        listed = inventory()
    else:
        from huggingface_hub import HfApi

        api = HfApi()
        info = api.dataset_info(repo_id, revision=source.get("revision", "main"))
        revision = info.sha

        def remote_inventory():
            for entry in api.list_repo_tree(repo_id, repo_type="dataset", revision=revision, recursive=True):
                if hasattr(entry, "size") and fnmatch.fnmatch(entry.path, pattern):
                    lfs = getattr(entry, "lfs", None)
                    yield {
                        "path": entry.path,
                        "size": entry.size,
                        "checksum": lfs.sha256 if lfs else None,
                        "uri": f"hf://datasets/{repo_id}@{revision}/{entry.path}",
                    }

        listed = remote_inventory()
    return revision, listed


def image_metadata(data):
    try:
        with Image.open(io.BytesIO(data)) as im:
            return f"{im.format}:{im.width}x{im.height}"
    except Exception:
        return "undecodable"


def discover(config, output=None):
    started = time.monotonic()
    cache = Cache(
        config.execution.cache_dir, config.execution.cache_max_bytes, config.execution.download_attempts
    )
    roi, mapping = verify_artifacts(config, cache)
    sources = read_json(config.input.sources_uri)
    if set(sources) != set(config.input.repo_ids):
        raise ConfigurationError("source descriptors must match configured repo IDs exactly")
    reports, parts, distribution, formats = [], [], Counter(), Counter()
    first, last, total_bytes = None, None, 0
    with tempfile.TemporaryDirectory() as tempdir:
        index = duckdb.connect(str(Path(tempdir) / "discovery.duckdb"))
        index.execute("SET memory_limit='256MB'")
        index.execute(
            "CREATE TABLE observations(id VARCHAR, camera VARCHAR, observed TIMESTAMPTZ, hash VARCHAR)"
        )
        try:
            for repo_id in config.input.repo_ids:
                source = sources[repo_id]
                revision, files = pin_source(repo_id, source)
                source_report = {
                    "repo_id": repo_id,
                    "revision": revision,
                    "parser": {k: v for k, v in source.items() if k != "local_root"},
                    "files": 0,
                }
                reports.append(source_report)
                pending, pending_bytes = [], 0
                writer, part_rows, part_bytes = None, 0, 0
                local = Path(tempdir) / "part.parquet"

                def drain():
                    nonlocal pending, pending_bytes, writer, part_rows, part_bytes
                    if not pending:
                        return
                    table = pa.Table.from_pylist(pending, schema=MANIFEST_SCHEMA)
                    if writer is None:
                        writer = pq.ParquetWriter(local, MANIFEST_SCHEMA, compression="zstd")
                    writer.write_table(table)
                    index.register(
                        "new_rows",
                        pa.table(
                            {
                                "id": [r["source_image_id"] for r in pending],
                                "camera": [r["camera_id"] for r in pending],
                                "observed": [r["observed_at"] for r in pending],
                                "hash": [r["source_checksum"] for r in pending],
                            }
                        ),
                    )
                    index.execute("INSERT INTO observations SELECT * FROM new_rows")
                    index.unregister("new_rows")
                    part_rows += len(pending)
                    part_bytes += pending_bytes
                    pending, pending_bytes = [], 0

                def flush():
                    nonlocal writer, part_rows, part_bytes
                    drain()
                    if writer is None:
                        return
                    writer.close()
                    writer = None
                    checksum = sha256_file(local)
                    uri = join_uri(config.input.manifest_base_uri, checksum, "observations.parquet")
                    put_file(local, uri, immutable=True)
                    parts.append(
                        {
                            "manifest_uri": uri,
                            "manifest_sha256": checksum,
                            "expected_images": part_rows,
                            "source_bytes": part_bytes,
                        }
                    )
                    part_rows, part_bytes = 0, 0

                def observation(entry, metadata, row_index, content, shard_hash=None, image_uri=None):
                    nonlocal first, last, total_bytes, pending_bytes
                    camera, observed, origin = parse_camera_time(
                        entry["path"], metadata, source, mapping, config.input.source_timezone
                    )
                    roi.require(camera)
                    first = min(first, observed) if first else observed
                    last = max(last, observed) if last else observed
                    checksum = hashlib.sha256(content).hexdigest()
                    formats[image_metadata(content)] += 1
                    distribution[camera] += 1
                    total_bytes += len(content)
                    pending_bytes += len(content)
                    pending.append(
                        {
                            "source_image_id": source_id(repo_id, revision, entry["path"], row_index),
                            "repo_id": repo_id,
                            "revision": revision,
                            "source_path": entry["path"],
                            "row_index": row_index,
                            "camera_id": camera,
                            "observed_at": observed,
                            "timestamp_origin": origin,
                            "ingested_at": parse_timestamp(
                                metadata[source["ingested_at_column"]],
                                source_timezone=config.input.source_timezone,
                            )
                            if source.get("ingested_at_column")
                            and metadata.get(source["ingested_at_column"]) is not None
                            else None,
                            "image_uri": image_uri or (entry["uri"] if row_index is None else None),
                            "shard_uri": entry["uri"] if row_index is not None else None,
                            "expected_bytes": len(content),
                            "source_checksum": checksum,
                            "shard_sha256": shard_hash,
                            "image_column": source.get("image_column", "image")
                            if row_index is not None
                            else None,
                            "image_path_column": None,
                        }
                    )
                    if len(pending) >= 1024:
                        drain()

                for entry in files:
                    source_report["files"] += 1
                    safe_uri(entry["uri"])
                    if source["representation"] == "raw":
                        with cache.get(entry["uri"], entry["checksum"], entry["size"]) as path:
                            content = path.read_bytes()
                        if part_rows + len(pending) and (
                            part_rows + len(pending) >= config.execution.task_target_images
                            or part_bytes + pending_bytes + len(content) > config.execution.task_target_bytes
                        ):
                            flush()
                        observation(entry, {}, None, content)
                    else:
                        flush()
                        with cache.get(entry["uri"], entry["checksum"], entry["size"]) as path:
                            shard_hash = sha256_file(path)
                            parquet = pq.ParquetFile(path)
                            image_column = source.get("image_column", "image")
                            image_path_column = source.get("image_path_column")
                            columns = list(
                                dict.fromkeys(
                                    [
                                        image_column,
                                        source.get("camera_column", "camera_id"),
                                        source.get("timestamp_column", "observed_at"),
                                        image_path_column,
                                        source.get("ingested_at_column"),
                                    ]
                                )
                            )
                            columns = [c for c in columns if c in parquet.schema_arrow.names]
                            if image_column not in columns and image_path_column not in columns:
                                raise ConfigurationError("PARQUET_IMAGE_COLUMN_MISSING")
                            row_index = 0
                            # A small byte-bearing batch, never a whole embedded-image table.
                            for batch in parquet.iter_batches(batch_size=16, columns=columns):
                                for row in batch.to_pylist():
                                    image = row.get(image_column)
                                    reference = row.get(image_path_column)
                                    if isinstance(image, dict):
                                        reference = image.get("path")
                                        image = image.get("bytes")
                                    image_uri = None
                                    if image is None:
                                        if not reference or str(reference).startswith(
                                            ("/", "s3:", "http:", "https:")
                                        ):
                                            raise ConfigurationError(
                                                "image references must be relative to the pinned repo"
                                            )
                                        image_uri = (
                                            str(Path(source["local_root"]).resolve() / reference)
                                            if "local_root" in source
                                            else f"hf://datasets/{repo_id}@{revision}/{reference}"
                                        )
                                        safe_uri(image_uri)
                                        with cache.get(image_uri) as image_path:
                                            image = image_path.read_bytes()
                                    observation(entry, row, row_index, image, shard_hash, image_uri)
                                    row_index += 1
                        # Physical Parquet boundary wins over target row count.
                        flush()
                flush()
                if not source_report["files"]:
                    raise ConfigurationError(f"no source files matched in {repo_id}")
            count = index.execute("SELECT count(*) FROM observations").fetchone()[0]
            duplicates = index.execute("SELECT count(*)-count(DISTINCT id) FROM observations").fetchone()[0]
            conflicts = index.execute("""SELECT camera, observed, count(*) AS observations,
                count(DISTINCT hash) AS contents FROM observations GROUP BY camera, observed
                HAVING count(*)>1 ORDER BY observed LIMIT 100""").fetchall()
            conflict_total = index.execute("""SELECT count(*) FROM (SELECT camera, observed FROM observations
                GROUP BY camera, observed HAVING count(*)>1)""").fetchone()[0]
        finally:
            index.close()
    report = {
        "schema_version": 1,
        "sources": reports,
        "parts": parts,
        "expected_images": count,
        "total_image_bytes": total_bytes,
        "cameras": dict(distribution),
        "formats": dict(formats),
        "first_observed_at": first,
        "last_observed_at": last,
        "duplicate_source_ids": duplicates,
        "camera_timestamp_duplicate_groups": conflict_total,
        "conflict_examples": conflicts,
        "elapsed_seconds": time.monotonic() - started,
        "cache": cache.metrics,
    }
    report["manifest_hash"] = digest(
        {"sources": reports, "parts": [{k: v for k, v in p.items() if k != "manifest_uri"} for p in parts]}
    )
    write_json(output or config.input.discovery_uri, report)
    return report
