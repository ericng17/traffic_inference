import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from traffic_pipeline.core.common import ConfigurationError, digest

MANIFEST_SCHEMA = pa.schema(
    [
        ("source_image_id", pa.string()),
        ("repo_id", pa.string()),
        ("revision", pa.string()),
        ("source_path", pa.string()),
        ("row_index", pa.int64()),
        ("camera_id", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("timestamp_origin", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("image_uri", pa.string()),
        ("shard_uri", pa.string()),
        ("expected_bytes", pa.int64()),
        ("source_checksum", pa.string()),
        ("shard_sha256", pa.string()),
        ("image_column", pa.string()),
        ("image_path_column", pa.string()),
    ]
)

COUNT_COLUMNS = ["count_2wheel", "count_bus", "count_car", "count_truck"]
OUTPUT_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("spec_hash", pa.string()),
        ("task_id", pa.string()),
        ("attempt_id", pa.string()),
        ("source_image_id", pa.string()),
        ("camera_id", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("repo_id", pa.string()),
        ("revision", pa.string()),
        ("source_path", pa.string()),
        ("row_index", pa.int64()),
        ("image_sha256", pa.string()),
        *[(c, pa.int64()) for c in COUNT_COLUMNS + ["total_vehicles"]],
        ("status", pa.string()),
        ("error_code", pa.string()),
        ("processed_at", pa.timestamp("us", tz="UTC")),
        ("weights_sha256", pa.string()),
        ("roi_sha256", pa.string()),
        ("max_det_reached", pa.bool_()),
    ]
)


def source_id(repo, revision, path, row_index=None):
    return digest([repo, revision, path, row_index])


def parse_timestamp(value, fmt=None, source_timezone="Asia/Ho_Chi_Minh"):
    if isinstance(value, datetime):
        dt = value
    elif fmt:
        dt = datetime.strptime(str(value), fmt)
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        zone = ZoneInfo(source_timezone)
        # Reject ambiguous and nonexistent local times instead of guessing a fold.
        first, second = dt.replace(tzinfo=zone, fold=0), dt.replace(tzinfo=zone, fold=1)
        if first.utcoffset() != second.utcoffset():
            raise ConfigurationError("AMBIGUOUS_OR_NONEXISTENT_TIMESTAMP")
        dt = first
    return dt.astimezone(timezone.utc)


def parse_camera_time(path, metadata, source, mapping, tz):
    pattern = source.get("filename_regex")
    match = re.fullmatch(pattern, path) if pattern else None
    values = match.groupdict() if match else {}
    camera = metadata.get(source.get("camera_column", "camera_id"), values.get("camera"))
    timestamp = metadata.get(source.get("timestamp_column", "observed_at"), values.get("timestamp"))
    if camera is None or timestamp is None:
        raise ConfigurationError(f"CAMERA_TIMESTAMP_PARSE_FAILED: {path}")
    if str(camera) not in mapping:
        raise ConfigurationError(f"CAMERA_MAPPING_MISSING: {camera}")
    observed = parse_timestamp(timestamp, source.get("timestamp_format"), tz)
    origin = source.get("timestamp_column", "observed_at") if metadata else "filename_regex"
    return mapping[str(camera)], observed, origin


def rows(path, schema=None, batch_size=256):
    file = pq.ParquetFile(path)
    if schema and not file.schema_arrow.equals(schema):
        raise ConfigurationError("PARQUET_SCHEMA_MISMATCH")
    for batch in file.iter_batches(batch_size=batch_size):
        yield from batch.to_pylist()
