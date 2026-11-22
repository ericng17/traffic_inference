import hashlib
import io
import logging
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image, UnidentifiedImageError

from traffic_pipeline.data.cache import Cache
from traffic_pipeline.core.common import ConfigurationError, utcnow
from traffic_pipeline.core.config import Config
from traffic_pipeline.inference.detector import FakeDetector, YoloDetector
from traffic_pipeline.data.discovery import verify_artifacts
from traffic_pipeline.outputs.finalize import upload_candidate
from traffic_pipeline.core.manifest import COUNT_COLUMNS, MANIFEST_SCHEMA, OUTPUT_SCHEMA, rows

log = logging.getLogger(__name__)


def observation_bytes(manifest, cache):
    """Stream one physical shard once, preserving global Parquet row indices."""
    with ExitStack() as active:
        current_uri, current_column, iterator, current_index = None, None, None, -1
        for row in rows(manifest, MANIFEST_SCHEMA):
            if row["image_uri"]:
                with cache.get(row["image_uri"], row["source_checksum"], row["expected_bytes"]) as path:
                    yield row, path.read_bytes()
            else:
                if row["shard_uri"] != current_uri or row["image_column"] != current_column:
                    active.close()
                    path = active.enter_context(cache.get(row["shard_uri"], row["shard_sha256"]))
                    file = pq.ParquetFile(path)
                    current_uri, current_column = row["shard_uri"], row["image_column"]
                    iterator = (
                        value[current_column]
                        for batch in file.iter_batches(batch_size=16, columns=[current_column])
                        for value in batch.to_pylist()
                    )
                    current_index = -1
                if row["row_index"] <= current_index:
                    raise ConfigurationError("MANIFEST_PARQUET_ORDER_INVALID")
                value = None
                while current_index < row["row_index"]:
                    try:
                        value = next(iterator)
                    except StopIteration:
                        raise ConfigurationError("SOURCE_ROW_MISSING") from None
                    current_index += 1
                if isinstance(value, dict):
                    value = value.get("bytes")
                if not isinstance(value, bytes):
                    raise ConfigurationError("SOURCE_IMAGE_BYTES_MISSING")
                yield row, value


class GPUActor:
    def __init__(self, config, batch_size=None):
        self.config = Config.model_validate(config)
        cfg = self.config
        self.batch_size = batch_size or cfg.detector.batch_size
        self.cache = Cache(
            cfg.execution.cache_dir, cfg.execution.cache_max_bytes, cfg.execution.download_attempts
        )
        self.roi, _ = verify_artifacts(cfg, self.cache)
        with self.cache.get(cfg.artifacts.weights_uri, cfg.artifacts.weights_sha256) as weights:
            self.detector = (
                FakeDetector() if cfg.detector.backend == "fake" else YoloDetector(cfg.detector, weights)
            )

    def ready(self):
        return self.detector.environment

    def process(self, task):
        cfg = self.config
        start = time.monotonic()
        metrics = {
            "batch_size": self.batch_size,
            "decode_seconds": 0.0,
            "model_seconds": 0.0,
            "roi_seconds": 0.0,
            "output_seconds": 0.0,
            "observations": 0,
        }
        before = self.cache.metrics.copy()
        with tempfile.TemporaryDirectory(dir=cfg.execution.cache_dir) as temporary:
            output = Path(temporary) / "counts.parquet"
            with self.cache.get(task["manifest_uri"], task["manifest_sha256"]) as manifest:
                with pq.ParquetWriter(output, OUTPUT_SCHEMA, compression="zstd") as writer:
                    pending, images = [], []

                    def flush():
                        if not pending:
                            return
                        now = time.monotonic()
                        predictions = self.detector.predict(images) if images else []
                        metrics["model_seconds"] += time.monotonic() - now
                        if len(predictions) != len(images):
                            raise ConfigurationError("DETECTOR_BATCH_CARDINALITY_MISMATCH")
                        predictions = iter(predictions)
                        now = time.monotonic()
                        for result, size in pending:
                            if size is None:
                                continue
                            detections = next(predictions)
                            # Real backend performs class-aware NMS; threshold checked again for fake/injected backends.
                            detections = [d for d in detections if d[5] >= cfg.detector.confidence]
                            counts = self.roi.counts(result["camera_id"], size, detections)
                            result.update(dict(zip(COUNT_COLUMNS, counts)))
                            result.update(
                                total_vehicles=sum(counts),
                                status="ok",
                                error_code=None,
                                max_det_reached=len(detections) >= cfg.detector.max_det,
                            )
                            if result["max_det_reached"]:
                                log.warning(
                                    "MAX_DET_REACHED task=%s source=%s",
                                    task["task_id"],
                                    result["source_image_id"],
                                )
                        metrics["roi_seconds"] += time.monotonic() - now
                        now = time.monotonic()
                        writer.write_table(
                            pa.Table.from_pylist([p[0] for p in pending], schema=OUTPUT_SCHEMA)
                        )
                        metrics["output_seconds"] += time.monotonic() - now
                        metrics["observations"] += len(pending)
                        pending.clear()
                        for image in images:
                            image.close()
                        images.clear()

                    for observation, content in observation_bytes(manifest, self.cache):
                        checksum = hashlib.sha256(content).hexdigest()
                        if observation["source_checksum"] and checksum != observation["source_checksum"]:
                            raise ConfigurationError("SOURCE_CHECKSUM_MISMATCH")
                        base = {
                            key: observation[key]
                            for key in (
                                "source_image_id",
                                "camera_id",
                                "observed_at",
                                "repo_id",
                                "revision",
                                "source_path",
                                "row_index",
                            )
                        }
                        base.update(
                            {key: task[key] for key in ("run_id", "spec_hash", "task_id", "attempt_id")}
                        )
                        base.update(
                            image_sha256=checksum,
                            processed_at=utcnow(),
                            weights_sha256=cfg.artifacts.weights_sha256,
                            roi_sha256=cfg.artifacts.roi_sha256,
                            status="error",
                            error_code="IMAGE_DECODE_ERROR",
                            max_det_reached=False,
                            **{k: None for k in COUNT_COLUMNS + ["total_vehicles"]},
                        )
                        now = time.monotonic()
                        try:
                            with Image.open(io.BytesIO(content)) as source:
                                image = source.convert("RGB")  # Deliberately no exif_transpose.
                        except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
                            pending.append((base, None))
                        else:
                            self.roi.check_size(base["camera_id"], *image.size)
                            pending.append((base, image.size))
                            images.append(image)
                        metrics["decode_seconds"] += time.monotonic() - now
                        if len(pending) >= self.batch_size:
                            flush()
                    flush()  # A partial final batch is still materialized.
                now = time.monotonic()
                candidate = upload_candidate(output, manifest, task, cfg, self.cache)
                metrics["output_seconds"] += time.monotonic() - now
        metrics["wall_seconds"] = time.monotonic() - start
        metrics["cache"] = {k: self.cache.metrics[k] - v for k, v in before.items()}
        return {"candidate": candidate, "metrics": metrics, "environment": self.detector.environment}
