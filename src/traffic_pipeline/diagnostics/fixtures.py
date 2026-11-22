import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

from traffic_pipeline.core.common import sha256_file
from traffic_pipeline.core.config import Config
from traffic_pipeline.core.roi import COUNT_RULE


def make_fixture(directory, run_id="smoke", images_per_repo=5):
    """Entirely synthetic: these polygons/weights are never production artifacts."""
    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    weights = root / "fake.pt"
    weights.write_bytes(b"FAKE DETECTOR ONLY; not YOLO weights")
    roi = root / "roi.json"
    roi.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "review_status": "approved",
                "missing_cameras": [],
                "count_rule": COUNT_RULE,
                "cameras": [
                    {
                        "camera_id": "fixture-camera",
                        "review_status": "approved",
                        "reference_size": [80, 40],
                        "regions": [
                            {"region_id": "synthetic", "points_normalized": [[0, 0], [1, 0], [1, 1], [0, 1]]}
                        ],
                    }
                ],
            }
        )
    )
    mapping = root / "mapping.json"
    mapping.write_text(json.dumps({"c1": "fixture-camera"}))
    sources = {}
    for repo_index in range(7):
        repo_id = f"fixture/repo{repo_index}"
        source_root = root / f"source{repo_index}"
        source_root.mkdir(exist_ok=True)
        observations = []
        for i in range(images_per_repo):
            observed = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=repo_index, seconds=13 * i)
            name = "c1/" + observed.strftime("%Y%m%dT%H%M%S") + ".jpg"
            buffer = io.BytesIO()
            Image.new("RGB", (80, 40), "red" if i % 3 else "black").save(buffer, format="JPEG")
            content = b"corrupt jpeg fixture" if repo_index == 3 and i == 2 else buffer.getvalue()
            observations.append(
                {"camera_id": "c1", "observed_at": observed, "image": {"bytes": content, "path": name}}
            )
            if repo_index < 4:
                path = source_root / name
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(content)
        if repo_index >= 4:
            pq.write_table(pa.Table.from_pylist(observations), source_root / "part.parquet", row_group_size=2)
        sources[repo_id] = {
            "local_root": str(source_root),
            "representation": "raw" if repo_index < 4 else "parquet",
            "include": "*.jpg" if repo_index < 4 else "*.parquet",
        }
        if repo_index < 4:
            sources[repo_id].update(
                filename_regex=r"(?P<camera>c1)/(?P<timestamp>\d{8}T\d{6})\.jpg",
                timestamp_format="%Y%m%dT%H%M%S",
            )
    source_config = root / "sources.json"
    source_config.write_text(json.dumps(sources))
    config = Config.model_validate(
        {
            "run": {"id": run_id},
            "input": {
                "repo_ids": list(sources),
                "sources_uri": str(source_config),
                "discovery_uri": str(root / "discovery.json"),
                "manifest_base_uri": str(root / "manifests"),
                "source_timezone": "UTC",
            },
            "artifacts": {
                "weights_uri": str(weights),
                "weights_sha256": sha256_file(weights),
                "roi_uri": str(roi),
                "roi_sha256": sha256_file(roi),
                "camera_mapping_uri": str(mapping),
                "camera_mapping_sha256": sha256_file(mapping),
            },
            "detector": {"backend": "fake", "batch_size": 2},
            "execution": {
                "task_target_images": 3,
                "gpu_actors": 2,
                "cache_dir": str(root / "cache"),
                "cache_max_bytes": 50000000,
                "retry_backoff_seconds": [0, 0],
            },
            "output": {"base_uri": str(root / "output")},
        }
    )
    config_file = root / "config.json"
    config_file.write_text(config.model_dump_json(indent=2))
    return config, config_file
