import json
import os
import re
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, model_validator

from traffic_pipeline.core.common import ConfigurationError, digest

CLASS_NAMES = ["2-wheel vehicles", "bus", "car", "truck"]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Run(Strict):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]+$")
    expected_dataset_count: int = Field(default=7, ge=1)
    inference_budget_hours: float = Field(default=120, gt=0)


class Input(Strict):
    repo_ids: list[str]
    sources_uri: str
    discovery_uri: str
    manifest_base_uri: str
    source_timezone: str = "Asia/Ho_Chi_Minh"


class Artifacts(Strict):
    weights_uri: str
    weights_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    roi_uri: str
    roi_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    camera_mapping_uri: str
    camera_mapping_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class Detector(Strict):
    backend: Literal["fake", "pytorch"] = "pytorch"
    precision: Literal["fp32", "fp16"] = "fp32"
    imgsz: int = Field(default=640, gt=0)
    confidence: float = Field(default=0.35, ge=0, le=1)
    nms_iou: float = Field(default=0.7, ge=0, le=1)
    max_det: int = Field(default=1000, gt=0)
    classes: list[int] = [0, 1, 2, 3]
    class_names: list[str] = CLASS_NAMES
    agnostic_nms: Literal[False] = False
    augment: Literal[False] = False
    batch_size: int = Field(default=16, gt=0)
    allow_cpu_fallback: Literal[False] = False

    @model_validator(mode="after")
    def validate_classes(self):
        if self.classes != [0, 1, 2, 3] or self.class_names != CLASS_NAMES:
            raise ValueError("class mapping must match the four fine-tuned vehicle classes")
        return self


class Execution(Strict):
    gpu_actors: int = Field(default=1, ge=1)
    task_target_images: int = Field(default=4000, ge=1)
    task_target_bytes: int = Field(default=536870912, ge=1)
    cache_dir: str = "var/cache"
    cache_max_bytes: int = Field(default=150000000000, ge=1)
    lease_ttl_seconds: int = Field(default=180, ge=1)
    heartbeat_seconds: float = Field(default=30, gt=0)
    task_deadline_seconds: int = Field(default=1800, ge=1)
    max_attempts: int = Field(default=3, ge=1)
    retry_backoff_seconds: list[int] = [30, 120]
    oom_batch_sizes: list[int] = [16, 8, 4, 1]
    download_attempts: int = Field(default=3, ge=1, le=10)

    @model_validator(mode="after")
    def validate_limits(self):
        if self.heartbeat_seconds >= self.lease_ttl_seconds / 2:
            raise ValueError("heartbeat must be shorter than half the lease TTL")
        if any(x < 0 for x in self.retry_backoff_seconds):
            raise ValueError("negative backoff")
        if not self.oom_batch_sizes or any(x < 1 for x in self.oom_batch_sizes):
            raise ValueError("invalid OOM batch policy")
        return self


class Output(Strict):
    base_uri: str
    aggregate_method: Literal["mean", "median"] = "mean"


def safe_uri(uri):
    parts = urlsplit(uri)
    if parts.query or parts.fragment or parts.username or parts.password:
        raise ConfigurationError("URI must not contain credentials, query or fragment")
    if not uri or uri == "REQUIRED":
        raise ConfigurationError("required URI missing")
    if parts.scheme not in ("", "file", "s3", "hf"):
        raise ConfigurationError(f"unsupported durable URI scheme: {parts.scheme}")
    if ".." in parts.path.split("/"):
        raise ConfigurationError("parent traversal in URI")
    return uri


class Config(Strict):
    run: Run
    input: Input
    artifacts: Artifacts
    detector: Detector = Detector()
    execution: Execution = Execution()
    output: Output

    @model_validator(mode="after")
    def complete(self):
        ids = self.input.repo_ids
        if len(set(ids)) != self.run.expected_dataset_count or len(ids) != len(set(ids)):
            raise ValueError("provide the exact verified dataset count, without duplicates")
        if self.detector.backend != "fake" and self.run.expected_dataset_count != 7:
            raise ValueError("production requires seven verified datasets")
        for obj in (self.artifacts, self.input, self.output):
            for key, value in obj.model_dump().items():
                if key.endswith("uri"):
                    safe_uri(value)
        return self

    def spec(self, discovery):
        # Operational batch size/cache/URI locations are not prediction parameters.
        from importlib.metadata import version
        from traffic_pipeline import __version__

        detector = self.detector.model_dump(exclude={"batch_size"})
        packages = ["numpy", "Pillow", "pyarrow", "shapely"]
        if self.detector.backend == "pytorch":
            packages += ["torch", "ultralytics", "torchvision", "opencv-python"]
        package_root = Path(__file__).resolve().parents[1]
        return {
            "code_version": __version__,
            "code_sha256": digest(
                {
                    p.relative_to(package_root).as_posix(): digest(p.read_text())
                    for p in sorted(package_root.rglob("*.py"))
                }
            ),
            "dependencies": {p: version(p) for p in packages},
            "detector": detector,
            "artifacts": {k: v for k, v in self.artifacts.model_dump().items() if k.endswith("sha256")},
            "manifest_hash": discovery["manifest_hash"],
            "sources": discovery["sources"],
            "source_timezone": self.input.source_timezone,
            "count_rule": "bottom_center_inside_or_on_boundary_of_polygon_union",
            "aggregate_method": self.output.aggregate_method,
        }


def load_config(path=None):
    load_dotenv(os.getenv("ENV_FILE", ".env"), override=False)
    path = path or os.getenv("PIPELINE_CONFIG", "configs/example.yaml")
    raw = yaml.safe_load(Path(path).read_text())

    def expand(value):
        if isinstance(value, dict):
            return {k: expand(v) for k, v in value.items()}
        if isinstance(value, list):
            return [expand(v) for v in value]
        if isinstance(value, str):

            def replace(match):
                key = match.group(1)
                if key not in os.environ:
                    raise ConfigurationError(f"missing environment variable {key}; fill .env")
                return os.environ[key]

            value = re.sub(r"\$\{([A-Z_][A-Z_0-9]*)\}", replace, value)
            if value.startswith("["):
                return json.loads(value)
        return value

    return Config.model_validate(expand(raw))
