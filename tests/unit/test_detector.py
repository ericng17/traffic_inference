import sys
from types import SimpleNamespace

import pytest

from traffic_pipeline.core.common import ConfigurationError
from traffic_pipeline.core.config import Detector
from traffic_pipeline.inference.detector import YoloDetector


def test_real_detector_requires_cuda_before_loading_weights(monkeypatch, tmp_path):
    torch = SimpleNamespace(cuda=SimpleNamespace(is_available=lambda: False))
    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(sys.modules, "ultralytics", SimpleNamespace(YOLO=lambda *a, **k: None))
    with pytest.raises(ConfigurationError, match="CUDA_REQUIRED"):
        YoloDetector(Detector(), tmp_path / "best.pt")


def test_checkpoint_class_mapping_checked_before_inference(monkeypatch, tmp_path):
    weights = tmp_path / "weights.blob"
    weights.write_bytes(b"checkpoint")
    torch = SimpleNamespace(cuda=SimpleNamespace(is_available=lambda: True))
    observed = []

    def yolo(path, task):
        from pathlib import Path

        observed.append(Path(path).suffix)
        assert Path(path).read_bytes() == b"checkpoint"
        return SimpleNamespace(names={0: "wrong"})

    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(sys.modules, "ultralytics", SimpleNamespace(YOLO=yolo))
    with pytest.raises(ConfigurationError, match="CLASS_MAPPING"):
        YoloDetector(Detector(), weights)
    assert observed == [".pt"]
