import os
import tempfile
from pathlib import Path

import numpy as np

from traffic_pipeline.core.common import ConfigurationError
from traffic_pipeline.core.config import CLASS_NAMES


class FakeDetector:
    """Deterministic fixture: red images have two detections, black images have none."""

    environment = {"backend": "fake", "gpu": None}

    def predict(self, images):
        results = []
        for image in images:
            w, h = image.size
            results.append(
                [[0.2 * w, 0.1 * h, 0.4 * w, 0.8 * h, 0, 0.9], [0.6 * w, 0.2 * h, 0.8 * w, 0.8 * h, 2, 0.95]]
                if image.getpixel((0, 0))[0] > 100
                else []
            )
        return results


class YoloDetector:
    def __init__(self, config, weights):
        import torch
        import ultralytics
        from ultralytics import YOLO

        if not torch.cuda.is_available():
            raise ConfigurationError("CUDA_REQUIRED; CPU fallback is disabled")
        self.config = config
        # Ultralytics dispatches by suffix; cache objects deliberately have .blob names.
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = Path(directory) / "best.pt"
            checkpoint.symlink_to(Path(weights).resolve())
            self.model = YOLO(str(checkpoint), task="detect")
        names = self.model.names
        actual = [names[i] for i in range(len(names))] if isinstance(names, dict) else names
        if actual != CLASS_NAMES:
            raise ConfigurationError("WEIGHTS_CLASS_MAPPING_MISMATCH")
        self.environment = {
            "backend": "pytorch",
            "torch": torch.__version__,
            "ultralytics": ultralytics.__version__,
            "cuda": torch.version.cuda,
            "gpu": torch.cuda.get_device_name(0),
            "driver": str(torch._C._cuda_getDriverVersion())
            if hasattr(torch._C, "_cuda_getDriverVersion")
            else os.getenv("NVIDIA_DRIVER_VERSION"),
            "container_digest": os.getenv("CONTAINER_DIGEST", "unknown"),
        }
        # The worker sees Ray's CUDA_VISIBLE_DEVICES; device=0 is its assigned GPU.
        self.predict([__import__("PIL.Image", fromlist=["Image"]).new("RGB", (config.imgsz, config.imgsz))])

    def predict(self, images):
        cfg = self.config
        # Explicit RGB -> BGR array avoids file loaders applying EXIF rotation.
        arrays = [np.asarray(image)[:, :, ::-1].copy() for image in images]
        results = self.model.predict(
            arrays,
            device=0,
            imgsz=cfg.imgsz,
            conf=cfg.confidence,
            iou=cfg.nms_iou,
            max_det=cfg.max_det,
            classes=cfg.classes,
            agnostic_nms=cfg.agnostic_nms,
            augment=cfg.augment,
            half=cfg.precision == "fp16",
            batch=len(images),
            verbose=False,
        )
        output = []
        for result in results:
            boxes = result.boxes
            output.append(
                np.column_stack(
                    (boxes.xyxy.cpu().numpy(), boxes.cls.cpu().numpy(), boxes.conf.cpu().numpy())
                ).tolist()
            )
        return output


def is_oom(error):
    try:
        import torch

        return isinstance(error, torch.cuda.OutOfMemoryError)
    except ImportError:
        return False
