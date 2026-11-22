import math

from shapely.geometry import Point, Polygon
from shapely.ops import unary_union

from traffic_pipeline.core.common import ConfigurationError

COUNT_RULE = "bottom_center_inside_or_on_boundary_of_polygon_union"


class ROI:
    def __init__(self, data):
        if (
            data.get("schema_version") != 1
            or data.get("review_status") != "approved"
            or data.get("missing_cameras") != []
            or data.get("count_rule") != COUNT_RULE
        ):
            raise ConfigurationError("ROI_NOT_APPROVED_OR_WRONG_SCHEMA")
        self.cameras = {}
        for camera in data["cameras"]:
            key = camera["camera_id"]
            if key in self.cameras or camera.get("review_status") != "approved":
                raise ConfigurationError("ROI_CAMERA_NOT_APPROVED_OR_DUPLICATE")
            size = camera["reference_size"]
            if len(size) != 2 or min(size) <= 0:
                raise ConfigurationError("ROI_REFERENCE_SIZE_INVALID")
            polygons = []
            for region in camera["regions"]:
                points = region["points_normalized"]
                if len(points) < 3 or any(
                    len(p) != 2 or any(not math.isfinite(v) or not 0 <= v <= 1 for v in p) for p in points
                ):
                    raise ConfigurationError("ROI_POLYGON_INVALID")
                polygon = Polygon(points)
                if not polygon.is_valid or polygon.area <= 0:
                    raise ConfigurationError("ROI_POLYGON_INVALID")
                polygons.append(polygon)
            if not polygons:
                raise ConfigurationError("ROI_EMPTY")
            self.cameras[key] = (size[0] / size[1], unary_union(polygons))

    def require(self, camera):
        if camera not in self.cameras:
            raise ConfigurationError(f"ROI_CAMERA_MISSING: {camera}")

    def check_size(self, camera, width, height):
        self.require(camera)
        if abs((width / height) / self.cameras[camera][0] - 1) > 0.01:
            raise ConfigurationError(f"ROI_ASPECT_RATIO_MISMATCH: {camera}")

    def counts(self, camera, size, detections):
        self.check_size(camera, *size)
        width, height = size
        polygon = self.cameras[camera][1]
        counts = [0, 0, 0, 0]
        for x1, y1, x2, y2, cls, confidence in detections:
            if int(cls) != cls or int(cls) not in range(4):
                raise ConfigurationError("DETECTION_CLASS_INVALID")
            if not all(math.isfinite(x) for x in (x1, y1, x2, y2, confidence)):
                raise ConfigurationError("DETECTION_COORDINATES_INVALID")
            if polygon.covers(Point((x1 + x2) / (2 * width), y2 / height)):
                counts[int(cls)] += 1
        return counts
