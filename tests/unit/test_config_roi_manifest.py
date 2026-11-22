import copy
import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from traffic_pipeline.core.common import ConfigurationError
from traffic_pipeline.core.config import Config, load_config, safe_uri
from traffic_pipeline.core.manifest import parse_timestamp, parse_camera_time, source_id
from traffic_pipeline.core.roi import ROI, COUNT_RULE


def test_config_rejects_incomplete_sources_and_artifacts(config):
    base = config.model_dump()
    for path, value in [
        (("input", "repo_ids"), []),
        (("artifacts", "weights_sha256"), ""),
        (("detector", "class_names"), ["car"]),
        (("detector", "allow_cpu_fallback"), True),
    ]:
        changed = copy.deepcopy(base)
        changed[path[0]][path[1]] = value
        with pytest.raises((ValidationError, ConfigurationError)):
            Config.model_validate(changed)


def test_env_substitution_is_structural_not_yaml_injection(config, tmp_path, monkeypatch):
    data = config.model_dump()
    data["input"]["repo_ids"] = "${TEST_REPO_IDS}"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data))
    monkeypatch.setenv("TEST_REPO_IDS", json.dumps(config.input.repo_ids))
    assert load_config(path).input.repo_ids == config.input.repo_ids
    monkeypatch.delenv("TEST_REPO_IDS")
    with pytest.raises(ConfigurationError):
        load_config(path)


@pytest.mark.parametrize(
    "uri", ["s3://bucket/a?token=secret", "https://user:pass@host/a", "", "REQUIRED", "/data/../secret"]
)
def test_no_credentials_in_durable_uris(uri):
    with pytest.raises(ConfigurationError):
        safe_uri(uri)


def roi_data():
    return {
        "schema_version": 1,
        "review_status": "approved",
        "missing_cameras": [],
        "count_rule": COUNT_RULE,
        "cameras": [
            {
                "camera_id": "a",
                "review_status": "approved",
                "reference_size": [100, 50],
                "regions": [
                    {"points_normalized": [[0, 0], [0.5, 0], [0.5, 1], [0, 1]]},
                    {"points_normalized": [[0.25, 0], [0.75, 0], [0.75, 1], [0.25, 1]]},
                ],
            }
        ],
    }


def test_polygon_union_boundary_counts_once():
    roi = ROI(roi_data())
    assert roi.counts(
        "a", (100, 50), [[25, 0, 75, 50, 0, 0.9], [70, 0, 80, 50, 1, 0.9], [80, 0, 100, 50, 2, 0.9]]
    ) == [1, 1, 0, 0]
    with pytest.raises(ConfigurationError, match="ASPECT_RATIO"):
        roi.check_size("a", 100, 100)
    with pytest.raises(ConfigurationError, match="MISSING"):
        roi.require("missing")


def test_invalid_and_unapproved_roi():
    for edit in ("approval", "polygon"):
        data = roi_data()
        if edit == "approval":
            data["review_status"] = "draft"
        else:
            data["cameras"][0]["regions"][0]["points_normalized"] = [[0, 0], [1, 1], [1, 0], [0, 1]]
        with pytest.raises(ConfigurationError):
            ROI(data)


def test_timestamp_explicit_timezone_and_no_guessing():
    assert parse_timestamp("2026-11-21T07:00:00") == datetime(2026, 11, 21, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        parse_timestamp("not a timestamp")
    with pytest.raises(ConfigurationError):
        parse_timestamp("2026-11-01T01:30:00", source_timezone="America/New_York")
    with pytest.raises(ConfigurationError):
        parse_camera_time("unknown.jpg", {}, {}, {"a": "a"}, "UTC")
    assert source_id("repo", "revision", "path", 0) != source_id("repo", "revision", "path", 1)


def test_timestamp_parser_matches_observed_hf_paths():
    from pathlib import Path

    sources = json.loads((Path(__file__).parents[2] / "configs/sources.json").read_text())
    for repo, path in [
        ("ericng17/hcm_traffic_snapshots", "56df807bc062921100c143da/20261102_230150.jpg"),
        ("ericng17/hcm_traffic_snapshots7", "snapshots/56df807bc062921100c143da/20261119_205907.jpg"),
    ]:
        camera, observed, origin = parse_camera_time(
            path, {}, sources[repo], {"56df807bc062921100c143da": "approved-camera"}, "Asia/Ho_Chi_Minh"
        )
        assert camera == "approved-camera" and observed.utcoffset().total_seconds() == 0
        assert origin == "filename_regex"
