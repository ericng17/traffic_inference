from pathlib import Path

import pytest
from PIL import Image

from traffic_pipeline.data.cache import Cache
from traffic_pipeline.core.common import CacheFull, ConfigurationError
from traffic_pipeline.inference.coordinator import next_batch
from traffic_pipeline.data.discovery import discover
from traffic_pipeline.inference.gpu_actor import GPUActor
from traffic_pipeline.core.manifest import MANIFEST_SCHEMA, rows


def task_for(config, part, **extra):
    return {
        "run_id": config.run.id,
        "spec_hash": "spec",
        "task_id": part["manifest_sha256"],
        "attempt_id": "attempt",
        **part,
        **extra,
    }


def test_manifest_coverage_physical_shards_and_partial_batch(config):
    report = discover(config)
    all_rows = [r for p in report["parts"] for r in rows(p["manifest_uri"], MANIFEST_SCHEMA)]
    assert len(all_rows) == len({r["source_image_id"] for r in all_rows}) == 21
    parquet_shards = [p for p in report["parts"] if next(rows(p["manifest_uri"]))["row_index"] is not None]
    assert len(parquet_shards) == 3
    assert all(p["expected_images"] == 3 for p in parquet_shards)
    actor = GPUActor(config.model_dump())
    results = [actor.process(task_for(config, part)) for part in report["parts"]]
    assert sum(r["candidate"]["row_count"] for r in results) == 21
    assert sum(r["candidate"]["error_count"] for r in results) == 1
    observations = [row for r in results for row in rows(r["candidate"]["uri"])]
    for row in observations:
        if row["status"] == "error":
            assert row["total_vehicles"] is None and row["error_code"]
        else:
            assert row["total_vehicles"] == sum(
                row[k] for k in ["count_2wheel", "count_bus", "count_car", "count_truck"]
            )
    assert any(row["status"] == "ok" and row["total_vehicles"] == 0 for row in observations)


def test_exif_orientation_is_ignored(config):
    path = next(Path(config.input.sources_uri).parent.glob("source0/c1/*.jpg"))
    exif = Image.Exif()
    exif[274] = 6
    Image.new("RGB", (80, 40), "red").save(path, exif=exif)
    report = discover(config)
    part = next(p for p in report["parts"] if next(rows(p["manifest_uri"]))["repo_id"] == "fixture/repo0")
    result = GPUActor(config.model_dump()).process(task_for(config, part))
    assert result["candidate"]["success_count"] == 3


def test_missing_source_fails_task_instead_of_zero(config):
    report = discover(config)
    part = next(p for p in report["parts"] if next(rows(p["manifest_uri"]))["repo_id"] == "fixture/repo0")
    Path(next(rows(part["manifest_uri"]))["image_uri"]).unlink()
    config.execution.cache_dir += "-empty"
    config.execution.download_attempts = 1
    with pytest.raises(OSError, match="SOURCE_UNAVAILABLE"):
        GPUActor(config.model_dump()).process(task_for(config, part))


def test_cache_does_not_evict_active_files(tmp_path):
    one, two = tmp_path / "one", tmp_path / "two"
    one.write_bytes(b"12345")
    two.write_bytes(b"67890")
    cache = Cache(tmp_path / "cache", 8, attempts=1)
    with cache.get(str(one)) as pinned:
        with pytest.raises(CacheFull):
            with cache.get(str(two)):
                pass
        assert pinned.exists()
    with cache.get(str(two)) as downloaded:
        assert downloaded.read_bytes() == b"67890"
    assert not pinned.exists()


def test_cache_checksum_failure_is_not_accepted(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"tampered")
    with pytest.raises(ConfigurationError, match="CHECKSUM"):
        with Cache(tmp_path / "cache", 100).get(str(source), "0" * 64):
            pass


def test_oom_policy_is_bounded():
    assert next_batch(16, [16, 8, 4, 1]) == 8
    assert next_batch(1, [16, 8, 4, 1]) is None


def test_cache_recovers_quota_after_interrupted_mutation(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"data")
    cache = Cache(tmp_path / "cache", 10, attempts=1)
    with cache.get(str(source)):
        pass
    (cache.root / "used_bytes").write_text("999999")
    (cache.root / "dirty").touch()
    (cache.root / "orphan.partial").write_bytes(b"partial")
    with cache.get(str(source)):
        pass
    assert (cache.root / "used_bytes").read_text() == "4"
    assert not (cache.root / "orphan.partial").exists()
