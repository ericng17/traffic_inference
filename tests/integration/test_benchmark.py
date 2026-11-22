import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from traffic_pipeline.diagnostics.benchmark import benchmark
from traffic_pipeline.data.discovery import discover
from traffic_pipeline.core.manifest import MANIFEST_SCHEMA

pytestmark = pytest.mark.integration


def test_benchmark_measures_two_durable_runs(config, state, tmp_path):
    report = discover(config)
    sample = tmp_path / "sample.parquet"
    with pq.ParquetWriter(sample, MANIFEST_SCHEMA) as writer:
        for part in report["parts"]:
            for batch in pq.ParquetFile(part["manifest_uri"]).iter_batches(batch_size=2):
                writer.write_table(pa.Table.from_batches([batch]))
    result = benchmark(config, str(sample), state, executor="local")
    assert [p["mode"] for p in result["profiles"]] == ["cold", "warm"]
    for profile in result["profiles"]:
        assert profile["observations"] == 21 and profile["errors"] == 1
        assert profile["ok_committed_per_second"] > 0
        assert state.status(profile["run_id"])["tasks"]["SUCCEEDED"] == 1
    cold, warm = result["profiles"]
    assert (
        cold["attempts"][0]["metrics"]["cache"]["download_bytes"]
        > warm["attempts"][0]["metrics"]["cache"]["download_bytes"]
    )
    assert json.loads(open(result["report_uri"]).read())["profiles"][1]["mode"] == "warm"
