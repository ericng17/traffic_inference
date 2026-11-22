# Traffic inference

Pipeline đếm xe trong ROI: Hugging Face → manifest bất biến → Kafka → PostgreSQL lease → Ray actors → Parquet → time series. PostgreSQL chọn duy nhất một output cho mỗi task; Kafka chỉ chứa metadata. Hỗ trợ fake detector trên CPU và YOLO PyTorch trên GPU.

```text
traffic_inference/
├── .env / .env.example       # Đường dẫn, credentials, dịch vụ
├── configs/                  # Run config và parser nguồn
├── src/traffic_pipeline/
│   ├── cli.py                # Entry point traffic-pipeline
│   ├── core/                 # Config, schema, ROI, hàm dùng chung
│   ├── data/                 # Discovery, planner, cache, storage
│   ├── control/              # PostgreSQL, migrations, Kafka, backup
│   ├── inference/            # Detector, GPU actor, coordinator
│   ├── outputs/              # Finalize, verify/export, aggregate
│   └── diagnostics/          # Benchmark và synthetic fixtures
├── infra/
│   ├── compose.yaml
│   └── docker/               # Dockerfiles control và GPU worker
├── scripts/
│   ├── dev/                  # Smoke test, integration tests
│   └── ops/                  # Retry, restore
├── tests/{unit,integration}/
├── docs/                     # Design, operations, reference, validation
├── data/                     # Artifacts, manifests, outputs bền vững
└── var/                      # Cache, discovery reports, smoke fixtures, backups
```

Xem [chỉ mục tài liệu](docs/README.md), [dữ liệu bền vững](data/README.md) và [dữ liệu runtime](var/README.md). `.env` vẫn là nơi cấu hình mọi đường dẫn; giá trị tùy chỉnh được giữ lại khi sắp xếp thư mục.

Cấu hình đường dẫn, credentials và địa chỉ dịch vụ nằm trong `.env`; xem [.env.example](.env.example). File này đã có bảy repo được xác minh; cần điền weights, ROI đã duyệt, camera mapping và SHA256 tương ứng để chạy thật. Không có artifact thật được tạo thay thế.

Smoke test một lệnh, không cần GPU:

```bash
bash scripts/dev/smoke.sh
```

Lệnh tạo fixture 35 observation, trong đó một JPEG hỏng; đi qua Kafka/PostgreSQL thật, hai Ray actors, verify, export và aggregate. Kết quả mong đợi `COMPLETED_WITH_ERRORS`, đủ 35 source IDs unique, counts ảnh lỗi là null. Dữ liệu nằm trong `var/smoke/`; Docker volumes giữ Kafka/PostgreSQL sau khi container dừng.

Chạy toàn bộ tests qua Compose:

```bash
bash scripts/dev/test.sh
```

Phát triển trên host với Python 3.12 và `uv`:

```bash
test -f .env || cp .env.example .env
uv sync --frozen --extra dev --extra ray
docker compose --env-file .env -f infra/compose.yaml up -d --wait postgres kafka
uv run --frozen --extra dev --extra ray pytest -q tests/unit
RUN_INTEGRATION=1 uv run --frozen --extra dev --extra ray pytest -q
uv run --frozen --extra ray traffic-pipeline smoke --executor ray
```

Tài liệu vận hành: [RUNBOOK](docs/operations/RUNBOOK.md), [schema và nguồn](docs/reference/SCHEMAS.md), [benchmark](docs/operations/BENCHMARK.md), [kết quả kiểm tra](docs/validation/VALIDATION.md), [thiết kế gốc](docs/design/KAFKA_RAY_GPU_BATCH_INFERENCE.md).

GPU smoke test và benchmark YOLO thật cần `best.pt`, bộ ROI, mapping đã duyệt và GPU CUDA. TensorRT/DALI chưa được kích hoạt; chỉ nên bổ sung sau khi đo baseline và kiểm chứng parity.
