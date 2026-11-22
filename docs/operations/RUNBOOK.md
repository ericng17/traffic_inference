# Vận hành pipeline

Thiết kế và phạm vi triển khai trong [tài liệu thiết kế](../design/KAFKA_RAY_GPU_BATCH_INFERENCE.md). Profile local dùng Kafka KRaft một broker, replication factor 1 và PostgreSQL một instance; không có HA. Ray retries cho actor/method đã tắt; PostgreSQL quyết định attempt, deadline và output selection. Đây là at-least-once handoff với chống chọn trùng output, không phải exactly-once xuyên Kafka/DB/storage.

## Cấu hình

Copy `.env.example` thành `.env` khi chưa có. `.env` được gitignore và loại khỏi Docker build context. CLI đọc tự động, biến môi trường có sẵn được ưu tiên. Điền `WEIGHTS_URI`, `ROI_URI`, `CAMERA_MAPPING_URI` và ba SHA256 tương ứng; lấy hash bằng `sha256sum`. URI nhận local path, `file://`, `s3://` hoặc `hf://datasets/owner/repo@commit/path` khi đọc. Không đặt token hay URL ký tạm vào URI.

`HF_REPO_IDS` là JSON list bảy ID đã xác minh. `SOURCE_CONFIG_URI` trỏ `configs/sources.json`, có representation và parser được chọn rõ theo repo. `HF_TOKEN` cần quyền đọc các nguồn gated. `DATABASE_URL`, Kafka bootstrap/log generation, credentials object storage, Ray address, cache và output đều lấy từ `.env`. `BACKUP_BASE_URI` tùy chọn đẩy thêm backup sang S3; nếu bỏ trống backup nằm trong `BACKUP_DIR`.

Với Docker, dùng `data/...` hoặc `/app/data/...` cho dữ liệu bền vững, `var/...` hoặc `/app/var/...` cho cache/fixture/backup. Compose mount cả `./data` và `./var` vào vị trí tương ứng. Đặt artifacts thật trong `data/artifacts` hoặc dùng S3. Không lấy absolute path trên host làm path trong container. Mọi Ray node cần nhìn thấy cùng URI storage và artifact; filesystem local chỉ phù hợp một máy với volume chung. Cache của các node là độc lập, không dùng NFS cho cache lock.

## Chạy local

```bash
bash scripts/dev/smoke.sh
bash scripts/dev/test.sh
```

Hoặc dùng Python 3.12/uv trên host:

```bash
uv sync --frozen --extra dev --extra ray
docker compose --env-file .env -f infra/compose.yaml up -d --wait postgres kafka
uv run --frozen --extra ray traffic-pipeline smoke --executor ray
RUN_INTEGRATION=1 uv run --frozen --extra dev --extra ray pytest -q
```

Fake fixture dùng bảy repo tổng hợp, raw và embedded Parquet, có ảnh zero/corrupt, batch cuối không đầy. Các tests gọi Kafka/PostgreSQL thật; một số fault injection chủ động mô phỏng offset commit thất bại, delivery acknowledgement không chắc chắn, CUDA OOM và lease hết hạn. Test riêng dùng hai Ray actors CPU thực. Đừng suy ra hiệu năng multi-GPU từ fake actors.

## Chuẩn bị chạy thật

1. Hoàn thành artifacts và hashes trong `.env`, xác nhận mapping dataset camera → ROI camera được duyệt. Không tự tạo hoặc sửa polygon để vượt validation.
2. Chạy discovery trên CPU trước. Nó listing theo commit và scan bytes để thống kê/kiểm tra nguồn, giữ bộ nhớ giới hạn; có thể tốn đáng kể I/O trước inference. Report ghi rõ thời gian scan và bytes. Parser mơ hồ, ROI thiếu hoặc conflicts phải xử lý trước plan.
3. Cài GPU extra/build worker, chạy sample manifest bằng model thật; kiểm tra mapping bốn classes, CUDA, FP32, EXIF/aspect ratio và output counts.
4. Benchmark cold/warm đại diện trước khi giữ máy GPU chạy dài. Điền `CONTAINER_DIGEST` bằng digest image đã build và lưu report. Chưa thuê tài nguyên cloud trả phí bằng scripts của repo.

```bash
traffic-pipeline migrate
traffic-pipeline init-topics
traffic-pipeline discover
traffic-pipeline plan
traffic-pipeline publish
traffic-pipeline ingest --once
traffic-pipeline work --gpu-actors 1
traffic-pipeline status
traffic-pipeline verify
traffic-pipeline export
traffic-pipeline aggregate --window 1min
```

`--run-id` mặc định `RUN_ID` trong `.env` cho các lệnh theo run. `publish` mặc định gửi outbox tất cả run. Các lệnh luôn cho phép override `--run-id`. `plan` chỉ báo `PLANNED`, không báo inference hoàn tất. `work` kết thúc sau khi không còn task runnable/retry; chạy `verify` để quyết định `COMPLETED` hay `COMPLETED_WITH_ERRORS`. Task failed hoặc output thiếu → `INCOMPLETE`, exit code 2 từ verify/export.

Profile Docker GPU:

```bash
docker compose --env-file .env -f infra/compose.yaml --profile control run --build --rm control traffic-pipeline migrate
docker compose --env-file .env -f infra/compose.yaml --profile control run --rm control traffic-pipeline init-topics
docker compose --env-file .env -f infra/compose.yaml --profile gpu run --build --rm prepare traffic-pipeline discover
docker compose --env-file .env -f infra/compose.yaml --profile gpu run --rm prepare traffic-pipeline plan
docker compose --env-file .env -f infra/compose.yaml --profile control up -d publisher ingestor backup
docker compose --env-file .env -f infra/compose.yaml --profile gpu up --build ray-head worker
```

Discovery/plan phải tạo bằng môi trường có dependencies dự đoán giống worker. Service `prepare` dùng `infra/docker/Dockerfile.worker`, cài dependencies GPU nhưng không giữ GPU để scan nguồn. Control image phục vụ fake/control-plane và không cài torch. Có thể dùng host `uv sync --frozen --extra gpu` thay cho `prepare`.

Actor giữ model một lần, warmup trước khi claim task, GPU resource = 1/actor, tối đa một task/actor. Batch thực được ghi từng attempt. CUDA OOM restart actor và chọn batch nhỏ hơn theo policy hữu hạn; không chuyển CPU, không đổi imgsz/threshold. MVP download tuần tự với cache dùng chung một node, buffer một batch; chưa có speculative prefetch giữa task hoặc DALI. Backpressure cache gây task retry hữu hạn; không xóa file đang pin. Đối với file Parquet quá lớn, repack trước thay vì tăng số GPU tải cùng file.

## Phục hồi

**Mất compute/actor/coordinator:** khởi động lại `work` với cùng `RUN_ID`. Resume kiểm tra spec, code/dependencies và artifact hashes. Task có lease sống không được claim; hết TTL/deadline sẽ `RETRY_WAIT` hoặc `FAILED`. Task `SUCCEEDED` không chạy lại. Candidate đã upload nhưng chưa finalize là orphan, không được export. Worker cũ không có token hiện hành bị fence.

**Lỗi nguồn tạm thời:** cache có timeout và tối đa ba download attempts, sau đó PostgreSQL retry task theo backoff. Hết số attempt → `FAILED`. Fix quyền truy cập/network/cache rồi chạy retry. JPEG hỏng là error row riêng, không retry task vô hạn. Lỗi cấu hình như aspect ratio hoặc ROI dừng coordinator để người vận hành xử lý; không sửa cấu hình dự đoán trong cùng run.

```bash
traffic-pipeline verify --repair
traffic-pipeline retry --failed-only
traffic-pipeline work
traffic-pipeline verify
traffic-pipeline export
```

Hoặc `bash scripts/ops/retry.sh RUN_ID` với CLI trên PATH. Manual retry cấp lại bounded attempt budget, giữ history. `verify --repair` tải lại output từ storage, kiểm tra checksum/schema/coverage; output mất/hỏng bị loại khỏi selection, task chuyển `FAILED`. Không để cached output che mất việc object gốc đã bị mất.

**Kafka dừng:** task đã `QUEUED` tiếp tục chạy. Pending outbox vẫn ở PostgreSQL; publisher gửi sau khi broker phục hồi. Nếu Kafka đã ack nhưng DB chưa ghi ack thì event có thể lặp, giữ cùng `event_id`. Không reset state theo Kafka lag.

**Kafka mất log:** dừng ingestor, thay `KAFKA_LOG_GENERATION` trong `.env` bằng giá trị mới, dựng topics, `traffic-pipeline reconcile --republish-planned`, rồi publish/khởi động lại ingestor. Reconcile chỉ republish task `PLANNED`, không reset task đã queued/succeeded. Inbox phân biệt generation/topic/partition/offset; event ID vẫn dedupe xuyên generation. Không xóa inbox để replay.

**Backup PostgreSQL:** `traffic-pipeline backup --watch` dùng `pg_export_snapshot` và `pg_dump --snapshot` để snapshot JSON và DB nhất quán. Chạy mỗi giờ và một lần lúc shutdown SIGINT/SIGTERM; `pg_dump` phải cùng hoặc mới hơn major version server. Compose backup có pg_dump 17 cho server 17. Sao lưu cả `data/output`, manifests, artifacts hoặc bật versioning/lifecycle ở object storage. DB dump không chứa bytes ảnh/output. Volume local không tồn tại sau khi terminate máy nếu provider xóa disk; cần volume/bucket bền vững riêng.

**Restore:** tạo database trống riêng, điền `RESTORE_DATABASE_URL` và `RESTORE_DUMP` trong `.env`, chạy `.venv/bin/python scripts/ops/restore.py`. Script từ chối database có bảng hoặc URI trùng DB đang chạy. Sau đó trỏ `DATABASE_URL` sang DB đã restore, giữ storage/manifests/artifacts tương ứng, kiểm tra `status`, `verify --repair`, `reconcile --republish-planned` và retry phần thiếu. Output được tạo sau snapshot có thể phải tính lại; chỉ một attempt được chọn. Integration test thực sự dump, restore vào database tạm riêng và resume không tính lại các task committed.

Để dừng local mà giữ dữ liệu: `docker compose --env-file .env -f infra/compose.yaml --profile control --profile gpu down`. Không dùng `down -v` nếu cần resume.

## Export và STGCN

Export lấy selection từ transaction snapshot, ghi immutable JSON theo checksum trong `runs/RUN_ID/exports/` và pointer `committed_outputs.json`. Chỉ đọc các URI trong selection; không glob `attempts/`. Error rows có Parquet riêng ở reports. Aggregate yêu cầu verify hoàn chỉnh, mặc định mean snapshots trong `(label-window,label]`, không cộng thành flow. Camera được đưa lên grid chung; zero, missing, valid/observation counts và observation age được tách rõ; không dùng ảnh tương lai. Chia train/val/test theo thời gian trước khi tạo cửa sổ STGCN.

Ports PostgreSQL/Kafka chỉ bind loopback; Ray không publish port quản trị ra host. Multi-node chỉ dùng mạng riêng/tunnel và cấu hình xác thực/mã hóa nếu đi qua mạng không tin cậy. Kafka client có thể nạp SSL/SASL config từ file do `KAFKA_CLIENT_CONFIG` chỉ định; file secrets không được ghi vào spec/messages.
