# Benchmark

Fake detector chỉ kiểm tra orchestration, idempotency và khôi phục; không dùng throughput của nó để dự báo GPU thật. Chưa có weights/ROI thật trong repo, nên chưa có kết quả benchmark YOLO hay ETA sản xuất đã đo.

Chuẩn bị sample manifest theo schema Arrow chuẩn, tối đa 50.000 observation, có đủ bảy repo, nhiều camera, ngày/đêm, mật độ xe và kích thước ảnh. Sample giữ source IDs/revisions; có thể ghép các manifest parts đã discovery bằng `pyarrow.parquet.ParquetWriter`, không sửa các trường nguồn. Chọn sample không có camera/timestamp conflicts.

```bash
uv sync --frozen --extra gpu
traffic-pipeline benchmark
```

`SAMPLE_MANIFEST_URI` và `PIPELINE_CONFIG` khai báo trong `.env`, CLI tự đọc. Có thể override bằng `--config`/`--sample-manifest`. Benchmark tạo hai run riêng `benchmark-*-cold` và `benchmark-*-warm`, qua Kafka, PostgreSQL, Ray và durable finalize. Cold dùng application cache mới, warm tái sử dụng nó; không tuyên bố kiểm soát OS cache hoặc CDN cache. Mỗi lần đều gồm startup/model warmup; discovery/repack không nằm trong timing đó.

Report JSON nằm dưới `OUTPUT_BASE_URI/benchmarks/`. Thước đo chính `ok_committed_per_second = số dòng ok đã commit / wall clock`; báo riêng tổng observations và error rows. Attempt metrics ghi batch thực, download bytes/thời gian, cache hits, decode, predict, ROI, output/upload, wall clock và môi trường GPU. Predict trả box về CPU nên đã đồng bộ CUDA trước khi hoàn tất timing; không cộng stage timings để suy ra wall clock.

Trước batch lớn, đo 30–60 phút workload đại diện trên một GPU. Benchmark batch 16/32/64 trong giới hạn VRAM, xác nhận sample thật chạy FP32, lưu report cùng spec/container digest. Chỉ bật FP16 bằng cấu hình run mới sau khi so phát hiện và counts tại threshold 0,35. TensorRT/DALI cần implementation, dependency/GPU matrix và parity tests riêng; backend hiện từ chối tên chưa hỗ trợ.

ETA = tổng ảnh discovery / throughput đã đo. Ngân sách inference 120 giờ yêu cầu N/432000 ảnh/giây. Chi phí/một triệu ảnh = tổng giá thuê mỗi giờ × 1000000/(3600×throughput), gồm compute/storage/transfer. Report có hệ số chi phí để nhân với giá thuê thật; không hardcode giá L4/T4. Chỉ thêm GPU sau khi ETA hoặc ưu tiên hoàn tất sớm cho thấy cần thiết.

Theo dõi thêm Kafka lag và GPU utilization/VRAM trong lúc benchmark:

```bash
docker compose --env-file .env -f infra/compose.yaml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --describe --group traffic-task-ingestor-v1
nvidia-smi --query-gpu=timestamp,name,utilization.gpu,memory.used,memory.total --format=csv -l 5
```

Kafka lag đo tiến độ handoff vào PostgreSQL; số dòng committed và task states trong `status` mới phản ánh tiến độ inference. Wall clock của report gồm phần điều phối/DB; stage timings chỉ giúp xác định bottleneck, không thay thế throughput committed.
