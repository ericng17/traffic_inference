# Kết quả kiểm tra local — 2026-11-22

Đã chạy 39 tests thành công trên host (38,36 giây) và 39 tests trong Docker Compose (40,79 giây), dùng PostgreSQL 17.4 và Kafka 3.9.0 thật. Ruff lint/format và `git diff --check` đều qua.

Các bài kiểm tra bao gồm config/artifact/class mapping, timestamp parser theo tên file HF thực, ROI boundary/union/aspect ratio/EXIF, coverage raw/Parquet và batch cuối, null error rows, cache pins/quota/crash recovery, planner idempotency, inbox dedupe và consumer reassignment, outbox uncertain acknowledgement, lease expiry/deadline/fencing, rollback finalize transaction, reconcile, compute resume, backup/restore sang database trống, aggregate missing/zero/mean/no future observations, benchmark cold/warm và hai Ray actors thực.

OOM được fault-inject với fake detector để kiểm tra restart actor/batch policy. Class mapping và yêu cầu CUDA của adapter YOLO có unit tests bằng mocks. Đây không phải GPU smoke test bằng weights thật.

Smoke test Compose với hai Ray actors CPU hoàn thành:

```json
{
  "run_id": "smoke-3b81b2750056",
  "state": "COMPLETED_WITH_ERRORS",
  "expected_images": 35,
  "committed_observations": 35,
  "unique_source_ids": 35,
  "ok": 34,
  "errors": 1,
  "missing_or_invalid_tasks": []
}
```

Export báo `final=true`, time series có 8.642 rows trên grid một phút cho fixture một camera trải sáu ngày. Một JPEG hỏng được giữ dưới dạng error row với counts null; không bị biến thành zero. Sau khi sắp xếp thư mục, artifacts ở `var/smoke/smoke-3b81b2750056/` trên host, tương ứng `/app/var/smoke/...` trong container. Symlink `data/smoke` giữ nguyên các URI đã lưu trong fixture cũ.

Bảy dataset IDs và các ví dụ tên file đã được xác minh qua Hugging Face API, nhưng chưa scan/download toàn bộ nguồn thật. Chưa có `best.pt`, ROI/mapping đã duyệt và GPU được cung cấp trong cấu hình nên chưa chạy GPU smoke test, benchmark YOLO thật hoặc full batch hai triệu ảnh. S3 và multi-node Ray chưa được kiểm thử end-to-end ở đây. Không dùng số đo fake để cam kết ETA/GPU throughput. Điền `.env` rồi thực hiện các bước trong `RUNBOOK.md` và `BENCHMARK.md` trước production.

## Kiểm tra sau khi sắp xếp thư mục

39 tests tiếp tục đạt sau refactor (39,64 giây). Wheel đã build, cài vào thư mục riêng và kiểm tra imports, fingerprint của source trong subpackages, cùng migration SQL được đóng gói. CLI entry point, cú pháp shell scripts, Compose config, link tài liệu và hai đường dẫn fixture cũ đều đã kiểm tra.

Docker smoke run mới `smoke-bf589817d493` tạo dữ liệu tại `var/smoke/`, giữ đủ 35 source IDs unique, 34 dòng ok và một error row, export `final=true`, aggregate 8.642 rows. Không sửa manifest hoặc run spec của các fixture cũ để chuyển thư mục.
