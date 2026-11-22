# Scripts

Chạy từ bất kỳ thư mục nào với đường dẫn script phù hợp:

| Script | Chức năng |
| --- | --- |
| `bash scripts/dev/smoke.sh` | Build và chạy full fake pipeline trong Compose |
| `bash scripts/dev/test.sh` | Chạy unit/integration tests trong Compose |
| `bash scripts/ops/retry.sh RUN_ID` | Verify/repair, retry, work, verify và export |
| `.venv/bin/python scripts/ops/restore.py` | Restore vào database trống theo `.env` |

Các shell scripts tự chuyển về root repo. Python restore script đọc `.env` tại thư mục làm việc, nên chạy từ root repo hoặc đặt `ENV_FILE` trỏ đến file cấu hình.
