# Dữ liệu bền vững

Thư mục này dành cho artifacts thật, manifests bất biến và output được commit:

```text
data/
├── artifacts/       # Weights, ROI, camera mapping do người vận hành cung cấp
├── manifests/       # Các manifest shard theo checksum
└── output/          # Artifacts đã pin, run specs, attempts, exports, time series
```

Các thư mục được tạo khi chạy hoặc khi cung cấp artifact. Đường dẫn thực lấy từ `.env`; nội dung dữ liệu được gitignore. PostgreSQL/Kafka lưu trong Docker named volumes, không nằm ở đây.

Cache và synthetic fixtures mới nằm trong `var/`. Hai symlink `data/dev-fixture` và `data/smoke` giữ đường dẫn của fixture đã tạo trước lần sắp xếp này; nội dung thực tương ứng ở `var/fixtures/dev` và `var/smoke`. Không sửa manifest/checksum để đổi các đường dẫn cũ.
