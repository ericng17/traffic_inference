# Dữ liệu runtime

```text
var/
├── cache/                 # Blob cache, lease locks, byte accounting
├── discovery/report.json  # Báo cáo chuẩn bị nguồn mặc định
├── fixtures/dev/          # Fixture phát triển cũ đã chuyển từ data/dev-fixture
├── smoke/                 # Mỗi smoke run là một thư mục riêng, gồm cả cache/output
└── backups/               # pg_dump và metadata snapshots; cần sao lưu ngoài máy
```

Các đường dẫn mặc định nằm trong `.env`, có thể đổi riêng. Nội dung được gitignore; các thư mục rỗng không cần commit. Docker mount `./var` vào `/app/var`, song song với mount dữ liệu bền vững `./data`.

Chỉ dọn cache khi workers đã dừng. Không xóa backup hoặc output còn cần đối chiếu. Fixtures cũ được giữ nguyên bytes và có symlink tương thích trong `data/`. Do run spec pin code hash, chạy smoke mới sau khi đổi phiên bản code; không sửa spec hoặc trạng thái DB của run cũ để ép resume.
