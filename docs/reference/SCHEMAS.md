# Manifest, outputs và nguồn

`traffic_pipeline.core.manifest.MANIFEST_SCHEMA` và `OUTPUT_SCHEMA` là schema Arrow chuẩn, được kiểm tra trước commit. Timestamps dùng `timestamp[us, UTC]`; `row_index` là index toàn file, không reset theo row group. `source_image_id = SHA256(canonical JSON [repo_id, revision, source_path, row_index])`.

Manifest chứa định danh nguồn, camera, observed/ingested timestamps, nguồn timestamp, URI raw/shard, số bytes, hash ảnh và hash shard. `image_uri` dùng cho raw hoặc bảng metadata tham chiếu ảnh; embedded bytes dùng `shard_uri` + `image_column` + `row_index`. Khi repack phải giữ nguyên định danh nguồn trong manifest.

Output thêm run/spec/task/attempt IDs, SHA256 ảnh, weights/ROI hashes, `processed_at`, bốn class counts, tổng, status/error và `max_det_reached`. `ok` không có xe = 0; `error` có counts null. Finalizer đối chiếu toàn bộ source identity, camera/timestamp với manifest, uniqueness, row count và count invariants. Verify đối chiếu artifact hashes và uniqueness xuyên task.

Ngày 2026-11-22 đã đọc Hugging Face API và tên file của đủ bảy repo, gồm hai repo bổ sung `ericng17/hcm_traffic_snapshots6` và `ericng17/hcm_traffic_snapshots7`. Không suy ra ID từ hậu tố. Bốn repo đầu báo gated; quyền tải bytes vẫn phải được kiểm tra với `HF_TOKEN` lúc discovery.

| Repo suffix | Ví dụ path thực đã đọc |
| --- | --- |
| không hậu tố | `56df807bc062921100c143da/20261102_230150.jpg` |
| 2 | `snapshots/56df807bc062921100c143da/20261106_215341.jpg` |
| 3 | `56df807bc062921100c143da/20261109_054947.jpg` |
| 4 | `snapshots/56df807bc062921100c143da/20261111_210952.jpg` |
| 5 | `snapshots/56df807bc062921100c143da/20261114_104306.jpg` |
| 6 | `snapshots/56df807bc062921100c143da/20261117_100946.jpg` |
| 7 | `snapshots/56df807bc062921100c143da/20261119_205907.jpg` |

`configs/sources.json` chọn raw `*.jpg` và parse `%Y%m%d_%H%M%S` với timezone cấu hình. Camera mapping là JSON `{ "dataset-camera-id": "approved-roi-camera-id" }`, có checksum riêng; không đoán mapping. Ví dụ trên không xác minh toàn bộ số ảnh, camera, schema hoặc khoảng thời gian. Discovery thực hiện lượt scan có giới hạn bộ nhớ, pin commit revision, đo bytes/dimensions/formats và chi phí scan, báo conflicts. Không giữ máy GPU trong khi chuẩn bị discovery.

Source descriptor cho Parquet embedded:

```json
{"representation":"parquet","include":"data/*.parquet","image_column":"image","camera_column":"camera_id","timestamp_column":"observed_at"}
```

`image` có thể là binary hoặc struct `{"bytes": ..., "path": ...}`. Với metadata references, cấu hình `image_path_column`; path phải tương đối trong cùng repo đã pin. Không chấp nhận URL ký tạm hoặc reference sang nguồn không pin. Representation được chọn rõ từng repo để không cộng raw và Parquet của cùng dữ liệu.

Manifest được ghi theo buffer tối đa 1.024 metadata rows; scan embedded bytes theo batch 16. Một file Parquet vật lý là một task, kể cả vượt target ảnh/bytes. File lớn hơn cache budget cần repack vật lý trước khi chạy; không chia hash dòng rồi bắt mọi GPU tải lại cùng file.

ROI dùng nguyên schema trong thiết kế gốc: schema version 1, approved ở cả file/camera, missing_cameras rỗng, polygon chuẩn hóa hợp lệ, reference aspect ratio. Điểm bottom-center nằm trong hoặc trên biên hợp polygon được đếm đúng một lần. Decode giữ hướng gốc, không EXIF transpose.
