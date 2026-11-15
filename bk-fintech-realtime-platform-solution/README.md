# BK Fintech E-Commerce Realtime Data Platform (Solution Repository)

Hệ thống mã nguồn mẫu hoàn chỉnh cho toàn bộ học phần **Realtime Data Engineering & Deployment (Module 3)** trải qua 6 buổi học thực chiến.

---

## 1. Bản Đồ Dịch Vụ & Kiến Trúc Tổng Thể

```text
[ Simulator ] ─────────────────> [ Kafka Broker (KRaft) ]
                                      │        │
                   ┌──────────────────┘        └──────────────────┐
                   ▼ (Hot Path)                                   ▼ (Historical Path)
        [ Analytics Consumer ]                             [ Raw Archiver ]
                   │ (Ghi SQLite)                                 │ (Ghi S3)
                   ▼                                              ▼
        [ Volume: SQLite DB ]                         [ SeaweedFS S3 Data Lake ]
                   ▲                                              │
                   │ (Đọc :ro)                                    ▼
        [ Streamlit Dashboard ]                       [ Warehouse Loader ]
            (:8501)                                               │ (Nạp DWH)
                                                                  ▼
                                                       [ ClickHouse Analytics ]
                                                               (:8123)

[ Prometheus (:9090) ] ◄── Scrapes ── [ Analytics:9108, Archiver:9109, Lag-Exporter:9110 ]
         │
         ▼
[ Grafana Dashboard (:3000) ]
```

---

## 2. Danh Sách Dịch Vụ Docker & Cổng Truy Cập

| Service | Port Host | Chức năng chính | Profile |
| :--- | :--- | :--- | :--- |
| `kafka` | `9092` | Kafka Broker KRaft (Topic: `order-events`, `customer-activity-events`...) | Mặc định |
| `analytics` | `9108` | Consumer xử lý phân tích thời gian thực + Expose metrics | Mặc định |
| `dashboard` | `8501` | Giao diện Streamlit thời gian thực | Mặc định |
| `object-storage`| `8333` | S3-compatible Object Storage (SeaweedFS) | `storage` |
| `clickhouse` | `8123`, `9000` | ClickHouse OLAP Data Warehouse (`ReplacingMergeTree`) | `storage` |
| `archiver` | `9109` | Kafka to S3 Raw Archiver + Expose metrics | `storage` |
| `warehouse-loader`| - | S3 to ClickHouse Batch Loader | `storage` |
| `prometheus` | `9090` | Thu thập metrics và cảnh báo sự cố | `monitoring` |
| `grafana` | `3000` | Bảng điều khiển trực quan hóa hệ sinh thái (User: `admin`/`admin`)| `monitoring` |
| `lag-exporter` | `9110` | Đo lường Kafka consumer lag | `monitoring` |

---

## 3. Hướng Dẫn Vận Hành Theo Từng Buổi Học

Chi tiết các bước thực hành và lệnh nghiệm thu cho từng buổi học nằm trong thư mục `docs/`:
- **Buổi 1:** Làm quen Kafka KRaft, CLI produce/consume, quan sát partition/offset ([docs/lesson1_runbook.md](docs/lesson1_runbook.md)).
- **Buổi 2:** Viết Python Kafka Producer & Consumer, validate schema, ghi JSONL ([docs/lesson2_runbook.md](docs/lesson2_runbook.md)).
- **Buổi 3:** Windowing 1 phút, SQLite WAL storage, Streamlit Dashboard ([docs/lesson3_runbook.md](docs/lesson3_runbook.md)).
- **Buổi 4:** Đóng gói toàn bộ hệ thống bằng Docker Compose ([docs/lesson4_runbook.md](docs/lesson4_runbook.md)).
- **Buổi 5:** Tích hợp Data Lake (S3) & Data Warehouse (ClickHouse OLAP) ([docs/lesson5_runbook.md](docs/lesson5_runbook.md)).
- **Buổi 6:** Giám sát (Prometheus/Grafana), đo lường Lag & xử lý sự cố đứt gãy ([docs/lesson6_runbook.md](docs/lesson6_runbook.md)).

---

## 4. Khởi Động Nhanh Toàn Bộ Hệ Thống

```bash
# 1. Khởi động toàn bộ stack dịch vụ (Hot Path, Storage và Monitoring)
docker compose --profile storage --profile monitoring up -d --build

# 2. Kiểm tra trạng thái các container
docker compose ps

# 3. Mở các giao diện trên trình duyệt:
# - Streamlit Dashboard: http://localhost:8501
# - Prometheus UI:       http://localhost:9090
# - Grafana UI:          http://localhost:3000 (admin/admin)
```
