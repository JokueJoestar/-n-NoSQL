# Đồ án 16 — Hệ thống thu thập lỗi ứng dụng (Cassandra + Redis)

## 0. Hiểu bức tranh trước khi chạy code

```
 Thiết bị ──POST /logs──▶ API (FastAPI)
                            │
        ┌───────────────────┼───────────────────────────────┐
        ▼                   ▼                               ▼
  ① ratelimit          ② dedup + gom nhóm              ③ ghi bền vững
  (Redis, Lua)         (Redis, Lua)                    (Cassandra, 3 bảng)
  >100 log/60s?        key dedup có chưa?
  → HTTP 429           chưa  → trả 1 → ghi chi tiết ───▶ error_logs
                       rồi   → trả 0 → chỉ đếm           error_logs_by_device
                       (luôn HINCRBY count/pending,      error_logs_by_hash
                        ZINCRBY errtrend, SADD dirty)
                                   │
                                   ▼  mỗi 1 phút
                           worker.py (chỉ đẩy delta)
                                   ▼
          error_group_counter (+delta)   error_groups   error_trend_daily
```

**Ví dụ trực quan** — 1 thiết bị gửi 5 log/giây cùng một lỗi, dedup 5 giây, ngưỡng 100/60s:

```
giây:   0    1    2    3    4    5    ...   10   ...   15   ...  20
log:   █████ █████ █████ █████ █████ █████       █████       █████  █████ ← từ log #101 trở đi: HTTP 429
ghi:    ▲                        ▲              ▲           ▲      ✗
      (mẫu)                    (mẫu)          (mẫu)       (mẫu)
count tăng ở MỌI log được chấp nhận; chi tiết chỉ ghi ở các mốc ▲  → 100 log → 4 bản ghi
```

Ý chính cần nhớ khi bảo vệ:
1. **Redis** lo thứ cần nhanh và tạm thời (đếm, TTL, xếp hạng). **Cassandra** lo thứ cần bền và ghi nhiều.
2. Mỗi bảng Cassandra phục vụ **đúng một kiểu truy vấn** (không JOIN → denormalize).
3. Counter Cassandra chỉ cộng dồn → worker chỉ đẩy **delta** (phần mới), không đẩy tổng.

## 1. Chuẩn bị
- Docker Desktop, Python 3.10–3.11 (không dùng 3.12 trở lên: cassandra-driver lỗi).
- `python -m venv .venv` rồi kích hoạt (`source .venv/bin/activate` hoặc `.venv\Scripts\activate` trên Windows).
- `pip install -r requirements.txt`

## 2. Chạy hạ tầng
```
docker compose up -d
docker ps          # chờ errlog-cassandra ở trạng thái (healthy), khoảng 1–2 phút
```

## 3. Tạo schema
```
python init_db.py                      # in "schema ok"
docker exec -it errlog-cassandra cqlsh -e "DESCRIBE KEYSPACE error_collector"   # xem 6 bảng
```
Chụp màn hình bước này cho báo cáo.

## 4. Chạy API và worker (2 terminal riêng)
```
uvicorn app.main:app --port 8000        # terminal 1
python worker.py                        # terminal 2 (đồng bộ mỗi 60s; thêm --once để chạy 1 lần)
```
Thử nhanh: mở **http://localhost:8000** để dùng giao diện (xem mục 5). Trang http://localhost:8000/docs là tài liệu API tự sinh.

## 5. Giao diện (dashboard)
Mở http://localhost:8000 trên trình duyệt. Trang có 6 khung:

| Khung | Chức năng | Lấy dữ liệu từ |
|---|---|---|
| 1. Gửi log lỗi thử | Nhập form và gửi 1 log, hoặc bấm "Giả lập crash loop" gửi 150 log liên tục. Ba ô đếm cho biết bao nhiêu log được ghi chi tiết, chỉ đếm (dedup), bị chặn (429) | `POST /logs` |
| 2. Top lỗi hôm nay | Xếp hạng lỗi | Redis `errtrend` |
| 3. Log mới nhất | 20 log mới | Redis `recent_errors` |
| 4. Nhóm lỗi + tổng số lần | Danh sách nhóm lỗi, lần đầu/cuối, tổng số lần | Cassandra `error_groups` + `error_group_counter` |
| 5. Tra cứu theo thiết bị | Nhập device_id, xem log hôm nay | Cassandra `error_logs_by_device` |
| 6. Xu hướng đã lưu | Top lỗi do worker chụp | Cassandra `error_trend_daily` |

Khung 4 và 6 chỉ có dữ liệu **sau khi worker chạy** (mỗi 1 phút, hoặc `python worker.py --once`).

## 6. Nhập dữ liệu mẫu (yêu cầu "nhập dữ liệu vào CSDL")
Khi API đang chạy:
```
python seed.py             # gửi 300 log (3 ứng dụng x 20 thiết bị x 5 log), in ra số log ghi/đếm
python worker.py --once    # đồng bộ sang error_groups, error_group_counter, error_trend_daily
```
Dữ liệu đi qua API nên đi đủ cả rate-limit, dedup và gom nhóm. Sau đó chụp màn hình các lệnh kiểm tra:
```
docker exec -it errlog-cassandra cqlsh -e "SELECT COUNT(*) FROM error_collector.error_logs"
docker exec -it errlog-cassandra cqlsh -e "SELECT * FROM error_collector.error_group_counter"
docker exec -it errlog-redis redis-cli KEYS "errgroup:*"
```
Lưu ý: `seed.py` dùng số ngẫu nhiên cố định (seed 42) nên mỗi lần chạy trên CSDL trống cho kết quả giống nhau.

## 7. Quan sát bên trong (để minh hoạ khi bảo vệ)
```
docker exec -it errlog-redis redis-cli
  KEYS *
  HGETALL errgroup:shop_app:<hash>        # count, pending, first_seen...
  ZREVRANGE errtrend:shop_app:<yyyy-mm-dd> 0 9 WITHSCORES
  TTL dedup:shop_app:device_882:<hash>    # đếm ngược 5 → 0
  GET ratelimit:shop_app:device_882
docker exec -it errlog-cassandra cqlsh
  SELECT toTimestamp(log_id), device_id, message FROM error_collector.error_logs
    WHERE app_id='shop_app' AND log_date='<yyyy-mm-dd>' AND log_hour=<giờ UTC> LIMIT 20;
```
`log_date`, `log_hour` tính theo **UTC**.

## 8. Chạy các kịch bản kiểm thử (mục 3.5 trong báo cáo)
| Lệnh | Kết quả mong đợi |
|---|---|
| `python simulate.py ratelimit` | `stored≈4, counted≈96, blocked=50` (mất khoảng 30 giây) |
| `python simulate.py dedup` | `stored≈4, counted≈96` |
| `python simulate.py multi` | `50 stored / 50` |
| Nút "Giả lập crash loop" trên giao diện | gửi rất nhanh nên `stored=1, counted=99, blocked=50` |
| `python worker.py --once` hai lần liên tiếp | lần 1 counter tăng, lần 2 không đổi |
| `docker stop errlog-redis` rồi gửi log | API vẫn trả `stored` (fail-open) |
| `python simulate.py burst` (có/không `NO_DEDUP=1`) | ghi lại số liệu đo thật |

Mỗi lần chạy lại kịch bản: `docker exec errlog-redis redis-cli FLUSHALL`. Muốn chạy nhanh hơn khi demo: đặt `RL_LIMIT=20 DEDUP_TTL=2` trước khi chạy API.

## 9. Kiểm tra logic không cần Docker
```
pip install "fakeredis[lua]" pytest
pytest -q test_logic.py        # 5 test: rate-limit, dedup, nhiều thiết bị, delta, hash
```

## 10. Chia việc cho nhóm (mỗi sinh viên phải tham gia cài đặt)
| Thành viên | Phần phụ trách | File chính |
|---|---|---|
| A | Nhận log, rate-limit, API `POST /logs` | `app/main.py` (hàm `ingest`), `app/scripts.py` (RATELIMIT) |
| B | Chuẩn hoá, hash, dedup, gom nhóm | `app/common.py`, `app/scripts.py` (GROUP) |
| C | Schema Cassandra, ghi/đọc dữ liệu, worker đồng bộ | `schema.cql`, `app/store.py`, `worker.py`, `init_db.py` |
| D | Giao diện, nhập dữ liệu mẫu, kịch bản kiểm thử | `static/index.html`, `seed.py`, `simulate.py` |

Mỗi người nên tự chạy được phần của mình và giải thích được các dòng code trong file đó.

## 11. Danh sách ảnh cần chụp cho báo cáo
1. `python init_db.py` in `schema ok`, và `DESCRIBE KEYSPACE error_collector`.
2. Giao diện http://localhost:8000 lúc mới mở và sau khi bấm "Giả lập crash loop".
3. Kết quả `python seed.py` và `python worker.py --once`.
4. `redis-cli`: `KEYS *`, `HGETALL errgroup:...`, `ZREVRANGE errtrend:...`.
5. `cqlsh`: `SELECT * FROM error_group_counter`, `SELECT COUNT(*) FROM error_logs`.
6. Kết quả từng kịch bản ở mục 8.

## 12. Lỗi thường gặp
- `NoHostAvailable`: Cassandra chưa sẵn sàng → chờ `healthy`.
- Python 3.12 lỗi `asyncore`: nâng `cassandra-driver>=3.29` hoặc dùng Python 3.11.
- Cassandra ăn RAM: giảm `MAX_HEAP_SIZE` trong docker-compose hoặc đóng ứng dụng khác.
- Giao diện trống ở khung 4/6: chưa chạy worker.
- `SELECT` báo "No results": kiểm tra giờ UTC của `log_hour`.

## 13. Nạp dữ liệu lớn (yêu cầu 500.000 – 1.000.000 bản ghi)

```powershell
python bulk_seed.py 500000 --reset     # hoặc 1000000; --reset xoá sạch 6 bảng trước khi nạp
python bulk_seed.py --verify           # đếm số dòng từng bảng
```

Script ghi thẳng vào Cassandra (3 bảng log song song, 5 app × 1000 thiết bị, 8 loại lỗi, rải đều 7 ngày gần nhất)
và tính sẵn các bảng thống kê. 500.000 log tạo ra 1.500.000 dòng ở 3 bảng log.
Chạy xong nên dùng `python bulk_seed.py --verify` thay cho `SELECT COUNT(*)` (đếm toàn bảng dễ bị timeout).

## 14. Chạy bằng cách bấm đúp (không cần gõ PowerShell)
Trong thư mục dự án có sẵn các file `.bat`; bấm đúp theo thứ tự (Docker Desktop phải đang bật):
`1_start_db.bat` → `2_run_api.bat` (để cửa sổ mở) → `3_seed_nho.bat` / `4_nap_du_lieu_lon.bat` / `5_kiem_thu_kich_ban.bat` / `6_mo_cqlsh.bat`.
