# Thang điểm Lab Day 9 (100 điểm)

> Bài cá nhân, nộp qua **repo GitHub công khai**. Bạn **không viết mã**: bạn **chạy** pipeline, **kiểm tra** từng cấu phần, **làm thí nghiệm** và **giải thích**. Xem [`README.md`](README.md) và [`GUIDE.md`](GUIDE.md).

## Cách chấm

- Giảng viên clone repo của bạn (commit mới nhất **trước hạn nộp**), **ghi đè lại mã của đề (`pipeline/`, `dbt/`, `dags/`, `docker/`, `scripts/`, `tests/`, `docker-compose.yml`) bằng bản gốc**, đặt `LAB_STUDENT_ID` bằng **MSSV ghi trong `REPORT.md`**, rồi chạy lại pipeline từ lake trống bằng `scripts/grade_student.py`.
- Vì MSSV được trộn vào cách chia split, **mỗi người có `dataset_version` riêng**. Giảng viên so `dataset_version` chạy lại với con số bạn nộp (`run_summary.json`, `REPORT.md`): khác nhau nghĩa là bài không phải của bạn hoặc không chạy được.
- Giảng viên đối chiếu báo cáo **giữa các repo** và có thể gọi **vấn đáp ngắn (5 phút)** để xác nhận bạn hiểu điều bạn viết.

## A. Hệ thống chạy đúng, từng cấu phần (40 điểm, tự động)

Chạy lại từ lake trống. Mỗi kiểm tra đạt cho điểm bằng điểm của phần chia cho số kiểm tra của phần đó.

| Cấu phần | Nội dung được kiểm tra | Kiểm tra | Điểm |
|---|---|---|---|
| Bước 1 · Bronze | Số dòng bronze bằng số dòng CSV; mỗi video một lần (sha256); trips | 5 | 6 |
| Bước 2 · Silver telemetry | Dòng cách ly đúng theo từng lý do; sạch + cách ly = nhận về; không trùng; tốc độ hợp lệ; **độ lệch đồng hồ của B đã được trừ** | 7 | 8 |
| Bước 3 · Silver frames | Khung đen, mờ, đứng hình bị bắt; khung tối được gắn cờ nhưng giữ lại | 6 | 8 |
| Bước 4 · Gold | Đủ train/val/test; **không trip nào ở hai split**; gold không chứa khung xấu; manifest và lineage; ảnh đủ | 7 | 10 |
| Bước 5 · Warehouse | Đủ mart; số dòng gallery bằng số khung của gold | 2 | 4 |
| DAG Airflow | `day09_video_pipeline` **xanh toàn bộ** từ lake trống | 1 | 4 |

Trừ điểm: hai lần chạy liên tiếp cho **`dataset_version` khác nhau** hoặc khác với `run_summary.json` của bạn: **−5**.

## B. Bằng chứng (20 điểm)

| Tiêu chí | Điểm | Đạt đủ điểm khi |
|---|---|---|
| `report/evidence/run_summary.json` | 4 | Có, `student_id` khớp MSSV trong `REPORT.md`, `verify_all_passed` là true, `dataset_version` khớp lần chạy lại của giảng viên |
| Bốn file bằng chứng thí nghiệm | 8 | `idempotency.json`, `break_gate.json`, `leakage.json`, `threshold.json` có đủ và **số liệu khớp** với báo cáo (2 điểm mỗi file) |
| Ba ảnh chụp màn hình | 4 | Airflow Graph xanh, MinIO `lake/`, Metabase dashboard (kèm câu hỏi riêng của bạn) |
| Bài tập sập và chạy lại | 4 | Ảnh log task đỏ **thấy đúng trip tính từ MSSV của bạn**, và ảnh DAG xanh sau khi clear đúng task đỏ |

## C. Báo cáo (35 điểm)

| Tiêu chí | Điểm | Đạt đủ điểm khi |
|---|---|---|
| 10 câu hỏi | 30 | Mỗi câu 3 điểm: **đúng bản chất**, **dùng số liệu từ bài chạy của bạn** (đầu ra `step`, file bằng chứng), không chép lại tài liệu. Câu 3 (error và warn), 4 (idempotent), 6 (rò rỉ), 7 (version và MSSV), 9 (sập và chạy lại) được chấm kỹ nhất |
| Câu hỏi riêng trên Metabase | 3 | Câu SQL có ý nghĩa và diễn giải đúng |
| Nhìn lại | 2 | Thành thật, cụ thể (kể cả điều chưa hiểu hoặc một lỗi dữ liệu pipeline chưa bắt được) |

## D. Git và trình bày (5 điểm)

- Ít nhất **5 commit** trải qua nhiều bước, thông điệp nêu số bước; repo **Public** và sạch (không dữ liệu, không `work/`, không khoá bí mật); `python3 scripts/check_submission.py` không còn FAIL: **5 điểm**. Mỗi mục FAIL còn lại trừ **1 điểm** (tối đa −5).

## Lỗi bị trừ điểm

| Lỗi | Trừ |
|---|---|
| Sửa `tests/`, `pipeline/verify.py` hoặc `docker-compose.yml` | **−20** (người chấm khôi phục bản gốc, nên sửa chúng cũng không có tác dụng) |
| Số liệu trong báo cáo **không khớp** file bằng chứng hoặc lần chạy lại | −2 mỗi chỗ (tối đa −10) |
| `MSSV` trong `REPORT.md` khác `LAB_STUDENT_ID` đã dùng khi chạy | −10 (và `dataset_version` sẽ không khớp) |
| Sao chép bài của người khác (báo cáo hoặc bằng chứng giống nhau bất thường, hoặc `dataset_version` không thuộc MSSV của bạn) | 0 điểm cả bài, cho cả hai bên |
| Repo không công khai lúc chấm hoặc không clone được | không chấm được: 0 điểm cho đến khi sửa (có thể bị trừ vì nộp trễ) |
| Commit khoá bí mật hoặc token thật vào repo | −5 (và hãy thu hồi khoá đó ngay) |

## Điểm cộng (tối đa +5, tổng không vượt 100)

- Một **thí nghiệm của riêng bạn** (ví dụ đổi `split_train`/`split_val` hoặc `sync_tol_ms` trong `dbt/dbt_project.yml`, chạy lại và báo cáo tác động bằng số).
- Một câu hỏi Metabase thứ hai trả lời một câu hỏi vận hành thật (ví dụ: trip nào có tỷ lệ khung bị loại cao nhất và vì sao).

## Xếp loại

| Điểm | Mức |
|---|---|
| 90–100 | Xuất sắc: hệ thống chạy đúng, bằng chứng đầy đủ, giải thích sâu |
| 75–89 | Tốt: đúng gần hết, thiếu một phần bằng chứng hoặc giải thích |
| 60–74 | Đạt: chạy được tới gold, báo cáo mỏng |
| < 60 | Chưa đạt: chưa có dataset gold hợp lệ, hoặc DAG không chạy |

## Nộp bài

- **Một đường dẫn repo GitHub Public** có: thư mục lab (không kèm dữ liệu), `REPORT.md` đã điền, `report/evidence/` (5 file JSON), `report/screenshots/` (≥ 3 ảnh), `report/verify.txt` (27/27).
- Không đưa `data/*.mp4|csv`, `work/`, file `.tar`, `.duckdb` vào repo (`.gitignore` đã loại sẵn).
- Giảng viên chạy lại được từ trạng thái trống bằng: đặt dữ liệu, `.env` (MSSV), `docker compose --profile orchestrate up -d`, `scripts/lab.sh step all --reset`.
- Tự kiểm tra: `python3 scripts/check_submission.py` phải không còn mục FAIL.
