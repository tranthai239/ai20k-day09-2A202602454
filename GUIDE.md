# Hướng dẫn thực hiện Lab Day 9

> Đọc [`README.md`](README.md) trước (sơ đồ hệ thống, dữ liệu, cách chạy). Pipeline đã dựng sẵn: bạn **chạy, quan sát, thử nghiệm, giải thích**. Không cần viết mã.
> File này đi qua **5 bước**, mỗi bước có bốn phần: **Chạy** · **Quan sát** · **Thí nghiệm** (nếu có) · **Câu hỏi** (điền vào [`REPORT.md`](REPORT.md)).

## 0. Chuẩn bị (≈ 30 phút)

```bash
cp .env.example .env                  # mở .env, đặt LAB_STUDENT_ID=<MSSV của bạn>
unzip dataset/day09-data.zip           # gói dữ liệu nằm sẵn trong repo; giải nén tại thư mục gốc lab
python3 scripts/check_data.py         # mọi file phải ĐÚNG
docker compose --profile orchestrate up -d --build
docker compose ps                     # chờ airflow, minio, postgres đều healthy
scripts/lab.sh step all --reset       # chạy thử cả pipeline một lần (≈ 2 phút): phải ra 27/27
```

`step all` đạt nghĩa là hệ thống của bạn chạy đúng. Từ giờ bạn chạy **lại từng bước** để đọc kết quả kỹ hơn. **Commit sau mỗi bước** (thông điệp nêu số bước): lịch sử commit là bằng chứng bạn tự làm.

**Xem dữ liệu bất cứ lúc nào:**

```bash
scripts/lab.sh cli sql "select * from lake.bronze_raw_telemetry_b limit 5"       # bảng Delta: lake.<lớp>_<tên>
scripts/lab.sh cli sql "select * from silver.silver_telemetry_rejects limit 5"   # bảng dbt (sau khi chạy bước tương ứng)
```

MinIO (http://localhost:9001, `minioadmin` / `minioadmin`): mở bucket `lake` để **thấy** bronze, silver, gold là các thư mục thật.

---

## Bước 1 · Bronze: ghi nguyên bản, chạy lại không đổi gì (≈ 20 phút)

**Slide**: 14 (Bronze ghi nguyên bản, chỉ thêm), 17 (video ở object storage, bảng chỉ giữ con trỏ), 27 (idempotent).

**Chạy**

```bash
scripts/lab.sh step 1 --reset         # xoá lake rồi nạp lại từ đầu
```

**Quan sát**

- Chương trình nạp mỗi drive **hai lần liên tiếp**. Lần hai phải báo `already present`: file được nhận diện bằng **sha256**, không phải theo tên hay giờ.
- So sánh hai bảng telemetry thô (script in 2 dòng đầu mỗi bảng): liệt kê **mọi khác biệt** giữa nguồn A và nguồn B (tên cột, đơn vị, kiểu timestamp, ...). Xem thêm: `scripts/lab.sh cli sql "select drive_id, telemetry_clock_offset_ms, sha256 from lake.bronze_video_files"`.
- Mọi cột trong bronze là **chữ** (kể cả ô trống). Vì sao không ép kiểu ngay? (gợi ý: slide 14).
- `scripts/lab.sh cli history bronze.raw_telemetry_a`: bảng chỉ có **một commit**, dù bạn đã nạp hai lần.
- Mở MinIO: video gốc ở `bronze/video/<drive>/raw.mp4`, các bảng Delta ở `bronze/delta/`.

**Câu hỏi → REPORT.md**: Câu 1.

---

## Bước 2 · Silver telemetry: hai schema thành một, cách ly dòng lỗi, cổng chất lượng (≈ 35 phút)

**Slide**: 15 (join theo event time), 28 (data quality), 29 (schema drift), 30.

**Chạy**

```bash
scripts/lab.sh step 2
```

**Quan sát**

- Mở `dbt/models/staging/stg_telemetry_a.sql` và `stg_telemetry_b.sql` (chỉ **đọc**): B được đưa về cùng cột, đơn vị (m/s), kiểu thời gian **và trừ độ lệch đồng hồ** khai báo trong `sources.json`.
- Bảng **quarantine** cho thấy bao nhiêu dòng bị loại theo từng lý do (trùng, thiếu tốc độ, tốc độ vô lý) ở từng drive. Dòng lỗi **không bị xoá**: chúng nằm trong `silver.silver_telemetry_rejects`.
- `dbt test` cho **WARN** ở `max_gap_ms` và **PASS** ở các test còn lại. Mở `dbt/models/schema.yml` và tìm vì sao test này là `warn` còn `in_range` là `error`.
- Kiểm tra bằng chứng đã trừ độ lệch đồng hồ: thời điểm bắt đầu của telemetry B (cột `bat_dau`) khớp thời điểm bắt đầu video (`2025-10-08 14:30:00`).

**Thí nghiệm 2 · Phá cổng chất lượng** (≈ 10 phút)

```bash
scripts/lab.sh exp break-gate
```

Thí nghiệm nới luật "tốc độ hợp lệ" từ 70 lên 1000 m/s, chạy `dbt test`, rồi khôi phục. Bằng chứng ghi ở `report/evidence/break_gate.json`.

**Câu hỏi → REPORT.md**: Câu 2 (cách pipeline đưa hai nguồn về một, kèm số quarantine), Câu 3 (`error` hay `warn`, kèm kết quả thí nghiệm).

---

## Bước 3 · Silver frames: video thành khung hình có tín hiệu chất lượng (≈ 45 phút)

**Slide**: 24 (decode video là nút cổ chai), 27 (idempotent, backfill), 15 (event time), 35.

**Chạy**

```bash
scripts/lab.sh step 3
scripts/lab.sh test                    # unit test của dHash và độ nét (9 test)
```

**Quan sát**

- Mỗi trip là **một partition** (26 trip): pipeline lấy mẫu 2 khung/giây bằng FFmpeg, tính **độ sáng, độ nét, dHash** và ghi vào Delta.
- Bảng "Khung hình đi đâu": drive A có **6 khung đen**, B có **4**. Cột `toi_giu_lai` của B rất lớn: khung **tối** (đường hầm, bóng râm) được **gắn cờ nhưng giữ lại**, khác khung đen. Xem thử vài ảnh trong MinIO (`silver/frames/drive_a/`).
- Cột `trung`: khung gần giống khung liền trước. Drive A có camera **đứng hình** 6 giây và xe chạy chậm nên nhiều khung trùng.
- Độ trễ đồng bộ trung bình giữa khung hình và telemetry nhỏ (vài chục ms): kết quả của **join theo event time** (ASOF). `max` lớn là các đoạn cảm biến mất tín hiệu.

**Thí nghiệm 1 · Idempotent** (≈ 10 phút)

```bash
scripts/lab.sh exp idempotency                       # chọn trip đầu tiên
scripts/lab.sh exp idempotency --trip trip-02134     # hoặc một trip bạn chọn
```

Chạy lại một partition **hai lần**: số dòng và dấu vân tay của bảng **không đổi**. Thí nghiệm cũng mô phỏng kiểu ghi `append` sai để bạn thấy dữ liệu **nhân đôi**. Bằng chứng: `report/evidence/idempotency.json`.

**Thí nghiệm 4 · Ngưỡng khung trùng** (≈ 10 phút)

```bash
scripts/lab.sh exp threshold
```

Số khung bị coi là trùng khi cho phép dHash khác 0, 1, 2, 4, 6, 8 bit. Pipeline dùng ngưỡng **2**. Bằng chứng: `report/evidence/threshold.json`.

**Câu hỏi → REPORT.md**: Câu 4 (idempotent bằng số, và vì sao Delta `overwrite` theo partition an toàn), Câu 5 (khung đen và khung tối khác nhau thế nào; chọn ngưỡng trùng nào, vì sao).

---

## Bước 4 · Gold: dataset huấn luyện, chia theo trip, có phiên bản (≈ 30 phút)

**Slide**: 20 (medallion), 30, 31 (lineage, version hoá), 35 (chia split theo cảnh, không theo khung hình).

**Chạy**

```bash
scripts/lab.sh step 4
```

**Quan sát**

- Bảng split: mỗi drive có đủ train, val, test; mỗi trip nằm trong **một** split. Số khung hình mỗi split và `dataset_version` **của bạn khác bạn bè**, vì MSSV được trộn vào cách xếp trip (mục "Mỗi người một kết quả riêng" ở README).
- Mở MinIO: `lake/gold/datasets/<version>/` có `train/`, `val/`, `test/` và `manifest.json`. Đọc `manifest.json`, đặc biệt phần **`lineage`**: nó nói dataset này được tạo từ phiên bản nào của bảng nào và từ video có sha256 nào.
- Chạy lại `scripts/lab.sh cli export-dataset`: `dataset_version` phải **giống hệt** (nó là băm của nội dung, không phụ thuộc giờ chạy).
- Thử: đổi MSSV trong `.env`, chạy `docker compose --profile orchestrate up -d`, rồi `scripts/lab.sh step 4`: `dataset_version` đổi. **Đặt lại MSSV của bạn** rồi chạy lại `scripts/lab.sh step 4`.

**Thí nghiệm 3 · Rò rỉ split** (≈ 10 phút)

```bash
scripts/lab.sh exp leakage
```

So sánh chia **theo khung hình** với chia **theo trip**: đếm số trip có khung hình ở cả train và test. Bằng chứng: `report/evidence/leakage.json`.

**Câu hỏi → REPORT.md**: Câu 6 (kết quả thí nghiệm rò rỉ, vì sao điểm kiểm thử đẹp giả tạo), Câu 7 (vì sao `dataset_version` là băm nội dung; MSSV đổi thì gì đổi), Câu 8 (đọc `lineage`: dataset của bạn được tạo từ những gì?).

---

## Bước 5 · Warehouse, DAG Airflow, dashboard (≈ 40 phút)

**Slide**: 26 (orchestration), 27 (backfill), 16 (warehouse).

**Chạy**

```bash
scripts/lab.sh step 5
```

**Chạy cả DAG trong Airflow** (http://localhost:8080, `admin` / `admin`):

1. Bật (**unpause**) hai DAG `day09_reset` và `day09_video_pipeline`.
2. **Trigger** `day09_reset` (xoá lake), đợi xanh.
3. **Trigger** `day09_video_pipeline`. Mở tab **Graph**: bạn thấy `silver_frames` tách thành **26 task**, hai hộp **dbt_test** là hai cổng chất lượng. Đợi cả DAG xanh (≈ 2 phút). **Chụp màn hình** (Graph xanh) lưu thành `report/screenshots/airflow_graph.png`.

**Bài tập: một task bị sập rồi chạy lại.** Mỗi người một trip riêng, tính từ MSSV:

```bash
scripts/lab.sh cli list-trips | python3 -c "import sys,json; t=json.load(sys.stdin); print(t[int(sys.argv[1]) % 26])" <MSSV>
# in ra một trip, ví dụ trip-02134: ghi vào đầu REPORT.md
```

Đặt `LAB_FAIL_ONCE=<trip của bạn>` vào cuối file `.env`, rồi `docker compose --profile orchestrate up -d`. Trigger `day09_reset`, rồi `day09_video_pipeline`:

- đúng **một** task `silver_frames` chuyển **đỏ** (mở log: dòng `simulated crash while extracting <trip của bạn>`); các task sau chuyển `upstream_failed`;
- trong giao diện, **chỉ clear task đỏ đó** để nó chạy lại: DAG chuyển xanh;
- chạy `scripts/lab.sh verify`: vẫn **27/27**, và `dataset_version` **không đổi**. **Chụp màn hình** log của task đỏ (thấy trip của bạn) và Graph sau khi xanh.

Xong, **xoá dòng `LAB_FAIL_ONCE`** khỏi `.env` và chạy lại `docker compose --profile orchestrate up -d`.

**Dashboard Metabase:**

```bash
docker compose --profile bi up -d             # đợi ≈ 1 phút
python3 scripts/setup_metabase.py             # in đường dẫn dashboard và tài khoản đăng nhập
```

Mở dashboard, **chụp màn hình** (`report/screenshots/metabase_dashboard.png`). **Thêm một câu hỏi của riêng bạn** (một câu SQL trên `marts.*`, ví dụ tỷ lệ khung bị loại theo từng trip). Chụp thư mục `lake/` trong MinIO (`report/screenshots/minio_lake.png`).

**Câu hỏi → REPORT.md**: Câu 9 (điều gì xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chạy lại, dữ liệu có nhân đôi không, bằng chứng), Câu 10 (nâng lên 500 xe mỗi ngày: ba thành phần nào vỡ trước), và mục "Câu hỏi riêng trên Metabase".

---

## Hoàn thành: báo cáo và nộp

```bash
scripts/lab.sh step all --reset                            # chạy lại toàn bộ lần cuối: phải 27/27, ghi run_summary.json
python3 scripts/check_submission.py --save-verify          # lưu report/verify.txt
python3 scripts/check_submission.py                        # mọi mục phải PASS
git add -A && git commit -m "chore: bài nộp cuối" && git push
```

`REPORT.md` cần có: thông tin cá nhân và trip của bạn; câu trả lời **10 câu hỏi** (ngắn gọn, dùng **số liệu từ các bước và file bằng chứng của bạn**); câu hỏi Metabase của bạn; mục "Nhìn lại". Ba ảnh chụp màn hình trong `report/screenshots/`.

Chấm điểm theo [`RUBRIC.md`](RUBRIC.md).
