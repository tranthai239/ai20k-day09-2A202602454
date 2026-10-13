# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Trần Thanh Thái |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602454 |
| GitHub | tranthai239 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-01037 |

## Kết quả cuối

- `dataset_version` của bạn: `4f8a54fa7d96`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `train: 713, val: 159, test: 147 (tổng 1019 khung)`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt rõ rệt giữa hai nguồn telemetry ở lớp Bronze:
1. Tên cột: Nguồn A sử dụng tên chuẩn (`timestamp`, `speed_mps`, `heading_deg`, `latitude`, `longitude`, `accel_x_mps2`, `throttle_pct`), trong khi nguồn B dùng quy ước đặt tên hoàn toàn khác (`ts_epoch_ms`, `vehicle_speed_kmh`, `yaw_angle_deg`, `lat`, `lon`, `ax_g`, `pedal_throttle_ratio`).
2. Đơn vị đo lường: Tốc độ ở A tính theo m/s, ở B tính theo km/h; gia tốc ở A là m/s² còn B theo g-force; độ mở bàn đạp ở A tính theo phần trăm (0..100) còn B tính theo tỷ lệ thập phân (0..1.0).
3. Kiểu dữ liệu thời gian: Nguồn A dùng chuỗi ISO 8601 UTC (`2025-10-07T08:00:00.000Z`), trong khi B dùng số nguyên Unix timestamp epoch tính bằng mili-giây (`1728397800000`).
4. Lệch đồng hồ (Clock Offset): Thiết bị B bị lệch đồng hồ +120,000 ms (2 phút) so với video và cần bù trừ theo `sources.json`, còn A không bị lệch.

Pipeline giữ mỗi nguồn một bảng Bronze riêng và lưu kiểu chữ (string) vì theo nguyên lý Medallion (Slide 14) và kiến trúc ELT:
- Bronze là lớp lưu trữ dữ liệu thô nguyên bản (raw, append-only source of truth), phản ánh trung thực định dạng thiết bị gửi lên phục vụ truy vết (audit/lineage).
- Ngăn ngừa đổ vỡ khi có schema drift: nếu ép kiểu ngay khi nạp (schema-on-write), một giá trị lỗi (ô trống, chuỗi ký tự lạ, sai format) sẽ làm sập pipeline hoặc mất dữ liệu im lặng. Lưu dạng chữ giúp hấp thụ 100% dữ liệu an toàn, việc làm sạch và ép kiểu được chuyển sang lớp Silver bằng các mô hình dbt có kiểm soát.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline chuẩn hóa hai nguồn về chung một schema thông qua các staging model (`stg_telemetry_a.sql` và `stg_telemetry_b.sql`):
- Đổi đơn vị: B chuyển tốc độ từ km/h sang m/s (`vehicle_speed_kmh / 3.6`), gia tốc từ g sang m/s² (`* 9.80665`), tỷ lệ bàn đạp nhân 100.
- Kiểu thời gian và độ lệch đồng hồ: Nguồn A parse trực tiếp ISO string thành UTC timestamp. Nguồn B đổi epoch ms thành UTC timestamp đồng thời trừ đi 120 giây lệch đồng hồ (`to_timestamp(ts_epoch_ms / 1000.0) - interval '120 seconds'`).
- Bằng chứng đã đúng: Bản ghi đầu tiên của telemetry B sạch bắt đầu lúc `2025-10-08 14:30:00.009000+00:00`, khớp chính xác với thời điểm bắt đầu video B (`video_start_utc = 2025-10-08 14:30:00+00:00`, độ chênh lệch chỉ 9 ms < 0.5s theo test module 2).

Bảng cách ly dòng lỗi (Quarantine):
| drive_id | duplicate | speed_missing | speed_out_of_range | Tổng rejects | Nhận về (Clean + Rejects) |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 + 53 = 3021 dòng |
| drive_b | 20 | 30 | 3 | 53 | 3181 + 53 = 3234 dòng |

Dòng lỗi được đưa vào bảng `silver_telemetry_rejects` thay vì bị xóa âm thầm vì nguyên lý Dead-letter queue / Quarantine (Slide 28): Dữ liệu lỗi là tín hiệu chẩn đoán quan trọng phản ánh sự cố phần cứng cảm biến hoặc nghẽn mạng. Việc giữ lại kèm lý do (`reject_reason`) giúp kỹ sư giám sát chất lượng dữ liệu (Data Observability), debug và có thể reprocess khi luật hoặc dữ liệu được khắc phục.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Phân biệt `warn` và `error`:
- `max_gap_ms` đặt mức `warn` vì việc mất tín hiệu telemetry tạm thời trong vài trăm mili-giây (xe đi qua hầm, che khuất GPS, độ trễ bus CAN) là hiện tượng vật lý bình thường. Đây là chỉ báo cần cảnh báo giám sát chứ không nên dừng toàn bộ pipeline, vì các đoạn dữ liệu còn lại vẫn hoàn toàn dùng được.
- `in_range` bắt buộc là `error` vì tốc độ xe phải nằm trong ngưỡng vật lý thực tế [0, 70] m/s (0 - 252 km/h). Giá trị âm hoặc cực lớn (như 1000 m/s) là dữ liệu rác nghiêm trọng; nếu để lọt vào Gold để huấn luyện mô hình lái xe tự động sẽ dẫn đến tai nạn thảm khốc. Cổng chất lượng phải dừng pipeline ngay lập tức.

Kết quả thí nghiệm `break-gate` (`report/evidence/break_gate.json`):
- Khi nới luật làm sạch từ 70 m/s lên 1000 m/s, dbt test lập tức phát hiện vi phạm:
  - Test bắt được: `in_range_silver_telemetry_speed_mps__70__0`.
  - Số dòng vi phạm: Bắt chính xác **6 dòng** lỗi cấy sẵn (3 dòng ở drive A và 3 dòng ở drive B). Kết quả test chuyển thành: `PASS=5 WARN=1 ERROR=1 SKIP=0`.
  - Sau khi khôi phục luật gốc 70 m/s: Cổng kiểm tra đạt trở lại `PASS=6 WARN=1 ERROR=0 SKIP=0`.
- Vai trò của Cổng chất lượng (Quality Gate): Đóng vai trò là lá chắn an toàn độc lập (automated safety net). Dù logic chuyển đổi phía trên có bị sửa sai hoặc nới lỏng ngoài ý muốn, cổng kiểm tra tự động vẫn ngăn chặn dữ liệu bẩn lọt xuống lớp hạ nguồn (Gold, Warehouse).

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Số liệu từ thí nghiệm `idempotency.json` (chạy trên partition `trip-00003` có 60 khung):
- Ban đầu: Bảng `silver.frames` có 1246 dòng, dấu vân tay bảng (fingerprint) là `6c7480dfde7e`.
- Sau 2 lần chạy lại partition: Bảng vẫn giữ nguyên chính xác 1246 dòng, dấu vân tay vẫn là `6c7480dfde7e` (tính chất idempotent tuyệt đối, không đổi).
- Với cách ghi `append` sai lầm: Sau 2 lần chạy, trip bị nhân đôi từ 60 lên 120 dòng (tăng 2x duplicate).

Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại vì:
- Delta Lake đảm bảo tính nguyên tử (ACID transaction): thao tác overwrite có điều kiện `replace_where = "trip_id = '...'"` ghi các file Parquet mới vào storage và chỉ cập nhật metadata commit vào `_delta_log` khi toàn bộ quá trình thành công.
- Với cách xóa rồi ghi truyền thống: nếu quá trình gặp sự cố (mất kết nối mạng, crash node, tràn đĩa) ngay sau khi xóa hoặc đang ghi dở dang, partition đó sẽ bị mất trắng hoặc chỉ còn dữ liệu què quặt. Hơn nữa, người dùng đọc bảng trong lúc đó sẽ thấy dữ liệu bị thiếu. Delta overwrite đảm bảo nếu có lỗi giữa chừng, toàn bộ transaction bị rollback và phiên bản cũ vẫn nguyên vẹn.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình đen vs khung hình tối:
- Khung đen (Black frames): Độ sáng trung bình xấp xỉ 0 (mean brightness < 5) do che ống kính, lỗi nguồn camera hoặc hỏng cảm biến. Khung hình này hoàn toàn vô giá trị trực quan nên pipeline **gắn cờ và LOẠI BỎ** khỏi Gold (Drive A có 6 khung, Drive B có 4 khung).
- Khung tối (Low-light frames): Độ sáng thấp nhưng lớn hơn ngưỡng đen (xe đi trong đường hầm, dưới bóng râm). Khung này phản ánh điều kiện lái xe thực tế nên pipeline **gắn cờ cảnh báo (`is_low_light`) nhưng GIỮ LẠI** trong Gold (Drive A có 4 khung, Drive B có 61 khung).
- Tôi hoàn toàn đồng ý với cách xử lý này, vì mô hình thị giác xe tự lái cần học cách quan sát trong điều kiện thiếu sáng để phản xạ tốt khi di chuyển trong hầm hoặc ban đêm, nhưng tuyệt đối không được học trên các khung đen vô nghĩa.

Bảng số khung bị coi là trùng theo ngưỡng dHash (`threshold.json`):
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng **2** (ngưỡng mặc định của pipeline):
- Ngưỡng 0 quá chặt: chỉ bắt được các khung giống hệt nhau từng bit, bỏ sót nhiều khung thực tế là trùng do nhiễu hạt cảm biến (sensor noise) hoặc nén video làm dHash lệch 1-2 bit dù cảnh không đổi.
- Ngưỡng 6 quá lỏng: loại bỏ tới hơn một nửa số khung hình (363 ở A và 244 ở B), loại nhầm cả các khung cảnh có xe cộ di chuyển hoặc góc lái thay đổi thực sự.
- Ngưỡng 2 cân bằng hoàn hảo: loại 149 khung ở A và 53 ở B, bắt trọn đoạn camera đứng hình 6 giây ở A mà không làm mất thông tin cảnh mới.
- Mối liên hệ với tốc độ xe: Khi xe chạy nhanh, khung cảnh thay đổi liên tục giữa các frame (khoảng cách Hamming giữa các dHash lớn), tỷ lệ trùng rất thấp. Khi xe chạy chậm hoặc dừng chờ đèn đỏ (tốc độ ~ 0 km/h), khung cảnh liên tục bất động, sinh ra nhiều khung trùng dHash. Ngưỡng dHash giúp loại bỏ độ dư thừa dữ liệu ở các đoạn xe dừng đỗ.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Số liệu từ thí nghiệm `leakage.json` (tổng 1019 khung hình đáng dùng thuộc 26 trip):
- Chia THEO KHUNG HÌNH (Random 70/15/15): Toàn bộ **26/26 trip** bị chia cắt trên nhiều split, và **26/26 trip** có khung hình xuất hiện ở cả tập Train lẫn tập Test.
- Chia THEO TRIP (Gold của pipeline): **0 trip** bị chia cắt (mỗi trip nằm trọn vẹn trong duy nhất một split: Train, Val hoặc Test).

Điểm kiểm thử đẹp một cách giả tạo (Data Leakage) vì:
- Trong dữ liệu video xe tự lái (10 fps, lấy mẫu 2 fps), hai khung hình cách nhau 0.5 giây trong cùng một chuyến đi gần như giống hệt nhau về góc nhìn, thời tiết, ánh sáng, cảnh quan hai bên đường.
- Nếu chia theo khung hình, mô hình đã "học thuộc lòng" khung hình ở giây thứ $t$ tại tập Train, rồi được kiểm tra đúng khung hình ở giây $t+0.5$ tại tập Test. Mô hình đạt độ chính xác rất cao nhờ ghi nhớ bối cảnh quen thuộc chứ không phải nhờ khả năng tổng quát hóa (generalization). Khi đưa mô hình ra chạy thực tế trên một cung đường mới, mô hình sẽ phán đoán sai lệch nghiêm trọng.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Tại sao dùng băm nội dung:
- Đảm bảo tính bất biến (immutability) và khả năng tái lập (reproducibility) trong MLOps. Nếu dùng ngày giờ hoặc số đếm, hai lần chạy trên cùng một dữ liệu sẽ sinh ra hai phiên bản khác nhau, gây nhiễu loạn theo dõi thí nghiệm. Ngược lại, nếu dữ liệu thay đổi mà ngày giờ không đổi thì version lại không phản ánh đúng. Băm nội dung đảm bảo: Cùng nội dung = cùng version; Thay đổi dù chỉ một khung hình hoặc nhãn = version lập tức đổi.
- Hai lần chạy liên tiếp: Cho kết quả `dataset_version` **giống hệt nhau** (`4f8a54fa7d96`).
- Phiên bản của tôi khác bạn bè vì: Mã MSSV `2A202602454` trong `.env` được dùng làm muối (salt) trong câu lệnh SQL chia split: `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`. Các MSSV khác nhau sinh ra mã MD5 khác nhau cho các trip, dẫn đến thứ tự sắp xếp và việc phân bổ các trip vào train/val/test khác nhau. Do nội dung của các tập dữ liệu split khác nhau nên mã băm `dataset_version` cuối cùng là duy nhất cho từng sinh viên.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Nội dung lineage trong `manifest.json` của phiên bản `4f8a54fa7d96`:
- Video đầu vào gốc:
  - `drive_a`: SHA-256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  - `drive_b`: SHA-256 `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- Phiên bản bảng Delta thượng nguồn (Delta time-travel versions):
  - `bronze.video_files`: version 1
  - `bronze.trips_raw`: version 0
  - `bronze.raw_telemetry_a`: version 0
  - `bronze.raw_telemetry_b`: version 0
  - `silver.frames`: version 49
  - `silver.telemetry`: version 0
  - `silver.frames_flagged`: version 0
  - `silver.frame_telemetry`: version 0
- Quy tắc lọc (rules drop): Loại bỏ các khung `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`.

Để tái tạo lại chính xác 100% dataset này sau 3 tháng, cần 3 thông tin cốt lõi:
1. Mã băm SHA-256 của 2 file video gốc `drive_a.mp4` và `drive_b.mp4`.
2. Phiên bản commit cụ thể của các bảng Delta trong Lakehouse (`VERSION AS OF ...`).
3. Giá trị MSSV `LAB_STUDENT_ID` (`2A202602454`) dùng làm salt phân chia split và mã commit Git của pipeline dbt.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Quá trình xảy ra khi mô phỏng sập trip của tôi (`LAB_FAIL_ONCE=trip-01037`):
- Khi chạy DAG, task `silver_frames` với `map_index = 2` (tương ứng với `trip-01037`) chuyển **màu đỏ** (`failed`), do ném ra ngoại lệ `RuntimeError: simulated crash while extracting trip-01037`.
- Có **4 task** trực tiếp phía sau chuyển sang trạng thái **`upstream_failed`** (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`), các task còn lại ở trạng thái `None`.
- Xử lý: Thực hiện clear đúng task `silver_frames` (cùng các task downstream) qua lệnh `airflow tasks clear -d -t silver_frames -y day09_video_pipeline`.
- Sau khi clear: Task `silver_frames` [2] chạy lại thành công (do file cờ `.failed-trip-01037` đã tồn tại nên không kích hoạt lỗi lần 2), toàn bộ 26 task và các task hạ nguồn đều chuyển **xanh hoàn toàn**.
- Dữ liệu có nhân đôi không: **HOÀN TOÀN KHÔNG**, dữ liệu trong Delta Lake và Postgres warehouse giữ nguyên chính xác 1019 khung hình (nhờ cơ chế idempotent overwrite theo partition của Delta Lake).
- `dataset_version` có đổi không: **GIỮ NGUYÊN**, vẫn là `4f8a54fa7d96`.
- Bằng chứng: Ảnh chụp màn hình log task lỗi `airflow_task_failed.png` hiển thị rõ traceback và thông báo `RuntimeError: simulated crash while extracting trip-01037`, ảnh `airflow_graph.png` xanh toàn bộ, và kết quả kiểm tra `verify` đạt 27/27.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba thành phần sẽ vỡ đầu tiên khi mở rộng lên 500 xe/ngày (~hàng chục nghìn giờ video và hàng trăm triệu telemetry records mỗi ngày):
1. **Giải mã video trên CPU đơn luồng bằng FFmpeg (Node decode - Slide 24):**
   - Vấn đề: Giải mã video là nút cổ chai tốn tài nguyên tính toán nhất. Một container CPU không thể xử lý hàng chục terabyte video mỗi ngày mà không bị trễ nhiều tuần.
   - Thay thế: Xây dựng cụm giải mã video phân tán (Distributed Video Processing) tận dụng phần cứng GPU (NVIDIA NVDEC) thông qua Ray Core hoặc Apache Spark on Kubernetes, chia nhỏ video thành các đoạn ngắn (chunks) và giải mã song song trên hàng trăm workers.
2. **DuckDB cục bộ và Airflow LocalExecutor trên 1 máy ảo (Storage & Query Engine - Slide 22, 23):**
   - Vấn đề: DuckDB là cơ sở dữ liệu in-process chạy trên đĩa đơn của container; khi dữ liệu vượt ngưỡng RAM/disk, nó sẽ tràn bộ nhớ (OOM). Airflow LocalExecutor cũng không thể điều phối hàng chục nghìn task song song trên một máy.
   - Thay thế: Chuyển đổi lớp tính toán sang **Apache Spark** (Databricks) hoặc **Trino** kết hợp Delta Lake / Apache Iceberg trên Object Storage (S3 / GCS); chuyển Airflow sang **KubernetesExecutor** hoặc **CeleryExecutor** để tự động co giãn worker pods theo tải công việc.
3. **Thu thập dữ liệu dạng file CSV thô theo lô (Batch Ingestion layer - Slide 12, 17):**
   - Vấn đề: Gom telemetry thành file CSV lớn và upload theo batch gây độ trễ lớn, dễ nghẽn I/O và mất dữ liệu khi xe mất sóng 4G/5G.
   - Thay thế: Chuyển sang kiến trúc Streaming Ingestion hướng sự kiện: dùng **Apache Kafka** hoặc **MQTT Broker** nhận dữ liệu trực tiếp từ gateway xe, tuần tự hóa bằng định dạng nhị phân nhỏ gọn như **Protobuf / Apache Avro**, sau đó dùng Spark Structured Streaming / Flink ghi micro-batch trực tiếp vào Bronze Delta Lake.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
  trip_id,
  drive_id,
  frames_total,
  frames_kept,
  round((frames_total - frames_kept)::numeric / frames_total * 100, 1) as drop_rate_pct,
  split
FROM marts.mart_trip_summary
ORDER BY drop_rate_pct DESC;
```

Ý nghĩa và thông tin câu truy vấn cung cấp:
- Truy vấn tính toán **tỷ lệ khung hình bị loại bỏ (drop rate %)** của từng trip trên bảng `marts.mart_trip_summary` và sắp xếp theo thứ tự giảm dần.
- Giúp đội ngũ kỹ sư Data/AI nhanh chóng phát hiện các chuyến đi có tỷ lệ khung hình xấu bất thường (do rung lắc camera, che ống kính, xe dừng lâu gây trùng lặp dHash, hoặc đi qua đường hầm thiếu sáng). Điều này hỗ trợ kiểm định chất lượng camera theo từng hành trình và đánh giá độ cân bằng chất lượng dữ liệu giữa các tập train, val và test.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- Điều bất ngờ nhất: Thuật toán dHash 64-bit cực kỳ đơn giản và tính toán siêu nhanh trên CPU (chỉ resize ảnh về 9x8 rồi so sánh bit lân cận) nhưng lại phát hiện khung hình trùng lặp và lỗi camera đứng hình cực kỳ chuẩn xác mà không cần mô hình AI phức tạp.
- Điều chưa rõ: Cách pipeline xử lý tối ưu khi đồng hồ thiết bị trôi lệch động theo thời gian (dynamic clock drift phi tuyến tính do nhiệt độ hoặc lỗi bộ dao động tinh thể RTC), vì hiện tại pipeline chỉ trừ một giá trị offset tĩnh trong file manifest.
- Lỗi dữ liệu chưa bắt được: Tình huống camera bị bám vết bẩn, giọt nước mưa hoặc lóa sáng cục bộ (lens flare / occlusion). Khi đó độ nét (Laplacian variance) và độ sáng trung bình vẫn đạt chuẩn, dHash vẫn thay đổi khi xe chạy, nhưng thông tin thị giác tại góc bị che khuất sẽ làm giảm chất lượng mô hình nhận diện.
