"""Chạy TỪNG cấu phần của pipeline và in kết quả để bạn kiểm tra.

  scripts/lab.sh step 1          Bronze
  scripts/lab.sh step 2          Silver telemetry + cổng chất lượng
  scripts/lab.sh step 3          Silver frames
  scripts/lab.sh step 4          Gold
  scripts/lab.sh step 5          Warehouse
  scripts/lab.sh step all        cả năm bước, rồi ghi report/evidence/run_summary.json
  scripts/lab.sh step all --reset    xoá lake trước

Mỗi bước: chạy, in các con số quan trọng, nói bạn cần thấy gì, rồi chạy `verify` của module đó.
"""
from __future__ import annotations
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import bronze, explore, frames, publish, store, verify
from . import config as C


def _report_dir() -> Path:
    d = Path(os.getenv("REPORT_DIR", "/opt/lab/report"))
    if not d.parent.exists():
        d = Path(__file__).resolve().parent.parent / "report"
    (d / "evidence").mkdir(parents=True, exist_ok=True)
    return d


def _dbt(*args: str) -> tuple[int, str, str]:
    """Chạy dbt; trả về (mã thoát, dòng tóm tắt, toàn bộ đầu ra)."""
    dbt = str(Path(sys.executable).parent / "dbt")
    cwd = os.getenv("DBT_DIR", str(Path(__file__).resolve().parent.parent / "dbt"))
    r = subprocess.run([dbt, *args, "--no-use-colors"], cwd=cwd, capture_output=True, text=True)
    m = re.findall(r"Done\. (PASS=\d+ WARN=\d+ ERROR=\d+ SKIP=\d+)", r.stdout)
    return r.returncode, (m[-1] if m else "không có tóm tắt"), r.stdout


def _title(t: str):
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}", flush=True)


def _show(title: str, sql: str, limit: int = 30):
    if title:
        print(f"\n> {title}", flush=True)
    print(explore.sql(sql, limit), flush=True)


def _dbt_step(label: str, *args: str) -> bool:
    code, summary, out = _dbt(*args)
    print(f"  dbt {label}: {summary}", flush=True)
    if code:
        print(out[-1500:])
    return code == 0


def _verify(n: int) -> bool:
    print(f"\n--- Kiểm tra tự động của module {n} (verify --module {n}) ---", flush=True)
    return verify.run({n})


def step1() -> bool:
    _title("Bước 1 · BRONZE: ghi nguyên bản, chỉ thêm, chạy lại không đổi gì")
    for d in ("drive_a", "drive_b"):
        print(" ", bronze.ingest_drive(d), flush=True)
    print(" ", bronze.ingest_trips(), flush=True)
    print("  (chạy lại ngay một lần nữa, mọi thứ phải báo 'already present'):")
    print(" ", bronze.ingest_drive("drive_a"), flush=True)
    _show("Số dòng của từng bảng bronze", " union all ".join(
        f"select '{t}' as bang, count(*) as so_dong from lake.bronze_{t}" for t in
        ("raw_telemetry_a", "raw_telemetry_b", "trips_raw", "video_files")))
    _show("Hai nguồn telemetry khác nhau thế nào (3 dòng đầu mỗi bảng, cột thô)",
          "select * from lake.bronze_raw_telemetry_a limit 2", 2)
    _show("", "select * from lake.bronze_raw_telemetry_b limit 2", 2)
    print("\nCẦN THẤY: 3021 dòng (A) và 3234 dòng (B); B dùng tên cột, đơn vị và timestamp khác A; chạy lại không thêm dòng nào.")
    return _verify(1)


def step2() -> bool:
    _title("Bước 2 · SILVER TELEMETRY: hai schema thành một, cách ly dòng lỗi, cổng chất lượng")
    ok = _dbt_step("run (staging + silver telemetry)", "run", "--select", "stg_telemetry_a", "stg_telemetry_b",
                   "silver_telemetry_classified", "silver_telemetry", "silver_telemetry_rejects")
    ok &= _dbt_step("test (cổng chất lượng của silver_telemetry)", "test", "--select", "silver_telemetry")
    print("  WARN ở test max_gap_ms là ĐÚNG: nó báo cảm biến mất tín hiệu nhưng không dừng pipeline.")
    print(" ", publish.publish_lake(["telemetry", "telemetry_rejects"]))
    _show("Dòng bị cách ly (quarantine) theo lý do", "select drive_id, reject_reason, count(*) as so_dong "
          "from silver.silver_telemetry_rejects group by 1, 2 order by 1, 2")
    _show("Sau khi chuẩn hoá: hai nguồn cùng đơn vị (m/s) và cùng đồng hồ",
          "select drive_id, count(*) as dong_sach, min(ts) as bat_dau, round(max(speed_mps) * 3.6) as toc_do_toi_da_kmh "
          "from silver.silver_telemetry group by 1 order by 1")
    print("\nCẦN THẤY: mỗi drive 20 dòng trùng, 30 dòng thiếu tốc độ, 3 dòng tốc độ vô lý; tốc độ tối đa hợp lý (dưới 130 km/h); "
          "thời điểm bắt đầu của B trùng thời điểm bắt đầu video (đã trừ độ lệch đồng hồ 1,5 giây).")
    return ok & _verify(2)


def step3() -> bool:
    _title("Bước 3 · SILVER FRAMES: video thành khung hình, cờ chất lượng, join theo event time")
    res = [frames.extract_trip(t) for t in frames.list_trips()]
    print(f"  đã xử lý {len(res)} trip, tổng {sum(r['frames'] for r in res)} khung hình (mỗi trip một partition)", flush=True)
    ok = _dbt_step("run (trips + cờ + join)", "run", "--select", "silver_trips", "silver_frames_flagged", "silver_frame_telemetry")
    ok &= _dbt_step("test", "test", "--select", "silver_trips", "silver_frames_flagged", "silver_frame_telemetry")
    print(" ", publish.publish_lake(["frames_flagged", "frame_telemetry", "trips"]))
    _show("Khung hình đi đâu (một khung có thể bị nhiều cờ)", "select drive_id, count(*) as tong, "
          "count(*) filter (where is_black) as den, count(*) filter (where is_blurry) as mo, "
          "count(*) filter (where is_dark) as toi_giu_lai, count(*) filter (where is_near_dup) as trung, "
          "count(*) filter (where sync_lost) as mat_dong_bo from silver.silver_frame_telemetry group by 1 order by 1")
    _show("Độ trễ đồng bộ giữa khung hình và telemetry (ms)", "select drive_id, round(avg(sync_gap_ms)) as tb_ms, "
          "max(sync_gap_ms) as toi_da_ms from silver.silver_frame_telemetry group by 1 order by 1")
    print("\nCẦN THẤY: drive A có 6 khung đen, drive B có 4; 'tối_giữ_lại' của B lớn (đường hầm, bóng râm): tối KHÔNG phải đen và không bị bỏ; "
          "khung trùng nhiều (xe chạy chậm, camera đứng hình 6 giây ở A); độ trễ đồng bộ trung bình nhỏ.")
    return ok & _verify(3)


def step4() -> bool:
    _title("Bước 4 · GOLD: dataset huấn luyện, chia theo trip, có phiên bản")
    ok = _dbt_step("run (gold)", "run", "--select", "gold_dataset_frames")
    ok &= _dbt_step("test (gồm test chống rò rỉ split)", "test", "--select", "gold_dataset_frames")
    info = publish.export_dataset()
    print(" ", json.dumps({k: info[k] for k in ("dataset_version", "frames", "by_split", "trips_by_split")}), flush=True)
    _show("Mỗi split có bao nhiêu khung hình và trip, ở từng drive", "select drive_id, split, count(*) as khung_hinh, "
          "count(distinct trip_id) as trip, round(avg(speed_mps) * 3.6) as toc_do_tb_kmh from gold.gold_dataset_frames "
          "group by 1, 2 order by 1, case split when 'train' then 1 when 'val' then 2 else 3 end")
    sid = os.getenv("LAB_STUDENT_ID", "")
    print(f"\nMSSV dùng để chia split: {sid or '(chưa đặt: hãy đặt LAB_STUDENT_ID trong file .env)'}")
    print("CẦN THẤY: mỗi drive có đủ train, val, test; một trip chỉ nằm trong MỘT split; dataset_version là chuỗi 12 ký tự "
          "(đổi MSSV thì đổi version).")
    return ok & _verify(4)


def step5() -> bool:
    _title("Bước 5 · WAREHOUSE: mart vào Postgres cho dashboard")
    ok = _dbt_step("run (marts)", "run", "--select", "path:models/marts")
    print(" ", publish.publish_warehouse())
    print("\nCẦN THẤY: 6 mart trong Postgres (schema marts). Tiếp theo: chạy cả DAG trong Airflow (http://localhost:8080) và mở Metabase.")
    return ok & _verify(5)


STEPS = {1: step1, 2: step2, 3: step3, 4: step4, 5: step5}


def run(which: str, reset: bool = False) -> bool:
    if reset:
        store.ensure_bucket()
        print(f"đã xoá {store.purge_lake()} đối tượng trong lake")
    if which == "all":
        results = {n: STEPS[n]() for n in sorted(STEPS)}
        _title("TỔNG KẾT")
        for n, ok in results.items():
            print(f"  Bước {n}: {'ĐẠT' if ok else 'CHƯA ĐẠT'}")
        all_ok = all(results.values())
        print("\n--- Kiểm tra toàn bộ ---")
        all_ok &= verify.run()
        _write_summary(all_ok)
        return all_ok
    return STEPS[int(which)]()


def _write_summary(ok: bool):
    con = store.duck(C.DUCKDB_PATH)
    q = lambda s: con.execute(s).fetchall()
    ver = _latest_version()
    gold = json.loads(store.s3().get_object(Bucket=C.BUCKET, Key=f"gold/datasets/{ver}/manifest.json")["Body"].read())
    summary = {
        "student_id": os.getenv("LAB_STUDENT_ID", ""), "finished_at": datetime.now(timezone.utc).isoformat(),
        "verify_all_passed": ok, "dataset_version": gold["dataset_version"], "frames": gold["frames"], "by_split": gold["by_split"],
        "trips_by_split": gold["trips_by_split"],
        "quarantine": {f"{d}:{r}": n for d, r, n in q("select drive_id, reject_reason, count(*) from silver.silver_telemetry_rejects group by 1, 2")},
        "frame_funnel": [dict(zip(("drive", "total", "black", "blurry", "dark_kept", "near_dup", "sync_lost"), r)) for r in q(
            "select drive_id, count(*), count(*) filter (where is_black), count(*) filter (where is_blurry), "
            "count(*) filter (where is_dark), count(*) filter (where is_near_dup), count(*) filter (where sync_lost) "
            "from silver.silver_frame_telemetry group by 1 order by 1")],
    }
    out = _report_dir() / "evidence" / "run_summary.json"
    out.write_text(json.dumps(summary, indent=2, default=str))
    print(f"\nđã ghi {out}")


def _latest_version() -> str:
    objs = store.s3().list_objects_v2(Bucket=C.BUCKET, Prefix="gold/datasets/", Delimiter="/").get("CommonPrefixes", [])
    versions = []
    for p in objs:
        v = p["Prefix"].split("/")[-2]
        t = store.s3().head_object(Bucket=C.BUCKET, Key=f"gold/datasets/{v}/manifest.json")["LastModified"]
        versions.append((t, v))
    return max(versions)[1]
