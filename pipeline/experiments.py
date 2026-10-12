"""Bốn thí nghiệm ngắn. Mỗi thí nghiệm in kết quả và ghi bằng chứng vào report/evidence/<tên>.json để bạn trích dẫn trong báo cáo.

  scripts/lab.sh exp idempotency [--trip trip-xxxxx]   chạy lại một partition: số dòng đổi hay không? so với kiểu ghi 'append'
  scripts/lab.sh exp break-gate                        nới một luật làm sạch: cổng dbt test có bắt được không?
  scripts/lab.sh exp leakage                           chia theo khung hình và chia theo trip: bao nhiêu trip bị rò rỉ?
  scripts/lab.sh exp threshold                         ngưỡng dHash: bao nhiêu khung bị coi là trùng?

Cần chạy `scripts/lab.sh step all` trước.
"""
from __future__ import annotations
import json
import os
import re
from datetime import datetime, timezone

import pyarrow as pa

from . import explore, frames, store, verify
from . import config as C
from .steps import _dbt, _report_dir


def _save(name: str, data: dict):
    data = {"experiment": name, "student_id": os.getenv("LAB_STUDENT_ID", ""), "ran_at": datetime.now(timezone.utc).isoformat(), **data}
    p = _report_dir() / "evidence" / f"{name}.json"
    p.write_text(json.dumps(data, indent=2, default=str))
    print(f"\nbằng chứng đã ghi: {p}")


def _fingerprint():
    rows = store.read_table("silver", "frames").to_pylist()
    body = sorted((x["frame_id"], x["trip_id"], round(x["brightness"], 6), round(x["blur_score"], 6), x["dhash"]) for x in rows)
    return len(rows), __import__("hashlib").sha256(json.dumps(body).encode()).hexdigest()[:12]


def idempotency(trip: str | None = None) -> bool:
    trip = trip or frames.list_trips()[0]
    print(f"THÍ NGHIỆM 1 · Chạy lại partition {trip}\n")
    n0, f0 = _fingerprint()
    frames.extract_trip(trip)
    frames.extract_trip(trip)
    n1, f1 = _fingerprint()
    print(f"Cách của pipeline (Delta overwrite theo predicate trip_id):")
    print(f"  trước : {n0} dòng, dấu vân tay {f0}")
    print(f"  sau 2 lần chạy lại: {n1} dòng, dấu vân tay {f1}   -> {'KHÔNG ĐỔI' if (n0, f0) == (n1, f1) else 'ĐÃ ĐỔI'}")
    # cách sai: ghi 'append' hai lần vào một bảng thử
    mine = [r for r in store.read_table("silver", "frames").to_pylist() if r["trip_id"] == trip]
    scratch = "_scratch_append"
    for _ in range(2):
        store.write_table("silver", scratch, pa.Table.from_pylist(mine), mode="append")
    n_app = store.read_table("silver", scratch).num_rows
    print(f"\nCách sai (append, chạy 2 lần, chỉ một trip): {n_app} dòng cho một trip có {len(mine)} khung -> NHÂN ĐÔI ({n_app / len(mine):.0f}x)")
    for p in store.s3().get_paginator("list_objects_v2").paginate(Bucket=C.BUCKET, Prefix=f"silver/delta/{scratch}/"):
        keys = [{"Key": o["Key"]} for o in p.get("Contents", [])]
        if keys:
            store.s3().delete_objects(Bucket=C.BUCKET, Delete={"Objects": keys})
    _save("idempotency", {"trip": trip, "rows_before": n0, "rows_after_two_reruns": n1, "fingerprint_before": f0, "fingerprint_after": f1,
                          "unchanged": (n0, f0) == (n1, f1), "rows_of_trip": len(mine), "append_variant_rows_after_two_runs": n_app})
    return (n0, f0) == (n1, f1)


def break_gate() -> bool:
    print("THÍ NGHIỆM 2 · Nới luật làm sạch rồi xem cổng chất lượng\n")
    print("Luật bình thường: tốc độ hợp lệ là 0..70 m/s (max_speed_mps = 70). Nới lên 1000 m/s: các giá trị vô lý lọt vào silver.\n")
    _dbt("run", "--select", "silver_telemetry_classified", "silver_telemetry", "--vars", "{max_speed_mps: 1000}")
    code, summary, out = _dbt("test", "--select", "silver_telemetry")
    failed = re.findall(r"\d+ of \d+ FAIL (\d+) (\S+)", out)
    print(f"dbt test sau khi nới luật: {summary}")
    for rows, name in failed:
        print(f"  FAIL: {name} bắt được {rows} dòng")
    _dbt("run", "--select", "silver_telemetry_classified", "silver_telemetry")
    code2, summary2, _ = _dbt("test", "--select", "silver_telemetry")
    print(f"\nKhôi phục luật gốc: {summary2}")
    _save("break_gate", {"relaxed_max_speed_mps": 1000, "gate_failed": bool(failed), "failing_tests": [{"test": n, "rows": int(r)} for r, n in failed],
                         "summary_relaxed": summary, "summary_restored": summary2})
    return bool(failed) and code2 == 0


def leakage() -> bool:
    print("THÍ NGHIỆM 3 · Chia theo khung hình có làm rò rỉ không?\n")
    con = store.duck(C.DUCKDB_PATH)
    kept = ("select * from silver.silver_frame_telemetry where not is_black and not is_blurry and not is_near_dup and not sync_lost")
    con.execute(f"create or replace temp table kept as {kept}")
    con.execute("""create or replace temp table per_frame as select trip_id, frame_id,
        case when abs(hash(frame_id)) % 100 < 70 then 'train' when abs(hash(frame_id)) % 100 < 85 then 'val' else 'test' end as split from kept""")
    n_frames, n_trips = con.execute("select count(*), count(distinct trip_id) from kept").fetchone()
    straddle, = con.execute("select count(*) from (select trip_id from per_frame group by 1 having count(distinct split) > 1)").fetchone()
    both, = con.execute("select count(*) from (select trip_id from per_frame group by 1 having count(*) filter (where split='train') > 0 and count(*) filter (where split='test') > 0)").fetchone()
    per_trip, = con.execute("select count(*) from (select trip_id from gold.gold_dataset_frames group by 1 having count(distinct split) > 1)").fetchone()
    print(f"Khung hình đáng dùng: {n_frames}, thuộc {n_trips} trip")
    print(f"Chia THEO KHUNG HÌNH (ngẫu nhiên 70/15/15): {straddle} trip nằm ở nhiều hơn một split, {both} trip có khung ở cả train lẫn test")
    print(f"Chia THEO TRIP (gold của pipeline): {per_trip} trip bị chia cắt")
    print("\nVì sao quan trọng: các khung liền nhau gần như giống hệt; nếu một nửa nằm ở train và nửa kia ở test, mô hình 'đã thấy' đề thi.")
    _save("leakage", {"kept_frames": n_frames, "trips": n_trips, "per_frame_split_trips_straddling": straddle,
                      "per_frame_split_trips_in_both_train_and_test": both, "per_trip_split_trips_straddling": per_trip})
    return per_trip == 0


def threshold() -> bool:
    print("THÍ NGHIỆM 4 · Độ nhạy của ngưỡng khung trùng (dHash, số bit được phép khác)\n")
    ths = [0, 1, 2, 4, 6, 8]
    cols = ", ".join(f"count(*) filter (where h <= {t}) as t{t}" for t in ths)
    sql = (f"with d as (select drive_id, bit_count(xor(dhash, lag(dhash) over (partition by trip_id order by frame_idx))) as h "
           f"from silver.silver_frames_flagged) select drive_id, count(*) as tong_khung, {cols} from d group by 1 order by 1")
    print(explore.sql(sql))
    con = store.duck(C.DUCKDB_PATH)
    rows = con.execute(sql).fetchall()
    print("\nĐọc: t2 là ngưỡng đang dùng. Ngưỡng cao thì loại nhiều khung hơn (có thể loại cả khung khác nhau thật), ngưỡng thấp thì giữ lại cả khung gần như trùng.")
    _save("threshold", {"thresholds": ths, "per_drive": [dict(zip(["drive", "total"] + [f"within_{t}_bits" for t in ths], r)) for r in rows],
                        "threshold_in_use": 2})
    return True


EXPERIMENTS = {"idempotency": idempotency, "break-gate": break_gate, "leakage": leakage, "threshold": threshold}
