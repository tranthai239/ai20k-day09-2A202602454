"""Self-check for the whole pipeline. Recomputes the expected numbers from the RAW files with separate code, then compares.

  python -m pipeline.cli verify            exit code 0 only if every check passes

Checks are grouped like the lab modules, so a student sees which module is wrong.
"""
from __future__ import annotations
import csv
import hashlib
import json

from . import config as C
from . import store

# Faults planted in the data pack (seconds on each drive's timeline). Same values as scripts/make_dataset.py.
BLACK = {"drive_a": [(100.0, 103.0)], "drive_b": [(60.0, 62.0)]}
FREEZE = {"drive_a": [(150.0, 156.0)]}
BLUR = {"drive_a": [(200.0, 204.0)]}
DARK = {"drive_b": [(140.0, 145.0)]}
TELEMETRY_FILES = {"drive_a": "telemetry_a.csv", "drive_b": "telemetry_b.csv"}


def _read_csv(path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _expected_telemetry(drive: str) -> dict:
    """Independent re-derivation of the quarantine counts straight from the CSV (no Delta, no dbt)."""
    rows = _read_csv(C.DATA_DIR / TELEMETRY_FILES[drive])
    spd_col = "speed_mps" if drive == "drive_a" else "spd_kmh"
    scale = 1.0 if drive == "drive_a" else 1 / 3.6
    seen, dup, missing, bad = set(), 0, 0, 0
    key_cols = ["ts_utc", "lat", "lon", "heading_deg", "acc_x", "acc_y", "steer"] if drive == "drive_a" else \
               ["timestamp_ms", "latitude", "longitude", "hdg", "ax", "ay", "steering"]
    for r in rows:
        k = tuple(r[c] for c in key_cols)
        if k in seen:
            dup += 1
            continue
        seen.add(k)
        if r[spd_col] == "":
            missing += 1
        elif not 0 <= float(r[spd_col]) * scale <= 70:
            bad += 1
    return {"rows": len(rows), "duplicate": dup, "speed_missing": missing, "speed_out_of_range": bad}


def _in(t_s: float, windows) -> bool:
    return any(a <= t_s < b for a, b in windows)


class Report:
    def __init__(self):
        self.items: list[tuple[str, str, bool, str]] = []
        self.module = ""

    def section(self, title: str):
        self.module = title

    def check(self, name: str, ok: bool, detail: str = ""):
        self.items.append((self.module, name, bool(ok), detail))

    def print(self) -> bool:
        w = max(len(n) for _, n, _, _ in self.items)
        last = None
        for m, n, ok, d in self.items:
            if m != last:
                print(f"\n{m}"); last = m
            print(f"  {'PASS' if ok else 'FAIL'}  {n:<{w}}  {d}")
        bad = [n for _, n, ok, _ in self.items if not ok]
        print(f"\n{len(self.items) - len(bad)}/{len(self.items)} checks passed" + (f"; failing: {', '.join(bad)}" if bad else ""))
        return not bad


def run(modules=None) -> bool:
    """modules: None = all five, or a set such as {2} to run only that module's checks."""
    r = Report()

    def _m1():
        r.section("Module 1 · bronze")
        for drive, fname in TELEMETRY_FILES.items():
            tbl = store.open_table("bronze", f"raw_{fname.removesuffix('.csv')}")
            n_csv = len(_read_csv(C.DATA_DIR / fname))
            r.check(f"bronze raw_telemetry rows == CSV rows ({drive})", tbl is not None and tbl.to_pyarrow_table().num_rows == n_csv,
                    f"csv={n_csv}")
        vids = store.read_table("bronze", "video_files").to_pylist()
        r.check("bronze keeps each video once (sha256 unique)", len({v["sha256"] for v in vids}) == len(vids) == 2, f"{len(vids)} rows")
        sha_ok = all(hashlib.sha256((C.DATA_DIR / f"{v['drive_id']}.mp4").read_bytes()).hexdigest() == v["sha256"] for v in vids)
        r.check("bronze video sha256 matches the original file", sha_ok)
        r.check("bronze trips == 26", store.read_table("bronze", "trips_raw").num_rows == 26)

    def _m2():
        r.section("Module 2 · silver telemetry")
        clean = store.read_table("silver", "telemetry").to_pylist()
        rej = store.read_table("silver", "telemetry_rejects").to_pylist()
        for drive in TELEMETRY_FILES:
            exp = _expected_telemetry(drive)
            got = {k: sum(1 for x in rej if x["drive_id"] == drive and x["reject_reason"] == k) for k in ("duplicate", "speed_missing", "speed_out_of_range")}
            r.check(f"quarantine counts match raw CSV ({drive})", all(got[k] == exp[k] for k in got), f"expected={ {k: exp[k] for k in got} } got={got}")
            n_clean = sum(1 for x in clean if x["drive_id"] == drive)
            r.check(f"clean + quarantined == received ({drive})", n_clean + sum(got.values()) == exp["rows"], f"{n_clean}+{sum(got.values())}={exp['rows']}")
        keys = [(x["drive_id"], x["ts"]) for x in clean]
        r.check("no duplicate (drive, ts) in silver telemetry", len(keys) == len(set(keys)))
        r.check("all speeds in 0..70 m/s", all(0 <= x["speed_mps"] <= 70 for x in clean))
        b = [x for x in clean if x["drive_id"] == "drive_b"]
        # drive B arrives in km/h with a +1.5 s clock: after conforming, its first timestamp must sit near the video start
        first_b = min(x["ts"] for x in b)
        r.check("drive B clock offset removed (first sample within 0.5 s of video start)",
                abs((first_b - __import__("datetime").datetime.fromisoformat("2025-10-08T14:30:00+00:00")).total_seconds()) < 0.5, str(first_b))

    def _m3():
        r.section("Module 3 · silver frames")
        fr = store.read_table("silver", "frames_flagged").to_pylist()
        ids = [f["frame_id"] for f in fr]
        r.check("frame_id unique", len(ids) == len(set(ids)), f"{len(ids)} frames")

        def flagged(drive, windows, flag):
            sel = [f for f in fr if f["drive_id"] == drive and _in(f["frame_idx"] / 10, windows)]
            return sel, [f for f in sel if f[flag]]
        for drive, w in BLACK.items():
            sel, hit = flagged(drive, w, "is_black")
            r.check(f"black frames detected ({drive})", sel and len(hit) == len(sel), f"{len(hit)}/{len(sel)}")
        for drive, w in BLUR.items():
            sel, hit = flagged(drive, w, "is_blurry")
            r.check(f"blurry frames detected ({drive})", sel and len(hit) >= 0.75 * len(sel), f"{len(hit)}/{len(sel)}")
        for drive, w in FREEZE.items():
            sel, hit = flagged(drive, w, "is_near_dup")
            r.check(f"frozen camera = near-duplicate frames ({drive})", sel and len(hit) >= len(sel) - 1, f"{len(hit)}/{len(sel)}")
        for drive, w in DARK.items():
            sel, hit = flagged(drive, w, "is_dark")
            r.check(f"low-light frames flagged but not black ({drive})", sel and len(hit) >= 0.8 * len(sel) and not any(f["is_black"] for f in sel), f"{len(hit)}/{len(sel)}")

    def _m4():
        r.section("Module 4 · gold dataset")
        gold = store.read_table("gold", "dataset_frames").to_pylist()
        r.check("gold has frames in every split", {g["split"] for g in gold} == {"train", "val", "test"}, f"{len(gold)} frames")
        by_trip: dict[str, set] = {}
        for g in gold:
            by_trip.setdefault(g["trip_id"], set()).add(g["split"])
        r.check("no trip is in two splits (no leakage)", all(len(s) == 1 for s in by_trip.values()), f"{len(by_trip)} trips")
        r.check("every drive appears in train, val and test", all({g["split"] for g in gold if g["drive_id"] == d} == {"train", "val", "test"} for d in TELEMETRY_FILES))
        ft = store.read_table("silver", "frame_telemetry").to_pylist()
        bad_ids = {f["frame_id"] for f in ft if f["is_black"] or f["is_blurry"] or f["is_near_dup"] or not f["sync_ok"]}
        r.check("gold contains no black / blurry / near-duplicate / out-of-sync frame", not (bad_ids & {g["frame_id"] for g in gold}))
        # manifest + images
        vers = {g["dataset_version"] for g in gold}
        ver = next(iter(vers))
        mj = json.loads(store.s3().get_object(Bucket=C.BUCKET, Key=f"gold/datasets/{ver}/manifest.json")["Body"].read())
        r.check("one dataset_version, manifest.json agrees with the table", len(vers) == 1 and mj["frames"] == len(gold), f"version {ver}")
        r.check("manifest lists upstream table versions (lineage)", "silver.frames" in mj["lineage"] and "input_video_sha256" in mj["lineage"])
        keys = {o["Key"] for p in store.s3().get_paginator("list_objects_v2").paginate(Bucket=C.BUCKET, Prefix=f"gold/datasets/{ver}/") for o in p.get("Contents", [])}
        r.check("every manifest image exists under the dataset version folder", all(g["image_path"].split(f"{C.BUCKET}/", 1)[1] in keys for g in gold))

    def _m5():
        r.section("Module 5 · warehouse")
        con = store.duck()
        try:
            con.execute(f"ATTACH 'host={C.PG_HOST} port={C.PG_PORT} dbname={C.PG_WAREHOUSE_DB} user={C.PG_USER} password={C.PG_PASSWORD}' AS wh (TYPE postgres, READ_ONLY)")
            tabs = {t: con.execute(f"select count(*) from wh.marts.{t}").fetchone()[0] for (t,) in
                    con.execute("select table_name from information_schema.tables where table_catalog='wh' and table_schema='marts'").fetchall()}
            r.check("warehouse has the six marts", len(tabs) >= 6, str(tabs))
            r.check("warehouse gallery rows == gold frames", tabs.get("mart_frame_gallery") == store.read_table("gold", "dataset_frames").num_rows)
        except Exception as e:  # noqa: BLE001
            r.check("warehouse reachable", False, str(e)[:120])

    for n, fn in enumerate([_m1, _m2, _m3, _m4, _m5], 1):
        if modules is not None and n not in modules:
            continue
        try:
            fn()
        except (Exception, SystemExit) as e:      # a missing table or an unfinished TODO is a failed check, not a crash
            r.section(f"Module {n}")
            r.check("module could not run", False, str(e)[:200])
    return r.print()


def idempotent(trip: str | None = None) -> bool:
    """Re-run one silver partition and prove nothing changed (same rows, same values; only _processed_at may differ)."""
    from . import frames
    trip = trip or frames.list_trips()[0]

    def fingerprint():
        rows = [x for x in store.read_table("silver", "frames").to_pylist()]
        body = sorted((x["frame_id"], x["trip_id"], round(x["brightness"], 6), round(x["blur_score"], 6), x["dhash"]) for x in rows)
        return len(rows), hashlib.sha256(json.dumps(body).encode()).hexdigest()[:12]
    before = fingerprint()
    frames.extract_trip(trip)
    after = fingerprint()
    ok = before == after
    print(f"  {'PASS' if ok else 'FAIL'}  re-running {trip}: rows/fingerprint before={before} after={after}")
    return ok
