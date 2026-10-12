"""BRONZE: land the raw files exactly as received. Append-only, nothing is cleaned or typed.

  video     -> s3://lake/bronze/video/<drive>/raw.mp4   (+ one row in bronze.video_files: sha256, ffprobe facts)
  telemetry -> bronze.raw_telemetry_a / raw_telemetry_b   one table per source, every cell a string, plus lineage columns
  trips     -> bronze.trips_raw

Idempotent: a file is identified by its sha256 (`_batch_id`). Ingesting it twice adds nothing.
"""
from __future__ import annotations
import csv
import hashlib
import json
import subprocess
from datetime import datetime, timezone

import pyarrow as pa

from . import config as C
from . import store


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ffprobe(path) -> dict:
    out = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                          "stream=codec_name,width,height,r_frame_rate,nb_frames:format=duration,size", "-of", "json", str(path)],
                         capture_output=True, text=True, check=True).stdout
    j = json.loads(out)
    num, den = j["streams"][0]["r_frame_rate"].split("/")
    return dict(codec=j["streams"][0]["codec_name"], width=int(j["streams"][0]["width"]), height=int(j["streams"][0]["height"]),
                fps=float(num) / float(den), duration_s=float(j["format"]["duration"]))


def _batch_seen(name: str, batch_id: str) -> bool:
    dt = store.open_table("bronze", name)
    return dt is not None and batch_id in set(dt.to_pyarrow_table(columns=["_batch_id"]).column("_batch_id").to_pylist())


def _csv_to_table(path, drive_id: str | None, batch_id: str) -> pa.Table:
    """Every value stays a string. Lineage columns are prefixed with an underscore."""
    now = datetime.now(timezone.utc)
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    cols = {k: [r[k] for r in rows] for k in reader.fieldnames}
    if drive_id:
        cols["drive_id"] = [drive_id] * len(rows)
    cols["_source_file"] = [path.name] * len(rows)
    cols["_batch_id"] = [batch_id] * len(rows)
    cols["_row_no"] = list(range(len(rows)))
    cols["_ingested_at"] = [now] * len(rows)
    return pa.table(cols, schema=pa.schema([(k, pa.timestamp("us", tz="UTC") if k == "_ingested_at" else
                                             pa.int64() if k == "_row_no" else pa.string()) for k in cols]))


def ingest_drive(drive_id: str) -> dict:
    store.ensure_bucket()
    src = json.loads((C.DATA_DIR / "sources.json").read_text())["drives"][drive_id]
    video, tele = C.DATA_DIR / src["video"], C.DATA_DIR / src["telemetry"]
    report = {"drive": drive_id}

    # 1. raw video -> object storage, one pointer row in bronze.video_files
    vsha = sha256_file(video)
    key = f"bronze/video/{drive_id}/raw.mp4"
    if not store.key_exists(key):
        store.s3().upload_file(str(video), C.BUCKET, key)
    if not _batch_seen("video_files", vsha[:12]):
        p = ffprobe(video)
        row = pa.table({"drive_id": [drive_id], "uri": [C.lake(key)], "sha256": [vsha], "bytes": [video.stat().st_size],
                        "video_start_utc": [src["video_start_utc"]], "telemetry_clock_offset_ms": [src["telemetry_clock_offset_ms"]],
                        "codec": [p["codec"]], "width": [p["width"]], "height": [p["height"]], "fps": [p["fps"]],
                        "duration_s": [p["duration_s"]], "_batch_id": [vsha[:12]],
                        "_ingested_at": [datetime.now(timezone.utc)]})
        store.write_table("bronze", "video_files", row)
        report["video"] = "ingested"
    else:
        report["video"] = "already present"

    # 2. telemetry CSV -> bronze.telemetry_raw (all strings)
    # one bronze table per SOURCE: drive A and B send different columns, so they never share a table
    tsha, tname = sha256_file(tele)[:12], f"raw_{tele.stem}"
    if not _batch_seen(tname, tsha):
        t = _csv_to_table(tele, drive_id, tsha)
        store.write_table("bronze", tname, t)
        report["telemetry_rows"] = t.num_rows
    else:
        report["telemetry_rows"] = "already present"
    return report


def ingest_trips() -> dict:
    store.ensure_bucket()
    path = C.DATA_DIR / "trips.csv"
    sha = sha256_file(path)[:12]
    if _batch_seen("trips_raw", sha):
        return {"trips": "already present"}
    t = _csv_to_table(path, None, sha)          # trips.csv already carries its own drive_id
    store.write_table("bronze", "trips_raw", t)
    return {"trips": t.num_rows}
