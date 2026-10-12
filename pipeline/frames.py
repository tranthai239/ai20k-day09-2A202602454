"""SILVER (heavy step): decode one trip of video into sampled JPG frames + per-frame quality signals.

One call = one partition (a trip). It *replaces* that trip's rows in silver.frames, so a re-run after a
failure gives exactly the same table (idempotent), never duplicates.

Stored per frame: where the image is (frame_uri), when it was taken (event_time), and cheap signals the
quality gates and dedup use later: brightness, blur_score, dhash. No ML involved.
"""
from __future__ import annotations
import io
import os
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
from PIL import Image

from . import config as C
from . import store


def _lookup(trip_id: str) -> dict:
    trips = store.read_table("bronze", "trips_raw").to_pylist()
    t = next((r for r in trips if r["trip_id"] == trip_id), None)
    if t is None:
        raise SystemExit(f"unknown trip {trip_id}")
    v = next(r for r in store.read_table("bronze", "video_files").to_pylist() if r["drive_id"] == t["drive_id"])
    return {**t, "video_uri_key": v["uri"].split(f"{C.BUCKET}/", 1)[1], "video_start": datetime.fromisoformat(v["video_start_utc"])}


def list_trips() -> list[str]:
    return sorted(r["trip_id"] for r in store.read_table("bronze", "trips_raw", ["trip_id"]).to_pylist())


def dhash(gray: np.ndarray) -> int:
    """64-bit difference hash: shrink to 9x8, compare neighbours. Near-identical images differ in few bits."""
    im = Image.fromarray(gray.astype(np.uint8)).resize((9, 8), Image.BILINEAR)
    a = np.asarray(im, dtype=np.int16)
    bits = (a[:, 1:] > a[:, :-1]).flatten()
    v = 0
    for b in bits:
        v = (v << 1) | int(b)
    return v - (1 << 64) if v >= (1 << 63) else v      # signed so it fits Delta's BIGINT


def blur_score(gray: np.ndarray) -> float:
    """Variance of the Laplacian: low = few edges = blurry."""
    g = gray.astype(np.float32)
    lap = 4 * g[1:-1, 1:-1] - g[:-2, 1:-1] - g[2:, 1:-1] - g[1:-1, :-2] - g[1:-1, 2:]
    return float(lap.var())


def _fail_once(trip_id: str) -> None:
    """Exercise switch: LAB_FAIL_ONCE=<trip_id> makes the FIRST attempt for that trip crash half-way (a bad node, a full disk)."""
    marker = C.WORK_DIR / f".failed-{trip_id}"
    if os.getenv("LAB_FAIL_ONCE") == trip_id and not marker.exists():
        C.WORK_DIR.mkdir(parents=True, exist_ok=True); marker.write_text("failed once")
        raise RuntimeError(f"simulated crash while extracting {trip_id}; run it again")


def extract_trip(trip_id: str) -> dict:
    t = _lookup(trip_id)
    _fail_once(trip_id)
    start = datetime.fromisoformat(t["t_start_utc"])
    end = datetime.fromisoformat(t["t_end_utc"])
    offset = (start - t["video_start"]).total_seconds()
    duration = (end - start).total_seconds()
    drive = t["drive_id"]
    rows, s3 = [], store.s3()
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        raw = tmp / "raw.mp4"
        s3.download_file(C.BUCKET, t["video_uri_key"], str(raw))
        subprocess.run(["ffmpeg", "-v", "error", "-ss", f"{offset:.3f}", "-i", str(raw), "-t", f"{duration:.3f}",
                        "-vf", f"fps={C.SAMPLE_FPS}", "-q:v", "4", str(tmp / "f%05d.jpg")], check=True)
        for i, jpg in enumerate(sorted(tmp.glob("f*.jpg"))):
            sec = offset + i / C.SAMPLE_FPS                              # position on the drive timeline
            idx = int(round(sec * 10))                                    # source-video frame number (10 fps), unique per drive
            data = jpg.read_bytes()
            im = Image.open(io.BytesIO(data))
            gray = np.asarray(im.convert("L"))
            key = f"silver/frames/{drive}/{idx:05d}.jpg"
            s3.put_object(Bucket=C.BUCKET, Key=key, Body=data, ContentType="image/jpeg")
            rows.append(dict(frame_id=f"{drive}-{idx:05d}", drive_id=drive, trip_id=trip_id, frame_idx=idx,
                             event_time=t["video_start"] + timedelta(seconds=sec), frame_uri=C.lake(key),
                             width=im.width, height=im.height, jpg_bytes=len(data), brightness=float(gray.mean()),
                             blur_score=blur_score(gray), dhash=dhash(gray), _processed_at=datetime.now(timezone.utc)))
    table = pa.Table.from_pylist(rows, schema=pa.schema([
        ("frame_id", pa.string()), ("drive_id", pa.string()), ("trip_id", pa.string()), ("frame_idx", pa.int64()),
        ("event_time", pa.timestamp("us", tz="UTC")), ("frame_uri", pa.string()), ("width", pa.int64()), ("height", pa.int64()),
        ("jpg_bytes", pa.int64()), ("brightness", pa.float64()), ("blur_score", pa.float64()), ("dhash", pa.int64()),
        ("_processed_at", pa.timestamp("us", tz="UTC"))]))
    existing = store.open_table("silver", "frames")
    store.write_table("silver", "frames", table, mode="overwrite" if existing else "append",
                      predicate=f"trip_id = '{trip_id}'" if existing else None, partition_by=["drive_id"])
    return {"trip": trip_id, "frames": table.num_rows}
