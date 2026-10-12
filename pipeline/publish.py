"""Hand the results of the dbt run to the three places they belong.

  publish_lake       silver + gold tables  -> Delta tables in the lake (every run = a new table VERSION: time travel)
  export_dataset     gold frames           -> s3://lake/gold/datasets/<version>/{train,val,test}/*.jpg + manifest
  publish_warehouse  marts                 -> Postgres schema `marts` (what Metabase reads)

DuckDB (work/lakehouse.duckdb) is only the engine: delete the file and `dbt run` rebuilds it from the lake.
"""
from __future__ import annotations
import hashlib
import io
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from . import config as C
from . import store

SILVER = {"telemetry": "silver.silver_telemetry", "telemetry_rejects": "silver.silver_telemetry_rejects",
          "trips": "silver.silver_trips", "frames_flagged": "silver.silver_frames_flagged",
          "frame_telemetry": "silver.silver_frame_telemetry"}
GOLD = {"dataset_frames": "gold.gold_dataset_frames"}


def _engine():
    return store.duck(C.DUCKDB_PATH)


def publish_lake(only: list[str] | None = None) -> dict:
    """Write the dbt tables to the lake as Delta. `only` limits it to some table names (useful while a module is unfinished)."""
    con, out = _engine(), {}
    for layer, tables in (("silver", SILVER), ("gold", GOLD)):
        for name, rel in tables.items():
            if only and name not in only:
                continue
            t = con.execute(f"select * from {rel}").fetch_arrow_table()
            store.write_table(layer, name, t, mode="overwrite", replace_schema=True)
            out[f"{layer}.{name}"] = {"rows": t.num_rows, "version": store.open_table(layer, name).version()}
    return out


def _lineage() -> dict:
    """Which versions of which tables fed this dataset: the answer to 'what was this model trained on?'."""
    lin = {}
    for layer, name in [("bronze", "raw_telemetry_a"), ("bronze", "raw_telemetry_b"), ("bronze", "trips_raw"), ("bronze", "video_files"),
                        ("silver", "frames"), ("silver", "telemetry"), ("silver", "frames_flagged"), ("silver", "frame_telemetry")]:
        dt = store.open_table(layer, name)
        lin[f"{layer}.{name}"] = dt.version() if dt else None
    vids = store.read_table("bronze", "video_files").to_pylist()
    lin["input_video_sha256"] = {v["drive_id"]: v["sha256"] for v in vids}
    return lin


def _public_policy():
    """Lab only: let a browser read frame thumbnails from MinIO (Metabase shows them by URL)."""
    pol = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": ["*"]}, "Action": ["s3:GetObject"],
           "Resource": [f"arn:aws:s3:::{C.BUCKET}/silver/frames/*", f"arn:aws:s3:::{C.BUCKET}/gold/datasets/*"]}]}
    store.s3().put_bucket_policy(Bucket=C.BUCKET, Policy=json.dumps(pol))


def export_dataset() -> dict:
    con = _engine()
    rows = con.execute("select * from gold.gold_dataset_frames order by frame_id").fetch_arrow_table()
    ids, splits = rows.column("frame_id").to_pylist(), rows.column("split").to_pylist()
    version = hashlib.sha256("\n".join(f"{i}|{s}" for i, s in zip(ids, splits)).encode()).hexdigest()[:12]
    prefix, s3 = f"gold/datasets/{version}", store.s3()
    uris = rows.column("frame_uri").to_pylist()

    def copy(args):
        uri, split, fid = args
        dst = f"{prefix}/{split}/{fid}.jpg"
        if not store.key_exists(dst):
            s3.copy_object(Bucket=C.BUCKET, Key=dst, CopySource={"Bucket": C.BUCKET, "Key": uri.split(f"{C.BUCKET}/", 1)[1]})
        return dst
    with ThreadPoolExecutor(8) as ex:
        paths = list(ex.map(copy, zip(uris, splits, ids)))
    manifest = rows.append_column("image_path", pa.array([C.lake(p) for p in paths])).append_column(
        "dataset_version", pa.array([version] * rows.num_rows))
    buf = io.BytesIO(); pq.write_table(manifest, buf)
    s3.put_object(Bucket=C.BUCKET, Key=f"{prefix}/manifest.parquet", Body=buf.getvalue())
    info = {"dataset_version": version, "frames": rows.num_rows, "by_split": dict(Counter(splits)),
            "by_drive": dict(Counter(rows.column("drive_id").to_pylist())),
            "trips_by_split": {s: len({t for t, sp in zip(rows.column("trip_id").to_pylist(), splits) if sp == s}) for s in set(splits)},
            "manifest_sha256": hashlib.sha256(buf.getvalue()).hexdigest(),
            "rules": {"drop": ["is_black", "is_blurry", "is_near_dup", "not sync_ok"], "split": "per trip, stratified by drive, md5 order"},
            "lineage": _lineage(), "created_at": datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=C.BUCKET, Key=f"{prefix}/manifest.json", Body=json.dumps(info, indent=2, default=str).encode())
    # also record this dataset version in the Delta table (history of all versions ever built)
    store.write_table("gold", "dataset_frames", manifest, mode="overwrite", replace_schema=True)
    _public_policy()
    return {k: info[k] for k in ("dataset_version", "frames", "by_split", "trips_by_split", "manifest_sha256")}


def publish_warehouse() -> dict:
    con = _engine()
    con.execute(f"ATTACH 'host={C.PG_HOST} port={C.PG_PORT} dbname={C.PG_WAREHOUSE_DB} user={C.PG_USER} password={C.PG_PASSWORD}' AS wh (TYPE postgres)")
    con.execute("CREATE SCHEMA IF NOT EXISTS wh.marts")
    out = {}
    for (t,) in con.execute("select table_name from information_schema.tables where table_schema='marts' and table_catalog<>'wh' order by 1").fetchall():
        con.execute(f"CREATE OR REPLACE TABLE wh.marts.{t} AS SELECT * FROM marts.{t}")
        out[t] = con.execute(f"select count(*) from wh.marts.{t}").fetchone()[0]
    return out


def history(layer: str, name: str, n: int = 8) -> list[dict]:
    """Delta time travel: the commit log of one table."""
    dt = store.open_table(layer, name)
    return [{"version": h.get("version"), "operation": h.get("operation"),
             "rows_added": (h.get("operationMetrics") or {}).get("num_added_rows"),
             "predicate": (h.get("operationParameters") or {}).get("predicate")} for h in dt.history(n)]
