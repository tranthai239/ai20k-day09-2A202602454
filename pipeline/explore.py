"""Look at your data while you work: run SQL over every Delta table in the lake and every dbt table.

  scripts/lab.sh cli sql "select * from lake.bronze_raw_telemetry_b limit 5"
  scripts/lab.sh cli sql "select reject_reason, count(*) from silver.silver_telemetry_rejects group by 1"

Names: Delta tables are views `lake.<layer>_<name>` (e.g. lake.silver_frames, lake.gold_dataset_frames);
dbt tables are in their own schemas (staging.*, silver.*, gold.*, marts.*) once you have run dbt.
"""
from __future__ import annotations
from pathlib import Path

from . import config as C
from . import store


def sql(query: str, limit: int = 30) -> str:
    con = store.duck(C.DUCKDB_PATH if Path(C.DUCKDB_PATH).exists() else None)
    con.execute("create schema if not exists lake")
    s3 = store.s3()
    for layer in ("bronze", "silver", "gold"):
        for p in s3.list_objects_v2(Bucket=C.BUCKET, Prefix=f"{layer}/delta/", Delimiter="/").get("CommonPrefixes", []):
            name = p["Prefix"].rstrip("/").split("/")[-1]
            con.execute(f"create or replace view lake.{layer}_{name} as select * from delta_scan('{C.lake(layer, 'delta', name)}')")
    cur = con.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchmany(limit + 1)
    more = len(rows) > limit
    rows = [["" if v is None else str(v)[:40] for v in r] for r in rows[:limit]]
    w = [max([len(c)] + [len(r[i]) for r in rows]) for i, c in enumerate(cols)]
    line = lambda r: "  ".join(v.ljust(w[i]) for i, v in enumerate(r))
    out = [line(cols), "  ".join("-" * x for x in w)] + [line(r) for r in rows]
    if more:
        out.append(f"... (showing the first {limit} rows)")
    return "\n".join(out)
