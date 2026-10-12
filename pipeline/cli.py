"""Command line entry used by Airflow and by hand:  python -m pipeline.cli <command> [args]"""
import argparse
import json
import os
import sys

from . import bronze, experiments, explore, frames, publish, steps, store, verify


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("ingest"); p.add_argument("--drive", required=True)
    sub.add_parser("ingest-trips")
    sub.add_parser("list-trips")
    p = sub.add_parser("step"); p.add_argument("which", help="1..5 hoặc all"); p.add_argument("--reset", action="store_true")
    p = sub.add_parser("exp"); p.add_argument("name", choices=sorted(experiments.EXPERIMENTS)); p.add_argument("--trip")
    sub.add_parser("extract-all")
    p = sub.add_parser("sql"); p.add_argument("query"); p.add_argument("--limit", type=int, default=30)
    sub.add_parser("reset")
    p = sub.add_parser("verify"); p.add_argument("--module", type=int, action="append", help="only this module (1-5); repeatable")
    p = sub.add_parser("idempotent"); p.add_argument("--trip")
    p = sub.add_parser("publish-lake"); p.add_argument("--only", help="comma-separated table names, e.g. telemetry,telemetry_rejects")
    sub.add_parser("export-dataset"); sub.add_parser("publish-warehouse")
    p = sub.add_parser("history"); p.add_argument("table", help="layer.name, e.g. silver.frames")
    p = sub.add_parser("extract"); p.add_argument("--trip", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "ingest":
        out = bronze.ingest_drive(a.drive)
    elif a.cmd == "ingest-trips":
        out = bronze.ingest_trips()
    elif a.cmd == "verify":
        ok = verify.run(set(a.module) if a.module else None); print(json.dumps({"ok": ok})); return 0 if ok else 1
    elif a.cmd == "idempotent":
        ok = verify.idempotent(a.trip); print(json.dumps({"ok": ok})); return 0 if ok else 1
    elif a.cmd == "publish-lake":
        out = publish.publish_lake(a.only.split(",") if a.only else None)
    elif a.cmd == "export-dataset":
        out = publish.export_dataset()
    elif a.cmd == "publish-warehouse":
        out = publish.publish_warehouse()
    elif a.cmd == "history":
        out = publish.history(*a.table.split("."))
    elif a.cmd == "reset":
        store.ensure_bucket(); out = {"deleted_objects": store.purge_lake()}
    elif a.cmd == "step":
        ok = steps.run(a.which, a.reset); print(json.dumps({"ok": ok})); return 0 if ok else 1
    elif a.cmd == "exp":
        ok = experiments.EXPERIMENTS[a.name](a.trip) if a.name == "idempotency" else experiments.EXPERIMENTS[a.name]()
        print(json.dumps({"ok": ok})); return 0 if ok else 1
    elif a.cmd == "sql":
        print(explore.sql(a.query, a.limit), flush=True); return 0
    elif a.cmd == "extract-all":
        out = [frames.extract_trip(t) for t in frames.list_trips()]
        out = {"trips": len(out), "frames": sum(o["frames"] for o in out)}
    elif a.cmd == "list-trips":
        out = frames.list_trips()
    elif a.cmd == "extract":
        out = frames.extract_trip(a.trip)
    print(json.dumps(out, default=str), flush=True)
    return 0


if __name__ == "__main__":
    rc = main()
    # deltalake's Rust runtime can hang the interpreter at shutdown (seen on arm64, Python 3.13). The work is finished
    # and flushed, so leave immediately instead of letting Airflow wait on a process that never exits.
    sys.stdout.flush(); sys.stderr.flush()
    os._exit(rc)
