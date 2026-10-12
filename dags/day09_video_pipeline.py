"""Day 9 pipeline: raw dashcam video + telemetry -> bronze -> silver -> quality gate -> gold -> warehouse.

Airflow only ORCHESTRATES: every task is one command of the `pipeline` package or of dbt, run in the lab virtualenv.
The graph:

  bronze_drive_a ─┐
  bronze_drive_b ─┼─> trip_commands ─> silver_frames[trip x26] -> dbt_run_silver -> dbt_test_silver ─(gate)─> dbt_run_gold -> dbt_test_gold
  bronze_trips  ──┘                                                                                 │
                                                          publish_lake -> export_dataset -> publish_warehouse
"""
import json
import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

PY = "/home/airflow/lab-venv/bin/python"
LAB = f"cd /opt/lab && {PY} -m pipeline.cli"
DBT = f"cd /opt/dbt && /home/airflow/lab-venv/bin/dbt"
ARGS = "--no-use-colors"

with DAG(
    dag_id="day09_video_pipeline",
    description="video + telemetry -> lakehouse (bronze/silver/gold) -> warehouse",
    start_date=datetime(2025, 1, 1), schedule=None, catchup=False, max_active_runs=1,
    default_args={"retries": 0}, tags=["day09", "lab"],
) as dag:
    # ---- bronze: land raw files untouched. Safe to re-run: files are identified by sha256.
    bronze = [BashOperator(task_id=f"bronze_{d}", bash_command=f"{LAB} ingest --drive {d}") for d in ("drive_a", "drive_b")]
    bronze_trips = BashOperator(task_id="bronze_trips", bash_command=f"{LAB} ingest-trips")

    # ---- silver, heavy part: one task per trip (a partition). Re-running one trip replaces only its own rows.
    @task
    def trip_commands() -> list[str]:
        out = subprocess.check_output([PY, "-m", "pipeline.cli", "list-trips"], cwd="/opt/lab", text=True)
        return [f"{LAB} extract --trip {t}" for t in json.loads(out.strip().splitlines()[-1])]

    trips = trip_commands()                       # the list of partitions is only known once bronze has landed the trips table
    silver_frames = BashOperator.partial(task_id="silver_frames", max_active_tis_per_dagrun=1).expand(bash_command=trips)

    # ---- silver + gate (dbt): build, then test. A failing test stops everything downstream.
    dbt_run_silver = BashOperator(task_id="dbt_run_silver", bash_command=f"{DBT} run {ARGS} --select path:models/staging path:models/silver")
    dbt_test_silver = BashOperator(task_id="dbt_test_silver",
                                   bash_command=f"{DBT} test {ARGS} --select source:lake path:models/silver path:models/staging")
    # ---- gold + marts
    dbt_run_gold = BashOperator(task_id="dbt_run_gold", bash_command=f"{DBT} run {ARGS} --select path:models/gold path:models/marts")
    dbt_test_gold = BashOperator(task_id="dbt_test_gold", bash_command=f"{DBT} test {ARGS} --select path:models/gold path:models/marts")
    # ---- hand over: lake (Delta), image dataset + manifest, warehouse (Postgres, read by Metabase)
    publish_lake = BashOperator(task_id="publish_lake", bash_command=f"{LAB} publish-lake")
    export_dataset = BashOperator(task_id="export_dataset", bash_command=f"{LAB} export-dataset")
    publish_warehouse = BashOperator(task_id="publish_warehouse", bash_command=f"{LAB} publish-warehouse")

    [*bronze, bronze_trips] >> trips
    silver_frames >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold \
        >> publish_lake >> export_dataset >> publish_warehouse

with DAG(
    dag_id="day09_reset", description="wipe the lake and the local DuckDB file (start from zero)",
    start_date=datetime(2025, 1, 1), schedule=None, catchup=False, tags=["day09", "lab"],
) as reset_dag:
    BashOperator(task_id="reset_lake", bash_command=f"{LAB} reset && rm -f /opt/work/lakehouse.duckdb /opt/work/.failed-*")
