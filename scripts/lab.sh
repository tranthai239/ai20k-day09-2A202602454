#!/bin/bash
# Lệnh ngắn cho bài lab (chạy trên máy của bạn, thực thi trong container Airflow đang chạy).
#   scripts/lab.sh step <1..5|all> [--reset]   chạy TỪNG cấu phần, in kết quả và kiểm tra (step all = cả pipeline)
#   scripts/lab.sh exp <idempotency|break-gate|leakage|threshold>   thí nghiệm, ghi bằng chứng vào report/evidence/
#   scripts/lab.sh verify [--module N]     kiểm tra tự động (27 kiểm tra, 5 module); --module 2 chỉ chạy module 2
#   scripts/lab.sh test                    unit test (dhash, blur_score...)
#   scripts/lab.sh run [--reset]           chạy cả pipeline không qua Airflow (≈ 1 phút)
#   scripts/lab.sh cli <lệnh> [tham số]    ví dụ: cli ingest --drive drive_a | cli history silver.frames | cli sql "select 1"
#   scripts/lab.sh dbt <lệnh> [tham số]    ví dụ: dbt run --select silver_telemetry | dbt test --select gold_dataset_frames
#   scripts/lab.sh shell                   mở shell trong container
set -e
cd "$(dirname "$0")/.."
PY=/home/airflow/lab-venv/bin/python
X="docker compose exec -T"
case "${1:-}" in
  step)   shift; $X -w /opt/lab airflow $PY -m pipeline.cli step "$@" ;;
  exp)    shift; $X -w /opt/lab airflow $PY -m pipeline.cli exp "$@" ;;
  verify) shift; $X -w /opt/lab airflow $PY -m pipeline.cli verify "$@" ;;
  test)   $X -w /opt/lab airflow $PY -m unittest discover -s tests ;;
  run)    shift; $X -w /opt/lab airflow scripts/run_pipeline.sh "$@" ;;
  cli)    shift; $X -w /opt/lab airflow $PY -m pipeline.cli "$@" ;;
  dbt)    shift; $X -w /opt/dbt airflow /home/airflow/lab-venv/bin/dbt "$@" --no-use-colors ;;
  shell)  docker compose exec airflow bash ;;
  *) sed -n 2,11p "$0"; exit 1 ;;
esac
