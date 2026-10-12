#!/bin/bash
# Run the whole pipeline without Airflow (same commands as the DAG). Inside the Airflow container or on a laptop with the lab venv.
#   scripts/run_pipeline.sh            full run
#   scripts/run_pipeline.sh --reset    wipe the lake first
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PY:-python}"; DBT="${DBT:-dbt}"; DBT_DIR="${DBT_DIR:-dbt}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$PWD/$DBT_DIR}" DBT_TARGET_PATH="${DBT_TARGET_PATH:-/tmp/dbt-target}" DBT_LOG_PATH="${DBT_LOG_PATH:-/tmp/dbt-logs}"
[ "${1:-}" = "--reset" ] && { $PY -m pipeline.cli reset; rm -f "${DUCKDB_PATH:-work/lakehouse.duckdb}"; }
step() { echo "== $*"; }
step bronze;  $PY -m pipeline.cli ingest --drive drive_a; $PY -m pipeline.cli ingest --drive drive_b; $PY -m pipeline.cli ingest-trips
step "silver frames (one call per trip)"
for t in $($PY -m pipeline.cli list-trips | $PY -c "import sys,json;print(' '.join(json.loads(sys.stdin.read().splitlines()[-1])))"); do $PY -m pipeline.cli extract --trip "$t" >/dev/null; done
step "dbt silver + gate"; (cd "$DBT_DIR" && $DBT run --no-use-colors --select path:models/staging path:models/silver | tail -3 && $DBT test --no-use-colors --select source:lake path:models/silver path:models/staging | tail -3)
step "dbt gold + gate";   (cd "$DBT_DIR" && $DBT run --no-use-colors --select path:models/gold path:models/marts | tail -3 && $DBT test --no-use-colors --select path:models/gold path:models/marts | tail -3)
step publish; $PY -m pipeline.cli publish-lake >/dev/null; $PY -m pipeline.cli export-dataset; $PY -m pipeline.cli publish-warehouse
step verify;  $PY -m pipeline.cli verify | tail -3
