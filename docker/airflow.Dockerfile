FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl && rm -rf /var/lib/apt/lists/*
USER airflow
# Lab code (deltalake, DuckDB, dbt) lives in its own venv: no clashes with Airflow's dependency pins.
COPY docker/requirements-lab.txt /tmp/requirements-lab.txt
RUN python -m venv /home/airflow/lab-venv \
 && /home/airflow/lab-venv/bin/pip install --no-cache-dir -r /tmp/requirements-lab.txt \
 && /home/airflow/lab-venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.execute(f'INSTALL {e}') for e in ('httpfs','delta','postgres')]"
COPY --chmod=755 docker/airflow-entrypoint.sh /airflow-entrypoint.sh
ENTRYPOINT ["/airflow-entrypoint.sh"]
