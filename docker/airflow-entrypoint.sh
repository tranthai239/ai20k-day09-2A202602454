#!/bin/bash
# one container = scheduler + webserver (LocalExecutor) to keep RAM low
set -e
airflow db migrate
airflow users create --username admin --password admin --firstname Lab --lastname User --role Admin --email lab@example.com || true
airflow scheduler &
exec airflow webserver
