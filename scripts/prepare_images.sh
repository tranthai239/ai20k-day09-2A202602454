#!/bin/bash
# Run ONCE on a good network, before class. Pulls and builds every image, then (optionally) saves them to one tarball
# so students can `docker load` from a USB stick instead of pulling ~5 GB each.
#   scripts/prepare_images.sh            pull + build
#   scripts/prepare_images.sh --save     also write day09-images.tar (about 5 GB)
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile orchestrate --profile bi pull --ignore-buildable
docker compose --profile orchestrate build
if [ "${1:-}" = "--save" ]; then
  docker save -o day09-images.tar day09-airflow:latest bitnamilegacy/minio:2025.4.22 postgres:16-alpine metabase/metabase:latest
  ls -lh day09-images.tar; echo "students: docker load -i day09-images.tar"
fi
