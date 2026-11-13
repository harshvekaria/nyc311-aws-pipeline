#!/usr/bin/env bash
# Builds every deployable artifact into ./dist
#   dist/pg8000-layer.zip     Lambda layer (pure-Python Postgres driver)
#   dist/plan_dates.zip  dist/ingest_bronze.zip  dist/load_gold.zip  dist/api.zip
# Run from the repo root:  bash scripts/build.sh
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf dist build && mkdir -p dist build/layer/python

# Layer: pg8000 + deps, pure Python, so it builds on Mac/Windows/Linux alike.
pip3 install --quiet pg8000==1.31.* -t build/layer/python
(cd build/layer && zip -qr ../../dist/pg8000-layer.zip python)

for fn in plan_dates ingest_bronze load_gold api; do
  mkdir -p build/$fn && cp lambdas/$fn/* build/$fn/
  [ "$fn" = "load_gold" ] && cp sql/postgres_schema.sql build/load_gold/
  (cd build/$fn && zip -qr ../../dist/$fn.zip .)
done
ls -lh dist
