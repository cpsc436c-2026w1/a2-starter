#!/usr/bin/env bash
# A2 offline harness: the whole pipeline on synthetic data, zero AWS. Ring 2 of 3 —
# run before any cluster spend. GAA runs this first on their machine.
set -euo pipefail
cd "$(dirname "$0")/.."
SCRATCH="${1:-scratch/harness}"
rm -rf "$SCRATCH" && mkdir -p "$SCRATCH"
PY="uv run --with pyyaml --with numpy --with pandas<3 --with pyarrow --with duckdb --with pyspark python3"

echo "[1/4] sample data"
$PY stages/sample_data.py --out "$SCRATCH/sample" --rows 200000

echo "[2/4] stage 1 (duckdb)"
$PY stages/stage1_duckdb.py --trips "$SCRATCH/sample/trips/part-0.parquet" \
    --out "$SCRATCH/matrix.parquet" --window-start 2025-01-01 --window-end 2025-02-01 \
    | tee "$SCRATCH/stage1.log"

echo "[3/4] stage 2 (spark local[*], reads stage 1's output)"
$PY stages/stage2_spark.py --trips "$SCRATCH/matrix.parquet" \
    --zones "$SCRATCH/sample/zones.parquet" --out "$SCRATCH/stage2" \
    --platform local --combos 3 --folds 2 --fit-rounds 2 \
    | tee "$SCRATCH/stage2.log"

echo "[4/4] parse into results rows"
$PY stages/parse_run_log.py "$SCRATCH/stage1.log" --resource-id local --nodes 1
$PY stages/parse_run_log.py "$SCRATCH/stage2.log" --resource-id local --nodes 1
echo "HARNESS OK"
