#!/usr/bin/env bash
set -euo pipefail

# TGX Evaluation Suite Master Script
EVAL_ROOT="/volume2/docker/telegram_downloader_eval"
SCRIPTS_DIR="${EVAL_ROOT}/scripts"
MANIFEST_DIR="${EVAL_ROOT}/evaluation/manifests"
PROD_DB="/volume2/docker/telegram_media_downloader_us/state/download_records.sqlite3"
BASELINE_ROOT="/home/hittler/SpecialMedias/06-碎片整理区/碎片媒体/TG2/"
EVAL_API="http://127.0.0.1:5885"

SCENARIO="${1:-S1}"
COUNT="${2:-20}"

echo "=========================================================="
echo "  TGX Evaluation v1 - Scenario ${SCENARIO}"
echo "=========================================================="

mkdir -p "${MANIFEST_DIR}"

MANIFEST_FILE="${MANIFEST_DIR}/${SCENARIO}_manifest.jsonl"

echo "[1/3] Generating stratified manifest for scenario ${SCENARIO} (count: ${COUNT})..."
python3 "${SCRIPTS_DIR}/manifest_gen.py" \
    --db-path "${PROD_DB}" \
    --output-root "${BASELINE_ROOT}" \
    --scenario "${SCENARIO}" \
    --count "${COUNT}" \
    --out "${MANIFEST_FILE}"

echo "[2/3] Executing Evaluation Runner..."
python3 "${SCRIPTS_DIR}/runner.py" \
    --manifest "${MANIFEST_FILE}" \
    --api "${EVAL_API}" \
    --eval-dir "${EVAL_ROOT}/evaluation" \
    --output-dir "${EVAL_ROOT}/downloads" \
    --db-path "${EVAL_ROOT}/state/eval_records.sqlite3"

echo "[3/3] Scenario ${SCENARIO} complete!"
