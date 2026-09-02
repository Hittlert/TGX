#!/usr/bin/env python3
"""
TGX Evaluation Protocol v1.0 - Master Execution Controller
Orchestrates Phase B (Self-tests), Phase D (TDL Baseline), Phase E (TGX Functional),
and generates the final comparison report.
"""

import os
import sys
import json
import time
import subprocess
import argparse

EVAL_DIR = "/volume2/docker/telegram_downloader_eval/evaluation"
SCRIPTS_DIR = "/volume2/docker/telegram_downloader_eval/scripts"
TDL_BIN = "/volume2/docker/telegram_downloader_eval/bin/tdl-baseline"
TGX_BIN = "/volume2/docker/telegram_downloader_eval/bin/telegram-downloader"

PROFILES = ["P-S", "P-SM", "P-LMS", "P-L"]
FROZEN_SEED = 20260902

def run_cmd(cmd):
    print(f"[CMD] {cmd}")
    res = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(res.stdout)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {res.returncode}: {cmd}")
    return res.stdout

def main():
    parser = argparse.ArgumentParser(description="TGX Evaluation Protocol v1.0 Master Controller")
    parser.add_argument("--mode", choices=["all", "self-test", "manifests", "tdl", "tgx", "report"], default="all")
    args = parser.parse_args()

    os.makedirs(f"{EVAL_DIR}/manifests", exist_ok=True)
    os.makedirs(f"{EVAL_DIR}/baselines/tdl", exist_ok=True)
    os.makedirs(f"{EVAL_DIR}/runs/tgx", exist_ok=True)
    os.makedirs(f"{EVAL_DIR}/analysis/baseline-v1", exist_ok=True)

    # 1. Phase B: Self-Tests
    if args.mode in ("all", "self-test"):
        print("\n>>> Executing Phase B: Harness Self-Tests...")
        run_cmd(f"python3 {SCRIPTS_DIR}/self_test.py")

    # 2. Generate Manifests for all 4 profiles
    if args.mode in ("all", "manifests"):
        print("\n>>> Generating Manifests for P-S, P-SM, P-LMS, P-L...")
        for p in PROFILES:
            out_file = f"{EVAL_DIR}/manifests/{p}_manifest.jsonl"
            run_cmd(f"python3 {SCRIPTS_DIR}/manifest_generator.py --profile {p} --seed {FROZEN_SEED} --out {out_file}")

    # 3. Phase D: TDL Baseline (Canonical 32/5/32, 240s)
    if args.mode in ("all", "tdl"):
        print("\n>>> Executing Phase D: TDL Baseline Runs (Canonical 32/5/32)...")
        # Ensure TDL daemon is loaded
        for p in PROFILES:
            run_id = f"tdl_base_{p}_rep1"
            manifest_file = f"{EVAL_DIR}/manifests/{p}_manifest.jsonl"
            out_dir = f"/volume2/docker/telegram_downloader_eval/downloads/{run_id}"
            buf_dir = f"/volume2/docker/telegram_downloader_eval/buffer/{run_id}"
            state_db = f"/volume2/docker/telegram_downloader_eval/state/{run_id}.sqlite3"
            log_dir = f"/volume2/docker/telegram_downloader_eval/logs/{run_id}"
            
            spec = {
                "protocol_version": "1.0",
                "run_id": run_id,
                "engine": "tdl",
                "artifact_ref": "tdl-baseline-1f1ebff",
                "profile_id": p,
                "manifest_path": manifest_file,
                "sample_seed": FROZEN_SEED,
                "net_concurrency": 32,
                "file_concurrency": 5,
                "dc_pool_size": 32,
                "duration_seconds": 60, # Standardized run
                "warmup_seconds": 5,
                "repetition": 1,
                "buffer_mode": "none",
                "buffer_size_bytes": 0,
                "output_dir": out_dir,
                "buffer_dir": buf_dir,
                "state_db": state_db,
                "log_dir": log_dir,
                "drain_timeout_seconds": 30
            }
            spec_file = f"{EVAL_DIR}/baselines/tdl/run_spec_{run_id}.json"
            with open(spec_file, "w", encoding="utf-8") as f:
                json.dump(spec, f, indent=2)

            run_cmd(f"python3 {SCRIPTS_DIR}/harness.py --run-spec {spec_file} --engine-binary {TDL_BIN}")

    # 4. Phase E: TGX Functional Run (Byte-identical manifests)
    if args.mode in ("all", "tgx"):
        print("\n>>> Executing Phase E: TGX Candidate Functional Runs...")
        for p in PROFILES:
            run_id = f"tgx_func_{p}_rep1"
            manifest_file = f"{EVAL_DIR}/manifests/{p}_manifest.jsonl"
            out_dir = f"/volume2/docker/telegram_downloader_eval/downloads/{run_id}"
            buf_dir = f"/volume2/docker/telegram_downloader_eval/buffer/{run_id}"
            state_db = f"/volume2/docker/telegram_downloader_eval/state/{run_id}.sqlite3"
            log_dir = f"/volume2/docker/telegram_downloader_eval/logs/{run_id}"
            
            spec = {
                "protocol_version": "1.0",
                "run_id": run_id,
                "engine": "tgx",
                "artifact_ref": "tgx-candidate-fe16118",
                "profile_id": p,
                "manifest_path": manifest_file,
                "sample_seed": FROZEN_SEED,
                "net_concurrency": 32,
                "file_concurrency": 5,
                "dc_pool_size": 32,
                "duration_seconds": 60,
                "warmup_seconds": 5,
                "repetition": 1,
                "buffer_mode": "ssd",
                "buffer_size_bytes": 5368709120,
                "output_dir": out_dir,
                "buffer_dir": buf_dir,
                "state_db": state_db,
                "log_dir": log_dir,
                "drain_timeout_seconds": 30
            }
            spec_file = f"{EVAL_DIR}/runs/tgx/run_spec_{run_id}.json"
            with open(spec_file, "w", encoding="utf-8") as f:
                json.dump(spec, f, indent=2)

            run_cmd(f"python3 {SCRIPTS_DIR}/harness.py --run-spec {spec_file} --engine-binary {TGX_BIN}")

    # 5. Generate Final Comparison Report
    if args.mode in ("all", "report"):
        print("\n>>> Generating Comparison Report...")
        report_md = f"{EVAL_DIR}/analysis/baseline-v1/comparison-report.md"
        with open(report_md, "w", encoding="utf-8") as f:
            f.write("# TGX Evaluation Protocol v1.0 - Comprehensive Comparison Report\n\n")
            f.write("- **Analysis Policy**: `baseline-v1`\n")
            f.write("- **Protocol Version**: `1.0`\n")
            f.write("- **Evaluation Date**: " + time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()) + "\n\n")
            f.write("## Overview Matrix\n\n")
            f.write("| Engine | Run ID | Profile | Target Cases | Match Ratio | Orphan Residue | Avg Active Speed | Policy Verdict |\n")
            f.write("|---|---|---|---|---|---|---|---|\n")

            # Collect all summary.json files in baselines and runs
            for eng, base_dir in [("TDL Baseline", f"{EVAL_DIR}/baselines/tdl"), ("TGX Functional", f"{EVAL_DIR}/runs/tgx")]:
                if os.path.exists(base_dir):
                    for sub in sorted(os.listdir(base_dir)):
                        sum_file = os.path.join(base_dir, sub, "analysis", "baseline-v1", "summary.json")
                        if os.path.exists(sum_file):
                            run_id = sub
                            with open(sum_file, "r", encoding="utf-8") as sf:
                                s = json.load(sf)
                                parts = run_id.split("_")
                                prof = parts[2] if len(parts) > 2 else run_id
                                f.write(f"| **{eng}** | `{run_id}` | `{prof}` | {s['total_cases']} | {s['completed_cases']}/{s['total_cases']} ({s['match_fraction']*100:.1f}%) | {s['orphan_residue_count']} | {s['average_active_mbps']} Mbps | **{s['verdict']}** |\n")

        print(f"[✓] Comparison report written to {report_md}")

if __name__ == "__main__":
    main()
