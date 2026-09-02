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
    parser.add_argument("--mode", choices=["all", "self-test", "manifests", "benchmark", "report"], default="all")
    parser.add_argument("--duration", type=int, default=240, help="Duration seconds per run (default: 240)")
    parser.add_argument("--warmup", type=int, default=15, help="Warmup seconds (default: 15)")
    parser.add_argument("--repetitions", type=int, default=3, help="Repetitions per engine (default: 3)")
    parser.add_argument("--sweep", action="store_true", help="Run P-LMS concurrency sweep (8, 16, 32, 48)")
    parser.add_argument("--port", type=int, default=5890, help="Runner dynamic host port")
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

    # 3. Paired Benchmark Execution (Section 5)
    if args.mode in ("all", "benchmark"):
        print(f"\n>>> Executing Paired Benchmark Suite (Duration={args.duration}s, Reps={args.repetitions})...")
        
        rounds = []
        for rep in range(1, args.repetitions + 1):
            if rep % 2 == 1:
                rounds.append((rep, "tdl", TDL_BIN))
                rounds.append((rep, "tgx", TGX_BIN))
            else:
                rounds.append((rep, "tgx", TGX_BIN))
                rounds.append((rep, "tdl", TDL_BIN))

        concurrency_levels = [8, 16, 32, 48] if args.sweep else [32]

        for rep_idx, engine, bin_path in rounds:
            engine_prefix = "tdl_base" if engine == "tdl" else "tgx_func"
            spec_dir = f"{EVAL_DIR}/baselines/tdl" if engine == "tdl" else f"{EVAL_DIR}/runs/tgx"

            for p in PROFILES:
                manifest_file = f"{EVAL_DIR}/manifests/{p}_manifest.jsonl"
                
                # If sweeping P-LMS, run across concurrency levels
                levels = concurrency_levels if (p == "P-LMS" and args.sweep) else [32]

                for conc in levels:
                    conc_suffix = f"_c{conc}" if len(levels) > 1 else ""
                    run_id = f"{engine_prefix}_{p}{conc_suffix}_rep{rep_idx}"
                    
                    spec = {
                        "protocol_version": "1.0",
                        "run_id": run_id,
                        "engine": engine,
                        "artifact_ref": "tdl-baseline" if engine == "tdl" else "telegram-downloader",
                        "profile_id": p,
                        "manifest_path": manifest_file,
                        "sample_seed": FROZEN_SEED,
                        "net_concurrency": conc,
                        "file_concurrency": 5,
                        "dc_pool_size": 32,
                        "duration_seconds": args.duration,
                        "warmup_seconds": args.warmup,
                        "repetition": rep_idx,
                        "buffer_mode": "none" if engine == "tdl" else "ssd",
                        "buffer_size_bytes": 0 if engine == "tdl" else 5368709120,
                        "output_dir": f"/volume2/docker/telegram_downloader_eval/scratch_runs/{run_id}/output",
                        "buffer_dir": f"/volume2/docker/telegram_downloader_eval/scratch_runs/{run_id}/buffer",
                        "state_db": f"/volume2/docker/telegram_downloader_eval/scratch_runs/{run_id}/state/records.sqlite3",
                        "log_dir": f"/volume2/docker/telegram_downloader_eval/scratch_runs/{run_id}/logs",
                        "drain_timeout_seconds": 60
                    }
                    spec_file = f"{spec_dir}/run_spec_{run_id}.json"
                    with open(spec_file, "w", encoding="utf-8") as f:
                        json.dump(spec, f, indent=2)

                    print(f"\n>>> Running {run_id} ({engine.upper()} - {p}, conc={conc}, rep={rep_idx})...")
                    run_cmd(f"python3 {SCRIPTS_DIR}/harness.py --run-spec {spec_file} --engine-binary {bin_path} --port {args.port}")

    # 4. Generate Final Comparison Report
    if args.mode in ("all", "report"):
        print("\n>>> Generating Comparison Report...")
        report_md = f"{EVAL_DIR}/analysis/baseline-v1/comparison-report.md"
        with open(report_md, "w", encoding="utf-8") as f:
            f.write("# TGX Evaluation Protocol v1.0 - Comprehensive Comparison Report\n\n")
            f.write("- **Analysis Policy**: `baseline-v1`\n")
            f.write("- **Protocol Version**: `1.0`\n")
            f.write("- **Evaluation Date**: " + time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()) + "\n\n")
            f.write("## Overview Matrix\n\n")
            f.write("| Engine | Run ID | Profile | Repetition | Net Concurrency | Target Cases | Match Ratio | Orphan Residue | Avg Active Speed | Policy Verdict |\n")
            f.write("|---|---|---|---|---|---|---|---|---|---|\n")

            for eng_label, base_dir in [("TDL Baseline", f"{EVAL_DIR}/baselines/tdl"), ("TGX Functional", f"{EVAL_DIR}/runs/tgx")]:
                if os.path.exists(base_dir):
                    for sub in sorted(os.listdir(base_dir)):
                        sum_file = os.path.join(base_dir, sub, "analysis", "baseline-v1", "summary.json")
                        raw_spec_file = os.path.join(base_dir, sub, "raw", "run_spec.json")
                        if os.path.exists(sum_file):
                            with open(sum_file, "r", encoding="utf-8") as sf:
                                s = json.load(sf)
                            spec_data = {}
                            if os.path.exists(raw_spec_file):
                                with open(raw_spec_file, "r", encoding="utf-8") as rf:
                                    spec_data = json.load(rf)

                            p_id = spec_data.get("profile_id", "unknown")
                            r_num = spec_data.get("repetition", 1)
                            net_c = spec_data.get("net_concurrency", 32)
                            f.write(f"| **{eng_label}** | `{sub}` | `{p_id}` | {r_num} | {net_c} | {s['total_cases']} | {s['completed_cases']}/{s['total_cases']} ({s['match_fraction']*100:.1f}%) | {s['orphan_residue_count']} | {s['average_active_mbps']} Mbps | **{s['verdict']}** |\n")

        print(f"[✓] Comparison report written to {report_md}")

if __name__ == "__main__":
    main()
