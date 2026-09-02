#!/usr/bin/env python3
"""
TGX Evaluation v1 - Test Runner & Correctness Oracle
Executes test manifests against isolated eval daemon, collects 1s metrics,
validates strict correctness against baselines, and generates verdict.json + report.md.
"""

import os
import sys
import time
import json
import uuid
import hashlib
import sqlite3
import argparse
import threading
import urllib.request
import urllib.error

def http_get(url, timeout=5):
    req = urllib.request.Request(url, headers={"User-Agent": "TGX-Eval/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def http_post(url, data, timeout=5):
    payload = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={
        "Content-Type": "application/json",
        "User-Agent": "TGX-Eval/1.0"
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def compute_sha256(filepath):
    if not os.path.isfile(filepath):
        return None
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()

class MetricsCollector(threading.Thread):
    def __init__(self, api_base, metrics_file):
        super().__init__(daemon=True)
        self.api_base = api_base
        self.metrics_file = metrics_file
        self.running = True
        self.records = []

    def run(self):
        with open(self.metrics_file, "w", encoding="utf-8") as f:
            while self.running:
                t0 = time.time()
                sample = {
                    "timestamp": datetime_iso(),
                    "elapsed_sec": 0,
                    "rolling_5s_bps": 0,
                    "queue_depth": 0,
                    "active_files": 0,
                    "spool_used_bytes": 0,
                    "spool_reserved_bytes": 0,
                    "spool_ready_bytes": 0,
                    "spool_writing_bytes": 0,
                }
                try:
                    status = http_get(f"{self.api_base}/api/status", timeout=2)
                    sample["rolling_5s_bps"] = status.get("rolling_5s_bps", 0)
                    sample["queue_depth"] = status.get("queue_depth", 0)
                    sample["active_files"] = len(status.get("active_files") or [])
                except Exception:
                    pass

                try:
                    storage = http_get(f"{self.api_base}/api/system/storage", timeout=2)
                    buf = storage.get("buffer") or {}
                    sample["spool_used_bytes"] = buf.get("used_bytes", 0)
                    sample["spool_reserved_bytes"] = buf.get("reserved_bytes", 0)
                    sample["spool_ready_bytes"] = buf.get("ready_bytes", 0)
                    sample["spool_writing_bytes"] = buf.get("writing_bytes", 0)
                except Exception:
                    pass

                f.write(json.dumps(sample) + "\n")
                f.flush()
                self.records.append(sample)

                sleep_time = max(0.1, 1.0 - (time.time() - t0))
                time.sleep(sleep_time)

    def stop(self):
        self.running = False

def datetime_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def run_evaluation(manifest_file, api_base, eval_dir, eval_output_dir, eval_db_path, run_id=None):
    if not run_id:
        run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    run_dir = os.path.join(eval_dir, "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)
    print(f"=== Starting TGX Evaluation Run: {run_id} ===")
    print(f"[*] Run Directory: {run_dir}")
    print(f"[*] Target Output Dir: {eval_output_dir}")
    print(f"[*] Target State DB: {eval_db_path}")

    # Isolation Assertion (Section 2)
    prod_markers = ["telegram_media_downloader_us", "SpecialMedias"]
    for marker in prod_markers:
        if marker in eval_output_dir or marker in eval_db_path:
            raise RuntimeError(f"FATAL: Isolation violation! Path {eval_output_dir} contains production marker {marker}")

    # Read manifest
    cases = []
    with open(manifest_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                cases.append(json.loads(line))

    print(f"[*] Loaded {len(cases)} cases from {manifest_file}")

    # Start metrics collector
    metrics_file = os.path.join(run_dir, "metrics.jsonl")
    collector = MetricsCollector(api_base, metrics_file)
    collector.start()

    start_time = time.time()
    events = []

    def log_event(name, payload=None):
        ev = {"timestamp": datetime_iso(), "event": name, "payload": payload or {}}
        events.append(ev)
        print(f"[{ev['timestamp']}] Event: {name}")

    log_event("EVAL_STARTED", {"run_id": run_id, "cases_count": len(cases)})

    # Pause daemon while enqueuing all cases
    try:
        http_post(f"{api_base}/api/control", {"action": "pause"})
        log_event("DAEMON_PAUSED_FOR_SUBMIT")
    except Exception as e:
        print(f"[!] Warning: failed to pause daemon: {e}")

    # Submit tasks
    import posixpath
    submitted = 0
    for c in cases:
        clean_path = posixpath.normpath(c["expected_rel_path"].replace("\\", "/").strip("/"))
        task_id = f"{run_id}_{c['chat_id']}_{c['message_id']}"
        task_req = {
            "id": task_id,
            "peer": str(c["chat_id"]),
            "message_id": int(c["message_id"]),
            "final_path": clean_path,
            "expected_size": int(c["expected_size"]),
            "date": int(c.get("message_date", 0)),
        }
        try:
            http_post(f"{api_base}/api/tasks", task_req)
            submitted += 1
        except Exception as e:
            print(f"[!] Failed to submit task {task_req['id']}: {e}")

    log_event("TASKS_SUBMITTED", {"submitted": submitted, "total": len(cases)})

    # Resume daemon to begin downloading
    http_post(f"{api_base}/api/control", {"action": "resume"})
    log_event("DAEMON_RESUMED")

    # Monitor loop
    print("[*] Monitoring task execution progress...")
    last_speed = 0
    last_active = 0
    poll_count = 0
    max_idle_polls = 180 # 3 minutes idle timeout

    idle_count = 0
    while True:
        time.sleep(2)
        poll_count += 1
        try:
            st = http_get(f"{api_base}/api/status")
            active = len(st.get("active_files") or [])
            queue = st.get("queue_depth", 0)
            bps = st.get("rolling_5s_bps", 0)

            mbps = (bps * 8) / (1024 * 1024)
            print(f"\r[*] Active: {active}, Queue: {queue}, Speed: {mbps:.2f} Mbps (rolling 5s)      ", end="", flush=True)

            if active == 0 and queue == 0:
                idle_count += 1
                if idle_count >= 3: # 6 seconds with 0 active
                    print("\n[✓] All tasks finished processing!")
                    break
            else:
                idle_count = 0
        except Exception as e:
            print(f"\n[!] Status poll error: {e}")

    end_time = time.time()
    collector.stop()
    log_event("EVAL_FINISHED", {"duration_sec": end_time - start_time})

    # Run Correctness Oracle
    print("\n[*] Running Correctness Oracle & Integrity Verifications...")
    results = []
    pass_count = 0
    fail_count = 0

    for c in cases:
        case_id = c["case_id"]
        rel_path = c["expected_rel_path"]
        target_path = os.path.join(eval_output_dir, rel_path)
        expected_size = c["expected_size"]
        baseline_sha = c["baseline_sha256"]

        case_res = {
            "case_id": case_id,
            "rel_path": rel_path,
            "expected_size": expected_size,
            "actual_size": 0,
            "baseline_sha256": baseline_sha,
            "actual_sha256": "",
            "size_match": False,
            "sha_match": False,
            "verdict": "FAIL",
            "error": "",
        }

        if not os.path.exists(target_path):
            case_res["error"] = "Target file not found on disk"
        else:
            actual_size = os.path.getsize(target_path)
            case_res["actual_size"] = actual_size
            case_res["size_match"] = (actual_size == expected_size)

            actual_sha = compute_sha256(target_path)
            case_res["actual_sha256"] = actual_sha

            if baseline_sha:
                case_res["sha_match"] = (actual_sha == baseline_sha)
            else:
                case_res["sha_match"] = True # No baseline, size checked

            if case_res["size_match"] and case_res["sha_match"]:
                case_res["verdict"] = "PASS"
                pass_count += 1
            else:
                fail_count += 1
                case_res["error"] = f"Size/SHA mismatch: size({actual_size} vs {expected_size}), sha({actual_sha[:8]} vs {baseline_sha[:8]})"

        results.append(case_res)

    # Check for orphan moving/seg files
    orphan_files = []
    for root, _, files in os.walk(eval_output_dir):
        for f in files:
            if f.endswith(".moving") or f.endswith(".seg") or f.endswith(".meta"):
                orphan_files.append(os.path.join(root, f))

    # Compile Verdict
    verdict = {
        "run_id": run_id,
        "manifest": os.path.basename(manifest_file),
        "total_cases": len(cases),
        "passed_cases": pass_count,
        "failed_cases": fail_count,
        "pass_rate": f"{(pass_count / len(cases) * 100):.1f}%" if cases else "0%",
        "duration_sec": round(end_time - start_time, 2),
        "orphan_residue_count": len(orphan_files),
        "verdict": "GO" if (fail_count == 0 and len(orphan_files) == 0) else "NO-GO",
    }

    # Save verdict & report
    verdict_file = os.path.join(run_dir, "verdict.json")
    with open(verdict_file, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2, ensure_ascii=False)

    report_file = os.path.join(run_dir, "report.md")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(f"# TGX Evaluation Report - Run `{run_id}`\n\n")
        f.write(f"- **Verdict**: `{verdict['verdict']}`\n")
        f.write(f"- **Total Cases**: {len(cases)}\n")
        f.write(f"- **Passed**: {pass_count} | **Failed**: {fail_count} ({verdict['pass_rate']})\n")
        f.write(f"- **Duration**: {verdict['duration_sec']}s\n")
        f.write(f"- **Orphan Files Residue**: {len(orphan_files)}\n\n")
        f.write("## Case Breakdown\n\n")
        f.write("| Case ID | Rel Path | Expected Size | Actual Size | SHA Match | Verdict |\n")
        f.write("|---|---|---|---|---|---|\n")
        for r in results:
            f.write(f"| {r['case_id']} | `{r['rel_path']}` | {r['expected_size']} | {r['actual_size']} | {r['sha_match']} | **{r['verdict']}** |\n")

    print(f"\n[✓] Evaluation completed! Verdict: {verdict['verdict']}")
    print(f"[✓] Results written to: {verdict_file} and {report_file}")
    return verdict

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TGX Evaluation Runner")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--api", default="http://127.0.0.1:5885")
    parser.add_argument("--eval-dir", default="/volume2/docker/telegram_downloader_eval/evaluation")
    parser.add_argument("--output-dir", default="/volume2/docker/telegram_downloader_eval/downloads")
    parser.add_argument("--db-path", default="/volume2/docker/telegram_downloader_eval/state/eval_records.sqlite3")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    run_evaluation(args.manifest, args.api, args.eval_dir, args.output_dir, args.db_path, args.run_id)
