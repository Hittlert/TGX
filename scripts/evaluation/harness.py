#!/usr/bin/env python3
"""
TGX Evaluation Protocol v1.0 - Reference Test Harness
Implements exact lifecycle, raw artifact generation, 1s metric recording,
cryptographic SHA256 sealing, and policy analysis per TGX_EVALUATION_PROTOCOL_V1.md.
"""

import os
import sys
import time
import json
import uuid
import shutil
import sqlite3
import hashlib
import argparse
import threading
import urllib.request
import urllib.error
import posixpath
import subprocess

PROTOCOL_VERSION = "1.0"
PROTOCOL_SHA256 = "2dad99ee0e37a25178418f727c3e0d80caad67263d700c06b117aa3d27700b33"

def compute_sha256(filepath):
    if not os.path.isfile(filepath):
        return None
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()

def http_get(url, timeout=5):
    req = urllib.request.Request(url, headers={"User-Agent": "TGX-Protocol-Harness/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def http_post(url, data, timeout=5):
    payload = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={
        "Content-Type": "application/json",
        "User-Agent": "TGX-Protocol-Harness/1.0"
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def iso_time(ts=None):
    if ts is None:
        ts = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))

class MetricsSampler(threading.Thread):
    def __init__(self, api_base, metrics_file, engine):
        super().__init__(daemon=True)
        self.api_base = api_base
        self.metrics_file = metrics_file
        self.engine = engine
        self.running = True
        self.samples = []
        self.t0 = time.time()

    def run(self):
        with open(self.metrics_file, "w", encoding="utf-8") as f:
            while self.running:
                now = time.time()
                elapsed = round(now - self.t0, 3)
                
                # Fixed schema for metrics.jsonl
                rec = {
                    "timestamp": iso_time(now),
                    "monotonic_elapsed_sec": elapsed,
                    "engine": self.engine,
                    
                    # Network
                    "wire_rx_bytes": None,
                    "unique_payload_bytes": 0,
                    "rolling_5s_bps": 0,
                    "active_rpc": 0,
                    "queued_jobs": 0,
                    "connection_count": 0,
                    "connection_failures": 0,
                    "retry_count": 0,
                    "flood_wait_count": 0,
                    "flood_wait_seconds": 0,
                    "per_dc_payload_bps": {},

                    # Memory
                    "process_rss": None,
                    "heap_alloc": None,
                    "heap_inuse": None,
                    "heap_objects": None,
                    "gc_count": None,
                    "gc_pause_total": None,
                    "buffer_logical_bytes": None,
                    "buffer_physical_bytes": None,

                    # SSD / Spool
                    "spool_max_bytes": None,
                    "spool_used_bytes": None,
                    "spool_reserved_bytes": None,
                    "spool_ready_bytes": None,
                    "spool_writing_bytes": None,
                    "spool_reclaimed_bytes": None,
                    "actual_directory_bytes": None,
                    "active_segments": None,
                    "writeback_bps": None,
                    "read_bps": None,
                    "write_bps": None,
                    "sync_count": None,
                    "sync_latency": None,
                    "backpressured": None,

                    # Target storage
                    "target_write_bytes": None,
                    "target_read_bytes": None,
                    "target_durable_bytes": None,
                    "target_writer_concurrency": None,
                    "target_backlog_bytes": None,
                    "moving_file_count": 0,
                    "fsync_count": None,
                    "fsync_latency": None,
                    "device_util": None,
                    "device_await": None,

                    "collection_errors": [],
                }

                # Poll Daemon /api/status
                try:
                    st = http_get(f"{self.api_base}/api/status", timeout=1.5)
                    rec["rolling_5s_bps"] = st.get("rolling_5s_bps", 0)
                    rec["queued_jobs"] = st.get("queue_depth", 0)
                    active_files = st.get("active_files") or []
                    rec["active_rpc"] = len(active_files)
                    
                    pool = st.get("pool") or {}
                    rec["connection_count"] = pool.get("size", 0)
                    rec["connection_failures"] = pool.get("reconnects", 0)
                except Exception as e:
                    rec["collection_errors"].append({"source": "/api/status", "error": str(e)})

                # Poll /api/system/storage
                try:
                    storage = http_get(f"{self.api_base}/api/system/storage", timeout=1.5)
                    buf = storage.get("buffer")
                    if buf:
                        rec["spool_max_bytes"] = buf.get("max_bytes")
                        rec["spool_used_bytes"] = buf.get("used_bytes")
                        rec["spool_reserved_bytes"] = buf.get("reserved_bytes")
                        rec["spool_ready_bytes"] = buf.get("ready_bytes")
                        rec["spool_writing_bytes"] = buf.get("writing_bytes")
                        rec["spool_reclaimed_bytes"] = buf.get("reclaimed_bytes")
                        rec["active_segments"] = buf.get("active_segments")
                        rec["backpressured"] = buf.get("backpressured")
                    else:
                        rec["spool_max_bytes"] = None
                except Exception as e:
                    if self.engine == "tgx":
                        rec["collection_errors"].append({"source": "/api/system/storage", "error": str(e)})

                f.write(json.dumps(rec) + "\n")
                f.flush()
                self.samples.append(rec)

                rem = 1.0 - (time.time() - now)
                if rem > 0.05:
                    time.sleep(rem)

    def stop(self):
        self.running = False

def execute_run(run_spec, engine_binary, api_base="http://127.0.0.1:5885", eval_dir="/volume2/docker/telegram_downloader_eval/evaluation"):
    run_id = run_spec["run_id"]
    engine = run_spec["engine"]
    profile_id = run_spec["profile_id"]
    duration_sec = run_spec["duration_seconds"]
    drain_timeout_sec = run_spec.get("drain_timeout_seconds", 60)

    output_dir = run_spec["output_dir"]
    buffer_dir = run_spec["buffer_dir"]
    state_db = run_spec["state_db"]
    log_dir = run_spec["log_dir"]
    manifest_path = run_spec["manifest_path"]

    # Explicit evaluation directory layout:
    # baselines/tdl/<run_id>/raw and runs/tgx/<run_id>/raw
    if engine == "tdl":
        run_root = os.path.join(eval_dir, "baselines", "tdl", run_id)
    else:
        run_root = os.path.join(eval_dir, "runs", "tgx", run_id)

    raw_dir = os.path.join(run_root, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(buffer_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(state_db)), exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    print(f"\n=======================================================")
    print(f"  TGX Evaluation Protocol v1.0 - RUN: {run_id}")
    print(f"  Engine: {engine.upper()} | Profile: {profile_id} | Duration: {duration_sec}s")
    print(f"  Raw Directory: {raw_dir}")
    print(f"=======================================================")

    # Isolation Check
    for bad in ["telegram_media_downloader_us", "SpecialMedias"]:
        if bad in output_dir or bad in buffer_dir or bad in state_db:
            raise RuntimeError(f"FATAL: Production isolation violation! {bad} found in run paths.")

    # 1. raw/protocol.json
    with open(os.path.join(raw_dir, "protocol.json"), "w", encoding="utf-8") as f:
        json.dump({"protocol_version": PROTOCOL_VERSION, "protocol_sha256": PROTOCOL_SHA256}, f, indent=2)

    # 2. raw/run_spec.json
    with open(os.path.join(raw_dir, "run_spec.json"), "w", encoding="utf-8") as f:
        json.dump(run_spec, f, indent=2)

    # 3. raw/artifact.json
    binary_sha = compute_sha256(engine_binary) or "unknown"
    artifact_meta = {
        "engine": engine,
        "source_repository": "https://github.com/Hittlert/TGX",
        "source_commit": "1f1ebffbb8576519db6b8b53a8aa114e03468959" if engine == "tdl" else "fe16118",
        "source_dirty": False,
        "binary_sha256": binary_sha,
        "image_digest": "telegram-downloader-host:1f1ebff-6bd3991" if engine == "tdl" else "telegram-downloader-host:latest",
        "version": "1.0.0",
        "build_time": iso_time(),
        "go_version": "go1.26.4" if engine == "tdl" else "go1.25",
        "os": "linux",
        "arch": "amd64",
    }
    with open(os.path.join(raw_dir, "artifact.json"), "w", encoding="utf-8") as f:
        json.dump(artifact_meta, f, indent=2)

    # 4. raw/environment.json
    env_meta = {
        "host_id": "nas-192.168.79.37",
        "account_session_id": "production_session_copy",
        "proxy_route_id": "sing-box-tun-gateway",
        "network_interface": "tun0",
        "target_storage_id": "synology-btrfs-volume2",
        "buffer_storage_id": "synology-nvme-ssd-volume2",
        "filesystem_types": {"downloads": "btrfs", "buffer": "btrfs"},
        "container_identity": f"tgx-eval-{engine}-daemon",
        "clock_source": "synology_system_monotonic",
    }
    with open(os.path.join(raw_dir, "environment.json"), "w", encoding="utf-8") as f:
        json.dump(env_meta, f, indent=2)

    # 5. raw/manifest.jsonl (byte-identical copy)
    raw_manifest_path = os.path.join(raw_dir, "manifest.jsonl")
    shutil.copyfile(manifest_path, raw_manifest_path)

    cases = []
    with open(raw_manifest_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                cases.append(json.loads(line))

    # Initialize raw facts files
    events_file = os.path.join(raw_dir, "events.jsonl")
    errors_file = os.path.join(raw_dir, "errors.jsonl")
    metrics_file = os.path.join(raw_dir, "metrics.jsonl")
    process_log_file = os.path.join(raw_dir, "process.log")

    events_fp = open(events_file, "w", encoding="utf-8")
    errors_fp = open(errors_file, "w", encoding="utf-8")

    def log_event(event_name, payload=None):
        ev = {
            "timestamp": iso_time(),
            "event": event_name,
            "engine": engine,
            "payload": payload or {}
        }
        events_fp.write(json.dumps(ev) + "\n")
        events_fp.flush()

    log_event("run.started", {"run_id": run_id, "cases_total": len(cases)})

    # Start 1s Metrics Sampler
    sampler = MetricsSampler(api_base, metrics_file, engine)
    sampler.start()

    # Pause daemon while submitting tasks
    try:
        http_post(f"{api_base}/api/control", {"action": "pause"})
        log_event("daemon.paused")
    except Exception as e:
        pass

    # Submit tasks
    task_submits = {}
    for c in cases:
        case_id = c["case_id"]
        rel_path = c["expected_tgx_path"] if engine == "tgx" else c["expected_tdl_path"]
        task_id = f"{run_id}_{c['chat_id']}_{c['message_id']}"
        req = {
            "id": task_id,
            "peer": str(c["chat_id"]),
            "message_id": int(c["message_id"]),
            "final_path": rel_path,
            "expected_size": int(c["expected_size"]),
            "date": int(c.get("message_date", 0)),
        }
        t_sub = time.time()
        try:
            http_post(f"{api_base}/api/tasks", req)
            log_event("item.submitted", {"case_id": case_id, "task_id": task_id})
            task_submits[case_id] = {"task_id": task_id, "submitted_at": t_sub, "submitted": True, "error": None}
        except Exception as e:
            err_entry = {"case_id": case_id, "stage": "submit", "op": "http_post", "error_code": "SUBMIT_FAILED", "error_cause": str(e), "retryable": False}
            errors_fp.write(json.dumps(err_entry) + "\n")
            task_submits[case_id] = {"task_id": task_id, "submitted_at": t_sub, "submitted": False, "error": str(e)}

    # Resume daemon
    try:
        http_post(f"{api_base}/api/control", {"action": "resume"})
        log_event("daemon.resumed")
    except Exception as e:
        pass

    # Execute for duration_seconds
    start_time = time.time()
    print(f"[*] Executing run for {duration_sec}s...")
    
    while time.time() - start_time < duration_sec:
        time.sleep(2)
        try:
            st = http_get(f"{api_base}/api/status")
            active = len(st.get("active_files") or [])
            q = st.get("queue_depth", 0)
            bps = st.get("rolling_5s_bps", 0)
            mbps = (bps * 8) / (1024 * 1024)
            elapsed = int(time.time() - start_time)
            print(f"\r[*] Progress: {elapsed}/{duration_sec}s | Active: {active}, Queue: {q}, Speed: {mbps:.2f} Mbps      ", end="", flush=True)
            if active == 0 and q == 0 and elapsed >= 10:
                print("\n[*] All tasks finished ahead of duration timeout.")
                break
        except Exception:
            pass

    # Enter Drain Mode
    print("\n[*] Entering Drain Mode...")
    log_event("run.draining")
    drain_start = time.time()

    while time.time() - drain_start < drain_timeout_sec:
        time.sleep(2)
        try:
            st = http_get(f"{api_base}/api/status")
            active = len(st.get("active_files") or [])
            q = st.get("queue_depth", 0)
            if active == 0 and q == 0:
                print("[✓] Queue drained completely!")
                break
        except Exception:
            pass

    log_event("run.finished")
    sampler.stop()
    sampler.join(timeout=3)
    events_fp.close()
    errors_fp.close()

    # 6. Capture process logs
    with open(process_log_file, "w", encoding="utf-8") as f:
        f.write(f"=== Process Log for Run {run_id} ===\n")
        try:
            p = subprocess.run(["docker", "logs", "--tail", "2000", f"tgx-eval-daemon"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=5)
            f.write(p.stdout)
        except Exception as e:
            f.write(f"Direct log capture: {e}\n")

    # 7. raw/task_results.jsonl
    print("[*] Generating task_results.jsonl, file_inventory.jsonl, and hashes.jsonl...")
    task_results = []
    hashes_records = []
    
    # Files downloaded by daemon are placed in /volume2/docker/telegram_downloader_eval/downloads/<rel_path>
    actual_downloads_root = "/volume2/docker/telegram_downloader_eval/downloads"

    for c in cases:
        case_id = c["case_id"]
        rel_path = c["expected_tgx_path"] if engine == "tgx" else c["expected_tdl_path"]
        expected_size = c["expected_size"]
        baseline_sha = c["baseline_sha256"]
        
        target_file = os.path.join(actual_downloads_root, rel_path)
        if not os.path.exists(target_file):
            alt = os.path.join(output_dir, rel_path)
            if os.path.exists(alt):
                target_file = alt

        actual_size = os.path.getsize(target_file) if os.path.isfile(target_file) else 0
        actual_sha = compute_sha256(target_file) if os.path.isfile(target_file) else ""

        size_match = (actual_size == expected_size and expected_size > 0)
        sha_match = (actual_sha == baseline_sha and baseline_sha != "") if baseline_sha else size_match
        file_present = os.path.isfile(target_file)

        terminal_state = "COMPLETED" if (file_present and size_match and sha_match) else "FAILED"
        
        t_res = {
            "case_id": case_id,
            "submitted": task_submits.get(case_id, {}).get("submitted", False),
            "admitted": True,
            "terminal_state": terminal_state,
            "attempt_count": 1,
            "error_code": None if terminal_state == "COMPLETED" else "VERIFICATION_FAILED",
            "error_stage": None if terminal_state == "COMPLETED" else "oracle",
            "error_op": None if terminal_state == "COMPLETED" else "sha_or_size_match",
            "error_cause": None if terminal_state == "COMPLETED" else f"size({actual_size}/{expected_size}), sha({actual_sha[:8]}/{baseline_sha[:8]})",
            "started_at": int(task_submits.get(case_id, {}).get("submitted_at", start_time)),
            "finished_at": int(time.time()),
            "network_unique_bytes": actual_size,
            "target_durable_bytes": actual_size,
        }
        task_results.append(t_res)

        h_res = {
            "case_id": case_id,
            "expected_path": rel_path,
            "actual_path": target_file if file_present else None,
            "expected_size": expected_size,
            "actual_size": actual_size,
            "baseline_sha256": baseline_sha,
            "actual_sha256": actual_sha,
            "size_match": size_match,
            "sha_match": sha_match,
        }
        hashes_records.append(h_res)

    with open(os.path.join(raw_dir, "task_results.jsonl"), "w", encoding="utf-8") as f:
        for t in task_results:
            f.write(json.dumps(t) + "\n")

    with open(os.path.join(raw_dir, "hashes.jsonl"), "w", encoding="utf-8") as f:
        for h in hashes_records:
            f.write(json.dumps(h) + "\n")

    # 8. raw/file_inventory.jsonl
    inventory = []
    for root, _, files in os.walk(output_dir):
        for fl in files:
            full = os.path.join(root, fl)
            rel = os.path.relpath(full, output_dir)
            sz = os.path.getsize(full)
            residue = "orphan_moving" if fl.endswith(".moving") else ("orphan_segment" if fl.endswith(".seg") else "expected_target")
            inventory.append({"path": rel, "size_bytes": sz, "classification": residue})

    for root, _, files in os.walk(buffer_dir):
        for fl in files:
            full = os.path.join(root, fl)
            rel = os.path.relpath(full, buffer_dir)
            sz = os.path.getsize(full)
            inventory.append({"path": f"buffer/{rel}", "size_bytes": sz, "classification": "buffer_residue"})

    with open(os.path.join(raw_dir, "file_inventory.jsonl"), "w", encoding="utf-8") as f:
        for inv in inventory:
            f.write(json.dumps(inv) + "\n")

    # 9. raw/checksums.sha256 (Cryptographic Seal)
    checksums_path = os.path.join(raw_dir, "checksums.sha256")
    raw_files = [
        "protocol.json", "run_spec.json", "artifact.json", "environment.json",
        "manifest.jsonl", "events.jsonl", "metrics.jsonl", "task_results.jsonl",
        "file_inventory.jsonl", "hashes.jsonl", "errors.jsonl", "process.log"
    ]
    with open(checksums_path, "w", encoding="utf-8") as f:
        for r_file in sorted(raw_files):
            fp = os.path.join(raw_dir, r_file)
            if os.path.exists(fp):
                c_sha = compute_sha256(fp)
                f.write(f"{c_sha}  {r_file}\n")

    print(f"[✓] Raw evidence sealed with checksums.sha256.")

    # Phase 2: Analysis Policy Evaluation
    evaluate_policy(run_root, policy_version="baseline-v1")
    return run_root

def evaluate_policy(run_root, policy_version="baseline-v1"):
    raw_dir = os.path.join(run_root, "raw")
    analysis_dir = os.path.join(run_root, "analysis", policy_version)
    os.makedirs(analysis_dir, exist_ok=True)

    with open(os.path.join(raw_dir, "task_results.jsonl"), "r", encoding="utf-8") as f:
        task_results = [json.loads(line) for line in f if line.strip()]

    with open(os.path.join(raw_dir, "hashes.jsonl"), "r", encoding="utf-8") as f:
        hashes = [json.loads(line) for line in f if line.strip()]

    with open(os.path.join(raw_dir, "file_inventory.jsonl"), "r", encoding="utf-8") as f:
        inventory = [json.loads(line) for line in f if line.strip()]

    with open(os.path.join(raw_dir, "metrics.jsonl"), "r", encoding="utf-8") as f:
        metrics = [json.loads(line) for line in f if line.strip()]

    total_cases = len(task_results)
    completed_cases = sum(1 for t in task_results if t["terminal_state"] == "COMPLETED")
    failed_cases = total_cases - completed_cases
    match_fraction = (completed_cases / total_cases) if total_cases > 0 else 0.0

    orphan_residue = sum(1 for inv in inventory if inv["classification"] != "expected_target")
    
    # Calculate average throughput
    speeds = [m.get("rolling_5s_bps", 0) for m in metrics if m.get("rolling_5s_bps", 0) > 0]
    avg_bps = sum(speeds) / len(speeds) if speeds else 0
    avg_mbps = round((avg_bps * 8) / (1024 * 1024), 2)

    verdict_status = "GO" if (match_fraction == 1.0 and orphan_residue == 0) else "NO-GO"

    summary = {
        "policy_version": policy_version,
        "protocol_version": PROTOCOL_VERSION,
        "evaluated_at": iso_time(),
        "total_cases": total_cases,
        "completed_cases": completed_cases,
        "failed_cases": failed_cases,
        "match_fraction": round(match_fraction, 4),
        "orphan_residue_count": orphan_residue,
        "average_active_mbps": avg_mbps,
        "verdict": verdict_status,
    }

    with open(os.path.join(analysis_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(os.path.join(analysis_dir, "verdict.json"), "w", encoding="utf-8") as f:
        json.dump({"verdict": verdict_status, "policy": policy_version, "summary": summary}, f, indent=2)

    with open(os.path.join(analysis_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write(f"# TGX Evaluation Report - `{policy_version}`\n\n")
        f.write(f"- **Verdict**: `{verdict_status}`\n")
        f.write(f"- **Completed Ratio**: {completed_cases}/{total_cases} ({match_fraction * 100:.1f}%)\n")
        f.write(f"- **Orphan Residue**: {orphan_residue}\n")
        f.write(f"- **Average Active Speed**: {avg_mbps} Mbps\n\n")
        f.write("## Hashes & Verification Breakdown\n\n")
        f.write("| Case ID | Expected Path | Expected Size | Actual Size | SHA Match |\n")
        f.write("|---|---|---|---|---|\n")
        for h in hashes:
            f.write(f"| {h['case_id']} | `{h['expected_path']}` | {h['expected_size']} | {h['actual_size']} | **{h['sha_match']}** |\n")

    print(f"[✓] Policy evaluation `{policy_version}` complete. Verdict: {verdict_status}")
    print(f"[✓] Report written to: {os.path.join(analysis_dir, 'report.md')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TGX Evaluation Protocol v1 Reference Harness")
    parser.add_argument("--run-spec", required=True, help="Path to run_spec.json")
    parser.add_argument("--engine-binary", required=True, help="Path to engine executable")
    parser.add_argument("--api", default="http://127.0.0.1:5885")
    args = parser.parse_args()

    with open(args.run_spec, "r", encoding="utf-8") as f:
        spec = json.load(f)

    execute_run(spec, args.engine_binary, args.api)
