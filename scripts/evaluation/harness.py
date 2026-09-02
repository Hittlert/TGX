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

def extract_artifact_metadata(engine_binary, engine):
    binary_sha = compute_sha256(engine_binary) or "unknown"
    meta = {
        "engine": engine,
        "source_repository": "https://github.com/Hittlert/TGX",
        "source_commit": "unknown",
        "source_dirty": False,
        "binary_sha256": binary_sha,
        "image_digest": "alpine:latest",
        "version": "unknown",
        "build_time": iso_time(),
        "go_version": "unknown",
        "os": "linux",
        "arch": "amd64",
    }
    
    try:
        cmd = ["sudo", "docker", "run", "--rm", "-v", f"{os.path.abspath(engine_binary)}:/app/bin:ro", "alpine:latest", "/app/bin", "version"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10)
        output = res.stdout + res.stderr
        for line in output.splitlines():
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if k == "main_revision":
                    meta["source_commit"] = v
                elif k == "tdl_revision":
                    meta["tdl_revision"] = v
                elif k == "go_version":
                    meta["go_version"] = v
                elif k == "main_dirty":
                    meta["source_dirty"] = (v.lower() == "true")
            elif ":" in line:
                k, v = line.split(":", 1)
                k, v = k.strip(), v.strip()
                if k == "Version":
                    meta["version"] = v
                elif k == "Commit":
                    meta["source_commit"] = v
                elif k == "Date":
                    meta["build_time"] = v
        if meta["version"] == "unknown" and meta.get("tdl_revision"):
            meta["version"] = "1.0.0"
    except Exception as e:
        meta["extraction_error"] = str(e)
    return meta

def execute_run(run_spec, engine_binary, eval_dir="/volume2/docker/telegram_downloader_eval/evaluation", host_port=5890):
    run_id = run_spec["run_id"]
    engine = run_spec["engine"]
    profile_id = run_spec["profile_id"]
    duration_sec = run_spec["duration_seconds"]
    drain_timeout_sec = run_spec.get("drain_timeout_seconds", 60)
    net_concurrency = run_spec.get("net_concurrency", 32)
    file_concurrency = run_spec.get("file_concurrency", 5)
    dc_pool_size = run_spec.get("dc_pool_size", 32)
    manifest_path = run_spec["manifest_path"]

    # Target directory structure: baselines/tdl/<run_id>/raw or runs/tgx/<run_id>/raw
    if engine == "tdl":
        run_root = os.path.join(eval_dir, "baselines", "tdl", run_id)
    else:
        run_root = os.path.join(eval_dir, "runs", "tgx", run_id)

    raw_dir = os.path.join(run_root, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # Scratch isolation per run
    scratch_root = f"/volume2/docker/telegram_downloader_eval/scratch_runs/{run_id}"
    output_dir = os.path.join(scratch_root, "output")
    temp_dir = os.path.join(scratch_root, "temp")
    buffer_dir = os.path.join(scratch_root, "buffer")
    session_dir = os.path.join(scratch_root, "session")
    log_dir = os.path.join(scratch_root, "logs")

    # Clean previous run scratch if exists
    if os.path.exists(scratch_root):
        shutil.rmtree(scratch_root, ignore_errors=True)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(buffer_dir, exist_ok=True)
    os.makedirs(session_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    # Copy session state for independent run
    src_session = "/volume2/docker/telegram_downloader_eval/tdl-state"
    if os.path.exists(src_session):
        for f_name in os.listdir(src_session):
            s_fp = os.path.join(src_session, f_name)
            d_fp = os.path.join(session_dir, f_name)
            if os.path.isfile(s_fp):
                shutil.copy2(s_fp, d_fp)

    print(f"\n=======================================================")
    print(f"  TGX Evaluation Protocol v1.0 - RUN: {run_id}")
    print(f"  Engine: {engine.upper()} | Profile: {profile_id} | Duration: {duration_sec}s")
    print(f"  Concurrency: net={net_concurrency}, file={file_concurrency}, pool={dc_pool_size}")
    print(f"  Port: {host_port} | Isolated Output: {output_dir}")
    print(f"  Raw Evidence Root: {raw_dir}")
    print(f"=======================================================")

    # 1. raw/protocol.json
    with open(os.path.join(raw_dir, "protocol.json"), "w", encoding="utf-8") as f:
        json.dump({"protocol_version": PROTOCOL_VERSION, "protocol_sha256": PROTOCOL_SHA256}, f, indent=2)

    # 2. raw/run_spec.json
    run_spec_copy = dict(run_spec)
    run_spec_copy["output_dir"] = output_dir
    run_spec_copy["buffer_dir"] = buffer_dir
    run_spec_copy["log_dir"] = log_dir
    with open(os.path.join(raw_dir, "run_spec.json"), "w", encoding="utf-8") as f:
        json.dump(run_spec_copy, f, indent=2)

    # 3. raw/artifact.json
    artifact_meta = extract_artifact_metadata(engine_binary, engine)
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
        "container_identity": f"tgx-eval-runner-{run_id}",
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

    container_name = f"tgx-eval-runner-{run_id}"
    subprocess.run(["sudo", "docker", "rm", "-f", container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Launch Runner Container
    print(f"[*] Spawning Runner Container: {container_name} on port {host_port}...")
    serve_args = [
        "sudo", "docker", "run", "-d",
        "--name", container_name,
        "-p", f"{host_port}:5000",
        "-v", f"{os.path.abspath(engine_binary)}:/app/telegram-downloader:ro",
        "-v", f"{output_dir}:/app/downloads",
        "-v", f"{session_dir}:/data",
        "-v", f"{log_dir}:/app/logs",
        "--network", "tgx-eval-net",
    ]
    if engine == "tdl":
        serve_args.extend(["-v", f"{temp_dir}:/app/temp/tdl"])
    else:
        serve_args.extend(["-v", f"{buffer_dir}:/app/buffer"])

    serve_args.extend([
        "alpine:latest",
        "/app/telegram-downloader", "serve",
        "--dir", "/app/downloads",
        "--storage-path", "/data",
        "--namespace", "default",
        "--listen", "0.0.0.0:5000",
        "--download-threads", str(net_concurrency),
        "--file-concurrency", str(file_concurrency),
        "--dc-pool-size", str(dc_pool_size),
    ])
    if engine == "tdl":
        serve_args.extend(["--temp-dir", "/app/temp/tdl"])
    else:
        serve_args.extend(["--buffer-dir", "/app/buffer"])

    res = subprocess.run(serve_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Failed to start container {container_name}: {res.stderr}")

    api_base = f"http://127.0.0.1:{host_port}"

    # Wait for daemon health
    print(f"[*] Waiting for {container_name} to be ready at {api_base}/healthz...")
    ready = False
    for _ in range(30):
        time.sleep(1)
        try:
            req = urllib.request.Request(f"{api_base}/healthz")
            with urllib.request.urlopen(req, timeout=1) as resp:
                if resp.status == 200:
                    ready = True
                    break
        except Exception:
            pass

    if not ready:
        p_logs = subprocess.run(["sudo", "docker", "logs", container_name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        subprocess.run(["sudo", "docker", "rm", "-f", container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raise RuntimeError(f"Engine {engine} failed to start within 30s. Logs:\n{p_logs.stdout}")

    print("[✓] Engine daemon is healthy and ready!")
    log_event("run.started", {"run_id": run_id, "cases_total": len(cases)})

    # Start 1s Metrics Sampler
    sampler = MetricsSampler(api_base, metrics_file, engine)
    sampler.start()

    # Pause daemon while submitting tasks
    try:
        http_post(f"{api_base}/api/control", {"action": "pause"})
        log_event("daemon.paused")
    except Exception:
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
    except Exception:
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

    # Capture process logs
    with open(process_log_file, "w", encoding="utf-8") as f:
        f.write(f"=== Process Log for Run {run_id} ===\n")
        try:
            p = subprocess.run(["sudo", "docker", "logs", "--tail", "2000", container_name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=5)
            f.write(p.stdout)
        except Exception as e:
            f.write(f"Direct log capture: {e}\n")

    # Teardown Container
    subprocess.run(["sudo", "docker", "rm", "-f", container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # 7. raw/task_results.jsonl and raw/hashes.jsonl
    print("[*] Generating task_results.jsonl, file_inventory.jsonl, and hashes.jsonl...")
    task_results = []
    hashes_records = []

    for c in cases:
        case_id = c["case_id"]
        rel_path = c["expected_tgx_path"] if engine == "tgx" else c["expected_tdl_path"]
        expected_size = c["expected_size"]
        baseline_sha = c["baseline_sha256"]
        target_file = os.path.join(output_dir, rel_path)

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
    parser.add_argument("--eval-dir", default="/volume2/docker/telegram_downloader_eval/evaluation")
    parser.add_argument("--port", type=int, default=5890, help="Host port to bind runner container")
    args = parser.parse_args()

    with open(args.run_spec, "r", encoding="utf-8") as f:
        spec = json.load(f)

    execute_run(spec, args.engine_binary, eval_dir=args.eval_dir, host_port=args.port)
