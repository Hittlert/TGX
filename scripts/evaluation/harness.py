#!/usr/bin/env python3
"""Execute one isolated TGX evaluation run and seal its raw facts."""

import os
import time
import json
import shutil
import hashlib
import argparse
import threading
import urllib.request
import subprocess
from pathlib import Path, PurePosixPath

from analyze import seal_raw_directory

PROTOCOL_VERSION = "1.0"
PROTOCOL_SHA256 = "4dbdf4940f5c751d683c79f3392bbca770832ef258360f57244d69fc3d589c0e"

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
        self.stop_event = threading.Event()
        self.samples = []
        self.t0 = time.time()
        self.phase = "setup"

    def run(self):
        with open(self.metrics_file, "w", encoding="utf-8") as f:
            while not self.stop_event.is_set():
                now = time.time()
                elapsed = round(now - self.t0, 3)
                
                rec = {
                    "timestamp": iso_time(now),
                    "monotonic_elapsed_sec": elapsed,
                    "engine": self.engine,
                    "phase": self.phase,

                    "wire_rx_bytes": None,
                    "unique_payload_bytes": None,
                    "rolling_5s_bps": None,
                    "active_rpc": None,
                    "queued_jobs": None,
                    "connection_count": None,
                    "connection_failures": None,
                    "retry_count": None,
                    "flood_wait_count": None,
                    "flood_wait_seconds": None,
                    "per_dc_payload_bps": None,
                    "process_rss": None,
                    "heap_alloc": None,
                    "heap_inuse": None,
                    "heap_objects": None,
                    "gc_count": None,
                    "gc_pause_total": None,
                    "ssd_free_bytes": None,
                    "ssd_total_bytes": None,
                    "ssd_used_bytes": None,
                    "ssd_reserved_bytes": None,
                    "ssd_available_bytes": None,
                    "archive_backlog_files": None,
                    "archive_backlog_bytes": None,
                    "archive_active_workers": None,
                    "archive_archived_files": None,
                    "archive_conflict_count": None,
                    "target_write_bytes": None,
                    "target_read_bytes": None,
                    "target_durable_bytes": None,
                    "target_writer_concurrency": None,
                    "target_backlog_bytes": None,
                    "fsync_count": None,
                    "fsync_latency": None,
                    "device_util": None,
                    "device_await": None,
                    "collection_errors": [],
                }

                try:
                    st = http_get(f"{self.api_base}/api/status", timeout=1.5)
                    rec["rolling_5s_bps"] = st.get("rolling_5s_bps")
                    rec["queued_jobs"] = st.get("queue_depth")
                    active_files = st.get("active_files")
                    if isinstance(active_files, list):
                        rec["active_rpc"] = len(active_files)
                    pool = st.get("pool") or {}
                    rec["connection_count"] = pool.get("size")
                    rec["connection_failures"] = pool.get("reconnects")
                except Exception as e:
                    rec["collection_errors"].append({"source": "/api/status", "error": str(e)})

                try:
                    gate = http_get(f"{self.api_base}/api/gate", timeout=1.5)
                    rec["ssd_reserved_bytes"] = gate.get("ssd_reserved_bytes")
                    rec["ssd_available_bytes"] = gate.get("ssd_available_bytes")
                except Exception as e:
                    if self.engine == "tgx":
                        rec["collection_errors"].append(
                            {"source": "/api/gate", "error": str(e)}
                        )

                try:
                    storage = http_get(f"{self.api_base}/api/system/storage", timeout=1.5)
                    rec["ssd_free_bytes"] = storage.get("free_bytes")
                    rec["ssd_total_bytes"] = storage.get("total_bytes")
                    rec["ssd_used_bytes"] = storage.get("used_bytes")
                    archive = storage.get("archive") or {}
                    for field in (
                        "archive_backlog_files",
                        "archive_backlog_bytes",
                        "archive_active_workers",
                        "archive_archived_files",
                        "archive_conflict_count",
                    ):
                        rec[field] = archive.get(field)
                except Exception as e:
                    if self.engine == "tgx":
                        rec["collection_errors"].append({"source": "/api/system/storage", "error": str(e)})

                f.write(json.dumps(rec) + "\n")
                f.flush()
                self.samples.append(rec)

                rem = 1.0 - (time.time() - now)
                if rem > 0.05:
                    self.stop_event.wait(rem)

    def stop(self):
        self.stop_event.set()

    def set_phase(self, phase):
        self.phase = phase

def extract_artifact_metadata(
    engine_binary,
    engine,
    docker_command,
    runner_image,
    source_repository,
):
    binary_sha = compute_sha256(engine_binary) or "unknown"
    harness_sha = compute_sha256(__file__) if os.path.exists(__file__) else "unknown"

    img_digest = "unknown"
    try:
        d_cmd = docker_command + [
            "inspect",
            "--format={{range .RepoDigests}}{{.}}{{end}}",
            runner_image,
        ]
        d_res = subprocess.run(d_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
        d_str = d_res.stdout.strip()
        if d_str:
            img_digest = d_str
        else:
            id_cmd = docker_command + [
                "inspect",
                "--format={{.Id}}",
                runner_image,
            ]
            id_res = subprocess.run(id_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            if id_res.stdout.strip():
                img_digest = id_res.stdout.strip()
    except Exception:
        pass

    meta = {
        "engine": engine,
        "source_repository": source_repository,
        "source_commit": "unknown",
        "source_dirty": None,
        "binary_sha256": binary_sha,
        "harness_sha256": harness_sha,
        "image_digest": img_digest,
        "version": "unknown",
        "build_time": "unknown",
        "go_version": "unknown",
        "os": "linux",
        "arch": "amd64",
    }

    try:
        cmd = docker_command + [
            "run",
            "--rm",
            "-v",
            f"{os.path.abspath(engine_binary)}:/app/bin:ro",
            runner_image,
            "/app/bin",
            "version",
        ]
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
            else:
                parts = line.split()
                if len(parts) == 2 and parts[0].startswith("go") and "/" in parts[1]:
                    meta["go_version"] = parts[0]
                    meta["os"], meta["arch"] = parts[1].split("/", 1)
        if meta["version"] == "unknown" and meta.get("tdl_revision"):
            meta["version"] = "1.0.0"
    except Exception as e:
        meta["extraction_error"] = str(e)
    return meta

def ensure_new_directory(path):
    path = Path(path)
    if path.exists():
        raise FileExistsError(f"refusing to reuse existing evaluation path: {path}")
    path.mkdir(parents=True)
    return path


def safe_relative_path(value):
    path = PurePosixPath(str(value).replace("\\", "/"))
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError(f"unsafe manifest output path: {value!r}")
    return str(path)


def execute_run(run_spec, engine_binary, eval_dir, host_port=5890):
    run_id = run_spec["run_id"]
    engine = run_spec["engine"]
    profile_id = run_spec["profile_id"]
    duration_sec = run_spec["duration_seconds"]
    warmup_sec = run_spec.get("warmup_seconds", 0)
    drain_timeout_sec = run_spec.get("drain_timeout_seconds", 60)
    net_concurrency = run_spec.get("net_concurrency", 32)
    file_concurrency = run_spec.get("file_concurrency", 5)
    dc_pool_size = run_spec.get("dc_pool_size", 32)
    manifest_path = Path(run_spec["manifest_path"])
    manifest_sha256 = compute_sha256(manifest_path)
    if manifest_sha256 != run_spec.get("manifest_sha256"):
        raise ValueError("RunSpec manifest_sha256 does not match the selected manifest")
    if not run_spec.get("baseline_cohort_id"):
        raise ValueError("RunSpec baseline_cohort_id is required")

    docker_command = run_spec.get("docker_command")
    if not isinstance(docker_command, list) or not docker_command:
        raise ValueError("RunSpec docker_command must be a non-empty argument list")
    runner_image = run_spec.get("runner_image")
    if not runner_image:
        raise ValueError("RunSpec runner_image is required")
    api_base = run_spec.get("api_base") or f"http://127.0.0.1:{host_port}"
    artifact_meta = extract_artifact_metadata(
        engine_binary,
        engine,
        docker_command,
        runner_image,
        run_spec["source_repository"],
    )
    if artifact_meta["binary_sha256"] != run_spec.get("expected_binary_sha256"):
        raise ValueError("engine binary does not match RunSpec expected SHA-256")
    if artifact_meta["source_commit"] != run_spec.get("expected_source_commit"):
        raise ValueError("engine binary does not match RunSpec expected source commit")
    if artifact_meta.get("source_dirty") is not False:
        raise ValueError("engine binary reports dirty or unknown source state")
    if str(artifact_meta.get("version")).lower() in ("", "dev", "unknown"):
        raise ValueError("engine binary does not report a release identity")
    for field in ("build_time", "go_version", "os", "arch"):
        if str(artifact_meta.get(field) or "").lower() in ("", "unknown"):
            raise ValueError(f"engine binary does not report {field}")
    if artifact_meta.get("image_digest") == "unknown":
        raise ValueError("runner image identity could not be resolved")

    if engine == "tdl":
        run_root = Path(eval_dir) / "baselines" / "tdl" / run_id
    else:
        run_root = Path(eval_dir) / "runs" / "tgx" / run_id
    ensure_new_directory(run_root)
    raw_dir = run_root / "raw"
    raw_dir.mkdir()

    scratch_root = ensure_new_directory(run_spec["scratch_root"])
    output_dir = scratch_root / "output"
    temp_dir = scratch_root / "temp"
    session_dir = scratch_root / "session"
    state_dir = scratch_root / "state"
    log_dir = scratch_root / "logs"
    for path in (output_dir, temp_dir, session_dir, state_dir, log_dir):
        path.mkdir()

    source_session = Path(run_spec["session_source_dir"])
    if not source_session.is_dir():
        raise FileNotFoundError(f"session source does not exist: {source_session}")
    for source in source_session.iterdir():
        if source.is_file():
            shutil.copy2(source, session_dir / source.name)

    print(f"\n=======================================================")
    print(f"  TGX Evaluation Protocol v1.0 - RUN: {run_id}")
    print(f"  Engine: {engine.upper()} | Profile: {profile_id} | Duration: {duration_sec}s")
    print(f"  Concurrency: net={net_concurrency}, file={file_concurrency}, pool={dc_pool_size}")
    print(f"  Port: {host_port} | Isolated Output: {output_dir}")
    print(f"  Raw Evidence Root: {raw_dir}")
    print(f"=======================================================")

    # 1. raw/protocol.json
    with open(raw_dir / "protocol.json", "w", encoding="utf-8") as f:
        json.dump({"protocol_version": PROTOCOL_VERSION, "protocol_sha256": PROTOCOL_SHA256}, f, indent=2)

    # 2. raw/run_spec.json
    run_spec_copy = dict(run_spec)
    run_spec_copy["output_dir"] = str(output_dir)
    run_spec_copy["state_db"] = str(state_dir / "records.sqlite3")
    run_spec_copy["log_dir"] = str(log_dir)
    with open(raw_dir / "run_spec.json", "w", encoding="utf-8") as f:
        json.dump(run_spec_copy, f, indent=2)

    # 3. raw/artifact.json
    with open(raw_dir / "artifact.json", "w", encoding="utf-8") as f:
        json.dump(artifact_meta, f, indent=2)

    env_meta = dict(run_spec.get("environment") or {})
    env_meta["container_identity"] = f"tgx-eval-runner-{run_id}"
    with open(raw_dir / "environment.json", "w", encoding="utf-8") as f:
        json.dump(env_meta, f, indent=2)

    # 5. raw/manifest.jsonl (byte-identical copy)
    raw_manifest_path = raw_dir / "manifest.jsonl"
    shutil.copyfile(manifest_path, raw_manifest_path)

    cases = []
    with open(raw_manifest_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                cases.append(json.loads(line))

    # Initialize raw facts files
    events_file = raw_dir / "events.jsonl"
    errors_file = raw_dir / "errors.jsonl"
    metrics_file = raw_dir / "metrics.jsonl"
    process_log_file = raw_dir / "process.log"

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

    print(f"[*] Spawning Runner Container: {container_name}...")
    serve_args = docker_command + [
        "run", "-d",
        "--name", container_name,
        "-v", f"{os.path.abspath(engine_binary)}:/app/telegram-downloader:ro",
        "-v", f"{output_dir}:/app/downloads",
        "-v", f"{session_dir}:/data",
        "-v", f"{state_dir}:/app/state",
        "-v", f"{log_dir}:/app/logs",
    ]
    network_container = run_spec.get("network_container")
    if network_container:
        serve_args.extend(["--net", f"container:{network_container}"])
    else:
        serve_args.extend(["-p", f"127.0.0.1:{host_port}:5000"])
    if engine == "tdl":
        serve_args.extend(["-v", f"{temp_dir}:/app/temp/tdl"])

    serve_args.extend([
        runner_image,
        "/app/telegram-downloader", "serve",
        "--dir", "/app/downloads",
        "--storage-path", "/data",
        "--db-path", "/app/state/records.sqlite3",
        "--namespace", run_spec.get("namespace", "evaluation"),
        "--listen", "0.0.0.0:5000",
        "--download-threads", str(net_concurrency),
        "--file-concurrency", str(file_concurrency),
        "--dc-pool-size", str(dc_pool_size),
    ])
    if engine == "tdl":
        serve_args.extend(["--temp-dir", "/app/temp/tdl"])

    res = subprocess.run(serve_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Failed to start container {container_name}: {res.stderr}")

    # Wait for daemon health
    print(f"[*] Waiting for {container_name} to be ready at {api_base}/healthz...")
    ready = False
    for _ in range(90):
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
        p_logs = subprocess.run(
            docker_command + ["logs", container_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        subprocess.run(
            docker_command + ["rm", "-f", container_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        raise RuntimeError(f"Engine {engine} failed to start within 90s. Logs:\n{p_logs.stdout}")

    print("[OK] Engine daemon is healthy and ready!")
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
        rel_path = safe_relative_path(
            c["expected_tgx_path"] if engine == "tgx" else c["expected_tdl_path"]
        )
        task_id = f"{run_id}_{c['chat_id']}_{c['message_id']}"
        req = {
            "id": task_id,
            "peer": str(c["chat_id"]),
            "message_id": int(c["message_id"]),
            "final_path": rel_path,
            "expected_size": int(c["expected_size"]),
        }
        t_sub = time.time()
        try:
            http_post(f"{api_base}/api/tasks", req)
            log_event("item.submitted", {"case_id": case_id, "task_id": task_id})
            task_submits[case_id] = {"task_id": task_id, "submitted_at": t_sub, "submitted": True, "error": None}
        except Exception as e:
            err_entry = {
                "case_id": case_id,
                "attempt_id": "not-admitted",
                "stage": "submit",
                "op": "http_post",
                "error_code": "SUBMIT_FAILED",
                "error_cause": str(e),
                "retryable": False,
            }
            errors_fp.write(json.dumps(err_entry) + "\n")
            task_submits[case_id] = {"task_id": task_id, "submitted_at": t_sub, "submitted": False, "error": str(e)}

    # Resume daemon
    try:
        http_post(f"{api_base}/api/control", {"action": "resume"})
        log_event("daemon.resumed")
    except Exception:
        pass

    if warmup_sec:
        sampler.set_phase("warmup")
        log_event("run.warmup.started", {"duration_seconds": warmup_sec})
        time.sleep(warmup_sec)
        log_event("run.warmup.finished")

    sampler.set_phase("measurement")
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
            if active == 0 and q == 0 and elapsed >= 30:
                try:
                    tasks_resp = http_get(f"{api_base}/api/tasks")
                    term_count = sum(1 for t in tasks_resp if t.get("state") in ("success", "failed", "unavailable"))
                    if term_count >= len(cases):
                        print(f"\n[*] All {term_count}/{len(cases)} tasks finished ahead of duration timeout.")
                        break
                except Exception:
                    pass
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
                print("[OK] Queue drained completely!")
                break
        except Exception:
            pass

    # Fetch Daemon Task Snapshots before tearing down container
    engine_tasks = {}
    try:
        ts_resp = http_get(f"{api_base}/api/tasks", timeout=5)
        if isinstance(ts_resp, list):
            for t in ts_resp:
                if "id" in t:
                    engine_tasks[t["id"]] = t
    except Exception:
        pass

    log_event("run.finished")
    sampler.stop()
    sampler.join()
    events_fp.close()

    # Capture process logs
    with open(process_log_file, "w", encoding="utf-8") as f:
        f.write(f"=== Process Log for Run {run_id} ===\n")
        try:
            p = subprocess.run(
                docker_command + ["logs", container_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=10,
            )
            f.write(p.stdout)
        except Exception as e:
            f.write(f"Direct log capture: {e}\n")

    subprocess.run(
        docker_command + ["rm", "-f", container_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)

    # 7. raw/task_results.jsonl, raw/errors.jsonl, and raw/hashes.jsonl
    print("[*] Generating task_results.jsonl, file_inventory.jsonl, and hashes.jsonl...")
    task_results = []
    hashes_records = []

    for c in cases:
        case_id = c["case_id"]
        rel_path = safe_relative_path(
            c["expected_tgx_path"] if engine == "tgx" else c["expected_tdl_path"]
        )
        expected_size = c["expected_size"]
        baseline_sha = c["baseline_sha256"]
        target_file = output_dir / rel_path
        task_id = f"{run_id}_{c['chat_id']}_{c['message_id']}"

        actual_size = target_file.stat().st_size if target_file.is_file() else 0
        actual_sha = compute_sha256(target_file) if target_file.is_file() else ""

        size_match = (actual_size == expected_size and expected_size > 0)
        sha_match = (actual_sha == baseline_sha and baseline_sha != "") if baseline_sha else size_match
        file_present = target_file.is_file()

        eng_t = engine_tasks.get(task_id, {})
        eng_state = eng_t.get("state", "unknown")
        admitted = (task_id in engine_tasks or task_submits.get(case_id, {}).get("submitted", False))
        
        err_code = None
        err_stage = None
        err_op = None
        err_cause = None

        if file_present and size_match and sha_match:
            terminal_state = "COMPLETED"
        elif eng_state == "canceled" or (eng_t.get("error_class") == "canceled"):
            terminal_state = "CANCELED"
            err_code = "CANCELED"
            err_stage = "lifecycle"
            err_op = "cancel"
            err_cause = eng_t.get("error", "task canceled")
        elif not file_present and (time.time() - start_time >= duration_sec or eng_state in ("queued", "downloading")):
            terminal_state = "TIMED_OUT"
            err_code = "TIMED_OUT"
            err_stage = "lifecycle"
            err_op = "drain"
            err_cause = f"task did not complete before run duration ({duration_sec}s)"
        else:
            terminal_state = "FAILED"
            if eng_t.get("error_class"):
                err_code = eng_t.get("error_class")
                err_cause = eng_t.get("error", "engine error")
                err_stage = "engine"
                err_op = "download"
            elif not file_present:
                err_code = "FILE_MISSING"
                err_stage = "storage"
                err_op = "verify_presence"
                err_cause = f"expected file {rel_path} not created on disk (engine state: {eng_state})"
            elif not size_match:
                err_code = "SIZE_MISMATCH"
                err_stage = "oracle"
                err_op = "verify_size"
                err_cause = f"expected {expected_size} bytes, actual {actual_size} bytes"
            elif not sha_match:
                err_code = "SHA_MISMATCH"
                err_stage = "oracle"
                err_op = "verify_sha256"
                err_cause = f"expected sha {baseline_sha[:12]}, actual sha {actual_sha[:12]}"
            
        if terminal_state != "COMPLETED":
            err_entry = {
                "case_id": case_id,
                "task_id": task_id,
                "attempt_id": eng_t.get("attempt_generation"),
                "stage": err_stage or "oracle",
                "op": err_op or "verification",
                "error_code": err_code or "FAILED",
                "error_cause": err_cause or "unknown failure",
                "retryable": False,
            }
            errors_fp.write(json.dumps(err_entry) + "\n")

        t_res = {
            "case_id": case_id,
            "task_id": task_id,
            "attempt_id": eng_t.get("attempt_generation"),
            "submitted": task_submits.get(case_id, {}).get("submitted", False),
            "admitted": admitted,
            "terminal_state": terminal_state,
            "attempt_count": 1,
            "error_code": err_code,
            "error_stage": err_stage,
            "error_op": err_op,
            "error_cause": err_cause,
            "started_at": int(eng_t.get("started_at") or task_submits.get(case_id, {}).get("submitted_at", start_time)),
            "finished_at": int(eng_t.get("finished_at") or time.time()),
            "network_unique_bytes": eng_t.get("net_downloaded", actual_size),
            "target_durable_bytes": actual_size,
        }
        task_results.append(t_res)

        h_res = {
            "case_id": case_id,
            "expected_path": rel_path,
            "actual_path": str(target_file) if file_present else None,
            "expected_size": expected_size,
            "actual_size": actual_size,
            "baseline_sha256": baseline_sha,
            "actual_sha256": actual_sha,
            "size_match": size_match,
            "sha_match": sha_match,
        }
        hashes_records.append(h_res)

    errors_fp.close()

    with open(raw_dir / "task_results.jsonl", "w", encoding="utf-8") as f:
        for t in task_results:
            f.write(json.dumps(t) + "\n")

    with open(raw_dir / "hashes.jsonl", "w", encoding="utf-8") as f:
        for h in hashes_records:
            f.write(json.dumps(h) + "\n")

    # 8. raw/file_inventory.jsonl
    inventory = []
    expected_paths = {
        safe_relative_path(
            case["expected_tgx_path"]
            if engine == "tgx"
            else case["expected_tdl_path"]
        )
        for case in cases
    }
    for root, _, files in os.walk(output_dir):
        for fl in files:
            full = os.path.join(root, fl)
            rel = os.path.relpath(full, output_dir).replace(os.sep, "/")
            sz = os.path.getsize(full)
            if rel in expected_paths:
                classification = "expected_target"
            elif fl.endswith(".moving"):
                classification = "orphan_moving"
            elif fl.endswith((".seg", ".meta", ".part", ".tmp")) or ".part." in fl:
                classification = "orphan_temporary"
            else:
                classification = "unexpected_file"
            inventory.append(
                {"path": rel, "size_bytes": sz, "classification": classification}
            )

    if os.path.exists(temp_dir):
        for root, _, files in os.walk(temp_dir):
            for fl in files:
                full = os.path.join(root, fl)
                rel = os.path.relpath(full, temp_dir)
                sz = os.path.getsize(full)
                inventory.append({"path": f"temp/{rel}", "size_bytes": sz, "classification": "temp_residue"})

    with open(raw_dir / "file_inventory.jsonl", "w", encoding="utf-8") as f:
        for inv in inventory:
            f.write(json.dumps(inv) + "\n")

    seal_raw_directory(raw_dir)
    print("[OK] Raw evidence sealed with checksums.sha256.")
    return str(run_root)

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
