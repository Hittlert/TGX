#!/usr/bin/env python3
"""Execute one isolated TGX evaluation run and seal its raw facts."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from analyze import seal_raw_directory


PROTOCOL_VERSION = "1.0"
PROTOCOL_SHA256 = "17a829372cffeb6ccdd2a591117219d70885c431f0b5bc83f53c2ca0e64daeda"
METRIC_METADATA_FIELDS = {
    "timestamp",
    "monotonic_elapsed_sec",
    "engine",
    "phase",
    "collection_errors",
}
TGX_INTERNAL_METRICS = {
    "active_rpc",
    "ssd_free_bytes",
    "ssd_total_bytes",
    "ssd_used_bytes",
    "ssd_reserved_bytes",
    "ssd_available_bytes",
    "archive_backlog_files",
    "archive_backlog_bytes",
    "archive_active_workers",
    "archive_archived_files",
    "archive_conflict_count",
}


def compute_sha256(path):
    path = Path(path)
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iso_time(timestamp=None):
    timestamp = time.time() if timestamp is None else timestamp
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def write_jsonl(path, records):
    with Path(path).open("x", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def http_json(url, method="GET", data=None, timeout=5, retries=1):
    body = None if data is None else json.dumps(data).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "TGX-Protocol-Harness/1.0",
        },
    )
    last_err = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as err:
            last_err = err
            if attempt < retries:
                time.sleep(0.1)
    raise last_err


def http_ok(url, timeout=5):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "TGX-Protocol-Harness/1.0"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return 200 <= response.status < 300


def safe_relative_path(value):
    path = PurePosixPath(str(value).replace("\\", "/"))
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError(f"unsafe manifest output path: {value!r}")
    return str(path)


def ensure_new_directory(path):
    path = Path(path)
    if path.exists():
        raise FileExistsError(f"refusing to reuse existing evaluation path: {path}")
    path.mkdir(parents=True)
    return path


def parse_binary_version(output):
    metadata = {
        "source_commit": "unknown",
        "source_dirty": None,
        "version": "unknown",
        "build_time": "unknown",
        "go_version": "unknown",
        "os": "unknown",
        "arch": "unknown",
    }
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if "=" in line:
            key, value = (part.strip() for part in line.split("=", 1))
            if key == "main_revision":
                metadata["source_commit"] = value
            elif key == "main_dirty":
                metadata["source_dirty"] = value.lower() == "true"
            elif key == "go_version":
                metadata["go_version"] = value
            elif key == "tdl_revision":
                metadata["tdl_revision"] = value
            continue
        if ":" in line:
            key, value = (part.strip() for part in line.split(":", 1))
            if key == "Version":
                metadata["version"] = value
            elif key == "Commit":
                metadata["source_commit"] = value
            elif key == "Date":
                metadata["build_time"] = value
            continue
        parts = line.split()
        if len(parts) == 2 and parts[0].startswith("go") and "/" in parts[1]:
            metadata["go_version"] = parts[0]
            metadata["os"], metadata["arch"] = parts[1].split("/", 1)

    if metadata["version"] == "unknown" and metadata.get("tdl_revision"):
        metadata["version"] = metadata["tdl_revision"]
    return metadata


def inspect_runner_image(docker_command, runner_image):
    result = subprocess.run(
        docker_command
        + ["inspect", "--format={{json .RepoDigests}}", runner_image],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
    )
    if result.returncode == 0:
        try:
            digests = json.loads(result.stdout.strip())
        except json.JSONDecodeError:
            digests = []
        if digests:
            return sorted(digests)[0]

    result = subprocess.run(
        docker_command + ["inspect", "--format={{.Id}}", runner_image],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def extract_artifact_metadata(run_spec, engine_binary):
    docker_command = run_spec["docker_command"]
    runner_image = run_spec["runner_image"]
    result = subprocess.run(
        docker_command
        + [
            "run",
            "--rm",
            "-v",
            f"{Path(engine_binary).resolve()}:/app/bin:ro",
            runner_image,
            "/app/bin",
            "version",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    metadata = parse_binary_version(result.stdout + result.stderr)
    metadata.update(
        {
            "engine": run_spec["engine"],
            "source_repository": run_spec["source_repository"],
            "binary_sha256": compute_sha256(engine_binary) or "unknown",
            "harness_sha256": compute_sha256(__file__) or "unknown",
            "image_digest": inspect_runner_image(docker_command, runner_image),
        }
    )
    if result.returncode != 0:
        metadata["extraction_error"] = result.stderr.strip()
    return metadata


def validate_artifact(run_spec, artifact):
    checks = {
        "binary SHA-256": (
            artifact.get("binary_sha256"),
            run_spec.get("expected_binary_sha256"),
        ),
        "source commit": (
            artifact.get("source_commit"),
            run_spec.get("expected_source_commit"),
        ),
    }
    for name, (actual, expected) in checks.items():
        if not expected or actual != expected:
            raise ValueError(f"engine {name} does not match RunSpec expectation")
    if artifact.get("source_dirty") is not False:
        raise ValueError("engine binary reports dirty or unknown source state")
    for field in ("version", "build_time", "go_version", "os", "arch"):
        if run_spec.get("engine") == "tdl" and field == "build_time":
            continue
        if str(artifact.get(field) or "").lower() in ("", "dev", "unknown"):
            raise ValueError(f"engine binary does not report {field}")
    if artifact.get("image_digest") == "unknown":
        raise ValueError("runner image identity could not be resolved")


class JSONLWriter:
    def __init__(self, path):
        self.path = Path(path)
        self.stream = None

    def __enter__(self):
        self.stream = self.path.open("x", encoding="utf-8", newline="\n")
        return self

    def write(self, record):
        self.stream.write(json.dumps(record, sort_keys=True) + "\n")
        self.stream.flush()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stream.close()


class MetricsSampler(threading.Thread):
    def __init__(self, api_base, metrics_file, engine):
        super().__init__(daemon=True)
        self.api_base = api_base.rstrip("/")
        self.metrics_file = Path(metrics_file)
        self.engine = engine
        self.stop_event = threading.Event()
        self.started_at = time.monotonic()
        self.phase = "setup"
        self.error = None

    def set_phase(self, phase):
        self.phase = phase

    def stop(self):
        self.stop_event.set()

    def sample(self):
        record = {
            "timestamp": iso_time(),
            "monotonic_elapsed_sec": round(time.monotonic() - self.started_at, 3),
            "engine": self.engine,
            "phase": self.phase,
            "wire_rx_bytes": None,
            "unique_payload_bytes": None,
            "rolling_5s_bps": None,
            "active_files": None,
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
            status = http_json(f"{self.api_base}/api/status", timeout=3.0, retries=1)
            record["rolling_5s_bps"] = status.get("rolling_5s_bps")
            record["queued_jobs"] = status.get("queue_depth")
            active_files = status.get("active_files")
            if isinstance(active_files, list):
                record["active_files"] = len(active_files)
            elif isinstance(active_files, (int, float)):
                record["active_files"] = int(active_files)
            else:
                record["active_files"] = None
            pool = status.get("pool")
            if isinstance(pool, dict):
                record["connection_count"] = pool.get("size")
                record["connection_failures"] = pool.get("reconnects")
            else:
                record["connection_count"] = None
                record["connection_failures"] = None
        except Exception as error:
            record["collection_errors"].append(
                {"source": "/api/status", "error": str(error)}
            )

        if self.engine == "tgx":
            try:
                gate = http_json(f"{self.api_base}/api/gate", timeout=3.0, retries=1)
                record["active_rpc"] = gate.get("data_in_flight")
                record["ssd_reserved_bytes"] = gate.get("ssd_reserved_bytes")
                record["ssd_available_bytes"] = gate.get("ssd_available_bytes")
            except Exception as error:
                record["collection_errors"].append(
                    {"source": "/api/gate", "error": str(error)}
                )
            try:
                storage = http_json(
                    f"{self.api_base}/api/system/storage", timeout=3.0, retries=1
                )
                record["ssd_free_bytes"] = storage.get("free_bytes")
                record["ssd_total_bytes"] = storage.get("total_bytes")
                record["ssd_used_bytes"] = storage.get("used_bytes")
                archive = storage.get("archive")
                if isinstance(archive, dict):
                    for field in (
                        "archive_backlog_files",
                        "archive_backlog_bytes",
                        "archive_active_workers",
                        "archive_archived_files",
                        "archive_conflict_count",
                    ):
                        record[field] = archive.get(field)
                else:
                    for field in (
                        "archive_backlog_files",
                        "archive_backlog_bytes",
                        "archive_active_workers",
                        "archive_archived_files",
                        "archive_conflict_count",
                    ):
                        record[field] = None
            except Exception as error:
                record["collection_errors"].append(
                    {"source": "/api/system/storage", "error": str(error)}
                )
        for field, value in record.items():
            if value is not None or field in METRIC_METADATA_FIELDS:
                continue
            reason = "unavailable"
            if self.engine == "tdl" and field in TGX_INTERNAL_METRICS:
                reason = "unsupported_by_engine"
            record["collection_errors"].append(
                {"source": field, "error": reason}
            )
        return record

    def run(self):
        try:
            with self.metrics_file.open("x", encoding="utf-8", newline="\n") as stream:
                while not self.stop_event.is_set():
                    sample_started = time.monotonic()
                    stream.write(json.dumps(self.sample(), sort_keys=True) + "\n")
                    stream.flush()
                    remaining = 1.0 - (time.monotonic() - sample_started)
                    if remaining > 0:
                        self.stop_event.wait(remaining)
        except Exception as error:
            self.error = error


@dataclass
class RunPaths:
    run_root: Path
    raw: Path
    scratch: Path
    output: Path
    temp: Path
    session: Path
    state: Path
    logs: Path


def prepare_paths(run_spec, eval_dir):
    category = "baselines/tdl" if run_spec["engine"] == "tdl" else "runs/tgx"
    run_root = ensure_new_directory(Path(eval_dir) / category / run_spec["run_id"])
    scratch = ensure_new_directory(run_spec["scratch_root"])
    paths = RunPaths(
        run_root=run_root,
        raw=run_root / "raw",
        scratch=scratch,
        output=scratch / "output",
        temp=scratch / "temp",
        session=scratch / "session",
        state=scratch / "state",
        logs=scratch / "logs",
    )
    for path in (
        paths.raw,
        paths.output,
        paths.temp,
        paths.session,
        paths.state,
        paths.logs,
    ):
        path.mkdir()

    session_source = Path(run_spec["session_source_dir"])
    if not session_source.is_dir():
        raise FileNotFoundError(f"session source does not exist: {session_source}")
    for source in session_source.iterdir():
        if source.is_file():
            shutil.copy2(source, paths.session / source.name)
    return paths


class DockerEngine:
    def __init__(self, run_spec, engine_binary, paths, host_port):
        self.run_spec = run_spec
        self.engine_binary = Path(engine_binary).resolve()
        self.paths = paths
        self.host_port = host_port
        self.docker = run_spec["docker_command"]
        self.name = f"tgx-eval-runner-{run_spec['run_id']}"
        self.api_base = run_spec.get("api_base") or f"http://127.0.0.1:{host_port}"

    def _run_args(self):
        args = self.docker + [
            "run",
            "-d",
            "--name",
            self.name,
            "-v",
            f"{self.engine_binary}:/app/telegram-downloader:ro",
            "-v",
            f"{self.paths.output}:/app/downloads",
            "-v",
            f"{self.paths.session}:/data",
            "-v",
            f"{self.paths.state}:/app/state",
            "-v",
            f"{self.paths.logs}:/app/logs",
        ]
        network_container = self.run_spec.get("network_container")
        if network_container:
            args.extend(["--net", f"container:{network_container}"])
        else:
            args.extend(["-p", f"127.0.0.1:{self.host_port}:5000"])
        if self.run_spec["engine"] == "tdl":
            args.extend(["-v", f"{self.paths.temp}:/app/temp/tdl"])

        args.extend(
            [
                self.run_spec["runner_image"],
                "/app/telegram-downloader",
                "serve",
                "--dir",
                "/app/downloads",
                "--storage-path",
                "/data",
                "--namespace",
                self.run_spec.get("namespace", "evaluation"),
                "--listen",
                "0.0.0.0:5000",
                "--download-threads",
                str(self.run_spec["net_concurrency"]),
                "--file-concurrency",
                str(self.run_spec["file_concurrency"]),
                "--dc-pool-size",
                str(self.run_spec["dc_pool_size"]),
            ]
        )
        if self.run_spec["engine"] == "tgx":
            args.extend(["--db-path", "/app/state/records.sqlite3"])
        if self.run_spec["engine"] == "tdl":
            args.extend(["--temp-dir", "/app/temp/tdl"])
        return args

    def start(self):
        result = subprocess.run(
            self._run_args(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            self.stop()
            raise RuntimeError(f"failed to start {self.name}: {result.stderr.strip()}")

    def wait_ready(self, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if http_ok(f"{self.api_base}/healthz", timeout=1):
                    return
            except Exception:
                time.sleep(1)
        raise TimeoutError(f"{self.name} did not become healthy within {timeout}s")

    def capture_process(self):
        try:
            logs = subprocess.run(
                self.docker + ["logs", self.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=30,
            )
            state = subprocess.run(
                self.docker
                + [
                    "inspect",
                    "--format={{json .State}} restart_count={{.RestartCount}}",
                    self.name,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=10,
            )
            return logs.stdout + "\n=== Container State ===\n" + state.stdout
        except Exception as error:
            return f"process evidence collection failed: {error}\n"

    def stop(self):
        subprocess.run(
            self.docker + ["rm", "-f", self.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def __enter__(self):
        try:
            self.start()
            self.wait_ready()
        except Exception:
            self.stop()
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


def load_manifest(path, engine):
    cases = []
    seen = set()
    with Path(path).open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            case = json.loads(line)
            case_id = case.get("case_id")
            if not case_id or case_id in seen:
                raise ValueError(f"invalid or duplicate case_id at manifest line {line_number}")
            expected_path = case[
                "expected_tgx_path" if engine == "tgx" else "expected_tdl_path"
            ]
            safe_relative_path(expected_path)
            if int(case.get("expected_size") or 0) <= 0:
                raise ValueError(f"invalid expected_size at manifest line {line_number}")
            seen.add(case_id)
            cases.append(case)
    if not cases:
        raise ValueError("manifest contains no cases")
    return cases


def append_run_error(writer, error_code, cause, op):
    writer.write(
        {
            "case_id": "run",
            "attempt_id": "collector",
            "stage": "collection",
            "op": op,
            "error_code": error_code,
            "error_cause": str(cause),
            "retryable": False,
        }
    )


def wait_for_measurement(engine, cases, duration, events, task_submits=None):
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        time.sleep(min(2, max(0, deadline - time.monotonic())))
        try:
            status = http_json(f"{engine.api_base}/api/status")
            active = len(status.get("active_files") or [])
            queued = status.get("queue_depth", 0)
            speed = (status.get("rolling_5s_bps") or 0) * 8 / (1024 * 1024)
            print(
                f"Active={active} Queue={queued} Speed={speed:.2f} Mbps",
                flush=True,
            )
            if active == 0 and queued == 0:
                terminal = {"success", "failed", "unavailable", "canceled"}
                tasks = []
                try:
                    res = http_json(f"{engine.api_base}/api/tasks")
                    if isinstance(res, list):
                        tasks = res
                    elif isinstance(res, dict) and "tasks" in res and isinstance(res["tasks"], list):
                        tasks = res["tasks"]
                except Exception:
                    pass
                if not tasks and task_submits:
                    for task_info in task_submits.values():
                        tid = task_info.get("task_id")
                        if tid:
                            try:
                                snap = http_json(f"{engine.api_base}/api/tasks/{tid}")
                                if isinstance(snap, dict) and snap.get("id"):
                                    tasks.append(snap)
                            except Exception:
                                pass
                if (
                    tasks
                    and sum(
                        task.get("state") in terminal
                        for task in tasks
                        if isinstance(task, dict)
                    )
                    >= len(cases)
                ):
                    events.write({"timestamp": iso_time(), "event": "run.completed_early"})
                    return False
        except Exception:
            continue
    return True


def drain(engine, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status = http_json(f"{engine.api_base}/api/status")
            downloads_done = not status.get("active_files")
            archive_done = True
            if engine.run_spec["engine"] == "tgx":
                storage = http_json(f"{engine.api_base}/api/system/storage")
                archive = storage.get("archive") or {}
                archive_done = archive.get("archive_backlog_files") == 0
            if downloads_done and archive_done:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def classify_results(cases, engine_name, paths, task_submits, engine_tasks, timed_out):
    task_results = []
    hashes = []
    errors = []
    for case in cases:
        case_id = case["case_id"]
        relative = safe_relative_path(
            case["expected_tgx_path"]
            if engine_name == "tgx"
            else case["expected_tdl_path"]
        )
        target = paths.output / relative
        expected_size = int(case["expected_size"])
        expected_sha = case.get("baseline_sha256") or ""
        actual_size = target.stat().st_size if target.is_file() else 0
        actual_sha = compute_sha256(target) or ""
        size_match = actual_size == expected_size
        sha_match = bool(expected_sha) and actual_sha == expected_sha
        snapshot = engine_tasks.get(task_submits[case_id]["task_id"], {})
        state = snapshot.get("state", "unknown")

        error_code = None
        error_stage = None
        error_op = None
        error_cause = None
        if target.is_file() and size_match and sha_match:
            terminal_state = "COMPLETED"
        elif not task_submits[case_id]["submitted"]:
            terminal_state = "FAILED"
            error_code = "SUBMIT_FAILED"
            error_stage = "submit"
            error_op = "http_post"
            error_cause = task_submits[case_id]["error"]
        elif state == "canceled" or snapshot.get("error_class") == "canceled":
            terminal_state = "CANCELED"
            error_code = "CANCELED"
            error_stage = "lifecycle"
            error_op = "cancel"
            error_cause = snapshot.get("error") or "task canceled"
        elif timed_out and (
            state in ("queued", "resolving", "downloading")
            or engine_name == "tdl"
        ):
            terminal_state = "TIMED_OUT"
            error_code = "TIMED_OUT"
            error_stage = "lifecycle"
            error_op = "drain"
            error_cause = "task did not complete before measurement and drain ended"
        else:
            terminal_state = "FAILED"
            error_code = snapshot.get("error_class") or (
                "FILE_MISSING" if not target.is_file() else "VERIFICATION_FAILED"
            )
            error_stage = "engine" if snapshot.get("error_class") else "oracle"
            error_op = "download" if snapshot.get("error_class") else "verify_file"
            error_cause = snapshot.get("error") or (
                f"expected size/SHA {expected_size}/{expected_sha[:12]}, "
                f"actual {actual_size}/{actual_sha[:12]}"
            )

        attempt_id = snapshot.get("attempt_generation")
        if not task_submits[case_id]["submitted"]:
            attempt_id = "not-admitted"
        if terminal_state not in ("COMPLETED", "TIMED_OUT"):
            errors.append(
                {
                    "case_id": case_id,
                    "task_id": task_submits[case_id]["task_id"],
                    "attempt_id": attempt_id,
                    "stage": error_stage,
                    "op": error_op,
                    "error_code": error_code,
                    "error_cause": error_cause,
                    "retryable": False,
                }
            )
        task_results.append(
            {
                "case_id": case_id,
                "task_id": task_submits[case_id]["task_id"],
                "attempt_id": attempt_id,
                "submitted": task_submits[case_id]["submitted"],
                "admitted": bool(snapshot),
                "terminal_state": terminal_state,
                "attempt_count": snapshot.get("attempt_count"),
                "error_code": error_code,
                "error_stage": error_stage,
                "error_op": error_op,
                "error_cause": error_cause,
                "started_at": snapshot.get("started_at"),
                "finished_at": snapshot.get("finished_at"),
                "network_unique_bytes": snapshot.get("net_downloaded"),
                "target_durable_bytes": actual_size,
            }
        )
        hashes.append(
            {
                "case_id": case_id,
                "expected_path": relative,
                "actual_path": str(target) if target.is_file() else None,
                "expected_size": expected_size,
                "actual_size": actual_size,
                "baseline_sha256": expected_sha,
                "actual_sha256": actual_sha,
                "size_match": size_match,
                "sha_match": sha_match,
            }
        )
    return task_results, hashes, errors


def build_inventory(cases, engine_name, paths):
    expected = {
        safe_relative_path(
            case["expected_tgx_path"]
            if engine_name == "tgx"
            else case["expected_tdl_path"]
        )
        for case in cases
    }
    inventory = []
    for root, _, files in os.walk(paths.output):
        for filename in files:
            path = Path(root) / filename
            relative = path.relative_to(paths.output).as_posix()
            if relative in expected:
                classification = "expected_target"
            elif filename.endswith(".moving"):
                classification = "orphan_moving"
            elif filename.endswith((".seg", ".meta", ".part", ".tmp")) or ".part." in filename:
                classification = "orphan_temporary"
            else:
                classification = "unexpected_file"
            inventory.append(
                {
                    "path": relative,
                    "size_bytes": path.stat().st_size,
                    "classification": classification,
                }
            )
    if engine_name == "tdl":
        for root, _, files in os.walk(paths.temp):
            for filename in files:
                path = Path(root) / filename
                inventory.append(
                    {
                        "path": f"temp/{path.relative_to(paths.temp).as_posix()}",
                        "size_bytes": path.stat().st_size,
                        "classification": "temp_residue",
                    }
                )
    return sorted(inventory, key=lambda item: item["path"])


def execute_run(run_spec, engine_binary, eval_dir, host_port=5890):
    manifest_path = Path(run_spec["manifest_path"])
    if compute_sha256(manifest_path) != run_spec.get("manifest_sha256"):
        raise ValueError("RunSpec manifest_sha256 does not match the selected manifest")
    if not run_spec.get("baseline_cohort_id"):
        raise ValueError("RunSpec baseline_cohort_id is required")
    if not isinstance(run_spec.get("docker_command"), list):
        raise ValueError("RunSpec docker_command must be an argument list")
    if not run_spec["docker_command"]:
        raise ValueError("RunSpec docker_command must not be empty")

    artifact = extract_artifact_metadata(run_spec, engine_binary)
    validate_artifact(run_spec, artifact)
    cases = load_manifest(manifest_path, run_spec["engine"])
    paths = prepare_paths(run_spec, eval_dir)

    run_spec_copy = dict(run_spec)
    run_spec_copy.update(
        {
            "output_dir": str(paths.output),
            "state_db": str(paths.state / "records.sqlite3"),
            "log_dir": str(paths.logs),
        }
    )
    write_json(
        paths.raw / "protocol.json",
        {"protocol_version": PROTOCOL_VERSION, "protocol_sha256": PROTOCOL_SHA256},
    )
    write_json(paths.raw / "run_spec.json", run_spec_copy)
    write_json(paths.raw / "artifact.json", artifact)
    environment = dict(run_spec["environment"])
    environment["container_identity"] = f"tgx-eval-runner-{run_spec['run_id']}"
    write_json(paths.raw / "environment.json", environment)
    shutil.copyfile(manifest_path, paths.raw / "manifest.jsonl")

    events_path = paths.raw / "events.jsonl"
    errors_path = paths.raw / "errors.jsonl"
    metrics_path = paths.raw / "metrics.jsonl"
    process_path = paths.raw / "process.log"
    task_submits = {}
    engine_tasks = {}
    measurement_timed_out = False

    with JSONLWriter(events_path) as events, JSONLWriter(errors_path) as errors:
        engine = DockerEngine(run_spec, engine_binary, paths, host_port)
        with engine:
            events.write(
                {
                    "timestamp": iso_time(),
                    "event": "run.started",
                    "engine": run_spec["engine"],
                    "run_id": run_spec["run_id"],
                }
            )
            sampler = MetricsSampler(engine.api_base, metrics_path, run_spec["engine"])
            sampler.start()
            try:
                http_json(
                    f"{engine.api_base}/api/control",
                    method="POST",
                    data={"action": "pause"},
                )
                for case in cases:
                    task_id = (
                        f"{run_spec['run_id']}_{case['chat_id']}_{case['message_id']}"
                    )
                    request = {
                        "id": task_id,
                        "peer": str(case["chat_id"]),
                        "message_id": int(case["message_id"]),
                        "final_path": safe_relative_path(
                            case["expected_tgx_path"]
                            if run_spec["engine"] == "tgx"
                            else case["expected_tdl_path"]
                        ),
                        "expected_size": int(case["expected_size"]),
                    }
                    try:
                        http_json(
                            f"{engine.api_base}/api/tasks",
                            method="POST",
                            data=request,
                        )
                        task_submits[case["case_id"]] = {
                            "task_id": task_id,
                            "submitted": True,
                            "error": None,
                        }
                        events.write(
                            {
                                "timestamp": iso_time(),
                                "event": "item.submitted",
                                "engine": run_spec["engine"],
                                "case_id": case["case_id"],
                                "task_id": task_id,
                            }
                        )
                    except Exception as error:
                        task_submits[case["case_id"]] = {
                            "task_id": task_id,
                            "submitted": False,
                            "error": str(error),
                        }

                http_json(
                    f"{engine.api_base}/api/control",
                    method="POST",
                    data={"action": "resume"},
                )
                warmup = run_spec.get("warmup_seconds", 0)
                if warmup:
                    sampler.set_phase("warmup")
                    events.write(
                        {
                            "timestamp": iso_time(),
                            "event": "run.warmup.started",
                            "duration_seconds": warmup,
                        }
                    )
                    time.sleep(warmup)
                sampler.set_phase("measurement")
                measurement_timed_out = wait_for_measurement(
                    engine,
                    cases,
                    run_spec["duration_seconds"],
                    events,
                    task_submits=task_submits,
                )
                try:
                    http_json(
                        f"{engine.api_base}/api/control",
                        method="POST",
                        data={"action": "pause"},
                    )
                except Exception as error:
                    append_run_error(
                        errors,
                        "ADMISSION_STOP_FAILED",
                        error,
                        "pause_after_measurement",
                    )
                events.write({"timestamp": iso_time(), "event": "run.draining"})
                drained = drain(engine, run_spec.get("drain_timeout_seconds", 60))
                if not drained:
                    append_run_error(
                        errors,
                        "DRAIN_TIMEOUT",
                        "active downloads or archive backlog did not drain before timeout",
                        "drain",
                    )
                try:
                    snapshots = http_json(f"{engine.api_base}/api/tasks")
                    engine_tasks = {
                        snapshot["id"]: snapshot
                        for snapshot in snapshots
                        if snapshot.get("id")
                    }
                except Exception as error:
                    if run_spec.get("engine") == "tdl":
                        engine_tasks = {}
                        for task_info in task_submits.values():
                            tid = task_info.get("task_id")
                            if tid:
                                try:
                                    snap = http_json(
                                        f"{engine.api_base}/api/tasks/{tid}"
                                    )
                                    if isinstance(snap, dict) and snap.get("id"):
                                        engine_tasks[snap["id"]] = snap
                                except Exception:
                                    pass
                    else:
                        append_run_error(
                            errors,
                            "TASK_SNAPSHOT_FAILED",
                            error,
                            "get_tasks",
                        )
                events.write({"timestamp": iso_time(), "event": "run.finished"})
            except Exception as error:
                measurement_timed_out = True
                append_run_error(
                    errors,
                    "RUN_EXECUTION_FAILED",
                    error,
                    "execute_run",
                )
                events.write(
                    {
                        "timestamp": iso_time(),
                        "event": "run.aborted",
                        "error": str(error),
                    }
                )
            finally:
                sampler.stop()
                sampler.join()
                if sampler.error:
                    append_run_error(
                        errors,
                        "METRICS_COLLECTOR_FAILED",
                        sampler.error,
                        "sample_metrics",
                    )
                process_path.write_text(engine.capture_process(), encoding="utf-8")

        for case in cases:
            task_id = f"{run_spec['run_id']}_{case['chat_id']}_{case['message_id']}"
            task_submits.setdefault(
                case["case_id"],
                {
                    "task_id": task_id,
                    "submitted": False,
                    "error": "run aborted before task submission",
                },
            )
        task_results, hashes, task_errors = classify_results(
            cases,
            run_spec["engine"],
            paths,
            task_submits,
            engine_tasks,
            measurement_timed_out,
        )
        for error in task_errors:
            errors.write(error)

    write_jsonl(paths.raw / "task_results.jsonl", task_results)
    write_jsonl(paths.raw / "hashes.jsonl", hashes)
    write_jsonl(
        paths.raw / "file_inventory.jsonl",
        build_inventory(cases, run_spec["engine"], paths),
    )
    seal_raw_directory(paths.raw)
    print(f"Raw evidence sealed: {paths.raw}")
    return str(paths.run_root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-spec", required=True)
    parser.add_argument("--engine-binary", required=True)
    parser.add_argument("--eval-dir", required=True)
    parser.add_argument("--port", type=int, default=5890)
    args = parser.parse_args()
    with open(args.run_spec, "r", encoding="utf-8") as stream:
        run_spec = json.load(stream)
    execute_run(run_spec, args.engine_binary, args.eval_dir, args.port)


if __name__ == "__main__":
    main()
