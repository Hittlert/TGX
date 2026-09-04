#!/usr/bin/env python3
"""Self-tests for the real TGX evaluation manifest, collector, and analyzer."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from analyze import RAW_ARTIFACTS, compute_sha256, evaluate_policy, seal_raw_directory
from harness import ensure_new_directory
from manifest_generator import generate_profile_manifest


def find_policy_path():
    p = Path(__file__).resolve()
    for parent in [p.parent, p.parent.parent, p.parent.parent.parent]:
        candidate = parent / "docs/evaluation/analysis-policy/baseline-v1.json"
        if candidate.exists():
            return candidate
    return p.parent.parent / "docs/evaluation/analysis-policy/baseline-v1.json"

POLICY_PATH = find_policy_path()


def write_json(path, value):
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")


def write_jsonl(path, records):
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def required_metric_sample():
    with POLICY_PATH.open("r", encoding="utf-8") as stream:
        required = json.load(stream)["required_metrics"]
    sample = {field: 0 for field in required}
    sample.update(
        {
            "timestamp": "2026-09-04T00:00:00Z",
            "monotonic_elapsed_sec": 0.0,
            "engine": "tgx",
            "collection_errors": [],
        }
    )
    return sample


def create_raw_fixture(
    run_root,
    *,
    case_count=1,
    result_count=None,
    terminal_state="COMPLETED",
    artifact_valid=True,
    missing_metric=None,
):
    raw = run_root / "raw"
    raw.mkdir(parents=True)
    result_count = case_count if result_count is None else result_count

    manifest = []
    for index in range(case_count):
        manifest.append(
            {
                "case_id": f"P-S-{index + 1:04d}",
                "baseline_sha256": "a" * 64,
                "baseline_trust": "golden",
                "expected_size": 1,
            }
        )
    write_jsonl(raw / "manifest.jsonl", manifest)

    protocol_sha256 = "f" * 64
    write_json(
        raw / "protocol.json",
        {"protocol_version": "1.0", "protocol_sha256": protocol_sha256},
    )
    write_json(
        raw / "run_spec.json",
        {
            "baseline_cohort_id": "p-s-fixture",
            "dc_pool_size": 32,
            "engine": "tgx",
            "expected_binary_sha256": "2" * 64,
            "expected_source_commit": "1" * 40,
            "file_concurrency": 5,
            "manifest_sha256": compute_sha256(raw / "manifest.jsonl"),
            "net_concurrency": 32,
            "profile_id": "P-S",
            "protocol_sha256": protocol_sha256,
            "protocol_version": "1.0",
            "run_id": "fixture-run",
        },
    )
    artifact = {
        "arch": "amd64",
        "build_time": "2026-09-04T00:00:00Z",
        "source_commit": "1" * 40,
        "source_dirty": False,
        "binary_sha256": "2" * 64,
        "engine": "tgx",
        "go_version": "go1.25.0",
        "image_digest": "alpine@sha256:" + "3" * 64,
        "os": "linux",
        "source_repository": "https://example.invalid/tgx",
        "version": "v1.0.0",
    }
    if not artifact_valid:
        artifact["source_commit"] = "unknown"
        artifact["version"] = "dev"
    write_json(raw / "artifact.json", artifact)
    write_json(
        raw / "environment.json",
        {
            "account_session_id": "fixture-session",
            "clock_source": "fixture-clock",
            "filesystem_types": {"ssd": "fixture"},
            "host_id": "fixture-host",
            "network_interface": "fixture-net",
            "proxy_route_id": "fixture-route",
            "ssd_storage_id": "fixture-ssd",
            "target_storage_id": "fixture-target",
        },
    )
    write_jsonl(raw / "events.jsonl", [])

    metric = required_metric_sample()
    if missing_metric:
        metric[missing_metric] = None
        metric["collection_errors"] = [
            {"source": missing_metric, "error": "fixture unavailable"}
        ]
    write_jsonl(raw / "metrics.jsonl", [metric])

    task_results = []
    hashes = []
    errors = []
    for case in manifest[:result_count]:
        error_code = None if terminal_state == "COMPLETED" else "FILE_MISSING"
        task_results.append(
            {
                "case_id": case["case_id"],
                "terminal_state": terminal_state,
                "error_code": error_code,
            }
        )
        hashes.append(
            {
                "case_id": case["case_id"],
                "expected_size": 1,
                "actual_size": 1 if terminal_state == "COMPLETED" else 0,
                "sha_match": terminal_state == "COMPLETED",
            }
        )
        if terminal_state != "COMPLETED":
            errors.append(
                {
                    "case_id": case["case_id"],
                    "attempt_id": "fixture-attempt",
                    "stage": "storage",
                    "op": "verify_presence",
                    "error_code": error_code,
                    "error_cause": "fixture target is missing",
                    "retryable": False,
                }
            )
    write_jsonl(raw / "task_results.jsonl", task_results)
    write_jsonl(raw / "hashes.jsonl", hashes)
    write_jsonl(
        raw / "file_inventory.jsonl",
        []
        if terminal_state != "COMPLETED"
        else [
            {
                "path": "fixture.bin",
                "size_bytes": 1,
                "classification": "expected_target",
            }
        ],
    )
    write_jsonl(raw / "errors.jsonl", errors)
    (raw / "process.log").write_text("fixture process log\n", encoding="utf-8")

    missing = [name for name in RAW_ARTIFACTS if not (raw / name).exists()]
    if missing:
        raise AssertionError(f"fixture omitted raw files: {missing}")
    seal_raw_directory(raw)
    return raw


class EvaluationSelfTest(unittest.TestCase):
    def test_manifest_is_seeded_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            baseline = root / "baseline"
            baseline.mkdir()
            db_path = root / "records.sqlite3"
            with sqlite3.connect(db_path) as connection:
                connection.executescript(
                    """
                    CREATE TABLE download_records (
                        chat_id TEXT, message_id INTEGER, file_name TEXT,
                        save_path TEXT, media_type TEXT, file_size INTEGER,
                        created_at INTEGER, status TEXT
                    );
                    CREATE TABLE dialog_cache (chat_id TEXT, chat_type TEXT);
                    """
                )
                sizes = [10, 70_000, 300_000, 400_000, 1_000_000]
                for index, size in enumerate(sizes, 1):
                    chat_id = f"-{index}"
                    relative = f"group-{index}/file-{index}.bin"
                    path = baseline / relative
                    path.parent.mkdir()
                    path.write_bytes(bytes([index]) * size)
                    connection.execute(
                        "INSERT INTO dialog_cache VALUES (?, 'channel')",
                        (chat_id,),
                    )
                    connection.execute(
                        "INSERT INTO download_records VALUES (?, ?, ?, ?, ?, ?, ?, 'success')",
                        (
                            chat_id,
                            index,
                            path.name,
                            relative,
                            "document",
                            size,
                            index,
                        ),
                    )

            first = root / "first.jsonl"
            second = root / "second.jsonl"
            generate_profile_manifest(db_path, baseline, "P-S", 42, first, 5)
            generate_profile_manifest(db_path, baseline, "P-S", 42, second, 5)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with self.assertRaises(FileExistsError):
                generate_profile_manifest(db_path, baseline, "P-S", 42, first, 5)

    def test_harness_refuses_existing_run_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            with self.assertRaises(FileExistsError):
                ensure_new_directory(path)

    def test_analyzer_accepts_valid_sealed_fixture_without_touching_raw(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            raw = create_raw_fixture(run_root)
            before = {name: compute_sha256(raw / name) for name in RAW_ARTIFACTS}
            summary = evaluate_policy(run_root, POLICY_PATH)
            after = {name: compute_sha256(raw / name) for name in RAW_ARTIFACTS}
            self.assertEqual("GO", summary["verdict"])
            self.assertEqual(before, after)

    def test_analyzer_rejects_checksum_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            raw = create_raw_fixture(run_root)
            (raw / "process.log").write_text("mutated\n", encoding="utf-8")
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("INVALID", summary["verdict"])

    def test_analyzer_rejects_invalid_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(run_root, artifact_valid=False)
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("INVALID", summary["verdict"])

    def test_analyzer_rejects_incomplete_task_coverage(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(run_root, case_count=2, result_count=1)
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("INVALID", summary["verdict"])

    def test_analyzer_blocks_missing_measurement(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(run_root, missing_metric="process_rss")
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("BLOCKED", summary["verdict"])

    def test_analyzer_reports_failed_target_as_no_go(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(run_root, terminal_state="FAILED")
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("NO-GO", summary["verdict"])


def run_self_tests():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(EvaluationSelfTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    raise SystemExit(0 if run_self_tests() else 1)
