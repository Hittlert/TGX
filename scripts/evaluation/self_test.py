#!/usr/bin/env python3
"""Self-tests for the real TGX evaluation manifest, collector, and analyzer."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analyze import (
    RAW_ARTIFACTS,
    _longest_zero_seconds,
    compute_sha256,
    evaluate_policy,
    seal_raw_directory,
)
from harness import (
    MetricsSampler,
    classify_results,
    ensure_new_directory,
    validate_artifact,
    wait_for_measurement,
)
from manifest_generator import generate_profile_manifest
from run_protocol_v1 import build_run_spec, validate_run_spec_shape


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
        metric_policy = json.load(stream)["required_metrics"]
    if isinstance(metric_policy, dict):
        required = metric_policy.get("common", []) + metric_policy.get("tgx", [])
    else:
        required = metric_policy
    sample = {field: 0 for field in required}
    sample.update(
        {
            "timestamp": "2026-09-04T00:00:00Z",
            "monotonic_elapsed_sec": 0.0,
            "engine": "tgx",
            "collection_errors": [],
            "rolling_5s_bps": 25 * 1024 * 1024,
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
    profile_id="P-S",
    size_bucket="small_low",
    active_bps=25 * 1024 * 1024,
    paired_tdl_mbps=200.0,
    engine="tgx",
    baseline_trust="golden",
    paired_attested_case_ids=None,
):
    raw = run_root / "raw"
    raw.mkdir(parents=True)
    result_count = case_count if result_count is None else result_count

    manifest = []
    for index in range(case_count):
        manifest.append(
            {
                "case_id": f"{profile_id}-{index + 1:04d}",
                "baseline_sha256": "a" * 64,
                "baseline_trust": baseline_trust,
                "expected_size": 1,
                "size_bucket": size_bucket,
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
            "engine": engine,
            "expected_binary_sha256": "2" * 64,
            "expected_source_commit": "1" * 40,
            "file_concurrency": 5,
            "manifest_sha256": compute_sha256(raw / "manifest.jsonl"),
            "net_concurrency": 32,
            "paired_tdl_artifact_sha256": "3" * 64,
            "paired_tdl_average_active_mbps": paired_tdl_mbps,
            "paired_tdl_run_id": "fixture-tdl",
            "paired_tdl_summary_sha256": "4" * 64,
            "profile_id": profile_id,
            "protocol_sha256": protocol_sha256,
            "protocol_version": "1.0",
            "run_id": "fixture-run",
        },
    )
    if paired_attested_case_ids is not None:
        run_spec_path = raw / "run_spec.json"
        run_spec = json.loads(run_spec_path.read_text(encoding="utf-8"))
        run_spec["paired_tdl_attested_case_ids"] = paired_attested_case_ids
        write_json(run_spec_path, run_spec)
    artifact = {
        "arch": "amd64",
        "build_time": "2026-09-04T00:00:00Z",
        "source_commit": "1" * 40,
        "source_dirty": False,
        "binary_sha256": "2" * 64,
        "engine": engine,
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
    metric["engine"] = engine
    metric["rolling_5s_bps"] = active_bps
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
        if terminal_state == "COMPLETED":
            error_code = None
        elif terminal_state == "TIMED_OUT":
            error_code = "TIMED_OUT"
        else:
            error_code = "FILE_MISSING"
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
                "size_match": terminal_state == "COMPLETED",
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
    (raw / "process.log").write_text(
        'fixture process log\n=== Container State ===\n'
        '{"OOMKilled":false,"Running":true} restart_count=0\n',
        encoding="utf-8",
    )

    missing = [name for name in RAW_ARTIFACTS if not (raw / name).exists()]
    if missing:
        raise AssertionError(f"fixture omitted raw files: {missing}")
    seal_raw_directory(raw)
    return raw


class EvaluationSelfTest(unittest.TestCase):
    def test_generated_run_spec_matches_schema_shape(self):
        config = {
            "api_base": "http://fixture",
            "artifacts": {
                "tgx": {
                    "binary": "/fixture/tgx",
                    "expected_commit": "1" * 40,
                    "expected_sha256": "2" * 64,
                    "source_repository": "https://example.invalid/tgx",
                }
            },
            "docker_command": ["docker"],
            "environment": {"host_id": "fixture"},
            "eval_dir": "/fixture/evaluation",
            "runner_image": "alpine@sha256:" + "3" * 64,
            "session_source_dir": "/fixture/session",
        }
        cohort = {
            "baseline_cohort_id": "p-s-fixture",
            "manifest_path": "/fixture/P-S.jsonl",
            "manifest_sha256": "4" * 64,
            "sample_seed": 42,
        }
        spec = build_run_spec(
            config,
            "tgx",
            "P-S",
            cohort,
            "fixture-run",
            240,
            15,
            1,
            32,
            5,
            32,
        )
        validate_run_spec_shape(spec)

    def test_collector_maps_current_runtime_endpoints(self):
        def fake_http(url, **_kwargs):
            if url.endswith("/api/status"):
                return {
                    "rolling_5s_bps": 123,
                    "queue_depth": 2,
                    "active_files": [{"id": "one"}],
                    "pool": {"size": 4, "reconnects": 1},
                }
            if url.endswith("/api/gate"):
                return {
                    "data_in_flight": 3,
                    "ssd_reserved_bytes": 10,
                    "ssd_available_bytes": 20,
                }
            if url.endswith("/api/system/storage"):
                return {
                    "free_bytes": 30,
                    "total_bytes": 40,
                    "used_bytes": 10,
                    "archive": {
                        "archive_backlog_files": 1,
                        "archive_backlog_bytes": 50,
                        "archive_active_workers": 1,
                    },
                }
            raise AssertionError(url)

        with tempfile.TemporaryDirectory() as temp, patch(
            "harness.http_json", side_effect=fake_http
        ):
            sampler = MetricsSampler("http://fixture", Path(temp) / "metrics", "tgx")
            sample = sampler.sample()
        self.assertEqual(123, sample["rolling_5s_bps"])
        self.assertEqual(1, sample["active_files"])
        self.assertEqual(3, sample["active_rpc"])
        self.assertEqual(10, sample["ssd_reserved_bytes"])
        self.assertEqual(50, sample["archive_backlog_bytes"])
        missing_sources = {error["source"] for error in sample["collection_errors"]}
        self.assertIn("wire_rx_bytes", missing_sources)
        self.assertIn("process_rss", missing_sources)

    def test_collector_never_turns_collection_failure_into_zero(self):
        with tempfile.TemporaryDirectory() as temp, patch(
            "harness.http_json", side_effect=OSError("fixture unavailable")
        ):
            sampler = MetricsSampler("http://fixture", Path(temp) / "metrics", "tgx")
            sample = sampler.sample()
        self.assertIsNone(sample["rolling_5s_bps"])
        self.assertIsNone(sample["ssd_free_bytes"])
        error_sources = {error["source"] for error in sample["collection_errors"]}
        self.assertIn("/api/status", error_sources)
        self.assertIn("rolling_5s_bps", error_sources)

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
            create_raw_fixture(run_root, missing_metric="active_rpc")
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("BLOCKED", summary["verdict"])

    def test_analyzer_reports_failed_target_as_no_go(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(run_root, terminal_state="FAILED")
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("NO-GO", summary["verdict"])
            self.assertFalse(
                any("lacks trace fields" in reason for reason in summary["failure_reasons"])
            )

    def test_tdl_attests_local_reference_cases(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(
                run_root,
                engine="tdl",
                baseline_trust="local_disk",
            )
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("GO", summary["verdict"])
            self.assertEqual(["P-S-0001"], summary["golden_attested_case_ids"])

    def test_tgx_local_reference_requires_matching_tdl_attestation(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(run_root, baseline_trust="local_disk")
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("BLOCKED", summary["verdict"])

        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(
                run_root,
                baseline_trust="local_disk",
                paired_attested_case_ids=["P-S-0001"],
            )
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("GO", summary["verdict"])

    def test_analyzer_enforces_small_file_throughput_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(
                run_root,
                active_bps=100 * 1024 * 1024 // 8,
                paired_tdl_mbps=400.0,
            )
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("NO-GO", summary["verdict"])

    def test_large_file_timeout_is_not_a_correctness_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            create_raw_fixture(
                run_root,
                terminal_state="TIMED_OUT",
                profile_id="P-L",
                size_bucket="large_low",
            )
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("GO", summary["verdict"])

    def test_analyzer_does_not_trust_completed_without_hash_match(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            raw = create_raw_fixture(run_root)
            (raw / "checksums.sha256").unlink()
            write_jsonl(
                raw / "hashes.jsonl",
                [
                    {
                        "case_id": "P-S-0001",
                        "expected_size": 1,
                        "actual_size": 1,
                        "size_match": True,
                        "sha_match": False,
                    }
                ],
            )
            seal_raw_directory(raw)
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("NO-GO", summary["verdict"])

    def test_tdl_timeout_classification_without_exception(self):
        class MockPaths:
            def __init__(self, temp_dir):
                self.output = Path(temp_dir)

        with tempfile.TemporaryDirectory() as temp:
            paths = MockPaths(temp)
            cases = [
                {
                    "case_id": "P-S-0001",
                    "expected_tgx_path": "tgx/P-S-0001.bin",
                    "expected_tdl_path": "tdl/P-S-0001.bin",
                    "expected_size": 1024,
                    "baseline_sha256": "a" * 64,
                }
            ]
            task_submits = {
                "P-S-0001": {"task_id": "task-1", "submitted": True, "error": None}
            }
            engine_tasks = {}
            results, hashes, errors = classify_results(
                cases=cases,
                engine_name="tdl",
                paths=paths,
                task_submits=task_submits,
                engine_tasks=engine_tasks,
                timed_out=True,
            )
            self.assertEqual(1, len(results))
            self.assertEqual("TIMED_OUT", results[0]["terminal_state"])
            self.assertEqual("TIMED_OUT", results[0]["error_code"])

    def test_measurement_continues_on_nonterminal_idle_sample(self):
        class MockEngine:
            api_base = "http://mock-engine"
            run_spec = {"engine": "tdl"}

        class MockEvents:
            def __init__(self):
                self.events = []

            def write(self, ev):
                self.events.append(ev)

        cases = [{"case_id": "case_1"}]
        task_submits = {"case_1": {"task_id": "t1", "submitted": True}}
        events = MockEvents()

        # Status says active=0, queued=0, but tasks endpoint returns task in "downloading" (nonterminal)
        def mock_http_json(url, timeout=None, retries=None):
            if "/api/status" in url:
                return {"active_files": [], "queue_depth": 0, "rolling_5s_bps": 0}
            if "/api/tasks" in url:
                return [{"id": "t1", "state": "downloading"}]
            return {}

        with patch("harness.http_json", side_effect=mock_http_json):
            timed_out = wait_for_measurement(
                MockEngine(), cases, duration=0.05, events=events, task_submits=task_submits
            )
            self.assertTrue(timed_out)
            self.assertNotIn("run.completed_early", [e.get("event") for e in events.events])

    def test_active_rpc_zero_throughput_counts_as_stall(self):
        metrics = [
            {
                "monotonic_elapsed_sec": 0.0,
                "active_files": 1,
                "queued_jobs": 0,
                "rolling_5s_bps": 0,
                "active_rpc": 5,
            },
            {
                "monotonic_elapsed_sec": 5.0,
                "active_files": 1,
                "queued_jobs": 0,
                "rolling_5s_bps": 0,
                "active_rpc": 5,
            },
            {
                "monotonic_elapsed_sec": 12.0,
                "active_files": 1,
                "queued_jobs": 0,
                "rolling_5s_bps": 0,
                "active_rpc": 5,
            },
        ]
        longest = _longest_zero_seconds(metrics)
        self.assertEqual(12.0, longest)

    def test_partial_http_metrics_payload_blocks_run(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "run"
            raw = create_raw_fixture(run_root)
            (raw / "checksums.sha256").unlink()
            bad_sample = required_metric_sample()
            bad_sample["active_files"] = None
            write_jsonl(raw / "metrics.jsonl", [bad_sample])
            seal_raw_directory(raw)
            summary = evaluate_policy(run_root, POLICY_PATH)
            self.assertEqual("BLOCKED", summary["verdict"])
            self.assertTrue(
                any("missing required field active_files" in r for r in summary["blocked_reasons"])
            )

    def test_absent_dirty_and_build_metadata_rejected(self):
        run_spec = {
            "engine": "tgx",
            "expected_binary_sha256": "1" * 64,
            "expected_source_commit": "abcdef",
        }
        artifact_missing_dirty = {
            "binary_sha256": "1" * 64,
            "source_commit": "abcdef",
            "source_dirty": None,
            "version": "v1.0.0",
            "build_time": "2026-09-04T00:00:00Z",
            "go_version": "go1.22",
            "os": "linux",
            "arch": "amd64",
            "image_digest": "sha256:abcd",
        }
        with self.assertRaises(ValueError) as cm:
            validate_artifact(run_spec, artifact_missing_dirty)
        self.assertIn("dirty or unknown source state", str(cm.exception))

        artifact_missing_build = dict(artifact_missing_dirty)
        artifact_missing_build["source_dirty"] = False
        artifact_missing_build["build_time"] = "unknown"
        with self.assertRaises(ValueError) as cm:
            validate_artifact(run_spec, artifact_missing_build)
        self.assertIn("does not report build_time", str(cm.exception))



def run_self_tests():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(EvaluationSelfTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    raise SystemExit(0 if run_self_tests() else 1)
