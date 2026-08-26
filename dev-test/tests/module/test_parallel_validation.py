"""Tests for read-only parallel-download validation reports."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from module.parallel_validation import (
    GIB,
    MIB,
    SampleResult,
    build_run_report,
    decide_sample,
    select_samples,
    write_report_atomic,
)


def create_records_db(path: Path, sizes):
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE download_records (
            chat_id TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            file_name TEXT,
            save_path TEXT,
            media_type TEXT,
            file_size INTEGER
        )
        """
    )
    for message_id, size in enumerate(sizes, start=1):
        connection.execute(
            """
            INSERT INTO download_records (
                chat_id, message_id, status, file_name,
                save_path, media_type, file_size
            ) VALUES (?, ?, 'success', ?, ?, 'video', ?)
            """,
            (
                "-100123",
                message_id,
                f"sample-{message_id}.bin",
                f"/app/downloads/sample-{message_id}.bin",
                size,
            ),
        )
    connection.commit()
    connection.close()


class ValidationSelectionTest(unittest.TestCase):
    """Selection uses successful existing archives and exact bucket quotas."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "records.sqlite3"

    def tearDown(self):
        self.temp_dir.cleanup()

    def open_read_only(self):
        return sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)

    def test_selects_exact_bucket_counts_from_success_rows(self):
        create_records_db(
            self.db_path,
            [1 * MIB, 2 * MIB, 20 * MIB, 30 * MIB, 300 * MIB, 2 * GIB],
        )

        with self.open_read_only() as connection:
            samples, gaps = select_samples(connection, lambda _path: True)

        counts = {}
        for sample in samples:
            counts[sample.bucket] = counts.get(sample.bucket, 0) + 1
        self.assertEqual(
            {"lt10MiB": 2, "10to200MiB": 2, "200MiBto1GiB": 1, "gt1GiB": 1},
            counts,
        )
        self.assertEqual([], gaps)

    def test_reports_missing_bucket_without_substitution(self):
        create_records_db(
            self.db_path,
            [1 * MIB, 2 * MIB, 20 * MIB, 30 * MIB, 300 * MIB],
        )

        with self.open_read_only() as connection:
            samples, gaps = select_samples(connection, lambda _path: True)

        self.assertEqual(5, len(samples))
        self.assertEqual(["gt1GiB"], gaps)

    def test_ignores_rows_whose_archive_no_longer_exists(self):
        create_records_db(
            self.db_path,
            [1 * MIB, 2 * MIB, 20 * MIB, 30 * MIB, 300 * MIB, 2 * GIB],
        )

        with self.open_read_only() as connection:
            samples, gaps = select_samples(
                connection,
                lambda path: not path.endswith("sample-6.bin"),
            )

        self.assertEqual(5, len(samples))
        self.assertEqual(["gt1GiB"], gaps)

    def test_selection_does_not_modify_database_bytes(self):
        create_records_db(
            self.db_path,
            [1 * MIB, 2 * MIB, 20 * MIB, 30 * MIB, 300 * MIB, 2 * GIB],
        )
        before = self.db_path.read_bytes()

        with self.open_read_only() as connection:
            select_samples(connection, lambda _path: True)

        self.assertEqual(before, self.db_path.read_bytes())


def sample_result(**overrides):
    values = {
        "bucket": "lt10MiB",
        "chat_id": "-100123",
        "message_id": 1,
        "file_size": 1024,
        "baseline_path": "/app/downloads/base.bin",
        "candidate_path": "/app/temp/candidate.bin",
        "baseline_sha256": "a" * 64,
        "candidate_sha256": "a" * 64,
        "baseline_verified": True,
        "candidate_verified": True,
        "telegram_covered_bytes": 1024,
        "telegram_range_count": 1,
        "elapsed_seconds": 1.0,
        "throughput_bytes_per_second": 1024.0,
        "retries": 0,
        "workers": 2,
        "decision": "pass",
        "reason": "candidate and baseline match Telegram",
        "unexplained_mismatch": False,
    }
    values.update(overrides)
    return SampleResult(**values)


class ValidationDecisionTest(unittest.TestCase):
    """Three-way decisions never pass on size or local hash alone."""

    def test_matching_verified_files_pass(self):
        decision = decide_sample(
            same_sha=True,
            baseline_verified=True,
            candidate_verified=True,
        )

        self.assertEqual("pass", decision.status)
        self.assertFalse(decision.blocks_parallel)

    def test_candidate_failure_blocks_parallel_mode(self):
        decision = decide_sample(
            same_sha=False,
            baseline_verified=True,
            candidate_verified=False,
        )

        self.assertEqual("fail", decision.status)
        self.assertTrue(decision.blocks_parallel)

    def test_candidate_pass_with_bad_baseline_marks_archive_suspect(self):
        decision = decide_sample(
            same_sha=False,
            baseline_verified=False,
            candidate_verified=True,
        )

        self.assertEqual("pass", decision.status)
        self.assertIn("baseline", decision.reason)

    def test_both_remote_fail_is_invalid_not_pass(self):
        decision = decide_sample(
            same_sha=True,
            baseline_verified=False,
            candidate_verified=False,
        )

        self.assertEqual("invalid", decision.status)
        self.assertTrue(decision.blocks_parallel)

    def test_verified_but_different_files_are_unexplained_failure(self):
        decision = decide_sample(
            same_sha=False,
            baseline_verified=True,
            candidate_verified=True,
        )

        self.assertEqual("fail", decision.status)
        self.assertTrue(decision.unexplained_mismatch)

    def test_report_requires_six_of_six(self):
        report = build_run_report([sample_result()] * 5, bucket_gaps=[])

        self.assertFalse(report["eligible"])
        self.assertEqual(5, report["valid_sample_count"])

    def test_report_rejects_any_candidate_failure(self):
        results = [sample_result()] * 5 + [
            sample_result(
                candidate_verified=False,
                decision="fail",
                reason="candidate hash mismatch",
            )
        ]

        report = build_run_report(results, bucket_gaps=[])

        self.assertFalse(report["eligible"])

    def test_atomic_report_is_deterministic_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "report.json"
            report = build_run_report([sample_result()] * 6, bucket_gaps=[])

            write_report_atomic(report, report_path)

            loaded = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(loaded["eligible"])
            self.assertEqual(
                report_path.read_text(encoding="utf-8"),
                json.dumps(report, indent=2, sort_keys=True) + "\n",
            )


if __name__ == "__main__":
    unittest.main()
