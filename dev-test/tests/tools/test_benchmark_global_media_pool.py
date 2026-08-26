"""Tests for the read-only global media-pool benchmark CLI."""

import asyncio
import gc
import hashlib
import inspect
import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import tools.benchmark_global_media_pool as benchmark
from module.media_session_pool import (
    GlobalMediaSessionPool,
    MediaSessionPoolConfig,
)
from module.parallel_downloader import (
    CHUNK_SIZE,
    ChunkSpec,
    DownloadManifest,
    InjectedAbort,
    IntegrityReport,
    MediaIdentity,
    ParallelDownloader,
)
from tools.benchmark_global_media_pool import (
    BenchmarkSample,
    _copy_session_workspace,
    _open_records_read_only,
    _reserve_output_paths,
    _run_benchmark_async,
    _select_successful_record,
    build_parser,
    build_report,
    main,
)


ISOLATED_MOUNTS = {
    "verified": True,
    "separate_output_device": True,
    "protected_read_only": True,
    "requirement": (
        "production downloads, config, records, and sessions are read-only; "
        "output is a physically separate writable mount"
    ),
}


class BenchmarkGlobalMediaPoolTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.downloads_root = self.root / "downloads"
        self.output_root = self.root / "output"
        self.downloads_root.mkdir()
        self.output_root.mkdir()
        self.baseline_path = self.downloads_root / "baseline.bin"
        self.baseline_path.write_bytes(b"benchmark-payload")
        self.records_path = self.root / "download_records.sqlite3"
        self.config_path = self.root / "config.yaml"
        self.config_path.write_text("api_id: 1\napi_hash: hash\n", encoding="utf-8")
        self.sessions_path = self.root / "sessions"
        self.sessions_path.mkdir()
        (self.sessions_path / "media_downloader.session").write_bytes(b"session")
        self._create_records()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_records(self):
        with sqlite3.connect(self.records_path) as connection:
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
            connection.executemany(
                """
                INSERT INTO download_records (
                    chat_id, message_id, status, file_name,
                    save_path, media_type, file_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "-1002313319912",
                        10341,
                        "failed",
                        "failed.bin",
                        str(self.downloads_root / "failed.bin"),
                        "video",
                        1,
                    ),
                    (
                        "-1002313319912",
                        10341,
                        "success",
                        self.baseline_path.name,
                        str(self.baseline_path),
                        "video",
                        self.baseline_path.stat().st_size,
                    ),
                    (
                        "-1002313319912",
                        10342,
                        "success",
                        "other.bin",
                        str(self.downloads_root / "other.bin"),
                        "video",
                        99,
                    ),
                ],
            )

    def _main_args(self):
        return [
            "--chat-id",
            "-1002313319912",
            "--message-id",
            "10341",
            "--output",
            str(self.output_root),
            "--records",
            str(self.records_path),
            "--downloads-root",
            str(self.downloads_root),
            "--config",
            str(self.config_path),
            "--sessions",
            str(self.sessions_path),
            "--session-target",
            "24",
            "--pipeline-depth",
            "2",
        ]

    def _sample(self, **changes):
        sample = BenchmarkSample(
            chat_id="-1002313319912",
            message_id=10341,
            save_path=str(self.baseline_path),
            file_name=self.baseline_path.name,
            media_type="video",
            file_size=self.baseline_path.stat().st_size,
        )
        return replace(sample, **changes) if changes else sample

    def _run_args(self):
        return SimpleNamespace(
            config=str(self.config_path),
            sessions=str(self.sessions_path),
            chat_id="-1002313319912",
            message_id=10341,
            session_target=24,
            pipeline_depth=2,
            start_timeout=1.0,
        )

    def _valid_message(self, sample=None):
        sample = sample or self._sample()
        return SimpleNamespace(
            id=sample.message_id,
            chat=SimpleNamespace(id=int(sample.chat_id), username=None),
            empty=False,
            media=SimpleNamespace(value="video"),
            video=SimpleNamespace(
                file_id="encoded",
                file_unique_id="unique",
                file_size=sample.file_size,
            ),
        )

    def _write_private_json(self, path, payload):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
            os.write(fd, encoded)
            os.fsync(fd)
        finally:
            os.close(fd)

    def _durable_abort_fixture(
        self,
        *,
        completed_chunks=1,
        total_chunks=3,
        report_name="report.json",
        run_mode="fresh",
        recovered_chunks=0,
    ):
        sample = self._sample(file_size=total_chunks * CHUNK_SIZE)
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "durable-abort",
            sample.message_id,
            24,
            2,
        )
        if report_name != paths.report_path.name:
            paths = replace(paths, report_path=paths.run_dir / report_name)
        artifacts = benchmark._reserve_download_artifacts(paths)
        identity = MediaIdentity(
            chat_id=sample.chat_id,
            message_id=sample.message_id,
            media_id=9,
            dc_id=4,
            file_unique_id="unique",
            file_size=sample.file_size,
        )
        candidate_fd = os.open(paths.candidate_path, os.O_RDWR)
        try:
            os.ftruncate(candidate_fd, sample.file_size)
            manifest = DownloadManifest(
                paths.manifest_path,
                expected_file_identity=artifacts.manifest.as_tuple(),
            )
            manifest.prepare(identity, sample.file_size, CHUNK_SIZE)
            durable_bytes = 0
            for index in range(completed_chunks):
                payload = bytes([index + 1]) * CHUNK_SIZE
                offset = index * CHUNK_SIZE
                os.pwrite(candidate_fd, payload, offset)
                manifest.mark_complete(
                    ChunkSpec(offset=offset, length=len(payload)),
                    hashlib.sha256(payload).hexdigest(),
                    attempts=1,
                )
                durable_bytes += len(payload)
            os.fsync(candidate_fd)
            synced_sidecars = manifest.checkpoint_and_sync()
        finally:
            os.close(candidate_fd)
        benchmark._fsync_directory(paths.run_dir)
        durability = {
            "verified": True,
            "candidate_synced": True,
            "manifest_checkpointed": True,
            "manifest_synced": True,
            "directory_synced": True,
            "manifest_sidecars_synced": list(synced_sidecars),
        }

        downloaded_chunks = completed_chunks - recovered_chunks
        report = {
            "schema_version": 2,
            "status": "aborted",
            "eligible": False,
            "incomplete": True,
            "run_mode": run_mode,
            "chat_id": sample.chat_id,
            "message_id": sample.message_id,
            "session_target": 24,
            "pipeline_depth": 2,
            "run_dir": str(paths.run_dir),
            "candidate_path": str(paths.candidate_path),
            "manifest_path": str(paths.manifest_path),
            "report_path": str(paths.report_path),
            "sample_identity": asdict(sample),
            "media_identity": {
                **asdict(identity),
                "stable_key": identity.stable_key(),
            },
            "artifact_identities": {
                "candidate": asdict(artifacts.candidate),
                "manifest": asdict(artifacts.manifest),
            },
            "recovery": {
                "mode": run_mode,
                "abort_after_chunks": max(downloaded_chunks, 1),
                "recovered_chunks": recovered_chunks,
                "recovered_bytes": recovered_chunks * CHUNK_SIZE,
                "downloaded_chunks": downloaded_chunks,
                "current_run_committed_bytes": downloaded_chunks * CHUNK_SIZE,
                "durable_chunks": completed_chunks,
                "durable_bytes": durable_bytes,
                "total_chunks": total_chunks,
                "whole_file_fallback": False,
                "provenance_verified": run_mode == "resume",
                "abort_durability": dict(durability),
                "partial_durability": dict(durability),
            },
            "fault_injection": {
                "requested": False,
                "triggered": False,
                "terminated_connections": 0,
                "failed_session_id": "",
                "failed_stripe": {},
                "failed_attempt": 0,
                "replacement_session_id": "",
                "replacement_stripe": {},
                "replacement_attempt": 0,
                "correlated_replacements": 0,
                "replacement_session_observed": False,
                "stop_error": "",
                "iterator_cleanup_errors": [],
            },
        }
        self._write_private_json(paths.report_path, report)
        return sample, paths, artifacts, identity, report

    def test_parser_requires_exact_sample_identity_and_output(self):
        args = build_parser().parse_args(
            [
                "--chat-id",
                "-1002313319912",
                "--message-id",
                "10341",
                "--output",
                "/app/temp/pool-gate",
                "--session-target",
                "24",
                "--pipeline-depth",
                "2",
            ]
        )

        self.assertEqual("-1002313319912", args.chat_id)
        self.assertEqual(10341, args.message_id)
        self.assertEqual("/app/temp/pool-gate", args.output)
        self.assertEqual(24, args.session_target)
        self.assertEqual(2, args.pipeline_depth)
        for missing in ("--chat-id", "--message-id", "--output"):
            values = [
                "--chat-id",
                "1",
                "--message-id",
                "2",
                "--output",
                "/tmp/output",
                "--session-target",
                "8",
                "--pipeline-depth",
                "1",
            ]
            index = values.index(missing)
            del values[index : index + 2]
            with self.assertRaises(SystemExit):
                build_parser().parse_args(values)

    def test_parser_accepts_resume_abort_and_leased_failure_flags(self):
        candidate = self.output_root / "run" / "candidate.part"
        prior_report = candidate.parent / "report.json"
        args = build_parser().parse_args(
            self._main_args()
            + [
                "--resume-candidate",
                str(candidate),
                "--resume-report",
                str(prior_report),
                "--abort-after-chunks",
                "7",
                "--inject-leased-connection-failure",
            ]
        )

        self.assertEqual(str(candidate), args.resume_candidate)
        self.assertEqual(str(prior_report), args.resume_report)
        self.assertEqual(7, args.abort_after_chunks)
        self.assertTrue(args.inject_leased_connection_failure)

        for invalid in ("0", "-1"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(SystemExit):
                    build_parser().parse_args(
                        self._main_args() + ["--abort-after-chunks", invalid]
                    )

    def test_report_blocks_when_sha_differs(self):
        report = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="b" * 64,
            file_size=100,
            elapsed_seconds=1.0,
            snapshot={"live": 8},
            retries=0,
            candidate_file_size=100,
            integrity_verified=True,
            committed_bytes=100,
        )

        self.assertFalse(report["eligible"])
        self.assertFalse(report["same_sha256"])

    def test_report_uses_committed_bytes_for_goodput(self):
        report = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="a" * 64,
            file_size=20 * 1024 * 1024,
            elapsed_seconds=2.0,
            snapshot={"live": 16},
            retries=0,
            candidate_file_size=20 * 1024 * 1024,
            integrity_verified=True,
            committed_bytes=20 * 1024 * 1024,
        )

        self.assertEqual(20 * 1024 * 1024, report["committed_bytes"])
        self.assertEqual(10 * 1024 * 1024, report["goodput_bytes_per_second"])
        self.assertTrue(report["eligible"])

    def test_resume_goodput_excludes_recovered_bytes(self):
        report = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="a" * 64,
            file_size=20 * 1024 * 1024,
            elapsed_seconds=2.0,
            snapshot={"live": 16},
            retries=0,
            candidate_file_size=20 * 1024 * 1024,
            integrity_verified=True,
            committed_bytes=20 * 1024 * 1024,
            recovered_bytes=8 * 1024 * 1024,
        )

        self.assertEqual(8 * 1024 * 1024, report["recovered_bytes"])
        self.assertEqual(20 * 1024 * 1024, report["total_committed_bytes"])
        self.assertEqual(12 * 1024 * 1024, report["committed_bytes"])
        self.assertEqual(6 * 1024 * 1024, report["goodput_bytes_per_second"])

    def test_resume_gate_rejects_zero_recovery_and_complete_redownload(self):
        def candidate(recovered, downloaded):
            report = build_report(
                baseline_sha256="a" * 64,
                candidate_sha256="a" * 64,
                file_size=4 * CHUNK_SIZE,
                elapsed_seconds=1.0,
                snapshot={},
                retries=0,
                candidate_file_size=4 * CHUNK_SIZE,
                integrity_verified=True,
                committed_bytes=4 * CHUNK_SIZE,
                recovered_bytes=recovered * CHUNK_SIZE,
            )
            report["recovery"] = {
                "mode": "resume",
                "recovered_chunks": recovered,
                "downloaded_chunks": downloaded,
                "durable_chunks": recovered + downloaded,
                "total_chunks": 4,
                "whole_file_fallback": False,
                "provenance_verified": True,
            }
            return report

        valid_context = SimpleNamespace(
            provenance_verified=True,
            recovered_chunks=2,
            total_chunks=4,
        )
        valid = candidate(2, 2)
        benchmark._enforce_resume_eligibility(valid, valid_context)
        self.assertTrue(valid["eligible"])

        for name, recovered, downloaded, context_recovered in (
            ("zero recovery", 0, 4, 0),
            ("full redownload", 0, 4, 2),
            ("nothing downloaded", 4, 0, 4),
        ):
            with self.subTest(name=name):
                report = candidate(recovered, downloaded)
                context = SimpleNamespace(
                    provenance_verified=True,
                    recovered_chunks=context_recovered,
                    total_chunks=4,
                )
                benchmark._enforce_resume_eligibility(report, context)
                self.assertFalse(report["eligible"])
                self.assertTrue(report["errors"])

    def test_report_requires_exact_size_and_verified_integrity(self):
        common = {
            "baseline_sha256": "a" * 64,
            "candidate_sha256": "a" * 64,
            "file_size": 100,
            "elapsed_seconds": 1.0,
            "snapshot": {},
            "retries": 0,
            "committed_bytes": 100,
        }

        wrong_size = build_report(
            candidate_file_size=99,
            integrity_verified=True,
            **common,
        )
        unverified = build_report(
            candidate_file_size=100,
            integrity_verified=False,
            **common,
        )

        self.assertFalse(wrong_size["eligible"])
        self.assertFalse(wrong_size["exact_size"])
        self.assertFalse(unverified["eligible"])
        self.assertFalse(unverified["integrity_verified"])

    def test_report_requires_candidate_integrity_and_committed_inputs(self):
        parameters = inspect.signature(build_report).parameters

        for name in (
            "candidate_file_size",
            "integrity_verified",
            "committed_bytes",
        ):
            with self.subTest(name=name):
                self.assertIs(inspect.Parameter.empty, parameters[name].default)

    @mock.patch("tools.benchmark_global_media_pool.sqlite3.connect")
    def test_records_database_uses_immutable_read_only_uri(self, connect):
        _open_records_read_only(self.records_path)

        uri = connect.call_args.args[0]
        self.assertTrue(uri.startswith("file:"))
        self.assertTrue(uri.endswith("?mode=ro&immutable=1"))
        self.assertEqual({"uri": True}, connect.call_args.kwargs)

    def test_selects_exactly_one_successful_identity(self):
        with sqlite3.connect(self.records_path) as connection:
            sample = _select_successful_record(
                connection,
                "-1002313319912",
                10341,
            )

        self.assertEqual("-1002313319912", sample.chat_id)
        self.assertEqual(10341, sample.message_id)
        self.assertEqual(str(self.baseline_path), sample.save_path)
        self.assertEqual(self.baseline_path.stat().st_size, sample.file_size)

    def test_rejects_duplicate_successful_identity(self):
        with sqlite3.connect(self.records_path) as connection:
            connection.execute(
                """
                INSERT INTO download_records (
                    chat_id, message_id, status, file_name,
                    save_path, media_type, file_size
                ) VALUES (?, ?, 'success', '', ?, 'video', ?)
                """,
                (
                    "-1002313319912",
                    10341,
                    str(self.baseline_path),
                    self.baseline_path.stat().st_size,
                ),
            )
            with self.assertRaisesRegex(ValueError, "exactly one successful"):
                _select_successful_record(connection, "-1002313319912", 10341)

    def test_output_paths_are_unique_contained_and_outside_downloads(self):
        first = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "-1002313319912",
            10341,
            24,
            2,
        )
        second = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "-1002313319912",
            10341,
            24,
            2,
        )

        self.assertNotEqual(first.run_dir, second.run_dir)
        self.assertEqual(first.run_dir, first.candidate_path.parent)
        self.assertEqual(first.run_dir, first.report_path.parent)
        self.assertTrue(first.run_dir.is_relative_to(self.output_root.resolve()))
        with self.assertRaisesRegex(ValueError, "outside downloads root"):
            _reserve_output_paths(
                self.downloads_root / "benchmark",
                self.downloads_root,
                "chat",
                1,
                8,
                1,
            )

        self.assertEqual(0o700, stat.S_IMODE(first.run_dir.stat().st_mode))

    def test_protected_output_relationships_are_rejected_before_any_write(self):
        protected = {
            "downloads": self.downloads_root,
            "sessions": self.sessions_path,
            "records": self.records_path,
            "config": self.config_path,
        }
        cases = {
            "inside-downloads": self.downloads_root / "benchmark",
            "inside-sessions": self.sessions_path / "benchmark",
            "records-file": self.records_path,
            "contains-records": self.records_path.parent,
            "inside-config": self.config_path / "benchmark",
        }

        before = sorted(str(path.relative_to(self.root)) for path in self.root.rglob("*"))
        for name, output in cases.items():
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "protected"):
                    benchmark._validate_protected_output(output, protected)
        after = sorted(str(path.relative_to(self.root)) for path in self.root.rglob("*"))

        self.assertEqual(before, after)

    def test_mount_isolation_requires_read_only_production_and_separate_output(self):
        protected = {
            "downloads": self.downloads_root,
            "sessions": self.sessions_path,
            "records": self.records_path,
            "config": self.config_path,
        }

        def details(path):
            resolved = Path(path).resolve()
            if resolved == self.output_root.resolve():
                return {
                    "path": str(resolved),
                    "device": 200,
                    "read_only": False,
                    "writable": True,
                }
            return {
                "path": str(resolved),
                "device": 100,
                "read_only": True,
                "writable": False,
            }

        with mock.patch.object(benchmark, "_mount_details", side_effect=details):
            evidence = benchmark._validate_mount_isolation(
                self.output_root,
                protected,
            )

        self.assertTrue(evidence["verified"])
        self.assertTrue(evidence["separate_output_device"])
        self.assertIn("read-only", evidence["requirement"])
        self.assertIn("separate", evidence["requirement"])

        def same_device(path):
            value = details(path)
            value["device"] = 100
            return value

        with mock.patch.object(benchmark, "_mount_details", side_effect=same_device):
            with self.assertRaisesRegex(ValueError, "separate"):
                benchmark._validate_mount_isolation(self.output_root, protected)

    def test_mount_details_rejects_symlink_before_resolving_target(self):
        real_mount = self.root / "real-mount"
        real_mount.mkdir()
        linked_mount = self.root / "linked-mount"
        linked_mount.symlink_to(real_mount, target_is_directory=True)

        with self.assertRaisesRegex(ValueError, "symlink"):
            benchmark._mount_details(linked_mount)

    def test_candidate_and_manifest_are_privately_reserved_without_links(self):
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "chat",
            1,
            8,
            1,
        )

        identities = benchmark._reserve_download_artifacts(paths)

        for path, identity in (
            (paths.candidate_path, identities.candidate),
            (paths.manifest_path, identities.manifest),
        ):
            metadata = path.lstat()
            self.assertTrue(stat.S_ISREG(metadata.st_mode))
            self.assertEqual(1, metadata.st_nlink)
            self.assertEqual((metadata.st_dev, metadata.st_ino), identity.as_tuple())
            self.assertEqual(0o600, stat.S_IMODE(metadata.st_mode))

    def test_resume_requires_identity_bound_abort_and_repins_artifacts(self):
        sample, paths, original_identities, _identity, _report = (
            self._durable_abort_fixture()
        )
        resumed = benchmark._resume_output_paths(
            self.output_root,
            self.downloads_root,
            paths.candidate_path,
            paths.report_path,
        )
        context = benchmark._validate_resume_provenance(resumed, sample)

        self.assertEqual(paths.run_dir, resumed.run_dir)
        self.assertEqual(paths.candidate_path, resumed.candidate_path)
        self.assertEqual(paths.manifest_path, resumed.manifest_path)
        self.assertIsNone(resumed.report_path)
        self.assertEqual(paths.report_path, resumed.prior_report_path)
        self.assertEqual(original_identities, context.artifacts)
        self.assertEqual(1, context.recovered_chunks)
        self.assertEqual(CHUNK_SIZE, context.recovered_bytes)
        self.assertEqual(3, context.total_chunks)

    def test_resume_rejects_missing_mismatched_linked_or_nonprivate_evidence(self):
        outside = self.root / "outside"
        outside.mkdir()
        outside_candidate = outside / "candidate.part"
        outside_candidate.write_bytes(b"outside")
        (outside / "candidate.part.manifest.sqlite3").write_bytes(b"manifest")
        with self.assertRaisesRegex(ValueError, "output"):
            benchmark._resume_output_paths(
                self.output_root,
                self.downloads_root,
                outside_candidate,
                outside / "report.json",
            )

        sample, paths, _artifacts, _identity, report = (
            self._durable_abort_fixture()
        )
        resumed = benchmark._resume_output_paths(
            self.output_root,
            self.downloads_root,
            paths.candidate_path,
            paths.report_path,
        )

        missing_report = paths.run_dir / "missing-report.json"
        with self.assertRaisesRegex(ValueError, "report"):
            benchmark._resume_output_paths(
                self.output_root,
                self.downloads_root,
                paths.candidate_path,
                missing_report,
            )

        report["message_id"] += 1
        paths.report_path.unlink()
        self._write_private_json(paths.report_path, report)
        with self.assertRaisesRegex(ValueError, "identity"):
            benchmark._validate_resume_provenance(resumed, sample)
        report["message_id"] -= 1
        paths.report_path.unlink()
        self._write_private_json(paths.report_path, report)

        report["artifact_identities"]["candidate"]["inode"] += 1
        paths.report_path.unlink()
        self._write_private_json(paths.report_path, report)
        with self.assertRaisesRegex(ValueError, "identity"):
            benchmark._validate_resume_provenance(resumed, sample)
        report["artifact_identities"]["candidate"]["inode"] -= 1
        paths.report_path.unlink()
        self._write_private_json(paths.report_path, report)

        os.chmod(paths.manifest_path, 0o644)
        with self.assertRaisesRegex(ValueError, "private"):
            benchmark._validate_resume_provenance(resumed, sample)
        os.chmod(paths.manifest_path, 0o600)

        hardlink = paths.run_dir / "candidate-hardlink"
        os.link(paths.candidate_path, hardlink)
        with self.assertRaisesRegex(ValueError, "hardlink"):
            benchmark._validate_resume_provenance(resumed, sample)
        hardlink.unlink()

        os.chmod(paths.report_path, 0o644)
        with self.assertRaisesRegex(ValueError, "private"):
            benchmark._validate_resume_provenance(resumed, sample)
        os.chmod(paths.report_path, 0o600)

        report_link = paths.run_dir / "report-hardlink.json"
        os.link(paths.report_path, report_link)
        with self.assertRaisesRegex(ValueError, "hardlink"):
            benchmark._validate_resume_provenance(resumed, sample)
        report_link.unlink()

        os.chmod(paths.run_dir, 0o755)
        with self.assertRaisesRegex(ValueError, "private"):
            benchmark._resume_output_paths(
                self.output_root,
                self.downloads_root,
                paths.candidate_path,
                paths.report_path,
            )

    def test_resume_report_sequence_is_explicit_private_and_collision_safe(self):
        sample, paths, _artifacts, _identity, _report = (
            self._durable_abort_fixture(
                completed_chunks=2,
                total_chunks=4,
                report_name="resume-report-0001.json",
                run_mode="resume",
                recovered_chunks=1,
            )
        )
        resumed = benchmark._resume_output_paths(
            self.output_root,
            self.downloads_root,
            paths.candidate_path,
            paths.report_path,
        )
        context = benchmark._validate_resume_provenance(resumed, sample)

        self.assertIsNone(resumed.report_path)
        self.assertEqual(paths.report_path, context.prior_report_path)

        prior_bytes = paths.report_path.read_bytes()
        collided = paths.run_dir / "resume-report-0002.json"
        self._write_private_json(collided, {"preserve": True})
        reserved, identity = benchmark._reserve_next_resume_report(paths.run_dir)
        self.assertEqual(paths.run_dir / "resume-report-0003.json", reserved)
        self.assertGreater(identity.inode, 0)
        self.assertEqual(prior_bytes, paths.report_path.read_bytes())
        self.assertEqual({"preserve": True}, json.loads(collided.read_text()))

    def test_failed_child_can_parent_atomic_sequence_past_empty_reservations(self):
        sample, paths, _artifacts, _identity, report = (
            self._durable_abort_fixture(
                completed_chunks=2,
                total_chunks=4,
                report_name="resume-report-0002.json",
                run_mode="resume",
                recovered_chunks=1,
            )
        )
        report["status"] = "failed"
        report["incomplete"] = True
        report["errors"] = ["CancelledError: interrupted resume"]
        report["interruptions"] = [
            {
                "phase": "benchmark",
                "class": "CancelledError",
                "message": "interrupted resume",
            }
        ]
        paths.report_path.unlink()
        self._write_private_json(paths.report_path, report)
        parent_bytes = paths.report_path.read_bytes()

        for sequence in (1, 3):
            identity = benchmark._reserve_report_path(
                paths.run_dir / f"resume-report-{sequence:04d}.json"
            )
            self.assertGreater(identity.inode, 0)

        resumed = benchmark._resume_output_paths(
            self.output_root,
            self.downloads_root,
            paths.candidate_path,
            paths.report_path,
        )
        context = benchmark._validate_resume_provenance(resumed, sample)

        def reserve_next():
            return benchmark._reserve_next_resume_report(paths.run_dir)[0]

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(reserve_next) for _index in range(2)]
            reserved = sorted(future.result().name for future in futures)

        self.assertEqual(
            ["resume-report-0004.json", "resume-report-0005.json"],
            reserved,
        )
        self.assertEqual(paths.report_path, context.prior_report_path)
        self.assertEqual(parent_bytes, paths.report_path.read_bytes())
        for name in reserved:
            reserved_path = paths.run_dir / name
            self.assertEqual(b"", reserved_path.read_bytes())
            self.assertEqual(0o600, stat.S_IMODE(reserved_path.stat().st_mode))
            self.assertEqual(1, reserved_path.stat().st_nlink)

    def test_resume_provenance_rejects_zero_or_complete_durable_state(self):
        for name, completed, total in (
            ("zero", 0, 3),
            ("complete", 3, 3),
        ):
            with self.subTest(name=name):
                sample, paths, _artifacts, _identity, _report = (
                    self._durable_abort_fixture(
                        completed_chunks=completed,
                        total_chunks=total,
                    )
                )
                resumed = benchmark._resume_output_paths(
                    self.output_root,
                    self.downloads_root,
                    paths.candidate_path,
                    paths.report_path,
                )
                with self.assertRaisesRegex(ValueError, "incomplete durable"):
                    benchmark._validate_resume_provenance(resumed, sample)

    def test_resume_provenance_requires_schema_abort_status_and_exact_paths(self):
        mutations = (
            ("schema", lambda report: report.__setitem__("schema_version", 1)),
            ("abort", lambda report: report.__setitem__("status", "completed")),
            (
                "path",
                lambda report: report.__setitem__(
                    "candidate_path",
                    str(self.output_root / "different.part"),
                ),
            ),
        )
        for name, mutate in mutations:
            with self.subTest(name=name):
                sample, paths, _artifacts, _identity, report = (
                    self._durable_abort_fixture()
                )
                resumed = benchmark._resume_output_paths(
                    self.output_root,
                    self.downloads_root,
                    paths.candidate_path,
                    paths.report_path,
                )
                mutate(report)
                paths.report_path.unlink()
                self._write_private_json(paths.report_path, report)
                with self.assertRaises(ValueError):
                    benchmark._validate_resume_provenance(resumed, sample)

    def test_report_reservation_and_artifacts_require_private_single_link_files(self):
        report_path = self.output_root / "reserved-report.json"
        identity = benchmark._reserve_report_path(report_path)
        metadata = report_path.lstat()
        self.assertTrue(stat.S_ISREG(metadata.st_mode))
        self.assertEqual(0o600, stat.S_IMODE(metadata.st_mode))
        self.assertEqual(1, metadata.st_nlink)
        self.assertEqual((metadata.st_dev, metadata.st_ino), identity.as_tuple())

        linked = self.output_root / "reserved-report-linked.json"
        os.link(report_path, linked)
        with self.assertRaisesRegex(ValueError, "hardlink"):
            benchmark._write_reserved_report_atomic(
                {"eligible": False},
                report_path,
                identity,
            )
        self.assertEqual(b"", report_path.read_bytes())

    def test_depth_two_quarantines_every_lease_for_the_failed_session(self):
        class Session:
            def __init__(self, name):
                self.name = name
                self.stop_count = 0

            async def stop(self):
                self.stop_count += 1

        class Lease:
            def __init__(self, session):
                self.session = session
                self.unhealthy = False
                self.released = False

            def mark_unhealthy(self):
                self.unhealthy = True

            async def __aenter__(self):
                return self

            async def __aexit__(self, _type, _value, _traceback):
                self.released = True
                return None

        class Source:
            async def iter_range_on_session(
                self,
                _session,
                _start_offset,
                _expected_length,
            ):
                yield b"first"
                yield b"second"

        failed_session = Session("failed")
        first_lease = Lease(failed_session)
        second_lease = Lease(failed_session)
        injector = benchmark._LeasedConnectionFailureInjector(Source())

        async def exercise():
            chunks = []
            first = injector.wrap_lease(first_lease)
            second = injector.wrap_lease(second_lease)
            await injector._register(first)
            await injector._register(second)
            with self.assertRaises(ConnectionResetError):
                async for chunk in injector.iter_range_on_session(
                    first.session,
                    0,
                    2,
                ):
                    chunks.append(chunk)
            await injector._unregister(first)
            await injector._unregister(second)
            return chunks, injector.evidence()

        chunks, evidence = asyncio.run(exercise())

        self.assertEqual([b"first"], chunks)
        self.assertEqual(1, failed_session.stop_count)
        self.assertTrue(first_lease.unhealthy)
        self.assertTrue(second_lease.unhealthy)
        self.assertTrue(evidence["triggered"])
        self.assertFalse(evidence["replacement_session_observed"])
        self.assertEqual(1, evidence["terminated_connections"])
        self.assertEqual("session-0001", evidence["failed_session_id"])

    def test_lease_state_machine_owns_every_lifecycle_transition(self):
        class Session:
            pass

        class Lease:
            def __init__(self):
                self.session = Session()
                self.released = False
                self.unhealthy = False

            def mark_unhealthy(self):
                self.unhealthy = True

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                self.released = True

        injector = benchmark._LeasedConnectionFailureInjector(SimpleNamespace())

        async def exercise():
            underlying = Lease()
            wrapped = injector.wrap_lease(underlying)
            states = [wrapped.state.value]
            self.assertTrue(await injector._register(wrapped))
            states.append(wrapped.state.value)
            await wrapped.__aenter__()
            states.append(wrapped.state.value)
            self.assertTrue(
                await injector._quarantine(
                    wrapped.session,
                    {"start_offset": 0, "expected_length": 5, "end_offset": 5},
                    1,
                    1,
                )
            )
            states.append(wrapped.state.value)
            await wrapped.__aexit__(None, None, None)
            states.append(wrapped.state.value)
            return states, underlying

        states, underlying = asyncio.run(exercise())

        self.assertEqual(
            ["acquired", "registered", "entered", "quarantined", "released"],
            states,
        )
        self.assertTrue(underlying.unhealthy)
        self.assertTrue(underlying.released)

    def test_unregister_base_exception_still_releases_owned_lease(self):
        class CleanupFailure(BaseException):
            pass

        class Lease:
            session = object()

            def __init__(self):
                self.released = False

            async def release(self):
                self.released = True

            def mark_unhealthy(self):
                return None

        async def exercise():
            injector = benchmark._LeasedConnectionFailureInjector(
                SimpleNamespace()
            )
            underlying = Lease()
            wrapped = injector.wrap_lease(underlying)
            await injector._register(wrapped)

            async def fail_unregister(_lease):
                raise CleanupFailure("unregister failed")

            injector._unregister = fail_unregister
            with self.assertRaisesRegex(CleanupFailure, "unregister failed"):
                await wrapped.release()
            return wrapped, underlying, injector.evidence()

        wrapped, underlying, evidence = asyncio.run(exercise())

        self.assertTrue(underlying.released)
        self.assertEqual("released", wrapped.state.value)
        self.assertTrue(
            any("CleanupFailure" in item for item in evidence["lease_cleanup_errors"])
        )

    def test_rejection_mark_base_exception_still_releases_owned_lease(self):
        class MarkFailure(BaseException):
            pass

        class Lease:
            session = object()

            def __init__(self):
                self.released = False

            def mark_unhealthy(self):
                raise MarkFailure("mark unhealthy failed")

            async def release(self):
                self.released = True

        async def exercise():
            injector = benchmark._LeasedConnectionFailureInjector(
                SimpleNamespace()
            )
            underlying = Lease()
            wrapped = injector.wrap_lease(underlying)
            with self.assertRaisesRegex(MarkFailure, "mark unhealthy failed"):
                await wrapped.reject()
            return wrapped, underlying

        wrapped, underlying = asyncio.run(exercise())

        self.assertTrue(underlying.released)
        self.assertEqual("released", wrapped.state.value)

    def test_session_audit_token_retains_object_lifetime(self):
        class Session:
            pass

        injector = benchmark._LeasedConnectionFailureInjector(SimpleNamespace())
        first = Session()
        first_reference = weakref.ref(first)

        async def identifier(session):
            async with injector._lock:
                return injector._session_identifier_locked(session)

        first_identifier = asyncio.run(identifier(first))
        del first
        gc.collect()

        self.assertIsNotNone(first_reference())
        second_identifier = asyncio.run(identifier(Session()))
        self.assertNotEqual(first_identifier, second_identifier)

    def test_pool_acquire_rejects_quarantined_session_until_replacement(self):
        class Session:
            async def stop(self):
                return None

        class Lease:
            def __init__(self, session):
                self.session = session
                self.unhealthy = False
                self.released = False

            def mark_unhealthy(self):
                self.unhealthy = True

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                self.released = True

        failed_session = Session()
        replacement_session = Session()
        quarantined = Lease(failed_session)
        replacement = Lease(replacement_session)

        class Pool:
            def __init__(self):
                self.leases = [quarantined, replacement]

            async def acquire(self, _dc_id, _transfer_id):
                return self.leases.pop(0)

        injector = benchmark._LeasedConnectionFailureInjector(SimpleNamespace())

        async def exercise():
            await injector._quarantine(
                failed_session,
                {"start_offset": 0, "expected_length": 1, "end_offset": 1},
                1,
                1,
            )
            wrapped = await injector.wrap_pool(Pool()).acquire(4, "transfer")
            async with wrapped:
                return wrapped.session

        selected = asyncio.run(exercise())

        self.assertIs(replacement_session, selected)
        self.assertTrue(quarantined.unhealthy)
        self.assertTrue(quarantined.released)

    def test_real_pool_cancellation_while_acquiring_drains_and_closes(self):
        async def exercise():
            factory_started = asyncio.Event()
            factory_release = asyncio.Event()

            class Session:
                async def stop(self):
                    return None

            async def factory(_dc_id):
                factory_started.set()
                await factory_release.wait()
                return Session()

            pool = GlobalMediaSessionPool(
                factory,
                MediaSessionPoolConfig(
                    soft_sessions=1,
                    max_sessions=1,
                    pipeline_depth=2,
                    adaptive=False,
                ),
            )
            pool.start()
            injector = benchmark._LeasedConnectionFailureInjector(
                SimpleNamespace()
            )
            task = asyncio.create_task(
                injector.wrap_pool(pool).acquire(4, "cancel-acquire")
            )
            try:
                await asyncio.wait_for(factory_started.wait(), 1)
                task.cancel("cancel while acquiring")
                with self.assertRaisesRegex(
                    asyncio.CancelledError,
                    "cancel while acquiring",
                ):
                    await task
                self.assertEqual(0, pool.snapshot().active_slots)
                await asyncio.wait_for(pool.close(), 1)
            finally:
                factory_release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await asyncio.wait_for(pool.close(), 1)

        asyncio.run(exercise())

    def test_real_pool_registration_cancellation_releases_owned_lease(self):
        async def exercise():
            class Session:
                async def stop(self):
                    return None

            async def factory(_dc_id):
                return Session()

            pool = GlobalMediaSessionPool(
                factory,
                MediaSessionPoolConfig(
                    soft_sessions=1,
                    max_sessions=1,
                    pipeline_depth=2,
                    adaptive=False,
                ),
            )
            pool.start()
            acquired = asyncio.Event()
            leases = []

            class ObservedPool:
                async def acquire(self, dc_id, transfer_id):
                    lease = await pool.acquire(dc_id, transfer_id)
                    leases.append(lease)
                    acquired.set()
                    return lease

                def __getattr__(self, name):
                    return getattr(pool, name)

            injector = benchmark._LeasedConnectionFailureInjector(
                SimpleNamespace()
            )
            await injector._lock.acquire()
            task = asyncio.create_task(
                injector.wrap_pool(ObservedPool()).acquire(
                    4,
                    "cancel-registration",
                )
            )
            try:
                await asyncio.wait_for(acquired.wait(), 1)
                self.assertEqual(1, pool.snapshot().active_slots)
                task.cancel("cancel during registration")
                injector._lock.release()
                with self.assertRaisesRegex(
                    asyncio.CancelledError,
                    "cancel during registration",
                ):
                    await task
                self.assertEqual(0, pool.snapshot().active_slots)
                await asyncio.wait_for(pool.close(), 1)
            finally:
                if injector._lock.locked():
                    injector._lock.release()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                for lease in leases:
                    await lease.release()
                await asyncio.wait_for(pool.close(), 1)

        asyncio.run(exercise())

    def test_real_pool_entry_cancellation_releases_registered_lease(self):
        async def exercise():
            class Session:
                async def stop(self):
                    return None

            async def factory(_dc_id):
                return Session()

            pool = GlobalMediaSessionPool(
                factory,
                MediaSessionPoolConfig(
                    soft_sessions=1,
                    max_sessions=1,
                    pipeline_depth=2,
                    adaptive=False,
                ),
            )
            pool.start()
            leases = []

            class ObservedPool:
                async def acquire(self, dc_id, transfer_id):
                    lease = await pool.acquire(dc_id, transfer_id)
                    leases.append(lease)
                    return lease

                def __getattr__(self, name):
                    return getattr(pool, name)

            injector = benchmark._LeasedConnectionFailureInjector(
                SimpleNamespace()
            )
            wrapped = await injector.wrap_pool(ObservedPool()).acquire(
                4,
                "cancel-entry",
            )
            await injector._lock.acquire()
            task = asyncio.create_task(wrapped.__aenter__())
            try:
                await asyncio.sleep(0)
                task.cancel("cancel during entry")
                injector._lock.release()
                with self.assertRaisesRegex(
                    asyncio.CancelledError,
                    "cancel during entry",
                ):
                    await task
                self.assertEqual(0, pool.snapshot().active_slots)
                await asyncio.wait_for(pool.close(), 1)
            finally:
                if injector._lock.locked():
                    injector._lock.release()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await injector._unregister(wrapped)
                for lease in leases:
                    await lease.release()
                await asyncio.wait_for(pool.close(), 1)

        asyncio.run(exercise())

    def test_replacement_requires_the_exact_failed_stripe_retry_once(self):
        class Session:
            def __init__(self, name):
                self.name = name
                self.stop_count = 0

            async def stop(self):
                self.stop_count += 1

        class Lease:
            def __init__(self, session):
                self.session = session
                self.unhealthy = False

            def mark_unhealthy(self):
                self.unhealthy = True

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        class Source:
            async def iter_range_on_session(self, _session, _start, length):
                yield b"x"
                if length > 1:
                    yield b"y" * (length - 1)

        failed_session = Session("failed")
        replacement_session = Session("replacement")
        injector = benchmark._LeasedConnectionFailureInjector(Source())

        async def consume(session, start, length):
            lease = injector.wrap_lease(Lease(session))
            await injector._register(lease)
            try:
                return [
                    chunk
                    async for chunk in injector.iter_range_on_session(
                        session,
                        start,
                        length,
                    )
                ]
            finally:
                await injector._unregister(lease)

        async def exercise():
            with self.assertRaises(ConnectionResetError):
                await consume(failed_session, 0, 5)
            await consume(replacement_session, 10, 5)
            unrelated = injector.evidence()
            await consume(replacement_session, 1, 4)
            correlated = injector.evidence()
            await consume(replacement_session, 1, 4)
            repeated = injector.evidence()
            return unrelated, correlated, repeated

        unrelated, correlated, repeated = asyncio.run(exercise())

        self.assertFalse(unrelated["replacement_session_observed"])
        self.assertEqual(0, unrelated["correlated_replacements"])
        self.assertTrue(correlated["replacement_session_observed"])
        self.assertEqual(1, correlated["correlated_replacements"])
        self.assertEqual("session-0001", correlated["failed_session_id"])
        self.assertEqual("session-0002", correlated["replacement_session_id"])
        self.assertEqual(
            {"start_offset": 0, "expected_length": 5, "end_offset": 5},
            correlated["failed_stripe"],
        )
        self.assertEqual(
            {"start_offset": 1, "expected_length": 4, "end_offset": 5},
            correlated["replacement_stripe"],
        )
        self.assertEqual(1, correlated["failed_attempt"])
        self.assertEqual(2, correlated["replacement_attempt"])
        self.assertEqual(1, repeated["correlated_replacements"])

    def test_failed_replacement_candidate_is_not_promoted_before_completion(self):
        class Session:
            def __init__(self, name):
                self.name = name

            async def stop(self):
                return None

        class Lease:
            def __init__(self, session):
                self.session = session

            def mark_unhealthy(self):
                return None

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        failed = Session("failed")
        failed_candidate = Session("failed-candidate")
        completing = Session("completing")

        class Source:
            async def iter_range_on_session(
                self,
                session,
                _start_offset,
                expected_length,
            ):
                if session is failed_candidate:
                    raise ConnectionResetError("candidate failed before bytes")
                if session is failed:
                    yield b"x"
                    yield b"unused"
                    return
                yield b"z" * expected_length

        injector = benchmark._LeasedConnectionFailureInjector(Source())

        async def consume(session, start, length):
            lease = injector.wrap_lease(Lease(session))
            await injector._register(lease)
            try:
                return [
                    chunk
                    async for chunk in injector.iter_range_on_session(
                        session,
                        start,
                        length,
                    )
                ]
            finally:
                await injector._unregister(lease)

        async def exercise():
            with self.assertRaises(ConnectionResetError):
                await consume(failed, 0, 5)
            with self.assertRaises(ConnectionResetError):
                await consume(failed_candidate, 1, 4)
            after_failed_candidate = injector.evidence()
            self.assertEqual([b"zzzz"], await consume(completing, 1, 4))
            return after_failed_candidate, injector.evidence()

        after_failed_candidate, completed = asyncio.run(exercise())

        self.assertFalse(after_failed_candidate["replacement_session_observed"])
        self.assertEqual("", after_failed_candidate["replacement_session_id"])
        self.assertTrue(completed["replacement_session_observed"])
        self.assertEqual("session-0003", completed["replacement_session_id"])
        self.assertEqual(3, completed["replacement_attempt"])
        self.assertEqual(1, completed["correlated_replacements"])

    def test_real_pool_downloader_depth_two_completes_on_actual_replacement(self):
        async def exercise():
            file_size = 10 * CHUNK_SIZE
            payload = (bytes(range(251)) * ((file_size + 250) // 251))[
                :file_size
            ]
            candidate_path = self.root / "real-pool-integration.part"
            sessions = []

            class Session:
                def __init__(self, ordinal):
                    self.ordinal = ordinal
                    self.stopped = False
                    self.stop_count = 0

                async def stop(self):
                    self.stop_count += 1
                    self.stopped = True

            async def factory(_dc_id):
                session = Session(len(sessions) + 1)
                sessions.append(session)
                return session

            class Source:
                def __init__(self):
                    self.first_session_ranges = 0
                    self.first_session_ready = asyncio.Event()

                async def iter_range_on_session(
                    source_self,
                    session,
                    start_offset,
                    expected_length,
                ):
                    if session.ordinal == 1:
                        source_self.first_session_ranges += 1
                        if source_self.first_session_ranges == 2:
                            source_self.first_session_ready.set()
                        await source_self.first_session_ready.wait()
                    elif session.ordinal == 2:
                        raise ConnectionResetError(
                            "first replacement candidate failed"
                        )

                    end_offset = start_offset + expected_length
                    for offset in range(start_offset, end_offset, CHUNK_SIZE):
                        if session.stopped:
                            raise ConnectionResetError("session was stopped")
                        yield payload[offset : min(offset + CHUNK_SIZE, end_offset)]
                        await asyncio.sleep(0)

            async def no_sleep(_seconds):
                await asyncio.sleep(0)

            pool = GlobalMediaSessionPool(
                factory,
                MediaSessionPoolConfig(
                    soft_sessions=1,
                    max_sessions=1,
                    pipeline_depth=2,
                    adaptive=False,
                ),
            )
            pool.start()
            injector = benchmark._LeasedConnectionFailureInjector(Source())
            downloader = ParallelDownloader(
                injector,
                pool=injector.wrap_pool(pool),
                max_attempts=6,
                sleep=no_sleep,
                transfer_id="real-depth-two-fault",
            )
            try:
                result = await asyncio.wait_for(
                    downloader.download(
                        MediaIdentity(
                            chat_id="-100-real",
                            message_id=9,
                            media_id=10,
                            dc_id=4,
                            file_unique_id="real-depth-two",
                            file_size=file_size,
                        ),
                        candidate_path,
                    ),
                    10,
                )
                evidence = injector.evidence()
                async with injector._lock:
                    failed_candidate_id = injector._session_identifier_locked(
                        sessions[1]
                    )
                    completing_id = injector._session_identifier_locked(sessions[2])
                self.assertEqual(0, pool.snapshot().active_slots)
                await asyncio.wait_for(pool.close(), 2)
                return (
                    payload,
                    candidate_path,
                    result,
                    evidence,
                    sessions,
                    failed_candidate_id,
                    completing_id,
                    pool.snapshot(),
                )
            finally:
                await asyncio.wait_for(pool.close(), 2)

        (
            payload,
            candidate_path,
            result,
            evidence,
            sessions,
            failed_candidate_id,
            completing_id,
            final_snapshot,
        ) = asyncio.run(exercise())

        self.assertEqual(payload, candidate_path.read_bytes())
        self.assertTrue(result.integrity.verified)
        self.assertGreaterEqual(len(sessions), 3)
        self.assertTrue(evidence["triggered"])
        self.assertEqual(1, evidence["terminated_connections"])
        self.assertNotEqual(failed_candidate_id, completing_id)
        self.assertEqual(completing_id, evidence["replacement_session_id"])
        self.assertNotEqual(failed_candidate_id, evidence["replacement_session_id"])
        self.assertEqual(
            evidence["failed_stripe"]["end_offset"],
            evidence["replacement_stripe"]["end_offset"],
        )
        self.assertGreater(
            evidence["replacement_stripe"]["start_offset"],
            evidence["failed_stripe"]["start_offset"],
        )
        self.assertEqual(0, final_snapshot.active_slots)
        self.assertEqual(0, final_snapshot.live)

    def test_leased_failure_preserves_cancellation_during_session_stop(self):
        class Session:
            async def stop(self):
                raise asyncio.CancelledError("cancel injected stop")

        class Lease:
            session = Session()

            def mark_unhealthy(self):
                return None

            async def __aenter__(self):
                return self

            async def __aexit__(self, _type, _value, _traceback):
                return None

        class Source:
            async def iter_range_on_session(self, *_args):
                yield b"first"

        injector = benchmark._LeasedConnectionFailureInjector(Source())

        async def exercise():
            async with injector.wrap_lease(Lease()) as lease:
                async for _chunk in injector.iter_range_on_session(
                    lease.session,
                    0,
                    1,
                ):
                    pass

        with self.assertRaisesRegex(asyncio.CancelledError, "cancel injected stop"):
            asyncio.run(exercise())

    def test_injected_iterator_cleanup_is_shielded_and_preserves_primary(self):
        close_started = None
        close_release = None
        close_finished = None

        class CloseFailure(BaseException):
            pass

        class Session:
            async def stop(self):
                raise asyncio.CancelledError("primary stop cancellation")

        class Lease:
            session = Session()

            def mark_unhealthy(self):
                return None

        class Source:
            async def iter_range_on_session(self, *_args):
                try:
                    yield b"first"
                finally:
                    close_started.set()
                    while True:
                        try:
                            await close_release.wait()
                            break
                        except asyncio.CancelledError:
                            continue
                    close_finished.set()
                    raise CloseFailure("close failed after cleanup")

        async def exercise():
            nonlocal close_started, close_release, close_finished
            close_started = asyncio.Event()
            close_release = asyncio.Event()
            close_finished = asyncio.Event()
            injector = benchmark._LeasedConnectionFailureInjector(Source())
            lease = injector.wrap_lease(Lease())
            await injector._register(lease)

            async def consume():
                try:
                    async for _chunk in injector.iter_range_on_session(
                        lease.session,
                        0,
                        1,
                    ):
                        pass
                finally:
                    await injector._unregister(lease)

            task = asyncio.create_task(consume())
            await close_started.wait()
            task.cancel("repeated cleanup cancellation")
            close_release.set()
            with self.assertRaises(asyncio.CancelledError) as caught:
                await task
            return caught.exception, close_finished.is_set(), injector.evidence()

        error, closed, evidence = asyncio.run(exercise())

        self.assertEqual("primary stop cancellation", str(error))
        self.assertTrue(closed)
        self.assertTrue(
            any("CloseFailure" in item for item in evidence["iterator_cleanup_errors"])
        )

    def test_artifact_reservation_rejects_symlink_and_hardlink_without_mutation(self):
        target = self.downloads_root / "protected.bin"
        target.write_bytes(b"protected")

        symlink_paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "symlink",
            1,
            8,
            1,
        )
        symlink_paths.candidate_path.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "candidate"):
            benchmark._reserve_download_artifacts(symlink_paths)
        self.assertEqual(b"protected", target.read_bytes())

        hardlink_paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "hardlink",
            1,
            8,
            1,
        )
        os.link(target, hardlink_paths.manifest_path)
        with self.assertRaisesRegex(ValueError, "manifest"):
            benchmark._reserve_download_artifacts(hardlink_paths)
        self.assertEqual(b"protected", target.read_bytes())

    def test_candidate_inspection_rejects_inode_swap_and_added_hardlink(self):
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "swap",
            1,
            8,
            1,
        )
        identities = benchmark._reserve_download_artifacts(paths)
        original = paths.run_dir / "original-candidate"
        paths.candidate_path.rename(original)
        paths.candidate_path.write_bytes(b"replacement")

        with self.assertRaisesRegex(ValueError, "identity"):
            benchmark._inspect_candidate(paths.candidate_path, identities.candidate)

        paths.candidate_path.unlink()
        original.rename(paths.candidate_path)
        hardlink = paths.run_dir / "candidate-hardlink"
        os.link(paths.candidate_path, hardlink)
        with self.assertRaisesRegex(ValueError, "hardlink"):
            benchmark._inspect_candidate(paths.candidate_path, identities.candidate)

    def test_manifest_inspection_rejects_added_hardlink(self):
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "manifest-hardlink",
            1,
            8,
            1,
        )
        identities = benchmark._reserve_download_artifacts(paths)
        os.link(paths.manifest_path, paths.run_dir / "manifest-hardlink.sqlite3")

        with self.assertRaisesRegex(ValueError, "hardlink"):
            benchmark._verify_manifest_artifacts(
                paths.manifest_path,
                identities.manifest,
            )

    def test_session_workspace_is_private_copy_under_run_output(self):
        sessions = self.root / "private-copy-sessions"
        sessions.mkdir()
        source = sessions / "media_downloader.session"
        source.write_bytes(b"immutable-session")
        before = (source.read_bytes(), source.stat().st_mode, source.stat().st_mtime_ns)
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "chat",
            1,
            8,
            1,
        )

        workspace = _copy_session_workspace(sessions, paths.run_dir)
        copied = workspace / source.name
        copied.write_bytes(b"client-mutated-copy")

        self.assertTrue(workspace.is_relative_to(paths.run_dir))
        self.assertEqual(
            before,
            (source.read_bytes(), source.stat().st_mode, source.stat().st_mtime_ns),
        )

    def test_session_workspace_requires_noninteractive_client_session(self):
        sessions = self.root / "empty-sessions"
        sessions.mkdir()
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "chat",
            1,
            8,
            1,
        )

        with self.assertRaisesRegex(ValueError, "media_downloader.session"):
            _copy_session_workspace(sessions, paths.run_dir)

    def test_noninteractive_connect_rejects_unauthorized_without_prompting(self):
        class Client:
            async def connect(self):
                return False

        with mock.patch("builtins.input", side_effect=AssertionError("prompted")):
            with mock.patch("getpass.getpass", side_effect=AssertionError("prompted")):
                with self.assertRaisesRegex(PermissionError, "authorized"):
                    asyncio.run(
                        benchmark._connect_client_noninteractive(
                            Client(),
                            timeout=0.1,
                        )
                    )

    def test_noninteractive_connect_has_internal_timeout(self):
        cancelled = []

        class Client:
            async def connect(self):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.append(True)

        started = time.monotonic()
        with self.assertRaisesRegex(TimeoutError, "startup timed out"):
            asyncio.run(
                benchmark._connect_client_noninteractive(
                    Client(),
                    timeout=0.01,
                )
            )

        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual([True], cancelled)

    def test_unauthorized_and_timed_out_runs_clean_copied_sessions(self):
        sample = self._sample()

        class UnauthorizedClient:
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                return False

            async def disconnect(self):
                self.is_connected = False

        class TimedOutClient:
            is_connected = False
            is_initialized = False

            async def connect(self):
                await asyncio.Event().wait()

        for name, client_factory, expected in (
            ("unauthorized", UnauthorizedClient, "authorized"),
            ("timeout", TimedOutClient, "startup timed out"),
        ):
            with self.subTest(name=name):
                paths = _reserve_output_paths(
                    self.output_root,
                    self.downloads_root,
                    name,
                    sample.message_id,
                    24,
                    2,
                )
                args = self._run_args()
                args.start_timeout = 0.01
                report = asyncio.run(
                    _run_benchmark_async(
                        args,
                        sample,
                        paths,
                        client_factory=lambda *_args, factory=client_factory, **_kwargs: factory(),
                    )
                )

                self.assertFalse(report["eligible"])
                self.assertTrue(any(expected in error for error in report["errors"]))
                self.assertFalse((paths.run_dir / "sessions").exists())

    def test_fail_fast_client_authorization_hooks_never_prompt(self):
        client = object.__new__(benchmark._NonInteractiveHookClient)

        with mock.patch("builtins.input", side_effect=AssertionError("prompted")):
            with self.assertRaisesRegex(PermissionError, "non-interactive"):
                asyncio.run(client.authorize())
            with self.assertRaisesRegex(PermissionError, "non-interactive"):
                asyncio.run(client.authorize_qr())

    def test_telegram_identity_rejects_cardinality_and_basic_mismatches(self):
        sample = BenchmarkSample(
            chat_id="-1002313319912",
            message_id=10341,
            save_path=str(self.baseline_path),
            file_name=self.baseline_path.name,
            media_type="MessageMediaType.VIDEO",
            file_size=self.baseline_path.stat().st_size,
        )

        def message(**overrides):
            values = {
                "id": 10341,
                "chat": SimpleNamespace(id=-1002313319912, username="sample"),
                "empty": False,
                "media": SimpleNamespace(value="video"),
                "video": SimpleNamespace(
                    file_id="encoded",
                    file_unique_id="unique",
                    file_size=sample.file_size,
                ),
            }
            values.update(overrides)
            return SimpleNamespace(**values)

        cases = {
            "empty-list": [],
            "multiple": [message(), message()],
            "message-id": message(id=10342),
            "chat-id": message(chat=SimpleNamespace(id=-100999, username="other")),
            "media-type": message(
                media=SimpleNamespace(value="document"),
                video=None,
                document=SimpleNamespace(
                    file_id="encoded",
                    file_unique_id="unique",
                    file_size=sample.file_size,
                ),
            ),
            "file-size": message(
                video=SimpleNamespace(
                    file_id="encoded",
                    file_unique_id="unique",
                    file_size=sample.file_size + 1,
                )
            ),
        }
        for name, response in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    benchmark._validate_telegram_message(sample, response)

        media = benchmark._validate_telegram_message(sample, message())
        self.assertEqual("unique", media.file_unique_id)

    def test_telegram_identity_compares_available_stable_identifiers(self):
        sample = BenchmarkSample(
            chat_id="-1002313319912",
            message_id=10341,
            save_path=str(self.baseline_path),
            file_name=self.baseline_path.name,
            media_type="video",
            file_size=self.baseline_path.stat().st_size,
            file_unique_id="expected-unique",
            media_id=9,
            dc_id=4,
        )
        message = SimpleNamespace(
            id="10341",
            chat=SimpleNamespace(id="-1002313319912", username=None),
            empty=False,
            media=SimpleNamespace(value="VIDEO"),
            video=SimpleNamespace(
                file_id="encoded",
                file_unique_id="expected-unique",
                file_size=sample.file_size,
            ),
        )
        media = benchmark._validate_telegram_message(sample, [message])
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))

        actual_source, identity = benchmark._build_download_identity(
            sample,
            object(),
            media,
            source_factory=lambda *_args: source,
        )

        self.assertIs(source, actual_source)
        self.assertEqual(9, identity.media_id)
        self.assertEqual(4, identity.dc_id)
        with self.assertRaisesRegex(ValueError, "file_unique_id"):
            benchmark._validate_telegram_message(
                replace(sample, file_unique_id="different"),
                message,
            )
        with self.assertRaisesRegex(ValueError, "media_id"):
            benchmark._build_download_identity(
                replace(sample, media_id=10),
                object(),
                media,
                source_factory=lambda *_args: source,
            )

    def test_session_copy_is_cleaned_for_each_prestart_failure_boundary(self):
        original_payload = self.baseline_path.read_bytes()
        cases = ("missing-baseline", "changed-baseline", "hash-failure", "client-factory")

        for name in cases:
            with self.subTest(name=name):
                self.baseline_path.write_bytes(original_payload)
                sample = self._sample()
                paths = _reserve_output_paths(
                    self.output_root,
                    self.downloads_root,
                    name,
                    sample.message_id,
                    24,
                    2,
                )
                client_factory = mock.Mock(side_effect=AssertionError("unexpected"))
                hash_patch = mock.patch.object(
                    benchmark,
                    "_sha256_file",
                    wraps=benchmark._sha256_file,
                )
                if name == "missing-baseline":
                    self.baseline_path.unlink()
                elif name == "changed-baseline":
                    self.baseline_path.write_bytes(original_payload + b"changed")
                elif name == "hash-failure":
                    hash_patch = mock.patch.object(
                        benchmark,
                        "_sha256_file",
                        side_effect=OSError("baseline hash failed"),
                    )
                elif name == "client-factory":
                    client_factory = mock.Mock(
                        side_effect=RuntimeError("client constructor failed")
                    )

                with hash_patch:
                    report = asyncio.run(
                        _run_benchmark_async(
                            self._run_args(),
                            sample,
                            paths,
                            client_factory=client_factory,
                        )
                    )

                self.assertFalse(report["eligible"])
                self.assertFalse((paths.run_dir / "sessions").exists())
                expected_token = "client" if name == "client-factory" else "baseline"
                self.assertTrue(
                    any(expected_token in error.lower() for error in report["errors"]),
                    report["errors"],
                )

        self.baseline_path.write_bytes(original_payload)

    def test_wrong_size_candidate_is_still_hashed_independently(self):
        sample = self._sample()
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "wrong-size",
            sample.message_id,
            24,
            2,
        )
        captured = {}

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                return True

            async def disconnect(self):
                self.is_connected = False

            async def start(self):
                self.is_connected = True

            async def stop(self):
                self.is_connected = False

            async def get_messages(self, **_kwargs):
                return self_outer._valid_message(sample)

        class Pool:
            def start(self):
                pass

            async def close(self):
                pass

            def snapshot(self):
                return {"live": 1, "by_dc": {4: {"live": 1}}}

        class Downloader:
            _retries = 2

            async def download(self, _identity, candidate_path, progress=None, **kwargs):
                captured.update(kwargs)
                Path(candidate_path).write_bytes(b"short")
                progress(5, sample.file_size)
                return SimpleNamespace(
                    sha256=hashlib.sha256(b"short").hexdigest(),
                    retries=2,
                    integrity=IntegrityReport(
                        verified=True,
                        covered_bytes=5,
                        range_count=1,
                        mismatch_count=0,
                        method="test",
                    ),
                )

        self_outer = self
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))
        report = asyncio.run(
            _run_benchmark_async(
                self._run_args(),
                sample,
                paths,
                client_factory=lambda *_args, **_kwargs: Client(),
                pool_factory=lambda *_args, **_kwargs: Pool(),
                source_factory=lambda *_args, **_kwargs: source,
                downloader_factory=lambda *_args, **_kwargs: Downloader(),
            )
        )

        self.assertFalse(report["eligible"])
        self.assertEqual(5, report["candidate_file_size"])
        self.assertEqual(hashlib.sha256(b"short").hexdigest(), report["candidate_sha256"])
        self.assertEqual(5, report["committed_bytes"])
        self.assertIn("expected_target_identity", captured)
        self.assertIn("expected_manifest_identity", captured)

    def test_abort_report_rejects_fake_only_durability_transition(self):
        payload = b"a" * (3 * CHUNK_SIZE)
        self.baseline_path.write_bytes(payload)
        sample = self._sample()
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "durable-abort-run",
            sample.message_id,
            24,
            2,
        )

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                return True

            async def disconnect(self):
                self.is_connected = False

            async def get_messages(client_self, **_kwargs):
                return self._valid_message(sample)

        class Pool:
            def start(self):
                return None

            async def close(self):
                return None

            def snapshot(self):
                return {"live": 1, "fallbacks": 0, "by_dc": {4: {"live": 1}}}

        class Downloader:
            _retries = 0
            _recovered_chunks = 0
            _downloaded_chunks = 0

            async def download(
                downloader_self,
                identity,
                candidate_path,
                progress=None,
                **kwargs,
            ):
                candidate_fd = os.open(candidate_path, os.O_RDWR)
                try:
                    os.ftruncate(candidate_fd, identity.file_size)
                    chunk = payload[:CHUNK_SIZE]
                    os.pwrite(candidate_fd, chunk, 0)
                    os.fsync(candidate_fd)
                finally:
                    os.close(candidate_fd)
                manifest = DownloadManifest(
                    f"{candidate_path}.manifest.sqlite3",
                    expected_file_identity=kwargs["expected_manifest_identity"],
                )
                manifest.prepare(identity, identity.file_size, CHUNK_SIZE)
                manifest.mark_complete(
                    ChunkSpec(offset=0, length=CHUNK_SIZE),
                    hashlib.sha256(chunk).hexdigest(),
                    attempts=1,
                )
                downloader_self._downloaded_chunks = 1
                progress(CHUNK_SIZE, identity.file_size)
                raise InjectedAbort("aborted after 1 chunks")

        args = self._run_args()
        args.abort_after_chunks = 1
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))

        report = asyncio.run(
            _run_benchmark_async(
                args,
                sample,
                paths,
                client_factory=lambda *_args, **_kwargs: Client(),
                pool_factory=lambda *_args, **_kwargs: Pool(),
                source_factory=lambda *_args, **_kwargs: source,
                downloader_factory=lambda *_args, **_kwargs: Downloader(),
            )
        )

        self.assertEqual("failed", report["status"])
        self.assertFalse(report["eligible"])
        self.assertEqual(1, report["recovery"]["durable_chunks"])
        self.assertEqual(CHUNK_SIZE, report["recovery"]["durable_bytes"])
        self.assertEqual(3, report["recovery"]["total_chunks"])
        self.assertEqual(1, report["recovery"]["downloaded_chunks"])
        self.assertTrue(report["artifact_identities"])
        self.assertEqual(asdict(sample), report["sample_identity"])
        self.assertEqual(9, report["media_identity"]["media_id"])
        self.assertFalse(report["recovery"]["abort_durability"]["verified"])
        self.assertTrue(
            any("durability transition" in error for error in report["errors"])
        )

    def test_real_downloader_abort_report_proves_durable_partial(self):
        payload = b"d" * (3 * CHUNK_SIZE)
        self.baseline_path.write_bytes(payload)
        sample = self._sample()
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "real-durable-abort-run",
            sample.message_id,
            1,
            2,
        )
        sessions = []

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(client_self):
                client_self.is_connected = True
                return True

            async def disconnect(client_self):
                client_self.is_connected = False

            async def get_messages(client_self, **_kwargs):
                return self._valid_message(sample)

        class Session:
            def __init__(self):
                self.stop_count = 0

            async def stop(self):
                self.stop_count += 1

        async def session_factory(_dc_id):
            session = Session()
            sessions.append(session)
            return session

        class Source:
            file_id = SimpleNamespace(media_id=9, dc_id=4)

            async def iter_range_on_session(
                source_self,
                _session,
                start_offset,
                expected_length,
            ):
                end_offset = start_offset + expected_length
                for offset in range(start_offset, end_offset, CHUNK_SIZE):
                    yield payload[offset : min(offset + CHUNK_SIZE, end_offset)]

        def pool_factory(_factory, config):
            return GlobalMediaSessionPool(session_factory, config)

        args = self._run_args()
        args.session_target = 1
        args.abort_after_chunks = 1
        report = asyncio.run(
            _run_benchmark_async(
                args,
                sample,
                paths,
                client_factory=lambda *_args, **_kwargs: Client(),
                pool_factory=pool_factory,
                source_factory=lambda *_args, **_kwargs: Source(),
            )
        )

        self.assertEqual("aborted", report["status"])
        self.assertTrue(report["incomplete"])
        self.assertFalse(report["eligible"])
        self.assertEqual(1, report["recovery"]["durable_chunks"])
        self.assertEqual(CHUNK_SIZE, report["recovery"]["durable_bytes"])
        self.assertTrue(report["recovery"]["abort_durability"]["verified"])
        self.assertTrue(
            report["recovery"]["abort_durability"]["candidate_synced"]
        )
        self.assertTrue(
            report["recovery"]["abort_durability"]["manifest_checkpointed"]
        )
        self.assertTrue(
            report["recovery"]["abort_durability"]["manifest_synced"]
        )
        self.assertTrue(
            report["recovery"]["abort_durability"]["directory_synced"]
        )
        self.assertGreaterEqual(len(sessions), 1)

    def test_resume_passes_abort_trigger_and_reports_chunk_evidence(self):
        payload = b"r" * (2 * CHUNK_SIZE)
        self.baseline_path.write_bytes(payload)
        sample = self._sample()
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "resume-run",
            sample.message_id,
            24,
            2,
        )
        artifacts = benchmark._reserve_download_artifacts(paths)
        digest = hashlib.sha256(payload).hexdigest()
        captured = {}

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                return True

            async def disconnect(self):
                self.is_connected = False

            async def get_messages(client_self, **_kwargs):
                return self._valid_message(sample)

        class Pool:
            def start(self):
                return None

            async def close(self):
                return None

            def snapshot(self):
                return {
                    "live": 2,
                    "fallbacks": 0,
                    "by_dc": {4: {"live": 2}},
                }

        class Downloader:
            _retries = 1
            _recovered_chunks = 1
            _downloaded_chunks = 1

            async def download(
                self,
                _identity,
                candidate_path,
                progress=None,
                **_kwargs,
            ):
                Path(candidate_path).write_bytes(payload)
                progress(len(payload), len(payload))
                return SimpleNamespace(
                    sha256=digest,
                    retries=1,
                    recovered_chunks=1,
                    downloaded_chunks=1,
                    integrity=IntegrityReport(
                        verified=True,
                        covered_bytes=len(payload),
                        range_count=1,
                        mismatch_count=0,
                        method="test",
                    ),
                )

        def downloader_factory(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            return Downloader()

        args = self._run_args()
        args.resume_candidate = str(paths.candidate_path)
        args.resume_report = str(paths.report_path)
        args.abort_after_chunks = None
        args.inject_leased_connection_failure = False
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))
        media_identity = MediaIdentity(
            chat_id=sample.chat_id,
            message_id=sample.message_id,
            media_id=9,
            dc_id=4,
            file_unique_id="unique",
            file_size=sample.file_size,
        )
        resume_context = SimpleNamespace(
            artifacts=artifacts,
            recovered_chunks=1,
            recovered_bytes=CHUNK_SIZE,
            total_chunks=2,
            provenance_verified=True,
            prior_report_path=paths.report_path,
            prior_report_identity=benchmark.ArtifactIdentity(1, 2),
            prior_report={},
            media_identity={
                **asdict(media_identity),
                "stable_key": media_identity.stable_key(),
            },
        )

        report = asyncio.run(
            _run_benchmark_async(
                args,
                sample,
                paths,
                client_factory=lambda *_args, **_kwargs: Client(),
                pool_factory=lambda *_args, **_kwargs: Pool(),
                source_factory=lambda *_args, **_kwargs: source,
                downloader_factory=downloader_factory,
                resume_context=resume_context,
            )
        )

        self.assertIsNone(captured["kwargs"]["abort_after_chunks"])
        self.assertEqual("resume", report["run_mode"])
        self.assertEqual(1, report["recovered_chunks"])
        self.assertEqual(1, report["downloaded_chunks"])
        self.assertEqual(CHUNK_SIZE, report["committed_bytes"])
        self.assertEqual(CHUNK_SIZE, report["recovered_bytes"])
        self.assertFalse(report["whole_file_fallback"])
        self.assertTrue(report["eligible"])

    def test_requested_leased_failure_must_trigger_to_be_eligible(self):
        sample = self._sample()
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "missing-injection",
            sample.message_id,
            24,
            2,
        )
        payload = self.baseline_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                return True

            async def disconnect(self):
                self.is_connected = False

            async def get_messages(client_self, **_kwargs):
                return self._valid_message(sample)

        class Pool:
            def start(self):
                return None

            async def close(self):
                return None

            def snapshot(self):
                return {"live": 1, "fallbacks": 0, "by_dc": {4: {"live": 1}}}

        class Downloader:
            async def download(
                self,
                _identity,
                candidate_path,
                progress=None,
                **_kwargs,
            ):
                Path(candidate_path).write_bytes(payload)
                progress(len(payload), len(payload))
                return SimpleNamespace(
                    sha256=digest,
                    retries=0,
                    recovered_chunks=0,
                    downloaded_chunks=1,
                    integrity=IntegrityReport(
                        verified=True,
                        covered_bytes=len(payload),
                        range_count=1,
                        mismatch_count=0,
                        method="test",
                    ),
                )

        args = self._run_args()
        args.resume_candidate = ""
        args.abort_after_chunks = None
        args.inject_leased_connection_failure = True
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))

        report = asyncio.run(
            _run_benchmark_async(
                args,
                sample,
                paths,
                client_factory=lambda *_args, **_kwargs: Client(),
                pool_factory=lambda *_args, **_kwargs: Pool(),
                source_factory=lambda *_args, **_kwargs: source,
                downloader_factory=lambda *_args, **_kwargs: Downloader(),
            )
        )

        self.assertFalse(report["eligible"])
        self.assertTrue(report["fault_injection"]["requested"])
        self.assertFalse(report["fault_injection"]["triggered"])
        self.assertTrue(any("did not trigger" in error for error in report["errors"]))

    def test_transfer_elapsed_excludes_cleanup_and_evidence_on_return_and_failure(self):
        sample = self._sample()
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))
        payload = self.baseline_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                return True

            async def disconnect(self):
                self.is_connected = False

            async def get_messages(client_self, **_kwargs):
                return self._valid_message(sample)

        class Pool:
            def start(self):
                return None

            async def close(self):
                return None

            def snapshot(self):
                return {"live": 1, "by_dc": {4: {"live": 1}}}

        for outcome in ("return", "failure"):
            with self.subTest(outcome=outcome):
                paths = _reserve_output_paths(
                    self.output_root,
                    self.downloads_root,
                    f"timing-{outcome}",
                    sample.message_id,
                    24,
                    2,
                )

                class Downloader:
                    _retries = 0

                    async def download(
                        downloader_self,
                        _identity,
                        candidate_path,
                        progress=None,
                        **_kwargs,
                    ):
                        del downloader_self
                        Path(candidate_path).write_bytes(payload)
                        progress(len(payload), len(payload))
                        if outcome == "failure":
                            raise RuntimeError("timed transfer failure")
                        return SimpleNamespace(
                            sha256=digest,
                            retries=0,
                            integrity=IntegrityReport(
                                verified=True,
                                covered_bytes=len(payload),
                                range_count=1,
                                mismatch_count=0,
                                method="test",
                            ),
                        )

                clock = iter((100.0, 110.0, 112.0, 130.0))
                benchmark_clock = SimpleNamespace(
                    monotonic=mock.Mock(side_effect=lambda: next(clock))
                )
                with mock.patch.object(benchmark, "time", benchmark_clock):
                    report = asyncio.run(
                        _run_benchmark_async(
                            self._run_args(),
                            sample,
                            paths,
                            client_factory=lambda *_args, **_kwargs: Client(),
                            pool_factory=lambda *_args, **_kwargs: Pool(),
                            source_factory=lambda *_args, **_kwargs: source,
                            downloader_factory=lambda *_args, **_kwargs: Downloader(),
                        )
                    )

                self.assertEqual(2.0, report["transfer_elapsed_seconds"])
                self.assertEqual(2.0, report["elapsed_seconds"])
                self.assertEqual(30.0, report["total_elapsed_seconds"])
                self.assertEqual(
                    len(payload) / 2.0,
                    report["goodput_bytes_per_second"],
                )
                if outcome == "failure":
                    self.assertTrue(
                        any("timed transfer failure" in error for error in report["errors"])
                    )

    def test_pool_closes_before_client_stop_when_download_fails(self):
        events = []
        pool_configs = []
        client_workdirs = []

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                events.append("client-connect")
                self.is_connected = True
                return True

            async def disconnect(self):
                events.append("client-disconnect")

            async def start(self):
                self.is_connected = True
                events.append("client-start")

            async def stop(self):
                self.is_connected = False
                events.append("client-stop")

            async def get_messages(self, **_kwargs):
                return SimpleNamespace(
                    id=10341,
                    chat=SimpleNamespace(id=-1002313319912, username=None),
                    empty=False,
                    media=SimpleNamespace(value="video"),
                    video=SimpleNamespace(
                        file_id="encoded",
                        file_unique_id="unique",
                        file_size=self_outer.baseline_path.stat().st_size,
                    ),
                )

        class Pool:
            def start(self):
                events.append("pool-start")

            async def close(self):
                events.append("pool-close")

            def snapshot(self):
                return {"live": 0, "by_dc": {}}

        class Downloader:
            async def download(self, *_args, **_kwargs):
                raise RuntimeError("download failed")

        self_outer = self
        config_path = self.root / "config.yaml"
        config_path.write_text("api_id: 1\napi_hash: hash\n", encoding="utf-8")
        sessions_path = self.sessions_path
        sample = BenchmarkSample(
            chat_id="-1002313319912",
            message_id=10341,
            save_path=str(self.baseline_path),
            file_name=self.baseline_path.name,
            media_type="video",
            file_size=self.baseline_path.stat().st_size,
        )
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            sample.chat_id,
            sample.message_id,
            24,
            2,
        )
        args = SimpleNamespace(
            config=str(config_path),
            sessions=str(sessions_path),
            chat_id=sample.chat_id,
            message_id=sample.message_id,
            session_target=24,
            pipeline_depth=2,
            start_timeout=1.0,
        )
        pool = Pool()
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))

        def client_factory(*_args, **kwargs):
            client_workdirs.append(Path(kwargs["workdir"]))
            return Client()

        def pool_factory(_session_factory, config):
            pool_configs.append(config)
            return pool

        report = asyncio.run(
            _run_benchmark_async(
                args,
                sample,
                paths,
                client_factory=client_factory,
                pool_factory=pool_factory,
                source_factory=lambda *_args, **_kwargs: source,
                downloader_factory=lambda *_args, **_kwargs: Downloader(),
            )
        )

        self.assertEqual(
            [
                "client-connect",
                "pool-start",
                "pool-close",
                "client-disconnect",
            ],
            events,
        )
        self.assertFalse(report["eligible"])
        self.assertTrue(any("download failed" in error for error in report["errors"]))
        self.assertEqual(24, pool_configs[0].soft_sessions)
        self.assertEqual(24, pool_configs[0].max_sessions)
        self.assertEqual(2, pool_configs[0].pipeline_depth)
        self.assertFalse(pool_configs[0].adaptive)
        self.assertTrue(client_workdirs[0].is_relative_to(paths.run_dir))
        self.assertFalse((paths.run_dir / "sessions").exists())
        self.assertEqual(
            b"session",
            (sessions_path / "media_downloader.session").read_bytes(),
        )

    def test_cancellation_still_closes_pool_before_client(self):
        events = []
        entered = asyncio.Event()

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def start(self):
                events.append("client-start")

            async def stop(self):
                events.append("client-stop")

            async def connect(self):
                events.append("client-connect")
                self.is_connected = True
                return True

            async def disconnect(self):
                events.append("client-disconnect")
                self.is_connected = False

            async def get_messages(self, **_kwargs):
                return SimpleNamespace(
                    id=10341,
                    chat=SimpleNamespace(id=-1002313319912, username=None),
                    empty=False,
                    media=SimpleNamespace(value="video"),
                    video=SimpleNamespace(
                        file_id="encoded",
                        file_unique_id="unique",
                        file_size=self_outer.baseline_path.stat().st_size,
                    ),
                )

        class Pool:
            def start(self):
                events.append("pool-start")

            async def close(self):
                await asyncio.sleep(0)
                events.append("pool-close")

            def snapshot(self):
                return {"live": 0, "by_dc": {}}

        class Downloader:
            async def download(self, *_args, **_kwargs):
                entered.set()
                await asyncio.Event().wait()

        async def exercise():
            task = asyncio.create_task(
                _run_benchmark_async(
                    args,
                    sample,
                    paths,
                    client_factory=lambda *_args, **_kwargs: Client(),
                    pool_factory=lambda *_args, **_kwargs: Pool(),
                    source_factory=lambda *_args, **_kwargs: source,
                    downloader_factory=lambda *_args, **_kwargs: Downloader(),
                )
            )
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError) as caught:
                await task
            return caught.exception

        self_outer = self
        config_path = self.root / "config.yaml"
        config_path.write_text("api_id: 1\napi_hash: hash\n", encoding="utf-8")
        sessions_path = self.sessions_path
        sample = BenchmarkSample(
            chat_id="-1002313319912",
            message_id=10341,
            save_path=str(self.baseline_path),
            file_name=self.baseline_path.name,
            media_type="video",
            file_size=self.baseline_path.stat().st_size,
        )
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            sample.chat_id,
            sample.message_id,
            24,
            2,
        )
        args = SimpleNamespace(
            config=str(config_path),
            sessions=str(sessions_path),
            chat_id=sample.chat_id,
            message_id=sample.message_id,
            session_target=24,
            pipeline_depth=2,
            start_timeout=1.0,
        )
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))

        interruption = asyncio.run(exercise())

        self.assertEqual(
            [
                "client-connect",
                "pool-start",
                "pool-close",
                "client-disconnect",
            ],
            events,
        )
        self.assertFalse((paths.run_dir / "sessions").exists())
        report = interruption._benchmark_report
        self.assertFalse(report["eligible"])
        self.assertEqual("CancelledError", report["interruption"]["class"])
        self.assertEqual(
            hashlib.sha256(b"").hexdigest(),
            report["candidate_sha256"],
        )

    def test_system_exit_keeps_partial_evidence_after_ordered_cleanup(self):
        events = []
        sample = self._sample()
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "system-exit",
            sample.message_id,
            24,
            2,
        )

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                events.append("client-connect")
                return True

            async def disconnect(self):
                self.is_connected = False
                events.append("client-disconnect")

            async def start(self):
                self.is_connected = True
                events.append("client-start")

            async def stop(self):
                self.is_connected = False
                events.append("client-stop")

            async def get_messages(self, **_kwargs):
                return self_outer._valid_message(sample)

        class Pool:
            def start(self):
                events.append("pool-start")

            async def close(self):
                events.append("pool-close")

            def snapshot(self):
                return {"live": 2, "by_dc": {4: {"live": 2}}}

        class Downloader:
            _retries = 3

            async def download(self, _identity, candidate_path, progress=None, **_kwargs):
                Path(candidate_path).write_bytes(b"partial")
                progress(7, sample.file_size)
                raise SystemExit(23)

        self_outer = self
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))
        with self.assertRaises(SystemExit) as caught:
            asyncio.run(
                _run_benchmark_async(
                    self._run_args(),
                    sample,
                    paths,
                    client_factory=lambda *_args, **_kwargs: Client(),
                    pool_factory=lambda *_args, **_kwargs: Pool(),
                    source_factory=lambda *_args, **_kwargs: source,
                    downloader_factory=lambda *_args, **_kwargs: Downloader(),
                )
            )

        self.assertEqual(23, caught.exception.code)
        self.assertEqual(
            ["client-connect", "pool-start", "pool-close", "client-disconnect"],
            events,
        )
        report = caught.exception._benchmark_report
        self.assertEqual("SystemExit", report["interruption"]["class"])
        self.assertEqual(7, report["candidate_file_size"])
        self.assertEqual(7, report["committed_bytes"])
        self.assertEqual(3, report["retries"])
        self.assertEqual(
            hashlib.sha256(b"partial").hexdigest(),
            report["candidate_sha256"],
        )
        self.assertFalse((paths.run_dir / "sessions").exists())

    def test_cleanup_base_exceptions_keep_type_evidence_and_finish_cleanup(self):
        sample = self._sample()
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))
        payload = self.baseline_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        interruption_factories = (
            asyncio.CancelledError,
            KeyboardInterrupt,
            lambda: SystemExit(31),
        )

        for factory in interruption_factories:
            cleanup_error = factory()
            name = type(cleanup_error).__name__
            with self.subTest(interruption=name):
                events = []
                paths = _reserve_output_paths(
                    self.output_root,
                    self.downloads_root,
                    f"cleanup-{name}",
                    sample.message_id,
                    24,
                    2,
                )

                class Client:
                    sessions_lock = asyncio.Lock()
                    is_connected = False
                    is_initialized = False

                    async def connect(self):
                        self.is_connected = True
                        return True

                    async def disconnect(self):
                        self.is_connected = False
                        events.append("client-disconnect")

                    async def get_messages(client_self, **_kwargs):
                        return self._valid_message(sample)

                class Pool:
                    def start(self):
                        return None

                    async def close(self):
                        events.append("pool-close")
                        raise cleanup_error

                    def snapshot(self):
                        return {"live": 0, "by_dc": {}}

                class Downloader:
                    async def download(
                        self,
                        _identity,
                        candidate_path,
                        progress=None,
                        **_kwargs,
                    ):
                        Path(candidate_path).write_bytes(payload)
                        progress(len(payload), len(payload))
                        return SimpleNamespace(
                            sha256=digest,
                            retries=0,
                            integrity=IntegrityReport(
                                verified=True,
                                covered_bytes=len(payload),
                                range_count=1,
                                mismatch_count=0,
                                method="test",
                            ),
                        )

                with self.assertRaises(type(cleanup_error)) as caught:
                    asyncio.run(
                        _run_benchmark_async(
                            self._run_args(),
                            sample,
                            paths,
                            client_factory=lambda *_args, **_kwargs: Client(),
                            pool_factory=lambda *_args, **_kwargs: Pool(),
                            source_factory=lambda *_args, **_kwargs: source,
                            downloader_factory=lambda *_args, **_kwargs: Downloader(),
                        )
                    )

                if isinstance(cleanup_error, SystemExit):
                    self.assertEqual(31, caught.exception.code)
                self.assertEqual(["pool-close", "client-disconnect"], events)
                self.assertFalse((paths.run_dir / "sessions").exists())
                report = caught.exception._benchmark_report
                self.assertEqual(name, report["interruption"]["class"])
                self.assertTrue(
                    any(
                        item["phase"] == "pool close" and item["class"] == name
                        for item in report["interruptions"]
                    )
                )
                self.assertTrue(any("pool close" in error for error in report["errors"]))

    def test_repeated_cancellation_and_cleanup_failure_are_both_reported(self):
        sample = self._sample()
        source = SimpleNamespace(file_id=SimpleNamespace(media_id=9, dc_id=4))
        download_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        cleanup_release = asyncio.Event()
        events = []
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "repeated-cancel",
            sample.message_id,
            24,
            2,
        )

        class Client:
            sessions_lock = asyncio.Lock()
            is_connected = False
            is_initialized = False

            async def connect(self):
                self.is_connected = True
                return True

            async def disconnect(self):
                self.is_connected = False
                events.append("client-disconnect")

            async def get_messages(client_self, **_kwargs):
                return self._valid_message(sample)

        class Pool:
            def start(self):
                return None

            async def close(self):
                cleanup_started.set()
                await cleanup_release.wait()
                events.append("pool-close")
                raise RuntimeError("pool cleanup failed")

            def snapshot(self):
                return {"live": 0, "by_dc": {}}

        class Downloader:
            async def download(self, *_args, **_kwargs):
                download_started.set()
                await asyncio.Event().wait()

        async def exercise():
            task = asyncio.create_task(
                _run_benchmark_async(
                    self._run_args(),
                    sample,
                    paths,
                    client_factory=lambda *_args, **_kwargs: Client(),
                    pool_factory=lambda *_args, **_kwargs: Pool(),
                    source_factory=lambda *_args, **_kwargs: source,
                    downloader_factory=lambda *_args, **_kwargs: Downloader(),
                )
            )
            await download_started.wait()
            task.cancel("download cancellation")
            await cleanup_started.wait()
            task.cancel("cleanup cancellation")
            cleanup_release.set()
            with self.assertRaises(asyncio.CancelledError) as caught:
                await task
            return caught.exception

        interruption = asyncio.run(exercise())

        self.assertEqual("download cancellation", str(interruption))
        self.assertEqual(["pool-close", "client-disconnect"], events)
        self.assertFalse((paths.run_dir / "sessions").exists())
        report = interruption._benchmark_report
        cancellations = [
            item
            for item in report["interruptions"]
            if item["class"] == "CancelledError"
        ]
        self.assertGreaterEqual(len(cancellations), 2)
        self.assertTrue(any(item["phase"] == "cleanup wait" for item in cancellations))
        self.assertTrue(
            any("pool cleanup failed" in error for error in report["errors"])
        )

    @mock.patch("tools.benchmark_global_media_pool._run_benchmark_async")
    def test_main_atomically_writes_report(self, run_benchmark):
        run_benchmark.return_value = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="a" * 64,
            file_size=self.baseline_path.stat().st_size,
            elapsed_seconds=1.0,
            snapshot={"live": 8, "by_dc": {4: {"live": 8}}},
            retries=0,
            candidate_file_size=self.baseline_path.stat().st_size,
            integrity_verified=True,
            committed_bytes=self.baseline_path.stat().st_size,
        )
        emitted = []

        with mock.patch.object(
            benchmark,
            "_validate_mount_isolation",
            return_value=dict(ISOLATED_MOUNTS),
            create=True,
        ):
            exit_code = main(self._main_args(), emit=emitted.append)

        self.assertEqual(0, exit_code)
        reports = list(self.output_root.glob("*/report.json"))
        self.assertEqual(1, len(reports))
        report = json.loads(reports[0].read_text(encoding="utf-8"))
        self.assertTrue(report["eligible"])
        self.assertEqual(2, report["schema_version"])
        self.assertEqual("completed", report["status"])
        self.assertIn("recovery", report)
        self.assertIn("fault_injection", report)
        self.assertEqual(ISOLATED_MOUNTS, report["mount_isolation"])
        self.assertEqual(0o600, stat.S_IMODE(reports[0].stat().st_mode))
        self.assertEqual(1, reports[0].stat().st_nlink)
        self.assertEqual([], list(self.output_root.rglob("*.tmp")))
        self.assertEqual(str(reports[0]), json.loads(emitted[-1])["report_path"])

    @mock.patch("tools.benchmark_global_media_pool._run_benchmark_async")
    def test_abort_after_final_chunk_is_rejected_in_shared_failure_envelope(
        self,
        run_benchmark,
    ):
        emitted = []
        args = self._main_args() + ["--abort-after-chunks", "1"]

        with mock.patch.object(
            benchmark,
            "_validate_mount_isolation",
            return_value=dict(ISOLATED_MOUNTS),
        ):
            exit_code = main(args, emit=emitted.append)

        self.assertEqual(1, exit_code)
        run_benchmark.assert_not_called()
        report_path = next(self.output_root.glob("*/report.json"))
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual("failed", report["status"])
        self.assertFalse(report["eligible"])
        self.assertIn("recovery", report)
        self.assertIn("fault_injection", report)
        self.assertEqual(1, report["recovery"]["abort_after_chunks"])
        self.assertTrue(any("final chunk" in error for error in report["errors"]))
        self.assertEqual(report, json.loads(emitted[-1]))

    @mock.patch("tools.benchmark_global_media_pool._run_benchmark_async")
    def test_main_resume_preserves_first_report_and_reuses_candidate(
        self,
        run_benchmark,
    ):
        paths = _reserve_output_paths(
            self.output_root,
            self.downloads_root,
            "main-resume",
            10341,
            24,
            2,
        )
        benchmark._reserve_download_artifacts(paths)
        self._write_private_json(paths.report_path, {"first": "run-evidence"})
        first_report_bytes = paths.report_path.read_bytes()
        abandoned = paths.run_dir / "resume-report-0001.json"
        benchmark._reserve_report_path(abandoned)
        prior_attempt = paths.run_dir / "resume-report-0002.json"
        self._write_private_json(prior_attempt, {"failed": "prior-attempt"})
        prior_attempt_bytes = prior_attempt.read_bytes()
        run_benchmark.return_value = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="a" * 64,
            file_size=self.baseline_path.stat().st_size,
            elapsed_seconds=1.0,
            snapshot={"live": 8},
            retries=0,
            candidate_file_size=self.baseline_path.stat().st_size,
            integrity_verified=True,
            committed_bytes=self.baseline_path.stat().st_size,
        )
        run_benchmark.return_value["recovery"] = {
            "mode": "resume",
            "abort_after_chunks": None,
            "recovered_chunks": 1,
            "recovered_bytes": 1,
            "downloaded_chunks": 1,
            "current_run_committed_bytes": self.baseline_path.stat().st_size,
            "durable_chunks": 2,
            "durable_bytes": self.baseline_path.stat().st_size + 1,
            "total_chunks": 2,
            "whole_file_fallback": False,
            "provenance_verified": True,
        }
        args = self._main_args() + [
            "--resume-candidate",
            str(paths.candidate_path),
            "--resume-report",
            str(paths.report_path),
        ]

        with mock.patch.object(
            benchmark,
            "_validate_mount_isolation",
            return_value=dict(ISOLATED_MOUNTS),
        ):
            with mock.patch.object(
                benchmark,
                "_validate_resume_provenance",
                return_value=SimpleNamespace(
                    recovered_chunks=1,
                    recovered_bytes=1,
                    total_chunks=2,
                    provenance_verified=True,
                    prior_report_path=paths.report_path,
                    prior_report_identity=SimpleNamespace(device=1, inode=2),
                ),
            ):
                exit_code = main(args, emit=lambda _value: None)

        self.assertEqual(0, exit_code)
        self.assertEqual(first_report_bytes, paths.report_path.read_bytes())
        self.assertEqual(b"", abandoned.read_bytes())
        self.assertEqual(prior_attempt_bytes, prior_attempt.read_bytes())
        resume_report = paths.run_dir / "resume-report-0003.json"
        self.assertTrue(resume_report.is_file())
        called_paths = run_benchmark.await_args.args[2]
        self.assertEqual(paths.candidate_path, called_paths.candidate_path)
        self.assertEqual(paths.manifest_path, called_paths.manifest_path)
        self.assertEqual(
            paths.report_path,
            run_benchmark.await_args.kwargs["resume_context"].prior_report_path,
        )

    @mock.patch("tools.benchmark_global_media_pool._run_benchmark_async")
    def test_main_returns_nonzero_on_integrity_failure(self, run_benchmark):
        run_benchmark.return_value = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="b" * 64,
            file_size=self.baseline_path.stat().st_size,
            elapsed_seconds=1.0,
            snapshot={"live": 8},
            retries=0,
            candidate_file_size=self.baseline_path.stat().st_size,
            integrity_verified=True,
            committed_bytes=self.baseline_path.stat().st_size,
        )

        with mock.patch.object(
            benchmark,
            "_validate_mount_isolation",
            return_value=dict(ISOLATED_MOUNTS),
            create=True,
        ):
            exit_code = main(self._main_args(), emit=lambda _value: None)

        self.assertEqual(1, exit_code)
        report_path = next(self.output_root.glob("*/report.json"))
        self.assertFalse(json.loads(report_path.read_text(encoding="utf-8"))["eligible"])

    @mock.patch("tools.benchmark_global_media_pool._run_benchmark_async")
    def test_main_preserves_runtime_errors_in_atomic_report(self, run_benchmark):
        run_benchmark.side_effect = RuntimeError("session creation failed")

        with mock.patch.object(
            benchmark,
            "_validate_mount_isolation",
            return_value=dict(ISOLATED_MOUNTS),
            create=True,
        ):
            exit_code = main(self._main_args(), emit=lambda _value: None)

        self.assertEqual(1, exit_code)
        report_path = next(self.output_root.glob("*/report.json"))
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertFalse(report["eligible"])
        self.assertTrue(
            any("session creation failed" in error for error in report["errors"])
        )

    @mock.patch("tools.benchmark_global_media_pool._run_benchmark_async")
    def test_main_writes_nothing_when_output_overlaps_protected_tree(
        self,
        run_benchmark,
    ):
        args = self._main_args()
        output_index = args.index("--output") + 1
        invalid_output = self.sessions_path / "benchmark-output"
        args[output_index] = str(invalid_output)
        before = sorted(
            str(path.relative_to(self.sessions_path))
            for path in self.sessions_path.rglob("*")
        )
        emitted = []
        run_benchmark.return_value = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="a" * 64,
            file_size=self.baseline_path.stat().st_size,
            elapsed_seconds=1.0,
            snapshot={},
            retries=0,
            candidate_file_size=self.baseline_path.stat().st_size,
            integrity_verified=True,
            committed_bytes=self.baseline_path.stat().st_size,
        )

        exit_code = main(args, emit=emitted.append)

        self.assertEqual(1, exit_code)
        self.assertFalse(invalid_output.exists())
        self.assertEqual(
            before,
            sorted(
                str(path.relative_to(self.sessions_path))
                for path in self.sessions_path.rglob("*")
            ),
        )
        run_benchmark.assert_not_called()
        self.assertIn("protected", emitted[-1].lower())
        preflight = json.loads(emitted[-1])
        self.assertEqual(2, preflight["schema_version"])
        self.assertEqual("failed", preflight["status"])
        self.assertIn("recovery", preflight)
        self.assertIn("fault_injection", preflight)

    @mock.patch("tools.benchmark_global_media_pool._run_benchmark_async")
    def test_main_closes_records_connection_deterministically(self, run_benchmark):
        run_benchmark.return_value = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="a" * 64,
            file_size=self.baseline_path.stat().st_size,
            elapsed_seconds=1.0,
            snapshot={},
            retries=0,
            candidate_file_size=self.baseline_path.stat().st_size,
            integrity_verified=True,
            committed_bytes=self.baseline_path.stat().st_size,
        )
        real_connection = sqlite3.connect(self.records_path)

        class Connection:
            closed = False

            def execute(self, *args, **kwargs):
                return real_connection.execute(*args, **kwargs)

            def close(self):
                self.closed = True
                real_connection.close()

        connection = Connection()
        with mock.patch.object(
            benchmark,
            "_open_records_read_only",
            return_value=connection,
        ):
            with mock.patch.object(
                benchmark,
                "_validate_mount_isolation",
                return_value=dict(ISOLATED_MOUNTS),
                create=True,
            ):
                exit_code = main(self._main_args(), emit=lambda _value: None)

        self.assertEqual(0, exit_code)
        self.assertTrue(connection.closed)

    def test_main_atomically_persists_then_reraises_interruptions(self):
        interruption_factories = (
            ("cancelled", asyncio.CancelledError),
            ("keyboard", KeyboardInterrupt),
            ("system-exit", lambda: SystemExit(27)),
        )
        for name, factory in interruption_factories:
            with self.subTest(name=name):
                output = self.root / f"output-{name}"
                output.mkdir()
                args = self._main_args()
                args[args.index("--output") + 1] = str(output)
                report = build_report(
                    baseline_sha256="a" * 64,
                    candidate_sha256="",
                    file_size=self.baseline_path.stat().st_size,
                    elapsed_seconds=0.25,
                    snapshot={"peak_live": 3},
                    retries=2,
                    candidate_file_size=4,
                    integrity_verified=False,
                    committed_bytes=4,
                    errors=[name],
                )
                error = factory()
                report["interruption"] = {
                    "class": type(error).__name__,
                    "message": str(error),
                }
                error._benchmark_report = report

                with mock.patch.object(
                    benchmark,
                    "_validate_mount_isolation",
                    return_value=dict(ISOLATED_MOUNTS),
                    create=True,
                ):
                    with mock.patch.object(
                        benchmark,
                        "_run_benchmark_async",
                        side_effect=error,
                    ):
                        with self.assertRaises(type(error)) as caught:
                            main(args, emit=lambda _value: None)

                if isinstance(error, SystemExit):
                    self.assertEqual(27, caught.exception.code)
                report_path = next(output.glob("*/report.json"))
                persisted = json.loads(report_path.read_text(encoding="utf-8"))
                self.assertEqual(type(error).__name__, persisted["interruption"]["class"])
                self.assertEqual(4, persisted["candidate_file_size"])
                self.assertEqual(4, persisted["committed_bytes"])
                self.assertEqual(2, persisted["retries"])

    def test_real_sigint_keeps_inner_asyncio_cancellation_report(self):
        output = self.root / "sigint-output"
        output.mkdir()
        ready_path = self.root / "sigint-ready"
        script = textwrap.dedent(
            """
            import asyncio
            import sys
            from pathlib import Path

            import tools.benchmark_global_media_pool as benchmark

            output = Path(sys.argv[1])
            ready = Path(sys.argv[2])
            downloads = Path(sys.argv[3])
            baseline = Path(sys.argv[4])

            class Connection:
                def close(self):
                    return None

            benchmark._validate_mount_isolation = lambda *_args, **_kwargs: {
                "verified": True,
                "separate_output_device": True,
                "protected_read_only": True,
            }
            benchmark._open_records_read_only = lambda _path: Connection()
            benchmark._select_successful_record = lambda *_args, **_kwargs: benchmark.BenchmarkSample(
                chat_id="-1002313319912",
                message_id=10341,
                save_path=str(baseline),
                file_name=baseline.name,
                media_type="video",
                file_size=baseline.stat().st_size,
            )

            async def interrupted(*_args, **_kwargs):
                ready.write_text("ready", encoding="utf-8")
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError as error:
                    report = benchmark.build_report(
                        baseline_sha256="a" * 64,
                        candidate_sha256="",
                        file_size=baseline.stat().st_size,
                        elapsed_seconds=0.5,
                        snapshot={"peak_live": 4},
                        retries=3,
                        candidate_file_size=7,
                        integrity_verified=False,
                        committed_bytes=7,
                        errors=["inner cancellation evidence"],
                    )
                    report["inner_report_marker"] = "survived-sigint"
                    report["interruption"] = {
                        "class": "CancelledError",
                        "message": str(error),
                    }
                    report["interruptions"] = [
                        {
                            "phase": "benchmark",
                            "class": "CancelledError",
                            "message": str(error),
                        }
                    ]
                    error._benchmark_report = report
                    raise

            benchmark._run_benchmark_async = interrupted
            args = [
                "--chat-id", "-1002313319912",
                "--message-id", "10341",
                "--output", str(output),
                "--records", str(output.parent / "records.sqlite3"),
                "--downloads-root", str(downloads),
                "--config", str(output.parent / "config.yaml"),
                "--sessions", str(output.parent / "sessions"),
                "--session-target", "24",
                "--pipeline-depth", "2",
            ]
            try:
                benchmark.main(args, emit=lambda _value: None)
            except KeyboardInterrupt:
                raise SystemExit(130)
            raise SystemExit(99)
            """
        )
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                str(output),
                str(ready_path),
                str(self.downloads_root),
                str(self.baseline_path),
            ],
            cwd=Path(__file__).resolve().parents[2],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 10
            while not ready_path.exists() and process.poll() is None:
                if time.monotonic() >= deadline:
                    self.fail("SIGINT subprocess did not enter the async benchmark")
                time.sleep(0.01)
            self.assertIsNone(process.poll())
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=10)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

        self.assertEqual(130, process.returncode, (stdout, stderr))
        report_path = next(output.glob("*/report.json"))
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual("survived-sigint", report["inner_report_marker"])
        self.assertEqual(7, report["candidate_file_size"])
        self.assertEqual(7, report["committed_bytes"])
        self.assertEqual(3, report["retries"])
        self.assertEqual("CancelledError", report["interruption"]["class"])
        self.assertTrue(
            any(
                item["phase"] == "asyncio.run"
                and item["class"] == "KeyboardInterrupt"
                for item in report["interruptions"]
            )
        )


if __name__ == "__main__":
    unittest.main()
