"""Tests for the isolated parallel-download validation CLI."""

import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from module.parallel_downloader import InjectedAbort
from module.parallel_validation import GIB, MIB
from tools.validate_parallel_downloads import _run_validation_async, main


class ValidationCliTest(unittest.TestCase):
    """Dry selection is read-only and never opens a Telegram session."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "records.sqlite3"
        self.output_dir = self.root / "output"
        self._create_records(
            [1 * MIB, 2 * MIB, 20 * MIB, 30 * MIB, 300 * MIB, 2 * GIB]
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_records(self, sizes):
        connection = sqlite3.connect(self.db_path)
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
            path = self.root / f"sample-{message_id}.bin"
            path.touch()
            connection.execute(
                """
                INSERT INTO download_records (
                    chat_id, message_id, status, file_name,
                    save_path, media_type, file_size
                ) VALUES ('-100123', ?, 'success', ?, ?, 'video', ?)
                """,
                (message_id, path.name, str(path), size),
            )
        connection.commit()
        connection.close()

    def test_direct_script_entrypoint_can_import_project_modules(self):
        project_root = Path(__file__).resolve().parents[2]
        script = project_root / "tools" / "validate_parallel_downloads.py"

        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--dry-select", result.stdout)

    def test_dry_select_prints_six_samples_without_starting_client(self):
        client_calls = []
        output = []

        def forbidden_client_factory(*args, **kwargs):
            client_calls.append((args, kwargs))
            raise AssertionError("dry selection must not create Telegram client")

        exit_code = main(
            [
                "--dry-select",
                "--records",
                str(self.db_path),
                "--output-dir",
                str(self.output_dir),
            ],
            client_factory=forbidden_client_factory,
            emit=output.append,
        )

        self.assertEqual(0, exit_code)
        payload = json.loads(output[-1])
        self.assertEqual(6, len(payload["samples"]))
        self.assertEqual([], payload["bucket_gaps"])
        self.assertEqual([], client_calls)

    def test_dry_select_returns_two_when_a_bucket_is_missing(self):
        missing_db = self.root / "missing.sqlite3"
        self.db_path = missing_db
        self._create_records([1 * MIB, 2 * MIB, 20 * MIB, 30 * MIB, 300 * MIB])
        output = []

        exit_code = main(
            [
                "--dry-select",
                "--records",
                str(missing_db),
                "--output-dir",
                str(self.output_dir),
            ],
            emit=output.append,
        )

        self.assertEqual(2, exit_code)
        self.assertEqual(["gt1GiB"], json.loads(output[-1])["bucket_gaps"])

    @mock.patch(
        "tools.validate_parallel_downloads._run_validation_async",
        new_callable=mock.AsyncMock,
    )
    def test_injected_abort_returns_75_and_preserves_run_id(self, run_validation):
        run_validation.side_effect = InjectedAbort("aborted after 2 chunks")
        output = []

        exit_code = main(
            [
                "--records",
                str(self.db_path),
                "--output-dir",
                str(self.output_dir),
                "--run-id",
                "crash-check",
                "--abort-after-chunks",
                "2",
            ],
            emit=output.append,
        )

        self.assertEqual(75, exit_code)
        payload = json.loads(output[-1])
        self.assertEqual("crash-check", payload["run_id"])
        self.assertEqual("aborted", payload["status"])

    @mock.patch(
        "tools.validate_parallel_downloads._run_validation_async",
        new_callable=mock.AsyncMock,
    )
    def test_resume_run_reuses_exact_identifier(self, run_validation):
        run_validation.return_value = {
            "eligible": True,
            "samples": [{"decision": "pass"}],
        }

        exit_code = main(
            [
                "--records",
                str(self.db_path),
                "--output-dir",
                str(self.output_dir),
                "--resume-run",
                "crash-check",
            ]
        )

        self.assertEqual(0, exit_code)
        self.assertEqual("crash-check", run_validation.await_args.args[3])

    def test_client_stops_when_transmission_setup_fails(self):
        config_path = self.root / "config.yaml"
        config_path.write_text("api_id: 1\napi_hash: test\n", encoding="utf-8")
        client = mock.MagicMock()
        client.start = mock.AsyncMock()
        client.stop = mock.AsyncMock()
        args = SimpleNamespace(
            config=str(config_path),
            sessions=str(self.root / "sessions"),
            output_dir=str(self.output_dir),
            workers=2,
            downloads_root=str(self.root),
            abort_after_chunks=None,
            report="",
        )

        with mock.patch(
            "tools.validate_parallel_downloads.set_max_concurrent_transmissions",
            side_effect=RuntimeError("transmission setup failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "transmission setup failed"):
                asyncio.run(
                    _run_validation_async(
                        args,
                        [],
                        [],
                        "client-cleanup",
                        lambda *unused_args, **unused_kwargs: client,
                    )
                )

        client.start.assert_awaited_once()
        client.stop.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
