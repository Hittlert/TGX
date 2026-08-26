"""Regression tests for the Kurigram compatibility layer."""

import unittest
from pathlib import Path

import pyrogram

from module.pyrogram_extension import HookClient


class KurigramCompatibilityTestCase(unittest.TestCase):
    """Keep the application on Kurigram's connection lifecycle."""

    def test_requirements_pin_kurigram(self):
        requirements_path = Path(__file__).resolve().parents[2] / "requirements.txt"
        active_requirements = [
            line.strip()
            for line in requirements_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

        self.assertIn("kurigram==2.2.24", active_requirements)
        self.assertFalse(
            any("tangyoha/pyrogram" in line for line in active_requirements)
        )

    def test_hook_client_uses_library_connection_lifecycle(self):
        self.assertNotIn("connect", HookClient.__dict__)
        self.assertNotIn("start", HookClient.__dict__)

    def test_start_timeout_configures_library_session(self):
        original_timeout = pyrogram.session.Session.START_TIMEOUT
        try:
            HookClient(
                "kurigram_compat_test",
                api_id=12345,
                api_hash="0123456789abcdef0123456789abcdef",
                in_memory=True,
                no_updates=True,
                start_timeout=37,
            )

            self.assertEqual(pyrogram.session.Session.START_TIMEOUT, 37)
        finally:
            pyrogram.session.Session.START_TIMEOUT = original_timeout


if __name__ == "__main__":
    unittest.main()
