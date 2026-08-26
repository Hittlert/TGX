import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from unittest import mock

from module import web
from module.app import Application


class MediaPoolStatusTestCase(unittest.TestCase):
    def setUp(self):
        self.original_app = web._web_app
        self.original_login_disabled = web._flask_app.config.get("LOGIN_DISABLED")
        web._flask_app.config["LOGIN_DISABLED"] = True

    def tearDown(self):
        web._web_app = self.original_app
        web._flask_app.config["LOGIN_DISABLED"] = self.original_login_disabled

    def test_status_returns_disabled_pool_shape(self):
        web._web_app = SimpleNamespace(
            get_media_pool_status=lambda: {
                "enabled": False,
                "desired": 0,
                "live": 0,
                "hard_limit": 48,
            }
        )

        response = web.get_flask_app().test_client().get("/get_download_status")
        payload = response.get_json()

        self.assertEqual(200, response.status_code)
        self.assertFalse(payload["media_pool"]["enabled"])

    def test_status_exposes_compact_live_pool_counts(self):
        status = {
            "enabled": True,
            "desired": 24,
            "live": 22,
            "active_slots": 31,
            "hard_limit": 48,
            "pipeline_depth": 2,
            "last_scale_reason": "goodput_growth",
        }
        web._web_app = SimpleNamespace(get_media_pool_status=lambda: status)

        payload = (
            web.get_flask_app().test_client().get("/get_download_status").get_json()
        )

        self.assertEqual(status, payload["media_pool"])


@dataclass
class _PoolSnapshot:
    desired: int
    live: int
    active_slots: int
    hard_limit: int
    pipeline_depth: int
    last_scale_reason: str


class MediaPoolStatusAdapterTestCase(unittest.TestCase):
    def setUp(self):
        self.original_app = web._web_app
        self.original_login_disabled = web._flask_app.config.get("LOGIN_DISABLED")
        web._flask_app.config["LOGIN_DISABLED"] = True

    def tearDown(self):
        web._web_app = self.original_app
        web._flask_app.config["LOGIN_DISABLED"] = self.original_login_disabled

    def test_adapter_serializes_snapshot_for_the_status_thread(self):
        snapshot = _PoolSnapshot(24, 22, 31, 48, 2, "goodput_growth")
        app = SimpleNamespace(
            media_session_pool=SimpleNamespace(snapshot=lambda: snapshot),
            parallel_pool_max_sessions=48,
            parallel_pool_pipeline_depth=2,
        )

        payload = Application.get_media_pool_status(app)

        self.assertTrue(payload["enabled"])
        self.assertEqual("global", payload["mode"])
        self.assertEqual(24, payload["desired"])
        self.assertEqual(22, payload["live"])
        self.assertEqual(31, payload["active_slots"])
        self.assertEqual(48, payload["hard_limit"])
        self.assertEqual(2, payload["pipeline_depth"])
        self.assertEqual("goodput_growth", payload["last_scale_reason"])

    def test_adapter_returns_disabled_shape_without_a_pool(self):
        app = SimpleNamespace(
            media_session_pool=None,
            parallel_pool_max_sessions=48,
            parallel_pool_pipeline_depth=2,
        )

        payload = Application.get_media_pool_status(app)

        self.assertFalse(payload["enabled"])
        self.assertEqual("off", payload["mode"])
        self.assertEqual(0, payload["desired"])
        self.assertEqual(0, payload["live"])
        self.assertEqual(0, payload["active_slots"])
        self.assertEqual(48, payload["hard_limit"])
        self.assertEqual(2, payload["pipeline_depth"])
        self.assertEqual("disabled", payload["last_scale_reason"])

    def test_adapter_serializes_per_file_coordinator_metrics(self):
        snapshot = SimpleNamespace(
            used=8,
            hard_limit=60,
            creating=0,
            live=8,
            idle=3,
            created=12,
            reused=7,
            draining=0,
            active_files=2,
            committed_bytes_per_second=8 * 1024 * 1024,
            raw_bps=7 * 1024 * 1024,
            rolling_5s_bps=8 * 1024 * 1024,
            p10_5s_bps=6 * 1024 * 1024,
            mean_5s_bps=9 * 1024 * 1024,
            stddev_5s_bps=2 * 1024 * 1024,
            cv=0.22,
            sample_count=100,
            raw_samples=(7 * 1024 * 1024, 8 * 1024 * 1024),
            rolling_5s_samples=(8 * 1024 * 1024,),
            expansion_queue=0,
            dc_cooldowns={4: 0.0},
            fallbacks=1,
            pools={
                "-1001:42:unique": {
                    "used": 4,
                    "target": 4,
                    "live": 4,
                    "active": 4,
                    "draining": 0,
                    "pending": 10,
                    "rolling_5s_bps": 5 * 1024 * 1024,
                    "last_scale_reason": "initial",
                },
                "-1002:84:unique": {
                    "used": 4,
                    "target": 4,
                    "live": 4,
                    "active": 3,
                    "draining": 0,
                    "pending": 20,
                    "rolling_5s_bps": 3 * 1024 * 1024,
                    "last_scale_reason": "initial",
                },
            },
        )
        app = SimpleNamespace(
            parallel_pool_mode="per_file",
            media_transfer_coordinator=SimpleNamespace(snapshot=lambda: snapshot),
            media_session_pool=None,
            parallel_media_session_budget=60,
            parallel_file_pool_pipeline_depth=1,
            parallel_pool_max_sessions=48,
            parallel_pool_pipeline_depth=1,
        )

        payload = Application.get_media_pool_status(app)

        self.assertTrue(payload["enabled"])
        self.assertEqual("per_file", payload["mode"])
        self.assertEqual(8, payload["used"])
        self.assertEqual(3, payload["idle"])
        self.assertEqual(12, payload["created"])
        self.assertEqual(7, payload["reused"])
        self.assertEqual(60, payload["hard_limit"])
        self.assertEqual(2, payload["active_files"])
        self.assertEqual(8 * 1024 * 1024, payload["rolling_5s_bps"])
        self.assertEqual(2, len(payload["files"]))
        self.assertEqual(7, payload["active_slots"])
        self.assertEqual(8, payload["desired"])

    def test_status_endpoint_uses_per_file_rolling_committed_speed(self):
        status = {
            "enabled": True,
            "mode": "per_file",
            "rolling_5s_bps": 8 * 1024 * 1024,
            "active_files": 1,
            "used": 4,
            "hard_limit": 60,
            "files": {},
        }
        web._web_app = SimpleNamespace(get_media_pool_status=lambda: status)

        with mock.patch.object(web, "get_total_download_speed", return_value=1):
            payload = (
                web.get_flask_app().test_client().get("/get_download_status").get_json()
            )

        self.assertEqual("8.0MB/s", payload["download_speed"])

    def test_download_row_uses_matching_file_pool_rolling_speed(self):
        item = web._download_item_from_stat(
            -1001,
            42,
            {
                "down_byte": 1024,
                "total_size": 2048,
                "file_name": "/tmp/file.bin",
                "download_speed": 1,
                "last_progress_time": 100,
            },
            {
                "mode": "per_file",
                "files": {
                    "-1001:42:unique": {
                        "rolling_5s_bps": 5 * 1024 * 1024,
                    }
                },
            },
        )

        self.assertEqual("5.0MB/s", item["download_speed"])


if __name__ == "__main__":
    unittest.main()
