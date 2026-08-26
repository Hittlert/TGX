import unittest
from dataclasses import dataclass
from types import SimpleNamespace

from module.app import Application
from module import web


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

        payload = web.get_flask_app().test_client().get(
            "/get_download_status"
        ).get_json()

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
    def test_adapter_serializes_snapshot_for_the_status_thread(self):
        snapshot = _PoolSnapshot(24, 22, 31, 48, 2, "goodput_growth")
        app = SimpleNamespace(
            media_session_pool=SimpleNamespace(snapshot=lambda: snapshot),
            parallel_pool_max_sessions=48,
            parallel_pool_pipeline_depth=2,
        )

        self.assertEqual(
            {
                "enabled": True,
                "desired": 24,
                "live": 22,
                "active_slots": 31,
                "hard_limit": 48,
                "pipeline_depth": 2,
                "last_scale_reason": "goodput_growth",
            },
            Application.get_media_pool_status(app),
        )

    def test_adapter_returns_disabled_shape_without_a_pool(self):
        app = SimpleNamespace(
            media_session_pool=None,
            parallel_pool_max_sessions=48,
            parallel_pool_pipeline_depth=2,
        )

        self.assertEqual(
            {
                "enabled": False,
                "desired": 0,
                "live": 0,
                "active_slots": 0,
                "hard_limit": 48,
                "pipeline_depth": 2,
                "last_scale_reason": "disabled",
            },
            Application.get_media_pool_status(app),
        )


if __name__ == "__main__":
    unittest.main()
