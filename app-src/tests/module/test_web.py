"""Tests for web API loop bridging."""

import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from module import web


class RunOnAppLoopTestCase(unittest.TestCase):
    def setUp(self):
        self.original_web_app = web._web_app
        web._web_app = SimpleNamespace(loop=object())

    def tearDown(self):
        web._web_app = self.original_web_app

    def test_timeout_cancels_coroutine_and_returns_clear_error(self):
        future = mock.Mock()
        future.result.side_effect = TimeoutError()
        coroutine = mock.Mock()

        with mock.patch.object(
            web.asyncio, "run_coroutine_threadsafe", return_value=future
        ):
            with self.assertRaisesRegex(
                TimeoutError, "Telegram request timed out after 180 seconds"
            ):
                web._run_on_app_loop(coroutine, timeout=180)

        future.result.assert_called_once_with(timeout=180)
        future.cancel.assert_called_once_with()


class DialogPaginationTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_client = web._telegram_client
        web._telegram_client = mock.Mock()
        web._telegram_client.fetch_peers = mock.AsyncMock()
        web._telegram_client.resolve_peer = mock.AsyncMock(return_value="peer")

    async def asyncTearDown(self):
        web._telegram_client = self.original_client

    async def test_caches_page_peers_before_resolving_next_offset(self):
        result = SimpleNamespace(users=["user"], chats=["chat"])

        peer = await web._resolve_dialog_offset_peer(result, -100123)

        self.assertEqual("peer", peer)
        web._telegram_client.fetch_peers.assert_awaited_once_with(["user", "chat"])
        web._telegram_client.resolve_peer.assert_awaited_once_with(-100123)

    async def test_offset_failure_does_not_return_partial_dialog_cache(self):
        result = SimpleNamespace(users=[], chats=[])
        web._telegram_client.resolve_peer.side_effect = ValueError("missing peer")

        with self.assertRaisesRegex(
            RuntimeError, "Could not continue Telegram dialog pagination"
        ):
            await web._resolve_dialog_offset_peer(result, -100123)

    def test_offset_candidate_requires_a_real_message_date(self):
        chat = SimpleNamespace(id=-100123)
        missing_date = SimpleNamespace(id=77, date=None)
        dated_message = SimpleNamespace(
            id=77, date=datetime(2026, 7, 16, tzinfo=timezone.utc)
        )

        self.assertIsNone(web._dialog_offset_candidate(chat, missing_date))
        self.assertEqual(
            (77, 1784160000, -100123),
            web._dialog_offset_candidate(chat, dated_message),
        )


class TargetProgressRouteTestCase(unittest.TestCase):
    def setUp(self):
        self.original_web_app = web._web_app
        self.client = web.get_flask_app().test_client()
        web.get_flask_app().config["LOGIN_DISABLED"] = True

    def tearDown(self):
        web._web_app = self.original_web_app

    def test_returns_daemon_target_progress(self):
        tdl_client = mock.Mock()
        tdl_client.get_target_progress.return_value = [
            {"chat_id": "-1001", "total_files": 4, "downloaded_files": 1}
        ]
        web._web_app = SimpleNamespace(tdl_client=tdl_client)

        response = self.client.get("/api/target_progress")

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "ok": True,
                "progress": [
                    {
                        "chat_id": "-1001",
                        "total_files": 4,
                        "downloaded_files": 1,
                    }
                ],
            },
            response.get_json(),
        )
        tdl_client.get_target_progress.assert_called_once_with()

    def test_returns_503_when_daemon_progress_is_unavailable(self):
        tdl_client = mock.Mock()
        tdl_client.get_target_progress.side_effect = RuntimeError("offline")
        web._web_app = SimpleNamespace(tdl_client=tdl_client)

        response = self.client.get("/api/target_progress")

        self.assertEqual(503, response.status_code)
        self.assertEqual(False, response.get_json()["ok"])
        self.assertIn("offline", response.get_json()["error"])


class TargetProgressTemplateTestCase(unittest.TestCase):
    def test_targets_poll_and_patch_progress_without_reloading_dialogs(self):
        template = Path("module/templates/index.html").read_text(encoding="utf-8")

        self.assertIn("var TARGET_PROGRESS_POLL_INTERVAL = 10000;", template)
        self.assertIn('url: "api/target_progress"', template)
        self.assertIn("function update_target_progress_nodes()", template)
        self.assertIn('class="target-progress"', template)
        self.assertIn("target_progress_poll_int", template)
        self.assertIn('id="add_target_btn"', template)
        self.assertIn('function add_target()', template)

    def test_target_progress_styles_use_a_stable_mobile_track(self):
        stylesheet = Path("module/static/css/index.css").read_text(encoding="utf-8")

        self.assertIn(".target-progress-track", stylesheet)
        self.assertIn("height: 6px;", stylesheet)
        self.assertIn(".target-progress-detail", stylesheet)


class AddTargetRouteTestCase(unittest.TestCase):
    def setUp(self):
        self.original_web_app = web._web_app
        self.original_client = web._telegram_client
        self.client = web.get_flask_app().test_client()
        web.get_flask_app().config["LOGIN_DISABLED"] = True

    def tearDown(self):
        web._web_app = self.original_web_app
        web._telegram_client = self.original_client

    def test_target_match_keys_supports_with_and_without_prefix(self):
        keys = set(web._target_match_keys({"chat_id": "-100123456789", "username": "my_chan"}))
        self.assertIn("-100123456789", keys)
        self.assertIn("123456789", keys)
        self.assertIn("my_chan", keys)
        self.assertIn("@my_chan", keys)

    def test_dialog_match_keys_supports_with_and_without_prefix(self):
        keys = set(web._dialog_match_keys({"chat_id": -100123456789, "username": "my_chan"}))
        self.assertIn("-100123456789", keys)
        self.assertIn("123456789", keys)
        self.assertIn("my_chan", keys)
        self.assertIn("@my_chan", keys)

    def test_add_target_requires_query(self):
        web._web_app = SimpleNamespace()
        response = self.client.post("/api/add_target", json={})
        self.assertEqual(400, response.status_code)
        self.assertEqual(False, response.get_json()["ok"])

    def test_add_target_success_returns_dialog(self):
        web_app = SimpleNamespace(
            get_dialog_cache=mock.Mock(return_value={"items": []}),
            set_dialog_cache=mock.Mock(),
        )
        web._web_app = web_app

        with mock.patch.object(web, "_run_on_app_loop") as mock_run:
            mock_run.return_value = {
                "chat_id": -10012345,
                "title": "Test Group",
                "username": "test_grp",
                "type": "supergroup",
                "top_message_id": 100,
            }
            response = self.client.post("/api/add_target", json={"query": "@test_grp"})
            self.assertEqual(200, response.status_code)
            self.assertEqual(True, response.get_json()["ok"])
            self.assertEqual("Test Group", response.get_json()["dialog"]["title"])
            web_app.set_dialog_cache.assert_called_once()


class ChatContextRouteTestCase(unittest.TestCase):
    def setUp(self):
        self.original_web_app = web._web_app
        self.original_client = web._telegram_client
        self.client = web.get_flask_app().test_client()
        web.get_flask_app().config["LOGIN_DISABLED"] = True

    def tearDown(self):
        web._web_app = self.original_web_app
        web._telegram_client = self.original_client

    def test_chat_context_requires_chat_id_and_message_id(self):
        web._web_app = SimpleNamespace()
        response = self.client.get("/api/chat_context")
        self.assertEqual(400, response.status_code)
        self.assertEqual(False, response.get_json()["ok"])

    def test_chat_context_returns_stored_messages(self):
        web_app = SimpleNamespace(
            get_chat_message_context=mock.Mock(
                return_value=[
                    {
                        "chat_id": "-1001",
                        "message_id": 10,
                        "text": "Hello context",
                        "sender_name": "Alice",
                        "media_type": "text",
                        "date": 1700000010,
                    }
                ]
            )
        )
        web._web_app = web_app
        web._telegram_client = None

        response = self.client.get("/api/chat_context?chat_id=-1001&message_id=10")
        self.assertEqual(200, response.status_code)
        json_data = response.get_json()
        self.assertEqual(True, json_data["ok"])
        self.assertEqual(10, json_data["target_message_id"])
        self.assertEqual(1, len(json_data["messages"]))
        self.assertEqual("Hello context", json_data["messages"][0]["text"])


if __name__ == "__main__":
    unittest.main()
