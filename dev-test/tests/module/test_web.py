"""Tests for web API loop bridging."""

import unittest
from datetime import datetime, timezone
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


if __name__ == "__main__":
    unittest.main()
