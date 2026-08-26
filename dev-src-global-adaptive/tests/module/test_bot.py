import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from module.bot import _bot, stop_download_bot


class StopDownloadBotTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_values = {
            "bot": _bot.bot,
            "is_running": _bot.is_running,
            "reply_task": _bot.reply_task,
            "monitor_task": _bot.monitor_task,
        }
        self.tasks = []

    async def asyncTearDown(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        for name, value in self.original_values.items():
            setattr(_bot, name, value)

    async def test_stop_awaits_owned_tasks_and_clears_references(self):
        events = []
        reply_started = asyncio.Event()
        monitor_started = asyncio.Event()

        async def owned_task(name, started):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                events.append(name)

        reply_task = asyncio.create_task(owned_task("reply", reply_started))
        monitor_task = asyncio.create_task(owned_task("monitor", monitor_started))
        self.tasks.extend((reply_task, monitor_task))
        await asyncio.gather(reply_started.wait(), monitor_started.wait())

        _bot.is_running = True
        _bot.reply_task = reply_task
        _bot.monitor_task = monitor_task
        _bot.bot = SimpleNamespace(
            stop=mock.AsyncMock(side_effect=lambda: events.append("bot"))
        )

        with mock.patch.object(_bot, "update_config") as update_config, mock.patch.object(
            _bot, "stop_task"
        ) as stop_task:
            await stop_download_bot()

        self.assertCountEqual(["bot", "reply", "monitor"], events)
        self.assertTrue(reply_task.done())
        self.assertTrue(monitor_task.done())
        self.assertIsNone(_bot.reply_task)
        self.assertIsNone(_bot.monitor_task)
        update_config.assert_called_once_with()
        stop_task.assert_called_once_with("all")
        _bot.bot.stop.assert_awaited_once_with()

    async def test_repeated_cancellation_defers_owned_task_cleanup(self):
        events = []
        reply_started = asyncio.Event()
        monitor_started = asyncio.Event()
        reply_cleanup_started = asyncio.Event()
        monitor_cleanup_started = asyncio.Event()
        finish_cleanup = asyncio.Event()

        async def owned_task(name, started, cleanup_started):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await finish_cleanup.wait()
                events.append(name)

        reply_task = asyncio.create_task(
            owned_task("reply", reply_started, reply_cleanup_started)
        )
        monitor_task = asyncio.create_task(
            owned_task("monitor", monitor_started, monitor_cleanup_started)
        )
        self.tasks.extend((reply_task, monitor_task))
        await asyncio.gather(reply_started.wait(), monitor_started.wait())

        _bot.is_running = True
        _bot.reply_task = reply_task
        _bot.monitor_task = monitor_task
        _bot.bot = SimpleNamespace(stop=mock.AsyncMock())

        with mock.patch.object(_bot, "update_config"), mock.patch.object(
            _bot, "stop_task"
        ):
            stopping = asyncio.create_task(stop_download_bot())
            self.tasks.append(stopping)
            await asyncio.gather(
                reply_cleanup_started.wait(),
                monitor_cleanup_started.wait(),
            )
            stopping.cancel()
            await asyncio.sleep(0)
            stopping.cancel()
            finish_cleanup.set()

            with self.assertRaises(asyncio.CancelledError):
                await stopping

        self.assertCountEqual(["reply", "monitor"], events)
        self.assertTrue(reply_task.done())
        self.assertTrue(monitor_task.done())
        self.assertIsNone(_bot.reply_task)
        self.assertIsNone(_bot.monitor_task)

    async def test_pre_cleanup_failures_do_not_skip_owned_task_cleanup(self):
        for failing_step in ("update_config", "stop_task"):
            with self.subTest(failing_step=failing_step):
                events = []
                reply_started = asyncio.Event()
                monitor_started = asyncio.Event()

                async def owned_task(name, started):
                    started.set()
                    try:
                        await asyncio.Event().wait()
                    finally:
                        await asyncio.sleep(0)
                        events.append(name)

                reply_task = asyncio.create_task(owned_task("reply", reply_started))
                monitor_task = asyncio.create_task(
                    owned_task("monitor", monitor_started)
                )
                self.tasks.extend((reply_task, monitor_task))
                await asyncio.gather(reply_started.wait(), monitor_started.wait())

                _bot.is_running = True
                _bot.reply_task = reply_task
                _bot.monitor_task = monitor_task
                _bot.bot = SimpleNamespace(
                    stop=mock.AsyncMock(side_effect=lambda: events.append("bot"))
                )
                failure = RuntimeError(f"{failing_step} failed")
                update_config = mock.Mock(
                    side_effect=failure if failing_step == "update_config" else None
                )
                stop_task = mock.Mock(
                    side_effect=failure if failing_step == "stop_task" else None
                )

                with mock.patch.object(
                    _bot, "update_config", update_config
                ), mock.patch.object(_bot, "stop_task", stop_task):
                    with self.assertRaises(RuntimeError) as raised:
                        await stop_download_bot()

                self.assertIs(failure, raised.exception)
                self.assertCountEqual(["bot", "reply", "monitor"], events)
                self.assertTrue(reply_task.done())
                self.assertTrue(monitor_task.done())
                self.assertIsNone(_bot.reply_task)
                self.assertIsNone(_bot.monitor_task)
                update_config.assert_called_once_with()
                stop_task.assert_called_once_with("all")
                _bot.bot.stop.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
