"""Unittest module for media downloader."""
import asyncio
import os
import platform
import queue
import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from typing import List, Union
from unittest import mock

import pyrogram
from pyrogram.file_id import FileId, FileType
import media_downloader as downloader_module

from media_downloader import (
    _can_download,
    _check_config,
    _download_to_temp,
    _get_media_meta,
    _is_exist,
    _should_use_global_pool,
    _should_use_parallel_download,
    _shutdown_runtime,
    app,
    download_all_chat,
    download_media,
    download_task,
    main,
    save_msg_to_file,
    worker,
)
from module.app import Application, DownloadStatus, TaskNode
from module.cloud_drive import CloudDriveConfig
from module.parallel_downloader import RemoteHashUnavailable
from module.pyrogram_extension import (
    get_extension,
    record_download_status,
    reset_download_cache,
)

from .test_common import (
    Chat,
    Date,
    MockAudio,
    MockDocument,
    MockMessage,
    MockPhoto,
    MockVideo,
    MockVideoNote,
    MockVoice,
    get_extension,
    platform_generic_path,
)

MOCK_DIR: str = "/root/project"
if platform.system() == "Windows":
    MOCK_DIR = "\\root\\project"
MOCK_CONF = {
    "api_id": 123,
    "api_hash": "hasw5Tgawsuj67",
    "chat": [{"chat_id": 8654123, "last_read_message_id": 0, "ids_to_retry": [1, 2]}],
    "media_types": ["audio", "voice", "document", "photo", "video", "video_note"],
    "file_formats": {"audio": ["all"], "voice": ["all"], "video": ["all"]},
    "save_path": MOCK_DIR,
    "file_name_prefix": ["message_id", "caption", "file_name"],
}

event_str = "asyncio.AbstractEventLoop.run_forever"
if sys.version_info > (3, 8):
    event_str = "asyncio.ProactorEventLoop.run_forever"


def os_remove(_: str):
    pass


def is_exist(file: str):
    if os.path.basename(file).find("313 - sucess_exist_down.mp4") != -1:
        return True
    elif os.path.basename(file).find("422 - exception.mov") != -1:
        raise Exception
    return False


def os_get_file_size(file: str) -> int:
    if os.path.basename(file).find("311 - failed_down.mp4") != -1:
        return 0
    elif os.path.basename(file).find("312 - sucess_down.mp4") != -1:
        return 1024
    elif os.path.basename(file).find("313 - sucess_exist_down.mp4") != -1:
        return 1024
    return 0


def rest_app(conf: dict):
    config_test = os.path.join(os.path.abspath("."), "config_test.yaml")
    data_test = os.path.join(os.path.abspath("."), "data_test.yaml")
    if os.path.exists(config_test):
        os.remove(config_test)
    if os.path.exists(data_test):
        os.remove(data_test)
    app.total_download_task = 0
    app.is_running = True
    app.chat_download_config: dict = {}
    # app.already_download_ids_set = set()
    app.save_path = os.path.abspath(".")
    app.api_id: str = ""
    app.api_hash: str = ""
    app.media_types: List[str] = []
    app.file_formats: dict = {}
    app.proxy: dict = {}
    app.restart_program = False
    app.config: dict = {}
    app.app_data: dict = {}
    app.file_path_prefix: List[str] = ["chat_title", "media_datetime"]
    app.file_name_prefix: List[str] = ["message_id", "file_name"]
    app.file_name_prefix_split: str = " - "
    app.log_file_path = os.path.join(os.path.abspath("."), "log")
    app.cloud_drive_config = CloudDriveConfig()
    app.hide_file_name = False
    app.caption_name_dict: dict = {}
    app.max_concurrent_transmissions: int = 1
    app.web_host: str = "localhost"
    app.web_port: int = 5000
    app.config_file = "config_test.yaml"
    app.app_data_file = "data_test.yaml"
    app.config = conf
    app.assign_config(conf)
    app.assign_app_data(conf)


def mock_manage_duplicate_file(file_path: str) -> str:
    return file_path


def raise_keyboard_interrupt():
    raise KeyboardInterrupt


async def new_upload_telegram_chat(
    client: pyrogram.Client,
    upload_user: pyrogram.Client,
    app: Application,
    node: TaskNode,
    message: pyrogram.types.Message,
    download_status: DownloadStatus,
    file_name: str = None,
):
    pass


def raise_exception():
    raise Exception


def load_config():
    raise ValueError("error load config")


class MyQueue:
    def __init__(self, queue_list_obj):
        self._queue = queue.Queue()
        for item in queue_list_obj:
            self._queue.put(item)

    async def get(self):
        if self._queue.empty():
            raise Exception
        return self._queue.get()


class MockEventLoop:
    def __init__(self):
        pass

    def run_until_complete(self, *args, **kwargs):
        return {"api_id": 1, "api_hash": "asdf", "ids_to_retry": [1, 2, 3]}


class MockAsync:
    def __init__(self):
        pass

    def get_event_loop(self):
        return MockEventLoop()


async def async_get_media_meta(chat_id, message, message_media, _type):
    result = await _get_media_meta(chat_id, message, message_media, _type)
    return result


async def async_download_media(
    client, message, media_types, file_formats, chat_id=-123
):
    node = TaskNode(chat_id=chat_id)
    return await download_media(client, message, media_types, file_formats, node)


def mock_move_to_download_path(temp_download_path: str, download_path: str):
    pass


def mock_check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    pass


async def new_fetch_message(client: pyrogram.Client, message: pyrogram.types.Message):
    return message


async def get_chat_history(client, *args, **kwargs):
    items = [
        MockMessage(
            id=1213,
            media=True,
            voice=MockVoice(
                mime_type="audio/ogg",
                date=datetime(2019, 7, 25, 14, 53, 50),
            ),
        ),
        MockMessage(
            id=1214,
            media=False,
            text="test message 1",
        ),
        MockMessage(
            id=1215,
            media=False,
            text="test message 2",
        ),
        MockMessage(
            id=1216,
            media=False,
            text="test message 3",
        ),
    ]
    for item in items:
        yield item


class MockClient:
    def __init__(self, *args, **kwargs):
        pass

    def __aiter__(self):
        return self

    async def start(self):
        pass

    async def stop(self):
        pass

    async def get_messages(self, *args, **kwargs):
        if kwargs["message_ids"] == 7:
            return MockMessage(
                id=7,
                media=True,
                chat_id=123456,
                chat_title="123456",
                date=datetime.now(),
                video=MockVideo(
                    file_name="sample_video.mov",
                    mime_type="video/mov",
                ),
            )
        elif kwargs["message_ids"] == 8:
            return MockMessage(
                id=8,
                media=True,
                chat_id=234567,
                chat_title="234567",
                date=datetime.now(),
                video=MockVideo(
                    file_name="sample_video.mov",
                    mime_type="video/mov",
                ),
            )
        elif kwargs["message_ids"] == [1, 2]:
            return [
                MockMessage(
                    id=1,
                    media=True,
                    chat_id=234568,
                    chat_title="234568",
                    date=datetime.now(),
                    video=MockVideo(
                        file_name="sample_video.mov",
                        mime_type="video/mov",
                    ),
                ),
                MockMessage(
                    id=2,
                    media=True,
                    chat_id=234568,
                    chat_title="234568",
                    date=datetime.now(),
                    video=MockVideo(
                        file_name="sample_video2.mov",
                        mime_type="video/mov",
                    ),
                ),
            ]
        elif kwargs["message_ids"] == 313:
            return MockMessage(
                id=313,
                media=True,
                video=MockVideo(
                    file_name="sucess_exist_down.mp4",
                    mime_type="video/mp4",
                    file_size=1024,
                ),
            )
        elif kwargs["message_ids"] == 312:
            return MockMessage(
                id=312,
                media=True,
                video=MockVideo(
                    file_name="sucess_down.mp4",
                    mime_type="video/mp4",
                    file_size=1024,
                ),
            )
        elif kwargs["message_ids"] == 311:
            return MockMessage(
                id=311,
                media=True,
                video=MockVideo(
                    file_name="failed_down.mp4",
                    mime_type="video/mp4",
                    file_size=1024,
                ),
            )
        return []

    async def download_media(self, *args, **kwargs):
        mock_message = args[0]
        if mock_message.id in [7, 8]:
            raise pyrogram.errors.exceptions.bad_request_400.BadRequest
        elif mock_message.id == 9:
            raise pyrogram.errors.exceptions.unauthorized_401.Unauthorized
        elif mock_message.id == 11:
            raise TypeError
        elif mock_message.id == 420:
            raise pyrogram.errors.exceptions.flood_420.FloodWait(value=420)
        elif mock_message.id == 421:
            raise Exception
        return kwargs["file_name"]

    async def edit_message_text(self, *args, **kwargs):
        return True


def check_for_updates(_: dict = None):
    pass


class DownloadRoutingTest(unittest.IsolatedAsyncioTestCase):
    """Parallel transfer is an explicit, reversible canary path."""

    def setUp(self):
        self.original_enabled = app.parallel_download_enabled
        self.original_workers = app.parallel_download_workers
        self.original_min_size = app.parallel_download_min_size
        self.original_pool_values = {
            "parallel_session_pool_enabled": app.parallel_session_pool_enabled,
            "parallel_pool_file_threshold": app.parallel_pool_file_threshold,
            "parallel_pool_stripe_size": app.parallel_pool_stripe_size,
            "media_session_pool": app.media_session_pool,
        }
        app.parallel_session_pool_enabled = False
        decoded = FileId(
            file_type=FileType.DOCUMENT,
            dc_id=4,
            media_id=12345,
            access_hash=67890,
            file_reference=b"reference",
        )
        self.media = SimpleNamespace(
            file_id=decoded.encode(),
            file_unique_id="route-test",
            file_size=2048,
        )
        self.message = SimpleNamespace(
            id=42,
            chat=SimpleNamespace(id=-100123),
        )
        self.client = mock.Mock()
        self.client.download_media = mock.AsyncMock(return_value="/tmp/sequential")

    def tearDown(self):
        app.parallel_download_enabled = self.original_enabled
        app.parallel_download_workers = self.original_workers
        app.parallel_download_min_size = self.original_min_size
        for name, value in self.original_pool_values.items():
            setattr(app, name, value)

    def test_global_pool_threshold_is_strictly_greater_than_five_mib(self):
        app.parallel_session_pool_enabled = True
        app.parallel_pool_file_threshold = 5 * 1024 * 1024
        app.media_session_pool = mock.Mock()

        self.assertFalse(_should_use_global_pool(5 * 1024 * 1024))
        self.assertTrue(_should_use_global_pool(5 * 1024 * 1024 + 1))

    def test_global_pool_requires_enabled_runtime_pool(self):
        app.parallel_pool_file_threshold = 1024
        app.media_session_pool = mock.Mock()
        self.assertFalse(_should_use_global_pool(2048))

        app.parallel_session_pool_enabled = True
        app.media_session_pool = None
        self.assertFalse(_should_use_global_pool(2048))

    def test_size_gate_requires_enabled_flag_and_threshold(self):
        app.parallel_download_enabled = False
        app.parallel_download_min_size = 1024
        self.assertFalse(_should_use_parallel_download(2048))

        app.parallel_download_enabled = True
        self.assertFalse(_should_use_parallel_download(1023))
        self.assertTrue(_should_use_parallel_download(1024))

    async def test_disabled_flag_uses_existing_kurigram_download(self):
        app.parallel_download_enabled = False

        result = await _download_to_temp(
            self.client,
            self.message,
            self.media,
            "/tmp/candidate",
            -100123,
            (),
        )

        self.assertEqual("/tmp/sequential", result)
        self.client.download_media.assert_awaited_once()

    @mock.patch("media_downloader.ParallelDownloader")
    @mock.patch("media_downloader.KurigramRangeSource")
    async def test_large_file_uses_process_pool(
        self,
        source_factory,
        downloader_factory,
    ):
        app.parallel_session_pool_enabled = True
        app.parallel_pool_file_threshold = 1024
        app.parallel_pool_stripe_size = 5 * 1024 * 1024
        app.media_session_pool = mock.Mock()
        self.media.file_size = 2048
        downloader_factory.return_value.download = mock.AsyncMock(
            return_value=SimpleNamespace(
                path="/tmp/global.parallel.part",
                workers=2,
                retries=0,
                sha256="pool-sha",
            )
        )

        result = await _download_to_temp(
            self.client, self.message, self.media, "/tmp/candidate", -100123, ()
        )

        self.assertEqual("/tmp/global.parallel.part", result)
        kwargs = downloader_factory.call_args.kwargs
        self.assertIs(app.media_session_pool, kwargs["pool"])
        self.assertEqual(5 * 1024 * 1024, kwargs["stripe_size"])
        self.assertEqual("-100123:42:route-test", kwargs["transfer_id"])
        self.client.download_media.assert_not_awaited()

    async def test_five_mib_file_stays_on_sequential_path(self):
        app.parallel_session_pool_enabled = True
        app.parallel_download_enabled = True
        app.parallel_download_min_size = 1
        app.parallel_pool_file_threshold = 5 * 1024 * 1024
        app.media_session_pool = mock.Mock()
        self.media.file_size = 5 * 1024 * 1024

        result = await _download_to_temp(
            self.client, self.message, self.media, "/tmp/candidate", -100123, ()
        )

        self.assertEqual("/tmp/sequential", result)
        self.client.download_media.assert_awaited_once()

    @mock.patch("media_downloader.ParallelDownloader")
    @mock.patch("media_downloader.KurigramRangeSource")
    async def test_enabled_large_file_uses_parallel_downloader(
        self,
        source_factory,
        downloader_factory,
    ):
        app.parallel_download_enabled = True
        app.parallel_download_workers = 2
        app.parallel_download_min_size = 1024
        downloader_factory.return_value.download = mock.AsyncMock(
            return_value=SimpleNamespace(
                path="/tmp/candidate.parallel.part",
                workers=2,
                retries=0,
                sha256="abc123",
            )
        )

        result = await _download_to_temp(
            self.client,
            self.message,
            self.media,
            "/tmp/candidate",
            -100123,
            (),
        )

        self.assertEqual("/tmp/candidate.parallel.part", result)
        source_factory.assert_called_once()
        downloader_factory.assert_called_once_with(
            source_factory.return_value,
            workers=2,
        )
        downloader_factory.return_value.download.assert_awaited_once()
        self.client.download_media.assert_not_awaited()

    @mock.patch("media_downloader.ParallelDownloader")
    @mock.patch("media_downloader.KurigramRangeSource")
    async def test_parallel_failure_falls_back_to_existing_path(
        self,
        _source_factory,
        downloader_factory,
    ):
        app.parallel_download_enabled = True
        app.parallel_download_min_size = 1
        app.media_session_pool = mock.Mock()
        downloader_factory.return_value.download = mock.AsyncMock(
            side_effect=RemoteHashUnavailable("no hashes")
        )

        result = await _download_to_temp(
            self.client,
            self.message,
            self.media,
            "/tmp/candidate",
            -100123,
            (),
        )

        self.assertEqual("/tmp/sequential", result)
        app.media_session_pool.record_fallback.assert_not_called()
        self.client.download_media.assert_awaited_once()

    @mock.patch("media_downloader.ParallelDownloader")
    @mock.patch("media_downloader.KurigramRangeSource")
    async def test_parallel_cancellation_does_not_start_fallback(
        self,
        _source_factory,
        downloader_factory,
    ):
        app.parallel_download_enabled = True
        app.parallel_download_min_size = 1
        downloader_factory.return_value.download = mock.AsyncMock(
            side_effect=asyncio.CancelledError()
        )

        with self.assertRaises(asyncio.CancelledError):
            await _download_to_temp(
                self.client,
                self.message,
                self.media,
                "/tmp/candidate",
                -100123,
                (),
            )

        self.client.download_media.assert_not_awaited()

    @mock.patch("media_downloader.ParallelDownloader")
    @mock.patch("media_downloader.KurigramRangeSource")
    async def test_global_failure_records_one_fallback_then_uses_sequential_path(
        self,
        _source_factory,
        downloader_factory,
    ):
        app.parallel_session_pool_enabled = True
        app.parallel_pool_file_threshold = 1
        app.media_session_pool = mock.Mock()
        downloader_factory.return_value.download = mock.AsyncMock(
            side_effect=RemoteHashUnavailable("no hashes")
        )

        result = await _download_to_temp(
            self.client, self.message, self.media, "/tmp/candidate", -100123, ()
        )

        self.assertEqual("/tmp/sequential", result)
        app.media_session_pool.record_fallback.assert_called_once_with()
        self.client.download_media.assert_awaited_once()

    @mock.patch("media_downloader.ParallelDownloader")
    @mock.patch("media_downloader.KurigramRangeSource")
    async def test_global_cancellation_does_not_record_or_start_fallback(
        self,
        _source_factory,
        downloader_factory,
    ):
        app.parallel_session_pool_enabled = True
        app.parallel_pool_file_threshold = 1
        app.media_session_pool = mock.Mock()
        downloader_factory.return_value.download = mock.AsyncMock(
            side_effect=asyncio.CancelledError()
        )

        with self.assertRaises(asyncio.CancelledError):
            await _download_to_temp(
                self.client,
                self.message,
                self.media,
                "/tmp/candidate",
                -100123,
                (),
            )

        app.media_session_pool.record_fallback.assert_not_called()
        self.client.download_media.assert_not_awaited()


class RuntimeShutdownTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_pool = app.media_session_pool

    def tearDown(self):
        app.media_session_pool = self.original_pool

    async def test_shutdown_cancels_downloads_then_pool_then_client(self):
        events = []
        started = asyncio.Event()

        async def blocked_task():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("task")

        task = asyncio.create_task(blocked_task())
        await started.wait()
        pool = SimpleNamespace(
            close=mock.AsyncMock(side_effect=lambda: events.append("pool"))
        )
        client = SimpleNamespace(
            stop=mock.AsyncMock(side_effect=lambda: events.append("client"))
        )
        app.media_session_pool = pool

        await _shutdown_runtime(client, [task], pool)

        self.assertEqual(["task", "pool", "client"], events)
        self.assertIsNone(app.media_session_pool)

    async def test_shutdown_finishes_pool_and_client_cleanup_when_cancelled(self):
        events = []
        close_started = asyncio.Event()
        finish_close = asyncio.Event()

        async def close_pool():
            close_started.set()
            await finish_close.wait()
            events.append("pool")

        pool = SimpleNamespace(close=mock.AsyncMock(side_effect=close_pool))
        client = SimpleNamespace(
            stop=mock.AsyncMock(side_effect=lambda: events.append("client"))
        )
        app.media_session_pool = pool

        shutdown = asyncio.create_task(_shutdown_runtime(client, [], pool))
        await close_started.wait()
        shutdown.cancel()
        await asyncio.sleep(0)
        finish_close.set()

        with self.assertRaises(asyncio.CancelledError):
            await shutdown

        self.assertEqual(["pool", "client"], events)
        self.assertIsNone(app.media_session_pool)

    async def test_shutdown_cancels_publishers_before_bot_owned_tasks(self):
        events = []
        publisher_started = asyncio.Event()
        owned_started = asyncio.Event()
        owned_cleanup_started = asyncio.Event()
        finish_owned_cleanup = asyncio.Event()
        owned = {}

        async def owned_task():
            owned_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                owned_cleanup_started.set()
                await finish_owned_cleanup.wait()
                events.append("owned")

        async def startup_task():
            publisher_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("startup")
                owned["task"] = asyncio.create_task(owned_task())
                await owned_started.wait()

        async def stop_bot():
            events.append("bot")
            task = owned["task"]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        pool = SimpleNamespace(
            close=mock.AsyncMock(side_effect=lambda: events.append("pool"))
        )
        client = SimpleNamespace(
            stop=mock.AsyncMock(side_effect=lambda: events.append("client"))
        )
        app.media_session_pool = pool
        publisher = asyncio.create_task(startup_task())
        await publisher_started.wait()

        try:
            shutdown = asyncio.create_task(
                _shutdown_runtime(client, [publisher], pool, stop_bot)
            )
        except BaseException:
            publisher.cancel()
            await asyncio.gather(publisher, return_exceptions=True)
            if "task" in owned:
                owned["task"].cancel()
                finish_owned_cleanup.set()
                await asyncio.gather(owned["task"], return_exceptions=True)
            raise

        await owned_cleanup_started.wait()
        shutdown.cancel()
        await asyncio.sleep(0)
        shutdown.cancel()
        finish_owned_cleanup.set()

        with self.assertRaises(asyncio.CancelledError):
            await shutdown

        self.assertEqual(["startup", "bot", "owned", "pool", "client"], events)
        self.assertTrue(publisher.done())
        self.assertTrue(owned["task"].done())
        pool.close.assert_awaited_once_with()
        client.stop.assert_awaited_once_with()
        self.assertIsNone(app.media_session_pool)

    async def test_shutdown_preserves_bot_failure_after_pool_and_client_cleanup(self):
        events = []
        failure = RuntimeError("bot config failed")

        async def stop_bot():
            events.append("bot")
            raise failure

        pool = SimpleNamespace(
            close=mock.AsyncMock(side_effect=lambda: events.append("pool"))
        )
        client = SimpleNamespace(
            stop=mock.AsyncMock(side_effect=lambda: events.append("client"))
        )
        app.media_session_pool = pool

        with self.assertRaises(RuntimeError) as raised:
            await _shutdown_runtime(client, [], pool, stop_bot)

        self.assertIs(failure, raised.exception)
        self.assertEqual(["bot", "pool", "client"], events)
        pool.close.assert_awaited_once_with()
        client.stop.assert_awaited_once_with()
        self.assertIsNone(app.media_session_pool)


class RuntimeMainTest(unittest.TestCase):
    def setUp(self):
        self.original_values = {
            "parallel_session_pool_enabled": app.parallel_session_pool_enabled,
            "parallel_pool_soft_sessions": app.parallel_pool_soft_sessions,
            "parallel_pool_max_sessions": app.parallel_pool_max_sessions,
            "parallel_pool_pipeline_depth": app.parallel_pool_pipeline_depth,
            "parallel_pool_idle_ttl": app.parallel_pool_idle_ttl,
            "parallel_pool_control_interval": app.parallel_pool_control_interval,
            "media_session_pool": app.media_session_pool,
            "max_download_task": app.max_download_task,
            "bot_token": app.bot_token,
        }
        rest_app(MOCK_CONF)
        app.parallel_session_pool_enabled = False
        app.media_session_pool = None
        app.max_download_task = 0
        app.bot_token = ""

    def tearDown(self):
        for name, value in self.original_values.items():
            setattr(app, name, value)

    def _run_main(self, client, **patches):
        defaults = {
            "HookClient": mock.Mock(return_value=client),
            "init_web": mock.Mock(),
            "set_max_concurrent_transmissions": mock.Mock(),
            "_exec_loop": mock.Mock(),
            "logger": mock.Mock(),
        }
        defaults.update(patches)
        with mock.patch.multiple("media_downloader", **defaults), mock.patch.object(
            app, "pre_run"
        ), mock.patch.object(app, "update_config"):
            main()

    def test_main_starts_configured_pool_after_client_before_workers(self):
        events = []
        client = SimpleNamespace(
            start=mock.AsyncMock(side_effect=lambda: events.append("client-start")),
            stop=mock.AsyncMock(side_effect=lambda: events.append("client-stop")),
        )
        pool = SimpleNamespace(
            start=mock.Mock(side_effect=lambda: events.append("pool-start")),
            close=mock.AsyncMock(side_effect=lambda: events.append("pool-close")),
        )
        pool_factory = mock.Mock(
            side_effect=lambda factory, config: (
                events.append("pool-create") or pool
            )
        )

        def make_worker(_client):
            events.append("worker")

            async def idle_worker():
                await asyncio.Event().wait()

            return idle_worker()

        app.parallel_session_pool_enabled = True
        app.parallel_pool_soft_sessions = 12
        app.parallel_pool_max_sessions = 36
        app.parallel_pool_pipeline_depth = 2
        app.parallel_pool_idle_ttl = 900
        app.parallel_pool_control_interval = 45
        app.max_download_task = 1

        self._run_main(
            client,
            GlobalMediaSessionPool=pool_factory,
            KurigramMediaSessionFactory=mock.Mock(return_value="session-factory"),
            worker=mock.Mock(side_effect=make_worker),
        )

        self.assertEqual(
            [
                "client-start",
                "pool-create",
                "pool-start",
                "worker",
                "pool-close",
                "client-stop",
            ],
            events,
        )
        config = pool_factory.call_args.args[1]
        self.assertEqual(12, config.soft_sessions)
        self.assertEqual(36, config.max_sessions)
        self.assertEqual(2, config.pipeline_depth)
        self.assertEqual(900, config.idle_ttl)
        self.assertEqual(45, config.control_interval)
        self.assertIsNone(app.media_session_pool)

    def test_main_starts_pool_on_application_loop(self):
        running_loops = []
        client = SimpleNamespace(start=mock.AsyncMock(), stop=mock.AsyncMock())
        pool = SimpleNamespace(
            start=mock.Mock(
                side_effect=lambda: running_loops.append(asyncio.get_running_loop())
            ),
            close=mock.AsyncMock(),
        )
        app.parallel_session_pool_enabled = True

        self._run_main(
            client,
            GlobalMediaSessionPool=mock.Mock(return_value=pool),
            KurigramMediaSessionFactory=mock.Mock(return_value="session-factory"),
        )

        self.assertEqual([app.loop], running_loops)

    def test_main_does_not_construct_pool_when_feature_disabled(self):
        client = SimpleNamespace(start=mock.AsyncMock(), stop=mock.AsyncMock())
        pool_factory = mock.Mock()
        app.parallel_session_pool_enabled = False

        self._run_main(client, GlobalMediaSessionPool=pool_factory)

        pool_factory.assert_not_called()
        client.start.assert_awaited_once_with()
        client.stop.assert_awaited_once_with()
        self.assertIsNone(app.media_session_pool)

    def test_pool_start_failure_still_closes_pool_before_client(self):
        events = []
        client = SimpleNamespace(
            start=mock.AsyncMock(side_effect=lambda: events.append("client-start")),
            stop=mock.AsyncMock(side_effect=lambda: events.append("client-stop")),
        )

        def fail_start():
            events.append("pool-start")
            raise RuntimeError("pool start failed")

        pool = SimpleNamespace(
            start=mock.Mock(side_effect=fail_start),
            close=mock.AsyncMock(side_effect=lambda: events.append("pool-close")),
        )
        app.parallel_session_pool_enabled = True

        self._run_main(
            client,
            GlobalMediaSessionPool=mock.Mock(return_value=pool),
            KurigramMediaSessionFactory=mock.Mock(return_value="session-factory"),
        )

        self.assertEqual(
            ["client-start", "pool-start", "pool-close", "client-stop"],
            events,
        )
        self.assertIsNone(app.media_session_pool)

    def test_pool_start_failure_cleans_partial_start_before_client_shutdown(self):
        events = []
        pool_worker = None

        async def partial_pool_worker():
            events.append("pool-worker-start")
            try:
                await asyncio.Event().wait()
            finally:
                events.append("pool-worker-finished")

        def fail_after_partial_start():
            nonlocal pool_worker
            loop = asyncio.get_running_loop()
            events.append("pool-start")
            pool_worker = loop.create_task(partial_pool_worker())
            raise RuntimeError("pool start failed")

        async def close_pool():
            events.append("pool-close")
            await asyncio.sleep(0)
            if pool_worker is not None:
                pool_worker.cancel()
                await asyncio.gather(pool_worker, return_exceptions=True)

        async def stop_client():
            self.assertIsNone(app.media_session_pool)
            events.append("client-stop")

        client = SimpleNamespace(
            start=mock.AsyncMock(side_effect=lambda: events.append("client-start")),
            stop=mock.AsyncMock(side_effect=stop_client),
        )
        pool = SimpleNamespace(
            start=mock.Mock(side_effect=fail_after_partial_start),
            close=mock.AsyncMock(side_effect=close_pool),
        )
        worker_factory = mock.Mock()
        app.parallel_session_pool_enabled = True
        app.max_download_task = 1

        self._run_main(
            client,
            GlobalMediaSessionPool=mock.Mock(return_value=pool),
            KurigramMediaSessionFactory=mock.Mock(return_value="session-factory"),
            worker=worker_factory,
        )

        self.assertEqual(
            [
                "client-start",
                "pool-start",
                "pool-close",
                "pool-worker-start",
                "pool-worker-finished",
                "client-stop",
            ],
            events,
        )
        worker_factory.assert_not_called()
        self.assertIsNotNone(pool_worker)
        self.assertTrue(pool_worker.done())
        self.assertIsNone(app.media_session_pool)


@mock.patch("media_downloader.get_extension", new=get_extension)
@mock.patch("module.pyrogram_extension.get_extension", new=get_extension)
@mock.patch("media_downloader.fetch_message", new=new_fetch_message)
@mock.patch("media_downloader.get_chat_history_v2", new=get_chat_history)
@mock.patch("media_downloader.RETRY_TIME_OUT", new=0)
@mock.patch(
    "media_downloader.check_for_updates", new=check_for_updates, create=True
)
class MediaDownloaderTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.loop = asyncio.get_event_loop()
        except RuntimeError:
            cls.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(cls.loop)
        rest_app(MOCK_CONF)

    # @mock.patch("media_downloader.app.save_path", new=MOCK_DIR)
    def test_get_media_meta(self):
        rest_app(MOCK_CONF)
        app.save_path = MOCK_DIR
        # Test Voice notes
        message = MockMessage(
            id=1,
            media=True,
            chat_title="test1",
            date=datetime(2019, 7, 25, 14, 53, 50),
            voice=MockVoice(
                mime_type="audio/ogg",
                date=datetime(2019, 7, 25, 14, 53, 50),
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.voice, "voice")
        )

        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test1/2019_07/1 - voice_2019-07-25T14_53_50.ogg"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path, "test1/1 - voice_2019-07-25T14_53_50.ogg"
                    )
                ),
                "ogg",
            ),
            result,
        )

        # Test photos
        message = MockMessage(
            id=2,
            media=True,
            date=datetime(2019, 8, 5, 14, 35, 12),
            chat_title="test2",
            photo=MockPhoto(
                date=datetime(2019, 8, 5, 14, 35, 12), file_unique_id="ADAVKJYIFV"
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.photo, "photo")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/2019_08/2 - ADAVKJYIFV.jpg"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/2 - ADAVKJYIFV.jpg")
                ),
                None,
            ),
            result,
        )

        message = MockMessage(
            id=2,
            media=True,
            date=datetime(2019, 8, 5, 14, 35, 12),
            chat_title="test2",
            media_group_id="AAA213213",
            caption="#home #book",
            photo=MockPhoto(
                date=datetime(2019, 8, 5, 14, 35, 12), file_unique_id="ADAVKJYIFV"
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.photo, "photo")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/2019_08/2 - #home #book - ADAVKJYIFV.jpg"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path, "test2/2 - #home #book - ADAVKJYIFV.jpg"
                    )
                ),
                None,
            ),
            result,
        )

        # Test Documents
        message = MockMessage(
            id=3,
            media=True,
            chat_title="test2",
            document=MockDocument(
                file_name="sample_document.pdf",
                mime_type="application/pdf",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.document, "document")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/0/3 - sample_document.pdf"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/3 - sample_document.pdf")
                ),
                "pdf",
            ),
            result,
        )

        before_file_name_prefix_split = app.file_name_prefix_split
        app.file_name_prefix_split = "-"

        message = MockMessage(
            id=3,
            media=True,
            chat_title="test2",
            media_group_id="BBB213213",
            caption="#work",
            document=MockDocument(
                file_name="sample_document.pdf",
                mime_type="application/pdf",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.document, "document")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/0/3-#work-sample_document.pdf"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path, "test2/3-#work-sample_document.pdf"
                    )
                ),
                "pdf",
            ),
            result,
        )

        app.file_name_prefix_split = before_file_name_prefix_split
        # Test audio
        message = MockMessage(
            id=4,
            media=True,
            date=datetime(2021, 8, 5, 14, 35, 12),
            chat_title="test2",
            audio=MockAudio(
                file_name="sample_audio.mp3",
                mime_type="audio/mp3",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.audio, "audio")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/2021_08/4 - sample_audio.mp3"
                ),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/4 - sample_audio.mp3")
                ),
                "mp3",
            ),
            result,
        )

        # Test Video 1
        message = MockMessage(
            id=5,
            media=True,
            date=datetime(2022, 8, 5, 14, 35, 12),
            chat_title="test2",
            video=MockVideo(
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video, "video")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/2022_08/5.mp4"),
                platform_generic_path(os.path.join(app.temp_save_path, "test2/5.mp4")),
                "mp4",
            ),
            result,
        )

        # Test Video 2
        message = MockMessage(
            id=5,
            media=True,
            date=datetime(2022, 8, 5, 14, 35, 12),
            chat_title="test2",
            video=MockVideo(
                file_name="test.mp4",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video, "video")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/2022_08/5 - test.mp4"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/5 - test.mp4")
                ),
                "mp4",
            ),
            result,
        )

        # Test Video 3: not exist chat_title
        message = MockMessage(
            id=5,
            media=True,
            dis_chat=True,
            date=datetime(2022, 8, 5, 14, 35, 12),
            video=MockVideo(
                file_name="test.mp4",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video, "video")
        )

        self.assertEqual(
            (
                platform_generic_path("/root/project/-123/2022_08/5 - test.mp4"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "-123/5 - test.mp4")
                ),
                "mp4",
            ),
            result,
        )

        # Test VideoNote
        message = MockMessage(
            id=6,
            media=True,
            date=datetime(2019, 7, 25, 14, 53, 50),
            chat_title="test2",
            video_note=MockVideoNote(
                mime_type="video/mp4",
                date=datetime(2019, 7, 25, 14, 53, 50),
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video_note, "video_note")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/2019_07/6 - video_note_2019-07-25T14_53_50.mp4"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path,
                        "test2/6 - video_note_2019-07-25T14_53_50.mp4",
                    )
                ),
                "mp4",
            ),
            result,
        )

    @mock.patch("media_downloader.app.save_path", new=MOCK_DIR)
    @mock.patch("media_downloader.asyncio.sleep", return_value=None)
    @mock.patch("media_downloader.logger")
    @mock.patch("media_downloader._is_exist", new=is_exist)
    @mock.patch(
        "media_downloader._move_to_download_path", new=mock_move_to_download_path
    )
    @mock.patch(
        "media_downloader._check_download_finish", new=mock_check_download_finish
    )
    def test_download_media(self, mock_logger, patch_sleep):
        reset_download_cache()
        rest_app(MOCK_CONF)
        client = MockClient()
        app.hide_file_name = True
        message = MockMessage(
            id=5,
            media=True,
            video=MockVideo(
                file_name="sample_video.mp4",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}, -123
            )
        )
        self.assertEqual(
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/5 - sample_video.mp4"),
            ),
            result,
        )

        message = MockMessage(
            id=6,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual(
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/6 - sample_video.mov"),
            ),
            result,
        )

        # Test re-fetch message success
        message = MockMessage(
            id=7,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.warning.assert_called_with(
            "Message[7]: file reference expired, refetching..."
        )

        # Test re-fetch message failure
        message = MockMessage(
            id=8,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.error.assert_called_with(
            "Message[8]: file reference expired for 3 retries, download skipped."
        )

        # Test other exception
        message = MockMessage(
            id=9,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)

        # Check no media
        message = MockMessage(
            id=10,
            media=None,
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.SkipDownload, None), result)

        # Test timeout
        message = MockMessage(
            id=11,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.error.assert_called_with(
            "Message[11]: Timing out after 3 reties, download skipped."
        )

        # Test file name with out suffix
        message = MockMessage(
            id=12,
            media=True,
            video=MockVideo(
                file_name="sample_video",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual(
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/12 - sample_video.mp4"),
            ),
            result,
        )

        # Test FloodWait 420
        message = MockMessage(
            id=420,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.warning.assert_called_with("Message[{}]: FlowWait {}", 420, 420)

        # Test other Exception
        message = MockMessage(
            id=421,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)

        # Test other Exception
        message = MockMessage(
            id=422,
            media=True,
            video=MockVideo(
                file_name="422 - exception.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)

    @mock.patch("media_downloader.HookClient", new=MockClient)
    @mock.patch("media_downloader.queue.put", new_callable=mock.AsyncMock)
    @mock.patch("media_downloader.app.record_scanned_message")
    @mock.patch("media_downloader.app.get_retry_message_ids", return_value=[])
    def test_configured_scan_writes_database_without_memory_queue(
        self, _mock_retry_ids, mock_record_scanned, mock_queue_put
    ):
        rest_app(MOCK_CONF)
        client = MockClient()
        app.chat_download_config[8654123].download_filter = "id != 1213"

        self.loop.run_until_complete(download_all_chat(client))

        self.assertEqual(
            [1213, 1214, 1215, 1216],
            [call.args[1] for call in mock_record_scanned.call_args_list],
        )
        mock_queue_put.assert_not_awaited()

    @mock.patch("media_downloader.download_task", new_callable=mock.AsyncMock)
    def test_database_consumer_fetches_and_processes_claimed_message(
        self, mock_download_task
    ):
        rest_app(MOCK_CONF)
        client = MockClient()
        message = MockMessage(
            id=7,
            media=True,
            chat_id=8654123,
            chat_title="test",
            date=datetime.now(),
            video=MockVideo(file_name="sample.mp4", mime_type="video/mp4"),
        )
        client.get_messages = mock.AsyncMock(return_value=message)

        result = self.loop.run_until_complete(
            downloader_module.consume_database_task(
                client, {"chat_id": "8654123", "message_id": 7}
            )
        )

        self.assertTrue(result)
        client.get_messages.assert_awaited_once_with(chat_id=8654123, message_ids=7)
        mock_download_task.assert_awaited_once()

    def test_can_download(self):
        file_formats = {
            "audio": ["mp3"],
            "video": ["mp4"],
            "document": ["all"],
        }
        result = _can_download("audio", file_formats, "mp3")
        self.assertEqual(result, True)

        result1 = _can_download("audio", file_formats, "ogg")
        self.assertEqual(result1, False)

        result2 = _can_download("document", file_formats, "pdf")
        self.assertEqual(result2, True)

        result3 = _can_download("document", file_formats, "epub")
        self.assertEqual(result3, True)

    def test_configured_types_include_every_downloadable_attachment(self):
        rest_app(MOCK_CONF)

        self.assertTrue(
            {
                "audio",
                "document",
                "photo",
                "video",
                "voice",
                "video_note",
                "animation",
                "sticker",
            }.issubset(set(app.media_types))
        )

    def test_unhandled_pyrogram_connection_failure_exits_for_docker_restart(self):
        rest_app(MOCK_CONF)
        loop = mock.Mock()
        context = {
            "exception": RuntimeError(
                "read() called while another coroutine is already waiting "
                "for incoming data"
            )
        }

        with mock.patch.object(downloader_module.os, "_exit") as exit_process:
            downloader_module._handle_asyncio_exception(loop, context)

        self.assertFalse(app.is_running)
        self.assertTrue(app.restart_program)
        exit_process.assert_called_once_with(1)
        loop.default_exception_handler.assert_not_called()

    def test_is_exist(self):
        this_dir = os.path.dirname(os.path.abspath(__file__))
        result = _is_exist(os.path.join(this_dir, "__init__.py"))
        self.assertEqual(result, True)

        result1 = _is_exist(os.path.join(this_dir, "init.py"))
        self.assertEqual(result1, False)

        result2 = _is_exist(this_dir)
        self.assertEqual(result2, False)

    @mock.patch("media_downloader.os.makedirs")
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    def test_save_msg_to_file(self, mock_open, mock_makedirs):
        rest_app(MOCK_CONF)
        app.enable_download_txt = True
        app.temp_save_path = "/tmp"
        app.date_format = "%Y_%m"

        message = MockMessage(
            id=123,
            dis_chat=True,
            chat=Chat(chat_id=456, chat_title="Test Chat"),
            date=datetime(2023, 5, 15, 10, 30, 0),
            text="This is a test message",
        )

        expected_file_path = platform_generic_path(
            "/root/project/Test Chat/2023_05/123.txt"
        )

        result = self.loop.run_until_complete(save_msg_to_file(app, 456, message))

        self.assertEqual(result, (DownloadStatus.SuccessDownload, expected_file_path))
        mock_makedirs.assert_called_once_with(
            os.path.dirname(expected_file_path), exist_ok=True
        )
        mock_open.assert_called_once_with(expected_file_path, "w", encoding="utf-8")
        mock_open().write.assert_called_once_with("This is a test message")

    @mock.patch("media_downloader.RETRY_TIME_OUT", new=0)
    @mock.patch("media_downloader.os.path.getsize", new=os_get_file_size)
    @mock.patch("media_downloader.os.remove", new=os_remove)
    @mock.patch("media_downloader._is_exist", new=is_exist)
    @mock.patch(
        "media_downloader._move_to_download_path", new=mock_move_to_download_path
    )
    def test_issues_311(self):
        # see https://github.com/Dineshkarthik/telegram_media_downloader/issues/311
        rest_app(MOCK_CONF)

        client = MockClient()
        # 1. test `TimeOutError`
        message = MockMessage(
            id=311,
            media=True,
            video=MockVideo(
                file_name="failed_down.mp4",
                mime_type="video/mp4",
                file_size=1024,
            ),
        )

        media_size = getattr(message.video, "file_size")
        self.assertEqual(media_size, 1024)

        res = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}
            )
        )
        self.assertEqual(res, (DownloadStatus.FailedDownload, None))

        # 2. test sucess download
        rest_app(MOCK_CONF)
        message = MockMessage(
            id=312,
            media=True,
            video=MockVideo(
                file_name="sucess_down.mp4",
                mime_type="video/mp4",
                file_size=1024,
            ),
        )

        res = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}
            )
        )

        self.assertEqual(
            res,
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/312 - sucess_down.mp4"),
            ),
        )

        rest_app(MOCK_CONF)
        # 3. test already download
        message = MockMessage(
            id=313,
            media=True,
            video=MockVideo(
                file_name="sucess_exist_down.mp4",
                mime_type="video/mp4",
                file_size=1024,
            ),
        )

        res = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}
            )
        )

        self.assertEqual(res, (DownloadStatus.SkipDownload, None))

    @mock.patch("media_downloader.HookClient", new=MockClient)
    @mock.patch("media_downloader.RETRY_TIME_OUT", new=1)
    @mock.patch("media_downloader.logger")
    def test_main_with_bot(self, mock_logger):
        rest_app(MOCK_CONF)

        with mock.patch("media_downloader._exec_loop"), mock.patch(
            "media_downloader.init_web"
        ):
            main()

        mock_logger.success.assert_called_with(
            "Updated last read message_id to config file,total download 0, total upload file 0"
        )

    @mock.patch("media_downloader.app.pre_run", new=raise_keyboard_interrupt)
    @mock.patch("media_downloader.HookClient", new=MockClient)
    @mock.patch("media_downloader.RETRY_TIME_OUT", new=1)
    @mock.patch("media_downloader.logger")
    def test_keyboard_interrupt(self, mock_logger):
        rest_app(MOCK_CONF)

        main()

        mock_logger.info.assert_any_call("KeyboardInterrupt")
        mock_logger.success.assert_called_with(
            "Updated last read message_id to config file,total download 0, total upload file 0"
        )

    @mock.patch("media_downloader.app.pre_run", new=raise_exception)
    @mock.patch("media_downloader.HookClient", new=MockClient)
    @mock.patch("media_downloader.RETRY_TIME_OUT", new=1)
    @mock.patch("media_downloader.logger")
    def test_other_exception(self, mock_logger):
        rest_app(MOCK_CONF)

        main()

        mock_logger.success.assert_called_with(
            "Updated last read message_id to config file,total download 0, total upload file 0"
        )

    @mock.patch("media_downloader._load_config", new=load_config)
    @mock.patch("media_downloader.logger")
    def test_check_config(self, mock_logger):
        _check_config()
        mock_logger.exception.assert_called_with("load config error: error load config")

    def test_check_config_suc(self):
        app.update_config()
        self.assertEqual(_check_config(), True)

    # @mock.patch(
    #     "media_downloader.queue",
    #     new=MyQueue(
    #         [
    #             (
    #                 MockMessage(
    #                     id=312,
    #                     media=True,
    #                     chat_id=8654123,
    #                     chat_title="8654123",
    #                     video=MockVideo(
    #                         file_name="sucess_down.mp4",
    #                         mime_type="video/mp4",
    #                         file_size=1024,
    #                     ),
    #                 ),
    #                 TaskNode(chat_id=8654123, upload_telegram_chat_id=123456),
    #             ),
    #             (
    #                 MockMessage(
    #                     id=333,
    #                     media=True,
    #                     chat_id=8654123,
    #                     chat_title="8654123",
    #                     text="123",
    #                 ),
    #                 TaskNode(chat_id=8654123, upload_telegram_chat_id=123456),
    #             ),
    #         ]
    #     ),
    # )
    # @mock.patch("media_downloader.app.set_download_id", new=new_set_download_id)
    # @mock.patch("media_downloader.upload_telegram_chat", new=new_upload_telegram_chat)
    # @mock.patch("media_downloader.os.remove")
    # @mock.patch(
    #     "media_downloader._move_to_download_path", new=mock_move_to_download_path
    # )
    # @mock.patch("media_downloader.os.path.getsize", new=os_get_file_size)
    # def test_upload_telegram_chat(self, mock_remove):
    #     rest_app(MOCK_CONF)
    #     client = MockClient()
    #     app.chat_download_config[8654123].last_read_message_id = 0
    #     self.loop.run_until_complete(worker(client))
    #     mock_remove.assert_called_with(
    #         platform_generic_path("/root/project/8654123/0/312 - sucess_down.mp4")
    #     )

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
