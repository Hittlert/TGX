"""test app"""

import os
import sys
import unittest
from unittest import mock

import module.app
from module.app import Application, ChatDownloadConfig, DownloadStatus

sys.path.append("..")  # Adds higher directory to python modules path.


class AppTestCase(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        config_test = os.path.join(os.path.abspath("."), "config_test.yaml")
        data_test = os.path.join(os.path.abspath("."), "data_test.yaml")
        if os.path.exists(config_test):
            os.remove(config_test)
        if os.path.exists(data_test):
            os.remove(data_test)

    def test_app(self):
        app = Application("", "")
        self.assertEqual(app.save_path, os.path.join(os.path.abspath("."), "downloads"))
        self.assertEqual(app.proxy, {})
        self.assertEqual(app.restart_program, False)
        self.assertEqual(app.listen_interval, 600)

        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 13
        app.chat_download_config[123].node.download_status[
            6
        ] = DownloadStatus.Downloading
        app.chat_download_config[123].ids_to_retry.append(7)
        # download success
        app.chat_download_config[123].node.download_status[
            8
        ] = DownloadStatus.SuccessDownload
        app.chat_download_config[123].finish_task += 1
        # download success
        app.chat_download_config[123].node.download_status[
            10
        ] = DownloadStatus.SuccessDownload
        app.chat_download_config[123].finish_task += 1
        # not exist message
        app.chat_download_config[123].node.download_status[
            13
        ] = DownloadStatus.SuccessDownload
        app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 5}]

        app.update_config(False)

        self.assertEqual(
            app.chat_download_config[123].last_read_message_id,
            app.config["chat"][0]["last_read_message_id"],
        )
        self.assertEqual(
            [],
            app.app_data["chat"][0]["ids_to_retry"],
        )

    def test_parallel_download_defaults_are_conservative(self):
        app = Application("", "")

        self.assertFalse(app.parallel_download_enabled)
        self.assertEqual(2, app.parallel_download_workers)
        self.assertEqual(256 * 1024 * 1024, app.parallel_download_min_size)

    def test_assign_config_reads_parallel_canary_values(self):
        app = Application("", "")
        config = {
            "api_id": 123,
            "api_hash": "hash",
            "media_types": [],
            "file_formats": {},
            "parallel_download_enabled": True,
            "parallel_download_workers": 3,
            "parallel_download_min_size": 1024,
        }

        app.assign_config(config)

        self.assertTrue(app.parallel_download_enabled)
        self.assertEqual(3, app.parallel_download_workers)
        self.assertEqual(1024, app.parallel_download_min_size)

    def test_invalid_parallel_worker_count_falls_back_to_two(self):
        app = Application("", "")
        config = {
            "api_id": 123,
            "api_hash": "hash",
            "media_types": [],
            "file_formats": {},
            "parallel_download_workers": 99,
        }

        app.assign_config(config)

        self.assertEqual(2, app.parallel_download_workers)

    def test_update_config(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            app = Application("", "")
            app.config_file = os.path.join(tmp_dir, "config_test.yaml")
            app.app_data_file = os.path.join(tmp_dir, "data_test.yaml")
            app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 0}]
            app.filter_advertisement_list = []
            app.replace_advertisement_list = []

            app.update_config()

            self.assertTrue(os.path.exists(app.config_file))
            self.assertTrue(os.path.exists(app.app_data_file))

    def test_update_config_advances_dirty_cursor_without_finished_task(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 13
        app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 5}]

        app.update_config(False)

        self.assertEqual(13, app.config["chat"][0]["last_read_message_id"])

    def test_update_config_does_not_bump_cursor_for_old_retry(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 13
        app.chat_download_config[123].finish_task = 1
        app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 13}]

        app.update_config(False)

        self.assertEqual(13, app.config["chat"][0]["last_read_message_id"])

    def test_update_config_does_not_read_database_retry_ids(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 13
        app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 5}]

        with mock.patch.object(
            app,
            "get_retry_message_ids",
            side_effect=AssertionError("YAML config must not read the DB queue"),
        ):
            app.update_config(False)

        self.assertEqual([], app.app_data["chat"][0]["ids_to_retry"])

    def test_advance_last_read_message_id_marks_config_dirty(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        node = module.app.TaskNode(chat_id=123)

        self.assertTrue(app.advance_last_read_message_id(node, 9))
        self.assertEqual(9, app.chat_download_config[123].last_read_message_id)
        self.assertTrue(app.config_dirty)
        self.assertFalse(app.advance_last_read_message_id(node, 8))

    def test_set_download_id_does_not_mutate_legacy_retry_state(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 21507
        app.chat_download_config[123].ids_to_retry = [21504, 21505]
        app.chat_download_config[123].ids_to_retry_dict = {
            21504: True,
            21505: True,
        }
        app.last_config_update_time = 100.0
        node = module.app.TaskNode(chat_id=123)
        node.retry_message_ids = {21504, 21505}

        app.set_download_id(node, 21505, DownloadStatus.SuccessDownload)

        self.assertEqual(21507, app.chat_download_config[123].last_read_message_id)
        self.assertEqual([21504, 21505], app.chat_download_config[123].ids_to_retry)
        self.assertIn(21505, app.chat_download_config[123].ids_to_retry_dict)
        self.assertFalse(app.config_dirty)
        self.assertEqual(100.0, app.last_config_update_time)

    def test_completed_future_retry_does_not_jump_cursor(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 2145
        app.chat_download_config[123].ids_to_retry = [37509]
        app.chat_download_config[123].ids_to_retry_dict = {37509: True}
        node = module.app.TaskNode(chat_id=123)
        node.retry_message_ids = {37509}

        app.set_download_id(node, 37509, DownloadStatus.SuccessDownload)

        self.assertEqual(2145, app.chat_download_config[123].last_read_message_id)
        self.assertEqual([37509], app.chat_download_config[123].ids_to_retry)
        self.assertEqual({37509: True}, app.chat_download_config[123].ids_to_retry_dict)

    def test_in_flight_download_does_not_advance_producer_cursor(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 2145
        app.chat_download_config[123].ids_to_retry = [2158]
        app.chat_download_config[123].ids_to_retry_dict = {2158: True}
        node = module.app.TaskNode(chat_id=123)

        app.set_download_id(node, 2158, DownloadStatus.SuccessDownload)

        self.assertEqual(2145, app.chat_download_config[123].last_read_message_id)
        self.assertEqual([2158], app.chat_download_config[123].ids_to_retry)
        self.assertEqual({2158: True}, app.chat_download_config[123].ids_to_retry_dict)

    def test_failed_download_does_not_advance_cursor(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            app = Application("", "")
            app.download_record_db_path = os.path.join(tmp_dir, "records.sqlite3")
            app.chat_download_config[123] = ChatDownloadConfig()
            app.chat_download_config[123].last_read_message_id = 2145
            node = module.app.TaskNode(chat_id=123)

            app.set_download_id(node, 37509, DownloadStatus.FailedDownload)

            self.assertEqual(2145, app.chat_download_config[123].last_read_message_id)
            self.assertEqual(
                "failed",
                app.ensure_download_records().get_record(123, 37509)["status"],
            )

    def test_consumer_success_does_not_advance_producer_cursor(self):
        app = Application("", "")
        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 10
        node = module.app.TaskNode(chat_id=123)

        app.set_download_id(node, 41, DownloadStatus.SuccessDownload)

        self.assertEqual(10, app.chat_download_config[123].last_read_message_id)

    def test_producer_record_advances_sqlite_and_in_memory_cursor(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            app = Application("", "")
            app.download_record_db_path = os.path.join(tmp_dir, "records.sqlite3")
            app.chat_download_config[123] = ChatDownloadConfig()
            app.chat_download_config[123].last_read_message_id = 10
            app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 10}]
            app.initialize_scan_cursors()

            next_cursor = app.record_scanned_message(123, 41)

            self.assertEqual(42, next_cursor)
            self.assertEqual(42, app.chat_download_config[123].last_read_message_id)
            self.assertEqual(42, app.ensure_download_records().get_cursor(123))
            self.assertTrue(app.config_dirty)

    def test_initial_cursor_uses_database_progress_when_config_is_stale(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            app = Application("", "")
            app.download_record_db_path = os.path.join(tmp_dir, "records.sqlite3")
            app.chat_download_config[123] = ChatDownloadConfig()
            app.chat_download_config[123].last_read_message_id = 10
            app.initialize_scan_cursors()
            app.record_scanned_message(123, 41)

            app.chat_download_config[123].last_read_message_id = 10
            app.initialize_scan_cursors()

            self.assertEqual(42, app.chat_download_config[123].last_read_message_id)

    def test_save_listen_targets(self):
        app = Application("config_test.yaml", "data_test.yaml")
        app.config = {"chat": [{"chat_id": 123, "last_read_message_id": 3}]}
        app.app_data = {"chat": [{"chat_id": 123, "ids_to_retry": [5]}]}
        app.chat_download_config[123] = ChatDownloadConfig()

        saved_targets = app.save_listen_targets(
            [
                {
                    "chat_id": "123",
                    "last_read_message_id": "8",
                    "download_filter": "id > 10",
                },
                {
                    "chat_id": "-100456",
                    "last_read_message_id": 0,
                    "enabled": True,
                },
                {
                    "chat_id": "789",
                    "last_read_message_id": 1,
                    "enabled": False,
                },
            ]
        )

        self.assertEqual(2, len(saved_targets))
        self.assertEqual(123, app.config["chat"][0]["chat_id"])
        self.assertEqual(8, app.config["chat"][0]["last_read_message_id"])
        self.assertEqual("id > 10", app.config["chat"][0]["download_filter"])
        self.assertEqual(
            [-100456, 0],
            [
                app.config["chat"][1]["chat_id"],
                app.config["chat"][1]["last_read_message_id"],
            ],
        )
        self.assertEqual([5], app.app_data["chat"][0]["ids_to_retry"])
        self.assertIn(123, app.chat_download_config)
        self.assertIn(-100456, app.chat_download_config)
        self.assertNotIn(789, app.chat_download_config)

    def test_scan_status_is_returned_with_listen_targets(self):
        app = Application("config_test.yaml", "data_test.yaml")
        app.config = {"chat": [{"chat_id": 123, "last_read_message_id": 3}]}
        app.app_data = {}

        app.record_chat_scan_status(123, "error", "CHANNEL_INVALID")
        targets = app.get_listen_targets()

        self.assertEqual("error", targets[0]["scan_status"]["status"])
        self.assertEqual("CHANNEL_INVALID", targets[0]["scan_status"]["error"])
        self.assertIn("last_scan_finished_at", targets[0]["scan_status"])

    def test_scan_status_records_success_time(self):
        app = Application("config_test.yaml", "data_test.yaml")
        app.record_chat_scan_status("-100456", "scanning")
        app.record_chat_scan_status("-100456", "ok")

        status = app.get_chat_scan_status("-100456")

        self.assertEqual("ok", status["status"])
        self.assertEqual("", status["error"])
        self.assertIn("last_scan_started_at", status)
        self.assertIn("last_scan_finished_at", status)
        self.assertIn("last_success_at", status)
