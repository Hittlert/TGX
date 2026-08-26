import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from module import download_stat, web
from utils.format import format_byte


class DownloadSpeedSnapshotTestCase(unittest.TestCase):
    def setUp(self):
        self.original_result = download_stat._download_result
        self.original_total_speed = download_stat._total_download_speed
        self.original_total_size = download_stat._total_download_size
        self.original_last_time = download_stat._last_download_time
        self.original_state = download_stat.get_download_state()
        download_stat._download_result = {}
        download_stat._total_download_size = 0
        download_stat.set_download_state(download_stat.DownloadState.Downloading)

    def tearDown(self):
        download_stat._download_result = self.original_result
        download_stat._total_download_speed = self.original_total_speed
        download_stat._total_download_size = self.original_total_size
        download_stat._last_download_time = self.original_last_time
        download_stat.set_download_state(self.original_state)

    @staticmethod
    def _entry(speed, end_time=100.0):
        return {
            "down_byte": 50,
            "total_size": 100,
            "file_name": "/downloads/example.bin",
            "end_time": end_time,
            "download_speed": speed,
        }

    def test_stale_file_speed_is_zero_in_list_and_total(self):
        entry = self._entry(4096, end_time=96.0)
        download_stat._download_result = {1: {2: entry}}
        download_stat._total_download_speed = 4096
        download_stat._last_download_time = 96.0

        with mock.patch.object(download_stat.time, "time", return_value=100.0):
            item = web._download_item_from_stat(1, 2, entry)
            total = download_stat.get_total_download_speed()

        self.assertEqual(format_byte(0) + "/s", item["download_speed"])
        self.assertEqual(0, total)

    def test_speed_remains_current_at_exact_stale_boundary(self):
        entry = self._entry(4096, end_time=97.0)

        with mock.patch.object(download_stat.time, "time", return_value=100.0):
            speed = download_stat.get_current_download_speed(entry)

        self.assertEqual(4096, speed)

    def test_total_speed_sums_current_file_speeds(self):
        completed = self._entry(8192)
        completed["down_byte"] = completed["total_size"]
        download_stat._download_result = {
            1: {2: self._entry(1024)},
            3: {4: self._entry(2048)},
            5: {6: completed},
            7: {8: self._entry(16384, end_time=96.0)},
        }
        download_stat._total_download_speed = 999
        download_stat._last_download_time = 100.0

        with mock.patch.object(download_stat.time, "time", return_value=100.0):
            total = download_stat.get_total_download_speed()

        self.assertEqual(3072, total)

    def test_progress_inside_speed_window_refreshes_freshness(self):
        node = SimpleNamespace(
            chat_id=1,
            task_id="task-1",
            is_stop_transmission=False,
        )
        client = SimpleNamespace(stop_transmission=lambda: None)

        async def report_progress():
            with mock.patch.object(download_stat.time, "time", return_value=100.0):
                await download_stat.update_download_status(
                    1024, 4096, 2, "example.bin", 99.0, node, client
                )
            with mock.patch.object(download_stat.time, "time", return_value=100.9):
                await download_stat.update_download_status(
                    2048, 4096, 2, "example.bin", 99.0, node, client
                )

        asyncio.run(report_progress())
        entry = download_stat._download_result[1][2]
        with mock.patch.object(download_stat.time, "time", return_value=103.1):
            speed = download_stat.get_current_download_speed(entry)

        self.assertGreater(speed, 0)

    def test_download_result_is_a_detached_snapshot(self):
        download_stat._download_result = {1: {2: self._entry(1024)}}

        snapshot = download_stat.get_download_result()
        download_stat._download_result[1][2]["download_speed"] = 2048
        download_stat._download_result[3] = {4: self._entry(4096)}

        self.assertEqual(1024, snapshot[1][2]["download_speed"])
        self.assertNotIn(3, snapshot)


if __name__ == "__main__":
    unittest.main()
