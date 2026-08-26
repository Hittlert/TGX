"""Download Stat"""
import asyncio
import threading
import time
from enum import Enum
from typing import Optional

from pyrogram import Client

from module.app import TaskNode


class DownloadState(Enum):
    """Download state"""

    Downloading = 1
    StopDownload = 2


_download_result: dict = {}
_download_result_lock = threading.RLock()
_total_download_speed: int = 0
_total_download_size: int = 0
_last_download_time: float = time.time()
_download_state: DownloadState = DownloadState.Downloading
DOWNLOAD_SPEED_STALE_SECONDS = 3.0


def get_download_result() -> dict:
    """Return a detached snapshot of the global download result."""
    with _download_result_lock:
        return {
            chat_id: {
                message_id: value.copy()
                for message_id, value in messages.items()
            }
            for chat_id, messages in _download_result.items()
        }


def get_current_download_speed(value: dict, now: Optional[float] = None) -> int:
    """Return a file's current speed, excluding completed or stale samples."""
    if now is None:
        now = time.time()

    total_size = int(value.get("total_size", 0) or 0)
    down_byte = int(value.get("down_byte", 0) or 0)
    last_progress_time = float(
        value.get("last_progress_time", value.get("end_time", 0)) or 0
    )
    if (
        total_size <= 0
        or down_byte >= total_size
        or now - last_progress_time > DOWNLOAD_SPEED_STALE_SECONDS
    ):
        return 0

    return max(int(value.get("download_speed", 0) or 0), 0)


def get_total_download_speed() -> int:
    """Return the sum of the current per-file download speeds."""
    now = time.time()
    download_result = get_download_result()
    return sum(
        get_current_download_speed(value, now)
        for messages in download_result.values()
        for value in messages.values()
    )


def get_download_state() -> DownloadState:
    """get download state"""
    return _download_state


# pylint: disable = W0603
def set_download_state(state: DownloadState):
    """set download state"""
    global _download_state
    _download_state = state


async def update_download_status(
    down_byte: int,
    total_size: int,
    message_id: int,
    file_name: str,
    start_time: float,
    node: TaskNode,
    client: Client,
):
    """update_download_status"""
    cur_time = time.time()
    # pylint: disable = W0603
    global _total_download_speed
    global _total_download_size
    global _last_download_time

    if node.is_stop_transmission:
        client.stop_transmission()

    chat_id = node.chat_id

    while get_download_state() == DownloadState.StopDownload:
        if node.is_stop_transmission:
            client.stop_transmission()
        await asyncio.sleep(1)

    with _download_result_lock:
        if not _download_result.get(chat_id):
            _download_result[chat_id] = {}

        if _download_result[chat_id].get(message_id):
            value = _download_result[chat_id][message_id]
            last_download_byte = value["down_byte"]
            last_time = value["end_time"]
            download_speed = value["download_speed"]
            each_second_total_download = value["each_second_total_download"]
            end_time = value["end_time"]

            downloaded_delta = down_byte - last_download_byte
            _total_download_size += downloaded_delta
            each_second_total_download += downloaded_delta

            if cur_time - last_time >= 1.0:
                download_speed = int(
                    each_second_total_download / (cur_time - last_time)
                )
                end_time = cur_time
                each_second_total_download = 0

            value["down_byte"] = down_byte
            value["end_time"] = end_time
            value["download_speed"] = max(download_speed, 0)
            value["each_second_total_download"] = each_second_total_download
            if downloaded_delta > 0:
                value["last_progress_time"] = cur_time
        else:
            each_second_total_download = down_byte
            _download_result[chat_id][message_id] = {
                "down_byte": down_byte,
                "total_size": total_size,
                "file_name": file_name,
                "start_time": start_time,
                "end_time": cur_time,
                "last_progress_time": cur_time,
                "download_speed": down_byte / (cur_time - start_time),
                "each_second_total_download": each_second_total_download,
                "task_id": node.task_id,
            }
            _total_download_size += down_byte

        if cur_time - _last_download_time >= 1.0:
            _total_download_speed = int(
                _total_download_size / (cur_time - _last_download_time)
            )
            _total_download_speed = max(_total_download_speed, 0)
            _total_download_size = 0
            _last_download_time = cur_time
