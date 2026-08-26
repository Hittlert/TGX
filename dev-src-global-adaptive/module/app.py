"""Application module"""

import asyncio
import errno
import io
import os
import threading
import time
from asyncio import Lock
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger
from ruamel import yaml

from module.cloud_drive import CloudDrive, CloudDriveConfig
from module.download_records import DownloadRecordStore
from module.filter import Filter
from module.language import Language, set_language
from utils.format import replace_date_time, validate_title
from utils.meta_data import MetaData

_yaml = yaml.YAML()
MAX_DOWNLOAD_HISTORY = 2000
DOWNLOADABLE_FILE_TYPES = (
    "audio",
    "document",
    "photo",
    "video",
    "voice",
    "video_note",
    "animation",
    "sticker",
)
# pylint: disable = R0902


def _dump_yaml_file_atomic(file_path: str, data: Any):
    """Write YAML through a same-directory temp file before replacing."""
    stream = io.StringIO()
    _yaml.dump(data, stream)
    content = stream.getvalue()
    tmp_file_path = f"{file_path}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp_file_path, "w", encoding="utf-8") as yaml_file:
            yaml_file.write(content)
            yaml_file.flush()
            os.fsync(yaml_file.fileno())
        try:
            os.replace(tmp_file_path, file_path)
        except OSError as exc:
            if exc.errno != errno.EBUSY:
                raise
            with open(file_path, "w", encoding="utf-8") as yaml_file:
                yaml_file.write(content)
                yaml_file.flush()
                os.fsync(yaml_file.fileno())
    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)


def _thaw_snapshot(value):
    """Convert immutable coordinator snapshots into JSON-safe containers."""
    if isinstance(value, Mapping):
        return {key: _thaw_snapshot(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_thaw_snapshot(item) for item in value]
    return value


class DownloadStatus(Enum):
    """Download status"""

    SkipDownload = 1
    SuccessDownload = 2
    FailedDownload = 3
    Downloading = 4


class ForwardStatus(Enum):
    """Forward status"""

    SkipForward = 1
    SuccessForward = 2
    FailedForward = 3
    Forwarding = 4
    StopForward = 5
    CacheForward = 6


class UploadStatus(Enum):
    """Upload status"""

    SkipUpload = 1
    SuccessUpload = 2
    FailedUpload = 3
    Uploading = 4


class TaskType(Enum):
    """Task Type"""

    Download = 1
    Forward = 2
    ListenForward = 3


class QueryHandler(Enum):
    """Query handler"""

    StopDownload = 1
    StopForward = 2
    StopListenForward = 3


@dataclass
class UploadProgressStat:
    """Upload task"""

    file_name: str
    total_size: int
    upload_size: int
    start_time: float
    last_stat_time: float
    upload_speed: float


@dataclass
class CloudDriveUploadStat:
    """Cloud drive upload task"""

    file_name: str
    transferred: str
    total: str
    percentage: str
    speed: str
    eta: str


class QueryHandlerStr:
    """Query handler"""

    _strMap = {
        QueryHandler.StopDownload.value: "stop_download",
        QueryHandler.StopForward.value: "stop_forward",
        QueryHandler.StopListenForward.value: "stop_listen_forward",
    }

    @staticmethod
    def get_str(value):
        """
        Get the string value associated with the given value.

        Parameters:
            value (any): The value for which to retrieve the string value.

        Returns:
            str: The string value associated with the given value.
        """
        return QueryHandlerStr._strMap[value]


class TaskNode:
    """Task node"""

    # pylint: disable = R0913
    def __init__(
        self,
        chat_id: Union[int, str],
        from_user_id: Union[int, str] = None,
        reply_message_id: int = 0,
        replay_message: str = None,
        upload_telegram_chat_id: Union[int, str] = None,
        has_protected_content: bool = False,
        download_filter: str = None,
        limit: int = 0,
        start_offset_id: int = 0,
        end_offset_id: int = 0,
        bot=None,
        task_type: TaskType = TaskType.Download,
        task_id: int = 0,
        topic_id: int = 0,
    ):
        self.chat_id = chat_id
        self.from_user_id = from_user_id
        self.upload_telegram_chat_id = upload_telegram_chat_id
        self.reply_message_id = reply_message_id
        self.reply_message = replay_message
        self.has_protected_content = has_protected_content
        self.download_filter = download_filter
        self.limit = limit
        self.start_offset_id = start_offset_id
        self.end_offset_id = end_offset_id
        self.bot = bot
        self.task_id = task_id
        self.task_type = task_type
        self.total_task = 0
        self.total_download_task = 0
        self.failed_download_task = 0
        self.success_download_task = 0
        self.skip_download_task = 0
        self.last_reply_time = time.time()
        self.last_edit_msg: str = ""
        self.total_download_byte = 0
        self.forward_msg_detail_str: str = ""
        self.upload_user = None
        self.total_forward_task: int = 0
        self.success_forward_task: int = 0
        self.failed_forward_task: int = 0
        self.skip_forward_task: int = 0
        self.is_running: bool = False
        self.client = None
        self.upload_success_count: int = 0
        self.is_stop_transmission = False
        self.media_group_ids: dict = {}
        self.media_group_ids_lock: Lock = Lock()
        self.retry_message_ids: set = set()
        self.download_status: dict = {}
        self.upload_status: dict = {}
        self.upload_stat_dict: dict = {}
        self.topic_id = topic_id
        self.reply_to_message = None
        self.cloud_drive_upload_stat_dict: dict = {}

    def skip_msg_id(self, msg_id: int):
        """Skip if message id out of range"""
        if self.start_offset_id and msg_id < self.start_offset_id:
            return True

        if self.end_offset_id and msg_id > self.end_offset_id:
            return True

        return False

    def is_finish(self):
        """If is finish"""
        return self.is_stop_transmission or (
            self.is_running
            and self.task_type != TaskType.ListenForward
            and self.total_task == self.total_download_task
        )

    def stop_transmission(self):
        """Stop task"""
        self.is_stop_transmission = True

    def stat(self, status: DownloadStatus):
        """
        Updates the download status of the task.

        Args:
            status (DownloadStatus): The status of the download task.

        Returns:
            None
        """
        self.total_download_task += 1
        if status is DownloadStatus.SuccessDownload:
            self.success_download_task += 1
        elif status is DownloadStatus.SkipDownload:
            self.skip_download_task += 1
        else:
            self.failed_download_task += 1

    def stat_forward(self, status: ForwardStatus, count: int = 1):
        """Stat upload"""
        self.total_forward_task += count
        if status is ForwardStatus.SuccessForward:
            self.success_forward_task += count
        elif status is ForwardStatus.SkipForward:
            self.skip_forward_task += count
        else:
            self.failed_forward_task += count

    def can_reply(self):
        """
        Checks if the bot can reply to a message
            based on the time elapsed since the last reply.

        Returns:
            True if the time elapsed since
                the last reply is greater than 1 second, False otherwise.
        """
        cur_time = time.time()
        if cur_time - self.last_reply_time > 1.0:
            self.last_reply_time = cur_time
            return True

        return False


class LimitCall:
    """Limit call"""

    def __init__(
        self,
        max_limit_call_times: int = 0,
        limit_call_times: int = 0,
        last_call_time: float = 0,
    ):
        """
        Initializes the object with the given parameters.

        Args:
            max_limit_call_times (int): The maximum limit of call times allowed.
            limit_call_times (int): The current limit of call times.
            last_call_time (int): The time of the last call.

        Returns:
            None
        """
        self.max_limit_call_times = max_limit_call_times
        self.limit_call_times = limit_call_times
        self.last_call_time = last_call_time

    async def wait(self, node: TaskNode):
        """
        Wait for a certain period of time before continuing execution.

        This function does not take any parameters.

        This function does not return anything.
        """
        while True:
            now = time.time()
            time_span = now - self.last_call_time
            if node.is_stop_transmission:
                break

            if time_span > 60:
                self.limit_call_times = 0
                self.last_call_time = now

            if self.limit_call_times + 1 <= self.max_limit_call_times:
                self.limit_call_times += 1
                break

            # logger.debug("Waiting for 10 seconds...")
            await asyncio.sleep(1)


class ChatDownloadConfig:
    """Chat Message Download Status"""

    def __init__(self):
        self.ids_to_retry_dict: dict = {}

        # need storage
        self.download_filter: str = None
        self.ids_to_retry: list = []
        self.last_read_message_id = 0
        self.total_task: int = 0
        self.finish_task: int = 0
        self.need_check: bool = False
        self.upload_telegram_chat_id: Union[int, str] = None
        self.node: TaskNode = TaskNode(0)


def get_config(config, key, default=None, val_type=str, verbose=True):
    """
    Retrieves a configuration value from the given `config` dictionary
    based on the specified `key`.

    Args:
        config (dict): A dictionary containing the configuration values.
        key (str): The key of the configuration value to retrieve.
        default (Any, optional): The default value to be returned
            if the `key` is not found.
        val_type (type, optional): The data type of the configuration value.
        verbose (bool, optional): A flag indicating whether to print
            a warning message if the `key` is not found.

    Returns:
        The configuration value associated with the specified `key`,
         converted to the specified `type`. If the `key` is not found,
         the `default` value is returned.
    """
    val = config.get(key, default)
    if isinstance(val, val_type):
        return val

    if verbose:
        logger.warning(f"{key} is not {val_type.__name__}")

    return default


class Application:
    """Application load config and update config."""

    def __init__(
        self,
        config_file: str,
        app_data_file: str,
        application_name: str = "UndefineApp",
    ):
        """
        Init and update telegram media downloader config

        Parameters
        ----------
        config_file: str
            Config file name

        app_data_file: str
            App data file

        application_name: str
            Application Name

        """
        self.config_file: str = config_file
        self.app_data_file: str = app_data_file
        self.application_name: str = application_name
        self.download_filter = Filter()
        self.is_running = True

        self.total_download_task = 0

        self.chat_download_config: dict = {}

        self.save_path = os.path.join(os.path.abspath("."), "downloads")
        self.temp_save_path = os.path.join(os.path.abspath("."), "temp")
        self.api_id: str = ""
        self.api_hash: str = ""
        self.bot_token: str = ""
        self._chat_id: str = ""
        self.media_types: List[str] = []
        self.file_formats: dict = {}
        self.proxy: dict = {}
        self.restart_program = False
        self.config: dict = {}
        self.app_data: dict = {}
        self.config_lock = threading.RLock()
        self.config_dirty = False
        self.last_config_update_time = 0.0
        self.config_update_interval = 30
        self.file_path_prefix: List[str] = ["chat_title", "media_datetime"]
        self.file_name_prefix: List[str] = ["message_id", "file_name"]
        self.file_name_prefix_split: str = " - "
        self.log_file_path = os.path.join(os.path.abspath("."), "log")
        self.session_file_path = os.path.join(os.path.abspath("."), "sessions")
        self.state_file_path = os.path.join(os.path.abspath("."), "state")
        self.download_record_db_path = os.path.join(
            self.state_file_path, "download_records.sqlite3"
        )
        self.download_records: Optional[DownloadRecordStore] = None
        self.consumer_claim_lock = threading.Lock()
        self.consumer_chat_index = 0
        self.cloud_drive_config = CloudDriveConfig()
        self.hide_file_name = False
        self.caption_name_dict: dict = {}
        self.caption_entities_dict: dict = {}
        self.max_concurrent_transmissions: int = 1
        self.web_host: str = "0.0.0.0"
        self.web_port: int = 5000
        self.max_download_task: int = 5
        self.parallel_download_enabled: bool = False
        self.parallel_download_workers: int = 2
        self.parallel_download_min_size: int = 256 * 1024 * 1024
        self.parallel_pool_mode: str = "off"
        self.parallel_session_pool_enabled: bool = False
        self.parallel_pool_file_threshold: int = 5 * 1024 * 1024
        self.parallel_pool_stripe_size: int = 5 * 1024 * 1024
        self.parallel_pool_soft_sessions: int = 16
        self.parallel_pool_max_sessions: int = 48
        self.parallel_pool_pipeline_depth: int = 2
        self.parallel_pool_idle_ttl: int = 600
        self.parallel_pool_control_interval: int = 60
        self.media_session_pool = None
        self.parallel_file_pool_threshold: int = 5 * 1024 * 1024
        self.parallel_file_pool_stripe_size: int = 5 * 1024 * 1024
        self.parallel_file_pool_initial_sessions: int = 4
        self.parallel_file_pool_max_sessions: int = 12
        self.parallel_file_pool_control_interval: int = 10
        self.parallel_file_pool_growth_hold: int = 120
        self.parallel_media_session_budget: int = 60
        self.parallel_file_pool_pipeline_depth: int = 1
        self.media_transfer_coordinator = None
        self.language = Language.EN
        self.after_upload_telegram_delete: bool = True
        self.web_login_secret: str = ""
        self.debug_web: bool = False
        self.log_level: str = "INFO"
        self.start_timeout: int = 60
        self.listen_interval: int = 600
        self.allowed_user_ids: yaml.comments.CommentedSeq = yaml.comments.CommentedSeq(
            []
        )
        self.date_format: str = "%Y_%m"
        self.drop_no_audio_video: bool = False
        self.enable_download_txt: bool = False
        self.filter_advertisement_list: yaml.comments.CommentedSeq = (
            yaml.comments.CommentedSeq([])
        )
        self.replace_advertisement_list: yaml.comments.CommentedSeq = (
            yaml.comments.CommentedSeq([])
        )
        self.group_add_advertisement: dict = {}
        self.forward_limit_call = LimitCall(max_limit_call_times=33)

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.executor = ThreadPoolExecutor(
            min(32, (os.cpu_count() or 0) + 4), thread_name_prefix="multi_task"
        )

    @staticmethod
    def normalize_chat_id(chat_id: Any) -> Union[int, str]:
        """
        Normalize chat IDs loaded from the web UI.
        """
        if isinstance(chat_id, int):
            return chat_id

        chat_id_str = str(chat_id).strip()
        if chat_id_str.lstrip("-").isdigit():
            return int(chat_id_str)

        return chat_id_str

    @staticmethod
    def normalize_message_id(message_id: Any) -> int:
        """
        Normalize Telegram message IDs loaded from the web UI.
        """
        try:
            message_id_int = int(message_id or 0)
        except (TypeError, ValueError):
            message_id_int = 0

        return max(message_id_int, 0)

    @staticmethod
    def _chat_id_key(chat_id: Any) -> str:
        return str(chat_id)

    def _get_chat_download_config(
        self, chat_id: Union[int, str]
    ) -> Optional[ChatDownloadConfig]:
        chat_download_config = self.chat_download_config.get(chat_id)
        if chat_download_config:
            return chat_download_config

        chat_key = self._chat_id_key(chat_id)
        for key, value in self.chat_download_config.items():
            if self._chat_id_key(key) == chat_key:
                return value

        return None

    def get_media_pool_status(self) -> dict:
        """Return a status-thread-safe view of the media session pool."""
        mode = getattr(self, "parallel_pool_mode", None)
        coordinator = getattr(self, "media_transfer_coordinator", None)
        if mode == "per_file" and coordinator is not None:
            snapshot = coordinator.snapshot()
            files = {
                str(pool_id): _thaw_snapshot(values)
                for pool_id, values in snapshot.pools.items()
                if int(values.get("used", 0) or 0) > 0
                or int(values.get("pending", 0) or 0) > 0
                or int(values.get("active", 0) or 0) > 0
            }
            return {
                "enabled": True,
                "mode": "per_file",
                "desired": sum(
                    int(values.get("target", 0) or 0) for values in files.values()
                ),
                "live": snapshot.live,
                "idle": snapshot.idle,
                "created": snapshot.created,
                "reused": snapshot.reused,
                "active_slots": sum(
                    int(values.get("active", 0) or 0) for values in files.values()
                ),
                "hard_limit": snapshot.hard_limit,
                "pipeline_depth": self.parallel_file_pool_pipeline_depth,
                "last_scale_reason": "per_file",
                "used": snapshot.used,
                "creating": snapshot.creating,
                "draining": snapshot.draining,
                "active_files": snapshot.active_files,
                "raw_bps": snapshot.raw_bps,
                "rolling_5s_bps": snapshot.rolling_5s_bps,
                "p10_5s_bps": snapshot.p10_5s_bps,
                "mean_5s_bps": snapshot.mean_5s_bps,
                "stddev_5s_bps": snapshot.stddev_5s_bps,
                "cv": snapshot.cv,
                "sample_count": snapshot.sample_count,
                "raw_samples": list(snapshot.raw_samples),
                "rolling_5s_samples": list(snapshot.rolling_5s_samples),
                "committed_bytes_per_second": (
                    snapshot.committed_bytes_per_second
                ),
                "expansion_queue": snapshot.expansion_queue,
                "dc_cooldowns": _thaw_snapshot(snapshot.dc_cooldowns),
                "fallbacks": snapshot.fallbacks,
                "retries": sum(
                    int(values.get("retries", 0) or 0) for values in files.values()
                ),
                "resets": sum(
                    int(values.get("resets", 0) or 0) for values in files.values()
                ),
                "files": files,
            }

        pool = self.media_session_pool
        if pool is None:
            configured_mode = mode if mode in ("global", "per_file") else "off"
            per_file = configured_mode == "per_file"
            return {
                "enabled": False,
                "mode": configured_mode,
                "desired": 0,
                "live": 0,
                "active_slots": 0,
                "hard_limit": (
                    self.parallel_media_session_budget
                    if per_file
                    else self.parallel_pool_max_sessions
                ),
                "pipeline_depth": (
                    self.parallel_file_pool_pipeline_depth
                    if per_file
                    else self.parallel_pool_pipeline_depth
                ),
                "last_scale_reason": "disabled",
                "used": 0,
                "creating": 0,
                "draining": 0,
                "active_files": 0,
                "raw_bps": 0.0,
                "rolling_5s_bps": 0.0,
                "p10_5s_bps": 0.0,
                "mean_5s_bps": 0.0,
                "stddev_5s_bps": 0.0,
                "cv": 0.0,
                "sample_count": 0,
                "raw_samples": [],
                "rolling_5s_samples": [],
                "committed_bytes_per_second": 0.0,
                "expansion_queue": 0,
                "dc_cooldowns": {},
                "fallbacks": 0,
                "retries": 0,
                "resets": 0,
                "files": {},
            }

        payload = asdict(pool.snapshot())
        payload["enabled"] = True
        payload["mode"] = "global"
        payload["used"] = int(payload.get("live", 0) or 0) + int(
            payload.get("creating", 0) or 0
        )
        payload["draining"] = 0
        payload["raw_bps"] = float(
            payload.get("committed_bytes_per_second", 0) or 0
        )
        payload["rolling_5s_bps"] = payload["raw_bps"]
        payload["p10_5s_bps"] = 0.0
        payload["mean_5s_bps"] = payload["raw_bps"]
        payload["stddev_5s_bps"] = 0.0
        payload["cv"] = 0.0
        payload["sample_count"] = 0
        payload["raw_samples"] = []
        payload["rolling_5s_samples"] = []
        payload["expansion_queue"] = 0
        payload["dc_cooldowns"] = {}
        payload["resets"] = int(payload.get("unhealthy", 0) or 0)
        payload["files"] = {}
        return payload

    def get_listen_targets(self) -> List[Dict[str, Any]]:
        """
        Return configured chat targets for the web UI.
        """
        with self.config_lock:
            targets: List[Dict[str, Any]] = []
            for item in self.config.get("chat", []) or []:
                if "chat_id" not in item:
                    continue

                chat_id = item["chat_id"]
                target = {
                    "chat_id": chat_id,
                    "title": item.get("title", ""),
                    "last_read_message_id": self.normalize_message_id(
                        item.get("last_read_message_id", 0)
                    ),
                    "download_filter": item.get("download_filter", ""),
                    "upload_telegram_chat_id": item.get("upload_telegram_chat_id", ""),
                    "scan_status": self.get_chat_scan_status(chat_id),
                }

                chat_download_config = self._get_chat_download_config(chat_id)
                if chat_download_config:
                    target[
                        "last_read_message_id"
                    ] = chat_download_config.last_read_message_id

                targets.append(target)

            return targets

    def get_chat_scan_status(self, chat_id: Union[int, str]) -> Dict[str, Any]:
        """Return the latest scan status for a configured chat."""
        chat_key = self._chat_id_key(chat_id)
        with self.config_lock:
            statuses = self.app_data.get("chat_scan_status", {})
            if not isinstance(statuses, dict):
                return {}

            status = statuses.get(chat_key, {})
            if isinstance(status, dict):
                return dict(status)

            return {}

    def record_chat_scan_status(
        self,
        chat_id: Union[int, str],
        status: str,
        error: str = "",
    ):
        """Persist the latest scan status for a configured chat."""
        chat_key = self._chat_id_key(chat_id)
        if not chat_key:
            return

        now = int(time.time())
        with self.config_lock:
            statuses = self.app_data.get("chat_scan_status", {})
            if not isinstance(statuses, dict):
                statuses = {}

            scan_status = statuses.get(chat_key, {})
            if not isinstance(scan_status, dict):
                scan_status = {}

            scan_status["status"] = status
            if status == "scanning":
                scan_status["last_scan_started_at"] = now
            else:
                scan_status["last_scan_finished_at"] = now

            if status == "ok":
                scan_status["last_success_at"] = now

            scan_status["error"] = str(error or "")[:500]
            statuses[chat_key] = scan_status
            self.app_data["chat_scan_status"] = statuses
            self.write_app_data()

    def write_app_data(self):
        """Persist application data."""
        with self.config_lock:
            _dump_yaml_file_atomic(self.app_data_file, self.app_data)

    def get_dialog_cache(self) -> Dict[str, Any]:
        """
        Return cached Telegram dialogs for the web UI.
        """
        with self.config_lock:
            dialog_cache = self.app_data.get("dialog_cache", {})
            if isinstance(dialog_cache, dict):
                return dialog_cache

            return {}

    def set_dialog_cache(self, dialogs: List[Dict[str, Any]], updated_at: int):
        """
        Persist cached Telegram dialogs for the web UI.
        """
        with self.config_lock:
            self.app_data["dialog_cache"] = {
                "updated_at": updated_at,
                "items": dialogs,
            }
            self.write_app_data()

    def get_download_history(self) -> List[Dict[str, Any]]:
        """
        Return persisted completed downloads for the web UI.
        """
        with self.config_lock:
            download_history = self.app_data.get("download_history", [])
            if isinstance(download_history, list):
                return download_history

            return []

    def record_download_history(self, item: Dict[str, Any]):
        """
        Persist one completed download for the web UI.
        """
        with self.config_lock:
            download_history = self.get_download_history()
            chat_id = self._chat_id_key(item.get("chat", ""))
            message_id = self._chat_id_key(item.get("id", ""))
            download_history = [
                it
                for it in download_history
                if not (
                    self._chat_id_key(it.get("chat", "")) == chat_id
                    and self._chat_id_key(it.get("id", "")) == message_id
                )
            ]
            download_history.insert(0, item)
            self.app_data["download_history"] = download_history[:MAX_DOWNLOAD_HISTORY]
            self.write_app_data()

    def save_listen_targets(
        self, targets: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Save selected chat targets from the web UI.
        """
        with self.config_lock:
            retry_ids_by_chat: Dict[str, list] = {}
            for item in self.app_data.get("chat", []) or []:
                if "chat_id" in item:
                    retry_ids_by_chat[self._chat_id_key(item["chat_id"])] = item.get(
                        "ids_to_retry", []
                    )
            scan_status_by_chat = self.app_data.get("chat_scan_status", {})
            if not isinstance(scan_status_by_chat, dict):
                scan_status_by_chat = {}

            chat_config: Dict[Union[int, str], ChatDownloadConfig] = {}
            chat_config_list = []
            app_data_chat_list = []
            seen_chat_ids = set()

            for item in targets:
                if item.get("enabled") is False:
                    continue
                if "chat_id" not in item:
                    continue

                chat_id = self.normalize_chat_id(item["chat_id"])
                if not self._chat_id_key(chat_id):
                    continue

                chat_key = self._chat_id_key(chat_id)
                if chat_key in seen_chat_ids:
                    continue
                seen_chat_ids.add(chat_key)
                source_chat_key = self._chat_id_key(
                    item.get("source_chat_id", item["chat_id"])
                )

                last_read_message_id = self.normalize_message_id(
                    item.get("last_read_message_id", 0)
                )
                chat_item: Dict[str, Any] = {
                    "chat_id": chat_id,
                    "last_read_message_id": last_read_message_id,
                }

                title = str(item.get("title", "") or "").strip()
                if title and title != self._chat_id_key(chat_id):
                    chat_item["title"] = title

                download_filter = str(item.get("download_filter", "") or "").strip()
                if download_filter:
                    chat_item["download_filter"] = download_filter

                upload_telegram_chat_id = item.get("upload_telegram_chat_id", "")
                upload_telegram_chat_id = (
                    str(upload_telegram_chat_id).strip()
                    if upload_telegram_chat_id is not None
                    else ""
                )
                if upload_telegram_chat_id:
                    chat_item["upload_telegram_chat_id"] = self.normalize_chat_id(
                        upload_telegram_chat_id
                    )

                chat_config_list.append(chat_item)

                chat_download_config = self._get_chat_download_config(chat_id)
                if not chat_download_config:
                    chat_download_config = ChatDownloadConfig()
                chat_download_config.last_read_message_id = last_read_message_id
                chat_download_config.download_filter = replace_date_time(
                    download_filter
                )
                chat_download_config.upload_telegram_chat_id = chat_item.get(
                    "upload_telegram_chat_id", None
                )
                chat_config[chat_id] = chat_download_config

                app_data_chat_list.append(
                    {
                        "chat_id": chat_id,
                        "ids_to_retry": retry_ids_by_chat.get(
                            chat_key, retry_ids_by_chat.get(source_chat_key, [])
                        ),
                    }
                )
                if (
                    source_chat_key != chat_key
                    and source_chat_key in scan_status_by_chat
                    and chat_key not in scan_status_by_chat
                ):
                    scan_status_by_chat[chat_key] = scan_status_by_chat[
                        source_chat_key
                    ]

            self.config["chat"] = chat_config_list
            self.app_data["chat"] = app_data_chat_list
            self.app_data["chat_scan_status"] = scan_status_by_chat
            self.chat_download_config = chat_config

            if self.download_records is not None:
                for chat_id, chat_download_config in chat_config.items():
                    self.download_records.override_cursor(
                        chat_id, chat_download_config.last_read_message_id
                    )

            _dump_yaml_file_atomic(self.config_file, self.config)
            _dump_yaml_file_atomic(self.app_data_file, self.app_data)

            return self.get_listen_targets()

    # pylint: disable = R0915
    def assign_config(self, _config: dict) -> bool:
        """assign config from str.

        Parameters
        ----------
        _config: dict
            application config dict

        Returns
        -------
        bool
        """
        # pylint: disable = R0912
        # TODO: judge the storage if enough,and provide more path
        if _config.get("save_path") is not None:
            self.save_path = _config["save_path"]

        self.api_id = _config["api_id"]
        self.api_hash = _config["api_hash"]
        self.bot_token = _config.get("bot_token", "")

        configured_media_types = list(_config.get("media_types", []) or [])
        self.media_types = list(
            dict.fromkeys(configured_media_types + list(DOWNLOADABLE_FILE_TYPES))
        )
        self.file_formats = _config["file_formats"]

        self.hide_file_name = _config.get("hide_file_name", False)

        # option
        if _config.get("proxy"):
            self.proxy = _config["proxy"]
        if _config.get("download_record_db_path"):
            self.download_record_db_path = _config["download_record_db_path"]
        if _config.get("restart_program"):
            self.restart_program = _config["restart_program"]
        if _config.get("file_path_prefix"):
            self.file_path_prefix = _config["file_path_prefix"]
        if _config.get("file_name_prefix"):
            self.file_name_prefix = _config["file_name_prefix"]

        if _config.get("upload_drive"):
            upload_drive_config = _config["upload_drive"]
            if upload_drive_config.get("enable_upload_file"):
                self.cloud_drive_config.enable_upload_file = upload_drive_config[
                    "enable_upload_file"
                ]

            if upload_drive_config.get("rclone_path"):
                self.cloud_drive_config.rclone_path = upload_drive_config["rclone_path"]

            if upload_drive_config.get("remote_dir"):
                self.cloud_drive_config.remote_dir = upload_drive_config["remote_dir"]

            if upload_drive_config.get("before_upload_file_zip"):
                self.cloud_drive_config.before_upload_file_zip = upload_drive_config[
                    "before_upload_file_zip"
                ]

            if upload_drive_config.get("after_upload_file_delete"):
                self.cloud_drive_config.after_upload_file_delete = upload_drive_config[
                    "after_upload_file_delete"
                ]

            if upload_drive_config.get("upload_adapter"):
                self.cloud_drive_config.upload_adapter = upload_drive_config[
                    "upload_adapter"
                ]

        self.file_name_prefix_split = _config.get(
            "file_name_prefix_split", self.file_name_prefix_split
        )
        self.web_host = _config.get("web_host", self.web_host)
        self.web_port = _config.get("web_port", self.web_port)

        # TODO: add check if expression exist syntax error

        self.max_download_task = _config.get(
            "max_download_task", self.max_download_task
        )

        self.max_concurrent_transmissions = self.max_download_task * 5

        self.max_concurrent_transmissions = _config.get(
            "max_concurrent_transmissions", self.max_concurrent_transmissions
        )

        self.parallel_download_enabled = get_config(
            _config,
            "parallel_download_enabled",
            self.parallel_download_enabled,
            bool,
        )
        configured_parallel_workers = get_config(
            _config,
            "parallel_download_workers",
            self.parallel_download_workers,
            int,
        )
        self.parallel_download_workers = (
            configured_parallel_workers
            if 1 <= configured_parallel_workers <= 4
            else 2
        )
        configured_parallel_min_size = get_config(
            _config,
            "parallel_download_min_size",
            self.parallel_download_min_size,
            int,
        )
        self.parallel_download_min_size = (
            configured_parallel_min_size
            if configured_parallel_min_size > 0
            else 256 * 1024 * 1024
        )

        self.parallel_session_pool_enabled = get_config(
            _config,
            "parallel_session_pool_enabled",
            self.parallel_session_pool_enabled,
            bool,
        )
        configured_pool_mode = _config.get("parallel_pool_mode")
        if configured_pool_mode is None:
            self.parallel_pool_mode = (
                "global" if self.parallel_session_pool_enabled else "off"
            )
        else:
            normalized_pool_mode = str(configured_pool_mode).strip().lower()
            self.parallel_pool_mode = (
                normalized_pool_mode
                if normalized_pool_mode in ("off", "global", "per_file")
                else "off"
            )
        self.parallel_session_pool_enabled = self.parallel_pool_mode == "global"

        def bounded_int(name, default, minimum, maximum):
            value = get_config(_config, name, default, int)
            return value if minimum <= value <= maximum else default

        self.parallel_pool_file_threshold = bounded_int(
            "parallel_pool_file_threshold", 5 * 1024 * 1024, 1024 * 1024, 1024**3
        )
        if self.parallel_pool_file_threshold != 5 * 1024 * 1024:
            self.parallel_pool_file_threshold = 5 * 1024 * 1024
        self.parallel_pool_stripe_size = bounded_int(
            "parallel_pool_stripe_size", 5 * 1024 * 1024, 1024 * 1024, 64 * 1024 * 1024
        )
        if self.parallel_pool_stripe_size != 5 * 1024 * 1024:
            self.parallel_pool_stripe_size = 5 * 1024 * 1024
        self.parallel_pool_soft_sessions = bounded_int(
            "parallel_pool_soft_sessions", 16, 1, 48
        )
        self.parallel_pool_max_sessions = bounded_int(
            "parallel_pool_max_sessions", 48, self.parallel_pool_soft_sessions, 48
        )
        self.parallel_pool_pipeline_depth = bounded_int(
            "parallel_pool_pipeline_depth", 2, 1, 2
        )
        self.parallel_pool_idle_ttl = bounded_int(
            "parallel_pool_idle_ttl", 600, 60, 86400
        )
        self.parallel_pool_control_interval = bounded_int(
            "parallel_pool_control_interval", 60, 10, 600
        )
        self.parallel_file_pool_threshold = bounded_int(
            "parallel_file_pool_threshold",
            5 * 1024 * 1024,
            1024 * 1024,
            1024**3,
        )
        if self.parallel_file_pool_threshold != 5 * 1024 * 1024:
            self.parallel_file_pool_threshold = 5 * 1024 * 1024
        self.parallel_file_pool_stripe_size = bounded_int(
            "parallel_file_pool_stripe_size",
            5 * 1024 * 1024,
            1024 * 1024,
            64 * 1024 * 1024,
        )
        supported_file_pool_stripes = {
            size * 1024 * 1024 for size in (5, 10, 20)
        }
        if self.parallel_file_pool_stripe_size not in supported_file_pool_stripes:
            self.parallel_file_pool_stripe_size = 5 * 1024 * 1024
        configured_initial_sessions = bounded_int(
            "parallel_file_pool_initial_sessions", 4, 1, 12
        )
        self.parallel_file_pool_initial_sessions = (
            configured_initial_sessions if configured_initial_sessions == 4 else 4
        )
        configured_max_sessions = bounded_int(
            "parallel_file_pool_max_sessions", 12, 4, 12
        )
        self.parallel_file_pool_max_sessions = (
            configured_max_sessions if configured_max_sessions in (4, 8, 12) else 12
        )
        self.parallel_file_pool_control_interval = bounded_int(
            "parallel_file_pool_control_interval", 10, 1, 600
        )
        self.parallel_file_pool_growth_hold = bounded_int(
            "parallel_file_pool_growth_hold", 120, 1, 3600
        )
        self.parallel_media_session_budget = bounded_int(
            "parallel_media_session_budget", 60, 4, 60
        )
        configured_pipeline_depth = bounded_int(
            "parallel_file_pool_pipeline_depth", 1, 1, 1
        )
        self.parallel_file_pool_pipeline_depth = (
            configured_pipeline_depth if configured_pipeline_depth == 1 else 1
        )

        language = _config.get("language", "EN")

        try:
            self.language = Language[language.upper()]
        except KeyError:
            pass

        self.after_upload_telegram_delete = _config.get(
            "after_upload_telegram_delete", self.after_upload_telegram_delete
        )

        self.web_login_secret = str(
            _config.get("web_login_secret", self.web_login_secret)
        )
        self.debug_web = _config.get("debug_web", self.debug_web)
        self.log_level = _config.get("log_level", self.log_level)

        self.start_timeout = get_config(
            _config, "start_timeout", self.start_timeout, int
        )
        self.listen_interval = max(
            get_config(_config, "listen_interval", self.listen_interval, int),
            60,
        )

        self.allowed_user_ids = get_config(
            _config,
            "allowed_user_ids",
            self.allowed_user_ids,
            yaml.comments.CommentedSeq,
        )

        self.date_format = get_config(
            _config,
            "date_format",
            self.date_format,
            str,
        )

        self.drop_no_audio_video = get_config(
            _config, "drop_no_audio_video", self.drop_no_audio_video, bool
        )

        self.enable_download_txt = get_config(
            _config, "enable_download_txt", self.enable_download_txt, bool
        )

        self.filter_advertisement_list = get_config(
            _config,
            "filter_advertisement_list",
            self.filter_advertisement_list,
            yaml.comments.CommentedSeq,
        )

        self.replace_advertisement_list = get_config(
            _config,
            "replace_advertisement_list",
            self.replace_advertisement_list,
            yaml.comments.CommentedSeq,
        )

        if _config.get("group_add_advertisement"):
            self.group_add_advertisement = _config["group_add_advertisement"]
        try:
            date = datetime(2023, 10, 31)
            date.strftime(self.date_format)
        except Exception as e:
            logger.warning(f"config date format error: {e}")
            self.date_format = "%Y_%m"

        forward_limit = _config.get("forward_limit", None)
        if forward_limit:
            try:
                forward_limit = int(forward_limit)
                self.forward_limit_call.max_limit_call_times = forward_limit
            except ValueError:
                pass

        if _config.get("chat"):
            chat = _config["chat"]
            for item in chat:
                if "chat_id" in item:
                    self.chat_download_config[item["chat_id"]] = ChatDownloadConfig()
                    self.chat_download_config[
                        item["chat_id"]
                    ].last_read_message_id = item.get("last_read_message_id", 0)
                    self.chat_download_config[
                        item["chat_id"]
                    ].download_filter = item.get("download_filter", "")
                    self.chat_download_config[
                        item["chat_id"]
                    ].upload_telegram_chat_id = item.get(
                        "upload_telegram_chat_id", None
                    )
        elif _config.get("chat_id"):
            # Compatible with lower versions
            self._chat_id = _config["chat_id"]

            self.chat_download_config[self._chat_id] = ChatDownloadConfig()

            if _config.get("ids_to_retry"):
                self.chat_download_config[self._chat_id].ids_to_retry = _config[
                    "ids_to_retry"
                ]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[
                        it
                    ] = True

            self.chat_download_config[self._chat_id].last_read_message_id = _config[
                "last_read_message_id"
            ]
            download_filter_dict = _config.get("download_filter", None)

            self.config["chat"] = [
                {
                    "chat_id": self._chat_id,
                    "last_read_message_id": self.chat_download_config[
                        self._chat_id
                    ].last_read_message_id,
                }
            ]

            if download_filter_dict and self._chat_id in download_filter_dict:
                self.chat_download_config[
                    self._chat_id
                ].download_filter = download_filter_dict[self._chat_id]
                self.config["chat"][0]["download_filter"] = download_filter_dict[
                    self._chat_id
                ]

        # pylint: disable = R1733
        for key, value in self.chat_download_config.items():
            self.chat_download_config[key].download_filter = replace_date_time(
                value.download_filter
            )

        return True

    def assign_app_data(self, app_data: dict) -> bool:
        """Assign config from str.

        Parameters
        ----------
        app_data: dict
            application data dict

        Returns
        -------
        bool
        """
        if app_data.get("ids_to_retry"):
            if self._chat_id:
                self.chat_download_config[self._chat_id].ids_to_retry = app_data[
                    "ids_to_retry"
                ]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[
                        it
                    ] = True
                self.app_data.pop("ids_to_retry")
        else:
            if app_data.get("chat"):
                chats = app_data["chat"]
                for chat in chats:
                    if (
                        "chat_id" in chat
                        and chat["chat_id"] in self.chat_download_config
                    ):
                        chat_id = chat["chat_id"]
                        self.chat_download_config[chat_id].ids_to_retry = chat.get(
                            "ids_to_retry", []
                        )
                        for it in self.chat_download_config[chat_id].ids_to_retry:
                            self.chat_download_config[chat_id].ids_to_retry_dict[
                                it
                            ] = True
        return True

    async def upload_file(
        self,
        local_file_path: str,
        progress_callback: Callable = None,
        progress_args: tuple = (),
    ) -> bool:
        """Upload file"""

        if not self.cloud_drive_config.enable_upload_file:
            return False

        ret: bool = False
        if self.cloud_drive_config.upload_adapter == "rclone":
            ret = await CloudDrive.rclone_upload_file(
                self.cloud_drive_config,
                self.save_path,
                local_file_path,
                progress_callback,
                progress_args,
            )
        elif self.cloud_drive_config.upload_adapter == "aligo":
            ret = await self.loop.run_in_executor(
                self.executor,
                CloudDrive.aligo_upload_file(
                    self.cloud_drive_config, self.save_path, local_file_path
                ),
            )

        return ret

    def get_file_save_path(
        self, media_type: str, chat_title: str, media_datetime: str
    ) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        media_type: str
            see config.yaml media_types

        chat_title: str
            see channel or group title

        media_datetime: str
            media datetime

        Returns
        -------
        str
            file save path prefix
        """

        res: str = self.save_path
        for prefix in self.file_path_prefix:
            if prefix == "chat_title":
                res = os.path.join(res, chat_title)
            elif prefix == "media_datetime":
                res = os.path.join(res, media_datetime)
            elif prefix == "media_type":
                res = os.path.join(res, media_type)
        return res

    def get_file_name(
        self, message_id: int, file_name: Optional[str], caption: Optional[str]
    ) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        message_id: int
            Message id

        file_name: Optional[str]
            File name

        caption: Optional[str]
            Message caption

        Returns
        -------
        str
            File name
        """

        res: str = ""
        for prefix in self.file_name_prefix:
            if prefix == "message_id":
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{message_id}"
            elif prefix == "file_name" and file_name:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{file_name}"
            elif prefix == "caption" and caption:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{caption}"
        if res == "":
            res = f"{message_id}"

        return validate_title(res)

    def need_skip_message(
        self, download_config: ChatDownloadConfig, message_id: int
    ) -> bool:
        """if need skip download message.

        Parameters
        ----------
        chat_id: str
            Config.yaml defined

        message_id: int
            Readily to download message id
        Returns
        -------
        bool
        """
        if message_id in download_config.ids_to_retry_dict:
            return True

        return False

    def exec_filter(self, download_config: ChatDownloadConfig, meta_data: MetaData):
        """
        Executes the filter on the given download configuration.

        Args:
            download_config (ChatDownloadConfig): The download configuration object.
            meta_data (MetaData): The meta data object.

        Returns:
            bool: The result of executing the filter.
        """
        if download_config.download_filter:
            self.download_filter.set_meta_data(meta_data)
            return self.download_filter.exec(download_config.download_filter)

        return True

    # pylint: disable = R0912
    def update_config(self, immediate: bool = True):
        """update config

        Parameters
        ----------
        immediate: bool
            If update config immediate,default True
        """
        with self.config_lock:
            if "chat" not in self.config or self.config["chat"] is None:
                self.config["chat"] = []

            # TODO: fix this not exist chat
            if not self.app_data.get("chat") and self.config.get("chat"):
                self.app_data["chat"] = [
                    {"chat_id": i} for i in range(0, len(self.config["chat"]))
                ]
            idx = 0
            # pylint: disable = R1733
            for key, value in self.chat_download_config.items():
                if idx >= len(self.app_data["chat"]):
                    self.app_data["chat"].append({})

                if idx >= len(self.config["chat"]):
                    self.config["chat"].append(
                        {"chat_id": key, "last_read_message_id": 0}
                    )

                current_last_read_message_id = self.normalize_message_id(
                    self.config["chat"][idx].get("last_read_message_id", 0)
                )
                if value.last_read_message_id != current_last_read_message_id:
                    self.config["chat"][idx]["last_read_message_id"] = (
                        value.last_read_message_id
                    )

                self.app_data["chat"][idx]["chat_id"] = key
                self.app_data["chat"][idx]["ids_to_retry"] = []
                idx += 1

            self.config["save_path"] = self.save_path
            self.config["file_path_prefix"] = self.file_path_prefix

            if self.config.get("ids_to_retry"):
                self.config.pop("ids_to_retry")

            if self.config.get("chat_id"):
                self.config.pop("chat_id")

            if self.config.get("download_filter"):
                self.config.pop("download_filter")

            if self.config.get("last_read_message_id"):
                self.config.pop("last_read_message_id")

            self.config["language"] = self.language.name
            # for it in self.downloaded_ids:
            #    self.already_download_ids_set.add(it)

            # self.app_data["already_download_ids"] = list(self.already_download_ids_set)
            self.config["filter_advertisement_list"] = self.filter_advertisement_list
            self.config["replace_advertisement_list"] = self.replace_advertisement_list
            self.config["group_add_advertisement"] = self.group_add_advertisement

            if immediate:
                _dump_yaml_file_atomic(self.config_file, self.config)

            if immediate:
                _dump_yaml_file_atomic(self.app_data_file, self.app_data)
                if self.download_records is not None:
                    for key, value in self.chat_download_config.items():
                        self.download_records.mark_cursor_mirrored(
                            key, value.last_read_message_id
                        )
                self.config_dirty = False
                self.last_config_update_time = time.time()

    def update_config_if_due(self, force: bool = False) -> bool:
        """Persist config when cursor state changed and the throttle allows it."""
        with self.config_lock:
            if not self.config_dirty:
                return False

            if not force and time.time() - self.last_config_update_time < (
                self.config_update_interval
            ):
                return False

            self.update_config()
            return True

    def advance_last_read_message_id(self, node: TaskNode, message_id: int) -> bool:
        """Advance the in-memory cursor for a configured chat."""
        with self.config_lock:
            chat_download_config = self._get_chat_download_config(node.chat_id)
            if not chat_download_config:
                return False

            last_read_message_id = max(
                chat_download_config.last_read_message_id, message_id
            )
            if last_read_message_id == chat_download_config.last_read_message_id:
                return False

            chat_download_config.last_read_message_id = last_read_message_id
            self.config_dirty = True
            return True

    def is_chat_download_complete(self, chat_id: Union[int, str]) -> bool:
        """Return whether the current configured chat scan has finished downloads."""
        with self.config_lock:
            chat_download_config = self._get_chat_download_config(chat_id)
            if not chat_download_config:
                return False

            return (
                chat_download_config.need_check
                and chat_download_config.total_task == chat_download_config.finish_task
            )

    def set_language(self, language: Language):
        """Set Language"""
        self.language = language
        set_language(language)

    def load_config(self):
        """Load user config"""
        with open(
            os.path.join(os.path.abspath("."), self.config_file), encoding="utf-8"
        ) as f:
            config = _yaml.load(f.read())
            if config:
                self.config = config
                self.assign_config(self.config)

        if os.path.exists(os.path.join(os.path.abspath("."), self.app_data_file)):
            with open(
                os.path.join(os.path.abspath("."), self.app_data_file),
                encoding="utf-8",
            ) as f:
                app_data = _yaml.load(f.read())
                if app_data:
                    self.app_data = app_data
                    self.assign_app_data(self.app_data)

    def pre_run(self):
        """before run application do"""
        self.cloud_drive_config.pre_run()
        if not os.path.exists(self.session_file_path):
            os.makedirs(self.session_file_path)
        if not os.path.exists(self.state_file_path):
            os.makedirs(self.state_file_path)
        self.ensure_download_records()
        self.migrate_yaml_retries_to_db()
        recovered = self.ensure_download_records().recover_processing()
        if recovered:
            logger.info("Recovered {} interrupted download tasks", recovered)
        self.initialize_scan_cursors()
        set_language(self.language)

    def ensure_download_records(self) -> DownloadRecordStore:
        """Return the SQLite-backed download record store."""
        if self.download_records is None:
            self.download_records = DownloadRecordStore(self.download_record_db_path)

        return self.download_records

    def migrate_yaml_retries_to_db(self):
        """Move legacy YAML retry ids into SQLite-backed retry state."""
        store = self.ensure_download_records()
        with self.config_lock:
            for chat_id, chat_download_config in self.chat_download_config.items():
                for message_id in chat_download_config.ids_to_retry:
                    store.mark_failed(chat_id, self.normalize_message_id(message_id))

                chat_download_config.ids_to_retry = []
                chat_download_config.ids_to_retry_dict = {}

            for chat in self.app_data.get("chat", []) or []:
                if isinstance(chat, dict):
                    chat["ids_to_retry"] = []

    def initialize_scan_cursors(self):
        """Resolve producer cursors from SQLite and config mirrors."""
        store = self.ensure_download_records()
        with self.config_lock:
            for chat_id, chat_download_config in self.chat_download_config.items():
                chat_download_config.last_read_message_id = store.resolve_cursor(
                    chat_id, chat_download_config.last_read_message_id
                )

    def record_scanned_message(
        self, chat_id: Union[int, str], message_id: int
    ) -> int:
        """Persist a pending job before advancing the producer cursor."""
        next_cursor = self.ensure_download_records().enqueue_and_advance_cursor(
            chat_id, message_id
        )
        with self.config_lock:
            chat_download_config = self._get_chat_download_config(chat_id)
            if chat_download_config:
                chat_download_config.last_read_message_id = next_cursor
                self.config_dirty = True

        return next_cursor

    def claim_download_record(self) -> Optional[Dict[str, Any]]:
        """Claim one database job while rotating fairly across active chats."""
        with self.consumer_claim_lock:
            with self.config_lock:
                chat_ids = list(self.chat_download_config.keys())
            if not chat_ids:
                return None

            start = self.consumer_chat_index % len(chat_ids)
            for offset in range(len(chat_ids)):
                index = (start + offset) % len(chat_ids)
                record = self.ensure_download_records().claim_next(
                    [chat_ids[index]]
                )
                if record is not None:
                    self.consumer_chat_index = (index + 1) % len(chat_ids)
                    return record

            return None

    def is_match_advertisement(self, caption) -> bool:
        """is match advertisement

        Parameters
        ----------
        caption: str
        """
        for ad in self.filter_advertisement_list:
            if ad in caption:
                return True

        return False

    def set_caption_name(
        self, chat_id: Union[int, str], media_group_id: Optional[str], caption: str
    ):
        """set caption name map

        Parameters
        ----------
        chat_id: str
            Unique identifier for this chat.

        media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """
        if not media_group_id:
            return

        if chat_id in self.caption_name_dict:
            self.caption_name_dict[chat_id][media_group_id] = caption
        else:
            self.caption_name_dict[chat_id] = {media_group_id: caption}

    def get_caption_name(
        self, chat_id: Union[int, str], media_group_id: Optional[str]
    ) -> Optional[str]:
        """set caption name map
                media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """

        if (
            not media_group_id
            or chat_id not in self.caption_name_dict
            or media_group_id not in self.caption_name_dict[chat_id]
        ):
            return None

        return str(self.caption_name_dict[chat_id][media_group_id])

    def set_caption_entities(
        self, chat_id: Union[int, str], media_group_id: Optional[str], caption_entities
    ):
        """
        set caption entities map
        """
        if not media_group_id:
            return

        if chat_id in self.caption_entities_dict:
            self.caption_entities_dict[chat_id][media_group_id] = caption_entities
        else:
            self.caption_entities_dict[chat_id] = {media_group_id: caption_entities}

    def get_caption_entities(
        self, chat_id: Union[int, str], media_group_id: Optional[str]
    ):
        """
        get caption entities map
        """
        if (
            not media_group_id
            or chat_id not in self.caption_entities_dict
            or media_group_id not in self.caption_entities_dict[chat_id]
        ):
            return None

        return self.caption_entities_dict[chat_id][media_group_id]

    def set_download_id(
        self, node: TaskNode, message_id: int, download_status: DownloadStatus
    ):
        """Record consumer completion without modifying producer cursors."""
        if download_status is DownloadStatus.SuccessDownload:
            self.total_download_task += 1

        chat_download_config = self._get_chat_download_config(node.chat_id)
        if not chat_download_config:
            return

        chat_download_config.finish_task += 1
        if download_status is DownloadStatus.SkipDownload:
            self.record_download_skipped(node.chat_id, message_id)
        elif download_status is DownloadStatus.FailedDownload:
            self.record_download_failed(node.chat_id, message_id)

    def get_retry_message_ids(self, chat_id: Union[int, str]) -> List[int]:
        return self.ensure_download_records().get_retry_ids(chat_id)

    def is_download_success_recorded(
        self, chat_id: Union[int, str], message_id: int
    ) -> bool:
        return self.ensure_download_records().has_success(chat_id, message_id)

    def record_download_pending(self, chat_id: Union[int, str], message_id: int):
        self.ensure_download_records().mark_pending(chat_id, message_id)

    def record_download_failed(
        self,
        chat_id: Union[int, str],
        message_id: int,
        error: Optional[str] = None,
    ):
        self.ensure_download_records().mark_failed(
            chat_id,
            message_id,
            error,
            retry_delay=self.listen_interval,
        )

    def record_download_skipped(self, chat_id: Union[int, str], message_id: int):
        self.ensure_download_records().mark_skipped(chat_id, message_id)

    def record_download_success(
        self,
        chat_id: Union[int, str],
        message_id: int,
        file_name: Optional[str] = None,
        save_path: Optional[str] = None,
        media_type: Optional[str] = None,
        file_size: Optional[int] = None,
    ):
        self.ensure_download_records().mark_success(
            chat_id,
            message_id,
            file_name=file_name,
            save_path=save_path,
            media_type=media_type,
            file_size=file_size,
        )
