"""Downloads media from telegram."""
import asyncio
import logging
import os
import shutil
import time
from typing import List, Optional, Tuple, Union

import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import update_download_status
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.parallel_downloader import (
    KurigramRangeSource,
    MediaIdentity,
    ParallelDownloader,
)
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    update_cloud_upload_stat,
    upload_telegram_chat,
)
from module.web import init_web
from utils.format import truncate_filename, validate_title
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

queue: asyncio.Queue = asyncio.Queue()
RETRY_TIME_OUT = 3
CONNECTION_RESTART_WINDOW = 120
CONNECTION_RESTART_LIMIT = 3
_connection_error_times: List[float] = []

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)


def _is_connection_lost_error(error: Exception) -> bool:
    """Return whether an exception means the Telegram client should reconnect."""
    error_text = str(error).lower()
    return any(
        marker in error_text
        for marker in (
            "connection lost",
            "server disconnected",
            "socket.send",
            "error connecting to http proxy",
            "unable to connect due to network issues",
            "timed out",
            "timeout",
        )
    )


def _request_restart_for_connection_error(error: Exception):
    """Ask Docker to recreate the process after repeated Telegram link failures."""
    if not _is_connection_lost_error(error):
        return

    now = time.time()
    _connection_error_times[:] = [
        it for it in _connection_error_times if now - it <= CONNECTION_RESTART_WINDOW
    ]
    _connection_error_times.append(now)

    if (
        "connection lost" in str(error).lower()
        or len(_connection_error_times) >= CONNECTION_RESTART_LIMIT
    ):
        logger.warning(
            "Telegram connection appears broken; requesting process restart "
            "so Docker can rebuild the client session."
        )
        app.restart_program = True
        app.is_running = False


def _handle_asyncio_exception(loop, context):
    """Exit when Pyrogram's background reconnect task leaves the client broken."""
    error = context.get("exception")
    error_text = str(error).lower() if error else ""
    fatal_markers = (
        "read() called while another coroutine is already waiting for incoming data",
        "connection lost",
        "server disconnected",
        "error connecting to http proxy",
    )
    if error and any(marker in error_text for marker in fatal_markers):
        logger.error(
            "Unhandled Telegram connection failure; exiting for Docker restart: {}",
            error,
        )
        app.restart_program = True
        app.is_running = False
        os._exit(1)
        return

    loop.default_exception_handler(context)


app.loop.set_exception_handler(_handle_asyncio_exception)


def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """Check download task if finish

    Parameters
    ----------
    media_size: int
        The size of the downloaded resource
    download_path: str
        Resource download hold path
    ui_file_name: str
        Really show file name

    """
    download_size = os.path.getsize(download_path)
    if media_size == download_size:
        logger.success(f"{_t('Successfully downloaded')} - {ui_file_name}")
    else:
        logger.warning(
            f"{_t('Media downloaded with wrong size')}: "
            f"{download_size}, {_t('actual')}: "
            f"{media_size}, {_t('file name')}: {ui_file_name}"
        )
        os.remove(download_path)
        raise pyrogram.errors.exceptions.bad_request_400.BadRequest()


def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file to download path

    Parameters
    ----------
    temp_download_path: str
        Temporary download path

    download_path: str
        Download path

    """

    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """Check if message download timeout, then add message id into failed_ids

    Parameters
    ----------
    retry: int
        Retry download message times

    message_id: int
        Try to download message 's id

    """
    if retry == 2:
        return True
    return False


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)


def _should_use_parallel_download(media_size: int) -> bool:
    """Return whether this file is eligible for the opt-in canary path."""
    return bool(
        app.parallel_download_enabled
        and media_size >= app.parallel_download_min_size
    )


async def _download_to_temp(
    client: pyrogram.Client,
    message: pyrogram.types.Message,
    media,
    temp_file_name: str,
    chat_id: Union[int, str],
    progress_args: tuple,
) -> Optional[str]:
    """Use the verified parallel path when enabled, otherwise Kurigram's path."""

    async def sequential_download():
        return await client.download_media(
            message,
            file_name=temp_file_name,
            progress=update_download_status,
            progress_args=progress_args,
        )

    media_size = int(getattr(media, "file_size", 0) or 0)
    if not _should_use_parallel_download(media_size):
        return await sequential_download()

    try:
        source = KurigramRangeSource(client, media.file_id, media_size)
        identity = MediaIdentity(
            chat_id=str(chat_id),
            message_id=int(message.id),
            media_id=int(source.file_id.media_id or 0),
            dc_id=int(source.file_id.dc_id),
            file_unique_id=str(getattr(media, "file_unique_id", "") or ""),
            file_size=media_size,
        )

        async def report_progress(current: int, total: int):
            await update_download_status(current, total, *progress_args)

        downloader = ParallelDownloader(
            source,
            workers=app.parallel_download_workers,
        )
        result = await downloader.download(
            identity,
            f"{temp_file_name}.parallel.part",
            progress=report_progress,
        )
        logger.info(
            "Parallel candidate verified for message {}: {} workers, {} retries, {}",
            message.id,
            result.workers,
            result.retries,
            result.sha256,
        )
        return result.path
    except asyncio.CancelledError:
        raise
    except Exception as error:
        logger.exception(
            "Parallel candidate failed for message {}; falling back to the "
            "existing Kurigram downloader: {}",
            message.id,
            error,
        )
        return await sequential_download()


# pylint: disable = R0912


async def _get_media_meta(
    chat_id: Union[int, str],
    message: pyrogram.types.Message,
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
) -> Tuple[str, str, Optional[str]]:
    """Extract file name and file id from media object.

    Parameters
    ----------
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice]
        Media object to be extracted.
    _type: str
        Type of media object.

    Returns
    -------
    Tuple[str, str, Optional[str]]
        file_name, file_format
    """
    if _type in ["audio", "document", "video"]:
        # pylint: disable = C0301
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]  # type: ignore
    else:
        file_format = None

    file_name = None
    temp_file_name = None
    dirname = validate_title(f"{chat_id}")
    if message.chat and message.chat.title:
        dirname = validate_title(f"{message.chat.title}")

    if message.date:
        datetime_dir_name = message.date.strftime(app.date_format)
    else:
        datetime_dir_name = "0"

    if _type in ["voice", "video_note"]:
        # pylint: disable = C0209
        file_format = media_obj.mime_type.split("/")[-1]  # type: ignore
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        file_name = "{} - {}_{}.{}".format(
            message.id,
            _type,
            media_obj.date.isoformat(),  # type: ignore
            file_format,
        )
        file_name = validate_title(file_name)
        temp_file_name = os.path.join(app.temp_save_path, dirname, file_name)

        file_name = os.path.join(file_save_path, file_name)
    else:
        file_name = getattr(media_obj, "file_name", None)
        caption = getattr(message, "caption", None)

        file_name_suffix = ".unknown"
        if not file_name:
            file_name_suffix = get_extension(
                media_obj.file_id, getattr(media_obj, "mime_type", "")
            )
        else:
            # file_name = file_name.split(".")[0]
            _, file_name_without_suffix = os.path.split(os.path.normpath(file_name))
            file_name, file_name_suffix = os.path.splitext(file_name_without_suffix)
            if not file_name_suffix:
                file_name_suffix = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                )

        if caption:
            caption = validate_title(caption)
            app.set_caption_name(chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(chat_id, message.media_group_id)

        if not file_name and message.photo:
            file_name = f"{message.photo.file_unique_id}"

        gen_file_name = (
            app.get_file_name(message.id, file_name, caption) + file_name_suffix
        )

        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)

        temp_file_name = os.path.join(app.temp_save_path, dirname, gen_file_name)

        file_name = os.path.join(file_save_path, gen_file_name)
    return truncate_filename(file_name), truncate_filename(temp_file_name), file_format


async def add_download_task(
    message: pyrogram.types.Message,
    node: TaskNode,
):
    """Add Download task"""
    if message.empty:
        return False
    if not node.bot:
        app.record_download_pending(node.chat_id, message.id)
    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((message, node))
    node.total_task += 1
    return True


async def save_msg_to_file(
    app, chat_id: Union[int, str], message: pyrogram.types.Message
):
    """Write message text into file"""
    dirname = validate_title(
        message.chat.title if message.chat and message.chat.title else str(chat_id)
    )
    datetime_dir_name = message.date.strftime(app.date_format) if message.date else "0"

    file_save_path = app.get_file_save_path("msg", dirname, datetime_dir_name)
    file_name = os.path.join(
        app.temp_save_path,
        file_save_path,
        f"{app.get_file_name(message.id, None, None)}.txt",
    )

    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None

    with open(file_name, "w", encoding="utf-8") as f:
        f.write(message.text or "")

    return DownloadStatus.SuccessDownload, file_name


async def download_task(
    client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode
):
    """Download and Forward media"""

    download_status, file_name = await download_media(
        client, message, app.media_types, app.file_formats, node
    )

    if app.enable_download_txt and message.text and not message.media:
        download_status, file_name = await save_msg_to_file(app, node.chat_id, message)

    node.download_status[message.id] = download_status
    file_size = os.path.getsize(file_name) if file_name else 0
    if not node.bot:
        if download_status is DownloadStatus.SuccessDownload and file_name:
            app.record_download_success(
                node.chat_id,
                message.id,
                file_name=os.path.basename(file_name),
                save_path=file_name.replace("\\", "/"),
                media_type=str(message.media or ""),
                file_size=file_size,
            )
        elif download_status is DownloadStatus.FailedDownload:
            app.record_download_failed(node.chat_id, message.id)

    if not node.bot:
        app.set_download_id(node, message.id, download_status)

    if download_status is DownloadStatus.SuccessDownload and file_name:
        app.record_download_history(
            {
                "chat": node.chat_id,
                "id": message.id,
                "filename": os.path.basename(file_name),
                "total_size": file_size,
                "save_path": file_name.replace("\\", "/"),
            }
        )

    await upload_telegram_chat(
        client,
        node.upload_user if node.upload_user else client,
        app,
        node,
        message,
        download_status,
        file_name,
    )

    # rclone upload
    if (
        not node.upload_telegram_chat_id
        and download_status is DownloadStatus.SuccessDownload
    ):
        ui_file_name = file_name
        if app.hide_file_name:
            ui_file_name = f"****{os.path.splitext(file_name)[-1]}"
        if await app.upload_file(
            file_name, update_cloud_upload_stat, (node, message.id, ui_file_name)
        ):
            node.upload_success_count += 1

    await report_bot_download_status(
        node.bot,
        node,
        download_status,
        file_size,
    )


def _set_message_metadata(message, node: TaskNode) -> MetaData:
    """Build filter metadata in the consumer after it fetches the message."""
    meta_data = MetaData()
    caption = message.caption
    if caption:
        caption = validate_title(caption)
        app.set_caption_name(node.chat_id, message.media_group_id, caption)
        app.set_caption_entities(
            node.chat_id, message.media_group_id, message.caption_entities
        )
    else:
        caption = app.get_caption_name(node.chat_id, message.media_group_id)
    set_meta_data(meta_data, message, caption)
    return meta_data


async def consume_database_task(
    client: pyrogram.Client, record: dict
) -> bool:
    """Fetch and process one job previously claimed from SQLite."""
    chat_id = app.normalize_chat_id(record.get("chat_id"))
    message_id = app.normalize_message_id(record.get("message_id"))
    chat_download_config = app._get_chat_download_config(chat_id)
    if not chat_download_config:
        app.record_download_skipped(chat_id, message_id)
        return False

    node = chat_download_config.node
    if not node or app._chat_id_key(node.chat_id) != app._chat_id_key(chat_id):
        node = TaskNode(
            chat_id=chat_id,
            upload_telegram_chat_id=chat_download_config.upload_telegram_chat_id,
        )
        chat_download_config.node = node

    try:
        message = await client.get_messages(
            chat_id=chat_id, message_ids=message_id
        )
        if isinstance(message, list):
            message = message[0] if message else None
        if not message or message.empty:
            app.record_download_skipped(chat_id, message_id)
            return False

        meta_data = _set_message_metadata(message, node)
        if not app.exec_filter(chat_download_config, meta_data):
            node.download_status[message.id] = DownloadStatus.SkipDownload
            app.set_download_id(node, message.id, DownloadStatus.SkipDownload)
            return True

        node.total_task += 1
        node.download_status[message.id] = DownloadStatus.Downloading
        await download_task(client, message, node)
        return True
    except Exception as error:
        logger.exception(
            "Database download task failed for chat={} message={}: {}",
            chat_id,
            message_id,
            error,
        )
        app.record_download_failed(chat_id, message_id, str(error))
        _request_restart_for_connection_error(error)
        return False


# pylint: disable = R0915,R0914


@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    """
    Download media from Telegram.

    Each of the files to download are retried 3 times with a
    delay of 5 seconds each.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Current message id.
    """

    # pylint: disable = R0912

    file_name: str = ""
    ui_file_name: str = ""
    task_start_time: float = time.time()
    media_size = 0
    _media = None
    message = await fetch_message(client, message)
    try:
        for _type in media_types:
            _media = getattr(message, _type, None)
            if _media is None:
                continue
            file_name, temp_file_name, file_format = await _get_media_meta(
                node.chat_id, message, _media, _type
            )
            media_size = getattr(_media, "file_size", 0)

            ui_file_name = file_name
            if app.hide_file_name:
                ui_file_name = f"****{os.path.splitext(file_name)[-1]}"

            if _can_download(_type, file_formats, file_format):
                if app.is_download_success_recorded(node.chat_id, message.id):
                    logger.info(
                        f"id={message.id} {ui_file_name} "
                        "already recorded as downloaded, download skipped.\n"
                    )
                    return DownloadStatus.SkipDownload, None

                if _is_exist(file_name):
                    file_size = os.path.getsize(file_name)
                    if file_size or file_size == media_size:
                        app.record_download_success(
                            node.chat_id,
                            message.id,
                            file_name=os.path.basename(file_name),
                            save_path=file_name.replace("\\", "/"),
                            media_type=_type,
                            file_size=file_size,
                        )
                        logger.info(
                            f"id={message.id} {ui_file_name} "
                            f"{_t('already download,download skipped')}.\n"
                        )

                        return DownloadStatus.SkipDownload, None
            else:
                return DownloadStatus.SkipDownload, None

            break
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.FailedDownload, None
    if _media is None:
        return DownloadStatus.SkipDownload, None

    message_id = message.id

    for retry in range(3):
        try:
            temp_download_path = await _download_to_temp(
                client,
                message,
                _media,
                temp_file_name,
                node.chat_id,
                (
                    message_id,
                    ui_file_name,
                    task_start_time,
                    node,
                    client,
                ),
            )

            if temp_download_path and isinstance(temp_download_path, str):
                _check_download_finish(media_size, temp_download_path, ui_file_name)
                await asyncio.sleep(0.5)
                _move_to_download_path(temp_download_path, file_name)
                # TODO: if not exist file size or media
                return DownloadStatus.SuccessDownload, file_name
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"Message[{message.id}]: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message.id):
                # pylint: disable = C0301
                logger.error(
                    f"Message[{message.id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning("Message[{}]: FlowWait {}", message.id, wait_err.value)
            _check_timeout(retry, message.id)
        except TypeError:
            # pylint: disable = C0301
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{message.id}], "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                f"Message[{message.id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    return DownloadStatus.FailedDownload, None


def _load_config():
    """Load config"""
    app.load_config()


def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    try:
        _load_config()
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
        )
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False

    return True


async def worker(client: pyrogram.client.Client):
    """Consume bot memory jobs first, then configured jobs from SQLite."""
    while app.is_running:
        message = None
        node = None
        try:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                record = app.claim_download_record()
                if record is None:
                    await asyncio.sleep(1)
                    continue
                await consume_database_task(client, record)
                continue

            message = item[0]
            node = item[1]

            if node.is_stop_transmission:
                continue

            if node.client:
                await download_task(node.client, message, node)
            else:
                await download_task(client, message, node)
        except Exception as e:
            logger.exception(f"{e}")
            if message and node and not node.bot:
                try:
                    node.download_status[message.id] = DownloadStatus.FailedDownload
                    app.record_download_failed(node.chat_id, message.id, str(e))
                    app.set_download_id(node, message.id, DownloadStatus.FailedDownload)
                except Exception as update_e:
                    logger.exception(
                        "Failed to record failed task status: {}", update_e
                    )
            _request_restart_for_connection_error(e)


async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    """Download all task"""
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=node.end_offset_id,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )

    chat_download_config.node = node

    if not node.bot:
        async for message in messages_iter:  # type: ignore
            app.record_scanned_message(node.chat_id, message.id)
            node.total_task += 1
            app.update_config_if_due()

        chat_download_config.need_check = True
        chat_download_config.total_task = node.total_task
        node.is_running = True
        app.update_config_if_due(force=True)
        return

    async for message in messages_iter:  # type: ignore
        meta_data = _set_message_metadata(message, node)

        if app.exec_filter(chat_download_config, meta_data):
            await add_download_task(message, node)
        else:
            node.download_status[message.id] = DownloadStatus.SkipDownload
            if message.media_group_id:
                await upload_telegram_chat(
                    client,
                    node.upload_user,
                    app,
                    node,
                    message,
                    DownloadStatus.SkipDownload,
                )

    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True


async def download_all_chat(client: pyrogram.Client):
    """Download All chat"""
    with app.config_lock:
        chat_download_items = list(app.chat_download_config.items())

    scanned_chat_ids = []
    for key, value in chat_download_items:
        with app.config_lock:
            value = app.chat_download_config.get(key)
            if not value:
                continue
            value.total_task = 0
            value.finish_task = 0
            value.need_check = False
            value.node = TaskNode(chat_id=key)

        scanned_chat_ids.append(key)
        app.record_chat_scan_status(key, "scanning")
        try:
            await download_chat_task(client, value, value.node)
            app.record_chat_scan_status(key, "ok")
        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
            app.record_chat_scan_status(key, "error", str(e))
            _request_restart_for_connection_error(e)
        finally:
            value.need_check = True

        if app.restart_program or not app.is_running:
            break

    return scanned_chat_ids


async def run_until_all_task_finish(chat_ids=None):
    """Normal download"""
    wait_chat_keys = {str(chat_id) for chat_id in chat_ids} if chat_ids else None
    last_wait_log_time = 0.0
    while True:
        finish: bool = True
        unfinished_items = []
        with app.config_lock:
            chat_download_items = list(app.chat_download_config.items())
            for key, value in chat_download_items:
                if wait_chat_keys is not None and str(key) not in wait_chat_keys:
                    continue

                if not value.need_check or value.total_task != value.finish_task:
                    finish = False
                    unfinished_items.append(
                        f"{key}: {value.finish_task}/{value.total_task}"
                    )

        if finish or app.restart_program or not app.is_running:
            break

        now = time.time()
        if now - last_wait_log_time > 60:
            logger.warning(
                "Waiting for download tasks to finish: {}",
                ", ".join(unfinished_items),
            )
            last_wait_log_time = now

        await asyncio.sleep(1)


async def monitor_downloads(client: pyrogram.Client):
    """Continuously produce configured-chat jobs without waiting for consumers."""
    while app.is_running and not app.restart_program:
        logger.info("Starting configured chat scan")
        await download_all_chat(client)
        app.update_config_if_due(force=True)

        if app.restart_program or not app.is_running:
            break

        logger.info(
            f"Current scan finished, next scan in {app.listen_interval} seconds"
        )
        await asyncio.sleep(app.listen_interval)


async def start_download_bot_safely(client: pyrogram.Client):
    """Start the optional download bot without blocking configured chat scans."""
    try:
        await start_download_bot(app, client, add_download_task, download_chat_task)
    except Exception as e:
        logger.exception("Download bot startup failed: {}", e)


def _exec_loop(client: pyrogram.Client):
    """Exec loop"""

    app.loop.run_until_complete(monitor_downloads(client))


async def start_server(client: pyrogram.Client):
    """
    Start the server using the provided client.
    """
    await client.start()


async def stop_server(client: pyrogram.Client):
    """
    Stop the server using the provided client.
    """
    await client.stop()


def main():
    """Main function of the downloader."""
    tasks = []
    client = HookClient(
        "media_downloader",
        api_id=app.api_id,
        api_hash=app.api_hash,
        proxy=app.proxy,
        workdir=app.session_file_path,
        start_timeout=app.start_timeout,
        no_updates=True,
    )
    try:
        app.pre_run()
        init_web(app, client)

        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)

        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))

        for _ in range(app.max_download_task):
            task = app.loop.create_task(worker(client))
            tasks.append(task)

        if app.bot_token:
            task = app.loop.create_task(start_download_bot_safely(client))
            tasks.append(task)
        _exec_loop(client)
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except Exception as e:
        logger.exception("{}", e)
    finally:
        app.is_running = False
        if app.bot_token:
            app.loop.run_until_complete(stop_download_bot())
        app.loop.run_until_complete(stop_server(client))
        for task in tasks:
            task.cancel()
        logger.info(_t("Stopped!"))
        # check_for_updates(app.proxy)
        logger.info(f"{_t('update config')}......")
        app.update_config()
        logger.success(
            f"{_t('Updated last read message_id to config file')},"
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )


if __name__ == "__main__":
    if _check_config():
        main()
