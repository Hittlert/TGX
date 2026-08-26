"""web ui for media download"""

import asyncio
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from pyrogram import raw, types as pyrogram_types, utils as pyrogram_utils
from flask import Flask, jsonify, render_template, request
from flask_login import LoginManager, UserMixin, login_required, login_user

import utils
from module.app import Application
from module.download_stat import (
    DownloadState,
    get_current_download_speed,
    get_download_result,
    get_download_state,
    get_total_download_speed,
    set_download_state,
)
from utils.crypto import AesBase64
from utils.format import format_byte

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

_flask_app = Flask(__name__)

_flask_app.secret_key = "tdl"
_login_manager = LoginManager()
_login_manager.login_view = "login"
_login_manager.init_app(_flask_app)
web_login_users: dict = {}
deAesCrypt = AesBase64("1234123412ABCDEF", "ABCDEF1234123412")
_web_app: Optional[Application] = None
_telegram_client = None
DIALOG_PAGE_LIMIT = 100
DIALOG_REFRESH_TIMEOUT = 180
TARGET_PRIORITY_MIN = -1000000
TARGET_PRIORITY_MAX = 1000000


class User(UserMixin):
    """Web Login User"""

    def __init__(self):
        self.sid = "root"

    @property
    def id(self):
        """ID"""
        return self.sid


@_login_manager.user_loader
def load_user(_):
    """
    Load a user object from the user ID.

    Returns:
        User: The user object.
    """
    return User()


def get_flask_app() -> Flask:
    """get flask app instance"""
    return _flask_app


def run_web_server(app: Application):
    """
    Runs a web server using the Flask framework.
    """

    get_flask_app().run(
        app.web_host, app.web_port, debug=app.debug_web, use_reloader=False
    )


# pylint: disable = W0603
def init_web(app: Application, client=None):
    """
    Set the value of the users variable.

    Args:
        users: The list of users to set.

    Returns:
        None.
    """
    global web_login_users
    global _web_app
    global _telegram_client
    _web_app = app
    _telegram_client = client
    if app.web_login_secret:
        web_login_users = {"root": app.web_login_secret}
    else:
        _flask_app.config["LOGIN_DISABLED"] = True
    if app.debug_web:
        threading.Thread(target=run_web_server, args=(app,)).start()
    else:
        threading.Thread(
            target=get_flask_app().run, daemon=True, args=(app.web_host, app.web_port)
        ).start()


def set_telegram_client(client):
    """Set Telegram client for web API."""
    global _telegram_client
    _telegram_client = client


def _chat_title(chat) -> str:
    title = getattr(chat, "title", None)
    if title:
        return title

    name_parts = [
        getattr(chat, "first_name", None),
        getattr(chat, "last_name", None),
    ]
    title = " ".join([it for it in name_parts if it])
    if title:
        return title

    username = getattr(chat, "username", None)
    if username:
        return f"@{username}"

    return str(getattr(chat, "id", ""))


def _chat_type(chat) -> str:
    chat_type = getattr(chat, "type", "")
    chat_type_name = getattr(chat_type, "name", str(chat_type))
    return chat_type_name.replace("ChatType.", "").lower()


def _get_chat_message_id(dialog) -> int:
    top_message = getattr(dialog, "top_message", None)
    if top_message:
        return int(getattr(top_message, "id", 0) or 0)

    return 0


def _target_match_keys(target: Dict[str, Any]) -> List[str]:
    keys = []
    for field in ("chat_id", "peer_id", "target_chat_id", "source_chat_id"):
        value = str(target.get(field, "") or "").strip()
        if value:
            keys.append(value.casefold())
            if value.startswith("-100") and len(value) > 4:
                keys.append(value[4:].casefold())
            elif value.isdigit():
                keys.append(f"-100{value}".casefold())
    username = str(target.get("username", "") or "").strip().lstrip("@")
    if username:
        keys.extend([username.casefold(), f"@{username}".casefold()])
    return keys


def _dialog_match_keys(dialog: Dict[str, Any]) -> List[str]:
    if not isinstance(dialog, dict):
        return []

    chat_id = dialog.get("chat_id", "")
    keys = []
    if str(chat_id):
        chat_id_str = str(chat_id).strip()
        keys.append(chat_id_str.casefold())
        if chat_id_str.startswith("-100") and len(chat_id_str) > 4:
            keys.append(chat_id_str[4:].casefold())
        elif chat_id_str.isdigit():
            keys.append(f"-100{chat_id_str}".casefold())
    username = dialog.get("username")
    if username:
        username = str(username).strip().lstrip("@")
        keys.extend([username.casefold(), f"@{username}".casefold()])
    return keys


def _target_priority(value: Any) -> int:
    try:
        priority = int(value or 0)
    except (TypeError, ValueError) as error:
        raise ValueError("target priority must be an integer") from error
    if priority < TARGET_PRIORITY_MIN or priority > TARGET_PRIORITY_MAX:
        raise ValueError(
            f"target priority must be between {TARGET_PRIORITY_MIN} "
            f"and {TARGET_PRIORITY_MAX}"
        )
    return priority


def _matching_target(
    target: Dict[str, Any], candidates: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    keys = set(_target_match_keys(target))
    for candidate in candidates:
        if keys.intersection(_target_match_keys(candidate)):
            return candidate
    return None


def _merge_control_priorities(
    targets: List[Dict[str, Any]], control_targets: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Overlay Go-owned runtime target state onto editable web configuration."""
    merged = []
    for target in targets:
        item = dict(target)
        control_target = _matching_target(item, control_targets)
        source = control_target if control_target is not None else item
        try:
            item["priority"] = _target_priority(source.get("priority", 0))
        except ValueError:
            item["priority"] = 0
        if control_target is not None:
            if "last_read_message_id" in control_target:
                try:
                    cursor = int(control_target["last_read_message_id"])
                except (TypeError, ValueError):
                    pass
                else:
                    if cursor >= 0:
                        item["last_read_message_id"] = cursor
            scan_status = control_target.get("scan_status")
            if isinstance(scan_status, dict):
                item["scan_status"] = dict(scan_status)
        merged.append(item)
    return merged


def _sync_control_targets(targets: List[Dict[str, Any]]):
    if not _web_app or not getattr(_web_app, "tdl_client", None):
        return

    selected = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        item = dict(target)
        try:
            item["priority"] = _target_priority(item.get("priority", 0))
        except ValueError:
            item["priority"] = 0
        selected.append(item)

    client = _web_app.tdl_client
    try:
        current_targets = client.get_control_targets()
    except Exception as error:
        log.warning("Could not read Go target priorities: %s", error)
        return

    matched = set()
    for current in current_targets:
        selected_target = _matching_target(current, selected)
        payload = {
            "chat_id": str(current.get("chat_id")),
            "enabled": selected_target is not None,
        }
        if selected_target is not None:
            matched.add(id(selected_target))
            if selected_target.get("_priority_dirty"):
                payload["priority"] = selected_target["priority"]
            if selected_target.get("_cursor_dirty"):
                payload["last_read_message_id"] = int(
                    selected_target.get("last_read_message_id", 0) or 0
                )
            if selected_target.get("_filter_dirty"):
                payload["download_filter"] = str(
                    selected_target.get("download_filter", "") or ""
                )
        try:
            client.update_control_target(payload)
        except Exception as error:
            log.warning("Could not update Go control target %s: %s", current.get("chat_id"), error)

    refreshed_cache = False
    for selected_target in selected:
        if id(selected_target) in matched:
            continue
        target_chat_id = str(
            selected_target.get("chat_id")
            or selected_target.get("source_chat_id")
            or ""
        ).strip()
        if not target_chat_id:
            continue

        new_payload = {
            "chat_id": target_chat_id,
            "enabled": True,
            "last_read_message_id": int(
                selected_target.get("last_read_message_id", 0) or 0
            ),
            "download_filter": str(
                selected_target.get("download_filter", "") or ""
            ),
            "priority": selected_target["priority"],
        }
        try:
            client.update_control_target(new_payload)
        except Exception as initial_error:
            if not refreshed_cache and hasattr(client, "refresh_dialogs"):
                try:
                    client.refresh_dialogs()
                    refreshed_cache = True
                    client.update_control_target(new_payload)
                    continue
                except Exception:
                    pass
            log.warning("Could not sync new control target %s: %s", target_chat_id, initial_error)


def _filter_dialogs(items: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    query = query.strip().lower()
    if not query:
        return items

    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        scan_status = item.get("scan_status", {})
        if not isinstance(scan_status, dict):
            scan_status = {}
        search_text = " ".join(
            [
                str(item.get("chat_id", "")),
                str(item.get("title", "")),
                str(item.get("username", "")),
                str(item.get("type", "")),
                str(scan_status.get("status", "")),
                str(scan_status.get("error", "")),
            ]
        ).lower()
        if query in search_text:
            result.append(item)

    return result


def _decorate_dialog_with_target(
    dialog: Dict[str, Any], target_map: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    if not isinstance(dialog, dict):
        return {}

    decorated_dialog = dict(dialog)
    target = None
    for key in _dialog_match_keys(dialog):
        target = target_map.get(key)
        if target:
            break

    configured_title = ""
    if target:
        configured_title = str(target.get("title", "") or "").strip()

    decorated_dialog["enabled"] = target is not None
    if configured_title:
        decorated_dialog["title"] = configured_title
    decorated_dialog["last_read_message_id"] = (
        target.get("last_read_message_id", 0) if target else 0
    )
    decorated_dialog["download_filter"] = (
        target.get("download_filter", "") if target else ""
    )
    decorated_dialog["priority"] = target.get("priority", 0) if target else 0
    decorated_dialog["upload_telegram_chat_id"] = (
        target.get("upload_telegram_chat_id", "") if target else ""
    )
    decorated_dialog["scan_status"] = target.get("scan_status", {}) if target else {}
    decorated_dialog["target_chat_id"] = target.get("chat_id", "") if target else ""
    return decorated_dialog


def _dialog_from_chat(chat, top_message_id: int = 0) -> Dict[str, Any]:
    return {
        "chat_id": getattr(chat, "id", ""),
        "title": _chat_title(chat),
        "username": getattr(chat, "username", "") or "",
        "type": _chat_type(chat),
        "top_message_id": top_message_id,
    }


def _telegram_timestamp(value: Any) -> int:
    if not value:
        return 0
    if isinstance(value, (int, float)):
        return int(value)

    return pyrogram_utils.datetime_to_timestamp(value)


async def _resolve_dialog_offset_peer(result, peer_id: int):
    """Cache page peers before resolving the next Telegram dialog offset."""
    try:
        await _telegram_client.fetch_peers(list(result.users) + list(result.chats))
        return await _telegram_client.resolve_peer(peer_id)
    except Exception as error:
        raise RuntimeError("Could not continue Telegram dialog pagination") from error


def _dialog_offset_candidate(chat, top_message):
    """Return a safe Telegram dialogs offset from a parsed dialog."""
    chat_id = getattr(chat, "id", None)
    message_id = getattr(top_message, "id", None)
    message_date = getattr(top_message, "date", None)
    if not chat_id or not message_id or not message_date:
        return None

    return (
        int(message_id),
        _telegram_timestamp(message_date),
        int(chat_id),
    )


async def _load_dialogs() -> List[Dict[str, Any]]:
    dialogs = []
    seen_dialog_keys = set()
    offset_date = 0
    offset_id = 0
    offset_peer = raw.types.InputPeerEmpty()
    seen_offsets = set()

    while True:
        result = await _telegram_client.invoke(
            raw.functions.messages.GetDialogs(
                offset_date=offset_date,
                offset_id=offset_id,
                offset_peer=offset_peer,
                limit=DIALOG_PAGE_LIMIT,
                hash=0,
            ),
            sleep_threshold=60,
        )

        users = {item.id: item for item in result.users}
        chats = {item.id: item for item in result.chats}
        messages = {}

        for message in result.messages:
            if isinstance(message, raw.types.MessageEmpty):
                continue

            chat_id = pyrogram_utils.get_peer_id(message.peer_id)
            try:
                messages[chat_id] = await pyrogram_types.Message._parse(
                    _telegram_client, message, users, chats
                )
            except (AttributeError, KeyError, TypeError):
                continue

        offset_candidate = None
        for raw_dialog in result.dialogs:
            if not isinstance(raw_dialog, raw.types.Dialog):
                continue

            try:
                parsed_dialog = pyrogram_types.Dialog._parse(
                    _telegram_client, raw_dialog, messages, users, chats
                )
            except (AttributeError, KeyError, TypeError):
                continue

            chat = getattr(parsed_dialog, "chat", None)
            if not chat or not getattr(chat, "id", None):
                continue

            top_message = getattr(parsed_dialog, "top_message", None)
            top_message_id = getattr(top_message, "id", None) or getattr(
                raw_dialog, "top_message", 0
            )
            candidate = _dialog_offset_candidate(chat, top_message)
            if candidate:
                offset_candidate = candidate
            dialog_key = str(chat.id)
            if dialog_key in seen_dialog_keys:
                continue
            seen_dialog_keys.add(dialog_key)
            dialogs.append(_dialog_from_chat(chat, int(top_message_id or 0)))

        if len(result.dialogs) < DIALOG_PAGE_LIMIT:
            return dialogs

        if not offset_candidate:
            raise RuntimeError("Could not determine the next Telegram dialog page")

        offset_key = tuple(offset_candidate)
        if offset_key in seen_offsets:
            raise RuntimeError("Telegram dialog pagination repeated an offset")
        seen_offsets.add(offset_key)

        offset_id, offset_date, offset_peer_id = offset_candidate
        offset_peer = await _resolve_dialog_offset_peer(result, offset_peer_id)

    return dialogs


def _normalized_target_chat_id(item: Dict[str, Any]) -> Any:
    chat_id = item.get("chat_id", "")
    chat_type = str(item.get("type", "") or "").lower()
    username = str(item.get("username", "") or "").strip().lstrip("@")

    if username and chat_type in {"bot", "private", "user"}:
        return f"@{username}"

    normalized_chat_id = Application.normalize_chat_id(chat_id)
    if (
        isinstance(normalized_chat_id, int)
        and normalized_chat_id > 0
        and chat_type in {"channel", "supergroup"}
    ):
        return int(f"-100{normalized_chat_id}")

    return normalized_chat_id


async def _resolve_save_target(item: Dict[str, Any]) -> Dict[str, Any]:
    resolved_item = dict(item)
    resolved_item["source_chat_id"] = item.get("source_chat_id", item.get("chat_id"))
    resolved_item["chat_id"] = _normalized_target_chat_id(item)

    chat_id = resolved_item.get("chat_id", "")
    if not _telegram_client or not isinstance(chat_id, int) or chat_id <= 0:
        return resolved_item

    candidates = [chat_id]
    chat_type = str(item.get("type", "") or "").lower()
    if chat_type in {"", "channel", "supergroup"}:
        candidates.append(int(f"-100{chat_id}"))

    for candidate in candidates:
        try:
            chat = await _telegram_client.get_chat(candidate)
        except Exception:
            continue

        dialog = _dialog_from_chat(chat)
        username = dialog.get("username", "")
        resolved_type = str(dialog.get("type", "") or "").lower()
        if username and resolved_type in {"bot", "private", "user"}:
            resolved_item["chat_id"] = f"@{username}"
        else:
            resolved_item["chat_id"] = dialog.get("chat_id", candidate)
        resolved_item["title"] = resolved_item.get("title") or dialog.get("title", "")
        resolved_item["username"] = username
        resolved_item["type"] = resolved_type
        return resolved_item

    return resolved_item


async def _resolve_save_targets(targets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    resolved_targets = []
    for target in targets:
        if isinstance(target, dict):
            resolved_targets.append(await _resolve_save_target(target))

    return resolved_targets


async def _resolve_missing_targets(
    known_keys: set, targets: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    items = []
    for target in targets:
        if any(key in known_keys for key in _target_match_keys(target)):
            continue

        chat_id = target["chat_id"]
        chat_ids = [chat_id]
        if isinstance(chat_id, str) and chat_id and not chat_id.startswith("@"):
            chat_ids.append(f"@{chat_id}")

        chat = None
        for current_chat_id in chat_ids:
            try:
                chat = await _telegram_client.get_chat(current_chat_id)
                break
            except Exception:
                continue

        if not chat:
            continue

        try:
            item = _dialog_from_chat(chat)
        except Exception:
            continue
        items.append(item)
        known_keys.update(_dialog_match_keys(item))

    return items


async def _resolve_input_target(query: str) -> Dict[str, Any]:
    """Resolve a chat ID, @username, or t.me link into a dialog object."""
    if not _telegram_client:
        raise RuntimeError("Telegram client is not ready")

    query = str(query or "").strip()
    if not query:
        raise ValueError("Query cannot be empty")

    parsed_chat_id = None
    if "t.me/" in query or "telegram.me/" in query or query.startswith("http"):
        from utils.format import extract_info_from_link
        link_info = extract_info_from_link(query)
        if link_info.group_id:
            parsed_chat_id = link_info.group_id

    chat_identifier = parsed_chat_id if parsed_chat_id is not None else query

    try:
        if isinstance(chat_identifier, str):
            clean_str = chat_identifier.strip()
            if (clean_str.startswith("-") and clean_str[1:].isdigit()) or clean_str.isdigit():
                chat_identifier = int(clean_str)
    except ValueError:
        pass

    candidates = [chat_identifier]
    if isinstance(chat_identifier, int) and chat_identifier > 0:
        candidates.append(int(f"-100{chat_identifier}"))

    chat = None
    last_error = None
    for candidate in candidates:
        try:
            chat = await _telegram_client.get_chat(candidate)
            if chat:
                break
        except Exception as err:
            last_error = err
            continue

    if not chat:
        raise RuntimeError(f"Could not find or access chat: {last_error or 'Not found'}")

    return _dialog_from_chat(chat)


def _run_on_app_loop(coro, timeout=60):
    if not _web_app:
        raise RuntimeError("web application is not initialized")

    future = asyncio.run_coroutine_threadsafe(coro, _web_app.loop)
    try:
        return future.result(timeout=timeout)
    except TimeoutError as error:
        future.cancel()
        raise TimeoutError(
            f"Telegram request timed out after {timeout} seconds"
        ) from error


@_flask_app.route("/login", methods=["GET", "POST"])
def login():
    """
    Function to handle the login route.

    Parameters:
    - No parameters

    Returns:
    - If the request method is "POST" and the username and
      password match the ones in the web_login_users dictionary,
      it returns a JSON response with a code of "1".
    - Otherwise, it returns a JSON response with a code of "0".
    - If the request method is not "POST", it returns the rendered "login.html" template.
    """
    if request.method == "POST":
        username = "root"
        web_login_form = {}
        for key, value in request.form.items():
            if value:
                value = deAesCrypt.decrypt(value)
            web_login_form[key] = value

        if not web_login_form.get("password"):
            return jsonify({"code": "0"})

        password = web_login_form["password"]
        if username in web_login_users and web_login_users[username] == password:
            user = User()
            login_user(user)
            return jsonify({"code": "1"})

        return jsonify({"code": "0"})

    return render_template("login.html")


@_flask_app.route("/")
@login_required
def index():
    """Index html"""
    download_action = (
        "pause" if get_download_state() is DownloadState.Downloading else "continue"
    )
    if _web_app and _web_app.download_backend == "tdl":
        status = _web_app.get_media_pool_status()
        if status.get("online"):
            download_action = "continue" if status.get("paused") else "pause"
    return render_template(
        "index.html",
        download_state=download_action,
    )


@_flask_app.route("/get_download_status")
@login_required
def get_download_speed():
    """Get download speed"""
    media_pool = (
        _web_app.get_media_pool_status()
        if _web_app is not None
        else {
            "enabled": False,
            "mode": "off",
            "desired": 0,
            "live": 0,
            "active_slots": 0,
            "hard_limit": 48,
            "pipeline_depth": 1,
            "last_scale_reason": "disabled",
            "used": 0,
            "active_files": 0,
            "rolling_5s_bps": 0.0,
            "files": {},
        }
    )
    current_speed = get_total_download_speed()
    if media_pool.get("mode") == "tdl":
        current_speed = (
            int(media_pool.get("rolling_5s_bps", 0) or 0)
            if media_pool.get("enabled") and media_pool.get("online")
            else 0
        )
    elif media_pool.get("enabled") and media_pool.get("mode") == "per_file":
        current_speed = int(media_pool.get("rolling_5s_bps", 0) or 0)
    return jsonify(
        {
            "download_speed": format_byte(current_speed) + "/s",
            "upload_speed": "0.00 B/s",
            "media_pool": media_pool,
        }
    )


@_flask_app.route("/set_download_state", methods=["POST"])
@login_required
def web_set_download_state():
    """Set download state"""
    state = request.args.get("state")

    try:
        if (
            _web_app
            and _web_app.download_backend == "tdl"
            and state
            in (
                "continue",
                "pause",
            )
        ):
            if _web_app.tdl_client is None:
                raise RuntimeError("tdl daemon client is not configured")
            paused = state == "pause"
            _web_app.tdl_client.set_paused(paused)
            set_download_state(
                DownloadState.StopDownload if paused else DownloadState.Downloading
            )
            return "continue" if paused else "pause"

        if state == "continue" and get_download_state() is DownloadState.StopDownload:
            set_download_state(DownloadState.Downloading)
            return "pause"

        if state == "pause" and get_download_state() is DownloadState.Downloading:
            set_download_state(DownloadState.StopDownload)
            return "continue"
    except Exception as error:
        return jsonify({"ok": False, "error": str(error)}), 503

    return state


@_flask_app.route("/get_app_version")
def get_app_version():
    """Get telegram_media_downloader version"""
    return utils.__version__


def _matching_file_pool_speed(
    media_pool: Optional[dict], chat_id: Any, message_id: Any
) -> Optional[int]:
    if not media_pool:
        return None
    if media_pool.get("mode") == "tdl":
        task_id = f"{chat_id}:{message_id}"
        status = media_pool.get("files", {}).get(task_id)
        if status is None:
            status = next(
                (
                    item
                    for item in media_pool.get("files", {}).values()
                    if str(item.get("peer", "")) == str(chat_id)
                    and str(item.get("message_id", "")) == str(message_id)
                ),
                None,
            )
        if status is None:
            return None
        return max(int(status.get("rolling_5s_bps", 0) or 0), 0)
    if media_pool.get("mode") != "per_file":
        return None
    prefix = f"{chat_id}:{message_id}:"
    for pool_id, status in media_pool.get("files", {}).items():
        if str(pool_id).startswith(prefix):
            return max(int(status.get("rolling_5s_bps", 0) or 0), 0)
    return None


def _download_item_from_stat(
    chat_id: Any,
    message_id: Any,
    value: dict,
    media_pool: Optional[dict] = None,
) -> dict:
    total_size = int(value.get("total_size", 0) or 0)
    down_byte = int(value.get("down_byte", 0) or 0)
    download_speed = get_current_download_speed(value)
    pool_speed = _matching_file_pool_speed(media_pool, chat_id, message_id)
    if pool_speed is not None:
        download_speed = pool_speed
    file_name = value.get("file_name") or ""
    progress = 100 if total_size == 0 and down_byte == 0 else 0
    if total_size:
        progress = round(down_byte / total_size * 100, 1)

    return {
        "chat": f"{chat_id}",
        "id": f"{message_id}",
        "filename": os.path.basename(file_name),
        "total_size": format_byte(total_size),
        "download_progress": f"{progress}",
        "download_speed": format_byte(download_speed) + "/s",
        "save_path": file_name.replace("\\", "/"),
        "_raw_total_size": total_size,
    }


def _download_item_from_history(item: dict) -> dict:
    return {
        "chat": f"{item.get('chat', '')}",
        "id": f"{item.get('id', '')}",
        "filename": item.get("filename", ""),
        "total_size": format_byte(int(item.get("total_size", 0) or 0)),
        "download_progress": "100",
        "download_speed": "0.00 B/s",
        "save_path": item.get("save_path", ""),
    }


def _download_item_from_tdl_task(task: dict) -> dict:
    total_size = int(task.get("total_size", task.get("expected_size", 0)) or 0)
    downloaded = int(task.get("downloaded", 0) or 0)
    progress = task.get("progress")
    if progress is None:
        progress = round(downloaded / total_size * 100, 1) if total_size else 0
    progress = min(max(float(progress or 0), 0.0), 100.0)
    final_path = str(task.get("final_path", "") or "").replace("\\", "/")
    file_name = str(task.get("file_name", "") or "")
    return {
        "chat": str(task.get("peer", "")),
        "id": str(task.get("message_id", "")),
        "filename": file_name or os.path.basename(final_path),
        "total_size": format_byte(total_size),
        "download_progress": str(progress),
        "download_speed": format_byte(max(int(task.get("rolling_5s_bps", 0) or 0), 0))
        + "/s",
        "save_path": final_path,
    }


@_flask_app.route("/api/target_progress")
@login_required
def target_progress():
    """Return durable per-target progress from the Go control plane."""
    if not _web_app or not getattr(_web_app, "tdl_client", None):
        return jsonify({"ok": False, "error": "Go target control is unavailable"}), 503

    try:
        progress = _web_app.tdl_client.get_target_progress()
    except Exception as error:
        log.warning("Could not read target progress: %s", error)
        return jsonify({"ok": False, "error": str(error)}), 503
    return jsonify({"ok": True, "progress": progress})


@_flask_app.route("/api/add_target", methods=["POST"])
@_flask_app.route("/api/resolve_target", methods=["POST"])
@login_required
def add_target():
    """Resolve and add a new target group/channel."""
    if not _web_app:
        return jsonify({"ok": False, "error": "web application is not ready"}), 503

    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.form.to_dict()

    query = (
        payload.get("query")
        or payload.get("chat_id")
        or payload.get("chat")
        or payload.get("url")
        or ""
    )
    if not query:
        return jsonify({"ok": False, "error": "chat_id, username or link is required"}), 400

    try:
        dialog = _run_on_app_loop(_resolve_input_target(query), timeout=30)
        if not dialog:
            return jsonify({"ok": False, "error": "Chat not found"}), 404

        dialog_cache = _web_app.get_dialog_cache()
        items = dialog_cache.get("items", []) if dialog_cache else []
        known_keys = set()
        for item in items:
            known_keys.update(_dialog_match_keys(item))

        if not any(k in known_keys for k in _dialog_match_keys(dialog)):
            items.insert(0, dialog)
            _web_app.set_dialog_cache(items, int(time.time()))

        return jsonify({"ok": True, "dialog": dialog})
    except Exception as err:
        log.warning("Could not add target %s: %s", query, err)
        return jsonify({"ok": False, "error": str(err)}), 400


@_flask_app.route("/api/listen_targets", methods=["GET", "POST"])
@login_required
def listen_targets():
    """Get or save listen targets."""
    if not _web_app:
        return (
            jsonify({"ok": False, "error": "web application is not initialized"}),
            500,
        )

    if request.method == "GET":
        targets = _web_app.get_listen_targets()
        try:
            targets = _merge_control_priorities(
                targets, _web_app.tdl_client.get_control_targets()
            )
        except Exception as error:
            log.warning("Could not read Go target priorities: %s", error)
        return jsonify({"ok": True, "targets": targets})

    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.form.to_dict()

    targets = payload.get("targets", [])
    if isinstance(targets, str):
        try:
            targets = json.loads(targets)
        except json.JSONDecodeError:
            return jsonify({"ok": False, "error": "invalid targets json"}), 400

    if not isinstance(targets, list):
        return jsonify({"ok": False, "error": "targets must be a list"}), 400

    if _telegram_client:
        try:
            targets = _run_on_app_loop(_resolve_save_targets(targets))
        except Exception as error:
            log.warning("Could not resolve some save targets: %s", error)

    try:
        _sync_control_targets(targets)
    except Exception as error:
        log.warning("Could not sync targets to Go daemon: %s", error)

    saved_targets = _web_app.save_listen_targets(targets)
    return jsonify({"ok": True, "targets": saved_targets})


@_flask_app.route("/api/dialogs")
@login_required
def dialogs():
    """Get Telegram dialogs."""
    if not _web_app:
        return jsonify({"ok": False, "error": "web application is not ready"}), 503

    try:
        query = request.args.get("q", "")
        refresh = request.args.get("refresh") == "true"
        target_map = {}
        targets = _web_app.get_listen_targets()
        control_targets = []
        try:
            control_targets = _web_app.tdl_client.get_control_targets()
            targets = _merge_control_priorities(targets, control_targets)
        except Exception as error:
            log.warning("Could not read Go target priorities: %s", error)
        for target in targets:
            for key in _target_match_keys(target):
                target_map[key] = target

        dialog_cache = _web_app.get_dialog_cache()
        items = dialog_cache.get("items", []) if dialog_cache else []
        updated_at = dialog_cache.get("updated_at", 0) if dialog_cache else 0
        cache_refreshed = False

        if refresh:
            if not _telegram_client:
                return (
                    jsonify({"ok": False, "error": "telegram client is not ready"}),
                    503,
                )

            items = _run_on_app_loop(_load_dialogs(), timeout=DIALOG_REFRESH_TIMEOUT)
            known_keys = set()
            for item in items:
                known_keys.update(_dialog_match_keys(item))
            items.extend(
                _run_on_app_loop(_resolve_missing_targets(known_keys, targets))
            )
            updated_at = int(time.time())
            _web_app.set_dialog_cache(items, updated_at)
            cache_refreshed = True

        items = [_decorate_dialog_with_target(item, target_map) for item in items]
        items = _merge_control_priorities(items, control_targets)

        known_keys = set()
        for item in items:
            known_keys.update(_dialog_match_keys(item))

        for target in targets:
            target_key = str(target["chat_id"])
            if target_key in known_keys:
                continue
            items.append(
                {
                    "chat_id": target["chat_id"],
                    "title": target.get("title") or str(target["chat_id"]),
                    "username": "",
                    "type": "configured",
                    "top_message_id": 0,
                    "enabled": True,
                    "last_read_message_id": target.get("last_read_message_id", 0),
                    "download_filter": target.get("download_filter", ""),
                    "priority": target.get("priority", 0),
                    "upload_telegram_chat_id": target.get(
                        "upload_telegram_chat_id", ""
                    ),
                    "scan_status": target.get("scan_status", {}),
                }
            )

        items = _filter_dialogs(items, query)

        return jsonify(
            {
                "ok": True,
                "dialogs": items,
                "cache_updated_at": updated_at,
                "cache_refreshed": cache_refreshed,
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@_flask_app.route("/get_download_list")
@login_required
def get_download_list():
    """get download list"""
    if request.args.get("already_down") is None:
        return jsonify([])

    already_down = request.args.get("already_down") == "true"

    result = []
    seen = set()
    history_seen = set()
    if already_down and _web_app:
        history_seen = {
            (f"{item.get('chat', '')}", f"{item.get('id', '')}")
            for item in _web_app.get_download_history()
        }
    download_result = get_download_result()
    media_pool = _web_app.get_media_pool_status() if _web_app else None
    if (
        not already_down
        and media_pool
        and media_pool.get("mode") == "tdl"
        and media_pool.get("online")
    ):
        for task in media_pool.get("files", {}).values():
            item = _download_item_from_tdl_task(task)
            key = (item["chat"], item["id"])
            if key in seen:
                continue
            seen.add(key)
            result.append(item)

    for chat_id, messages in download_result.items():
        for idx, value in messages.items():
            is_already_down = value["down_byte"] == value["total_size"]

            if already_down and not is_already_down:
                continue
            if not already_down and is_already_down:
                continue

            item = _download_item_from_stat(chat_id, idx, value, media_pool)
            if (item["chat"], item["id"]) in seen:
                continue
            seen.add((item["chat"], item["id"]))
            result.append({key: val for key, val in item.items() if key[0] != "_"})

            if already_down and _web_app and item["save_path"]:
                key = (item["chat"], item["id"])
                if key in history_seen:
                    continue
                _web_app.record_download_history(
                    {
                        "chat": item["chat"],
                        "id": item["id"],
                        "filename": item["filename"],
                        "total_size": item["_raw_total_size"],
                        "save_path": item["save_path"],
                    }
                )
                history_seen.add(key)

    if already_down and _web_app:
        for item in _web_app.get_download_history():
            key = (f"{item.get('chat', '')}", f"{item.get('id', '')}")
            if key in seen:
                continue
            seen.add(key)
            result.append(_download_item_from_history(item))

    return jsonify(result)
