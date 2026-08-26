"""Small HTTP client for the project-local tdl download daemon."""

import asyncio
import threading
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import quote

import requests


class TDLError(RuntimeError):
    """Base tdl daemon client error."""


class TDLUnavailable(TDLError):
    """The daemon could not be reached or returned an invalid response."""


class TDLQueueFull(TDLError):
    """The daemon accepted no more queued work."""


class TDLRejected(TDLError):
    """The daemon rejected a task that cannot succeed unchanged."""


class TDLClient:
    """Submit stable tasks and read live daemon metrics."""

    TERMINAL_STATES = {"success", "failed", "unavailable"}

    def __init__(
        self,
        base_url: str,
        *,
        request: Optional[Callable[..., Any]] = None,
        session_factory: Callable[[], Any] = requests.Session,
        clock: Callable[[], float] = time.monotonic,
        request_timeout: float = 2.0,
        status_timeout: float = 0.5,
        status_cache_seconds: float = 2.0,
    ):
        self.base_url = str(base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("tdl base URL is required")
        self._session_factory = session_factory
        self._sessions = threading.local()
        self._request = request or self._direct_request
        self._clock = clock
        self.request_timeout = max(float(request_timeout), 0.1)
        self.status_timeout = max(float(status_timeout), 0.1)
        self.status_cache_seconds = max(float(status_cache_seconds), 0.0)
        self._status_lock = threading.RLock()
        self._cached_status: Optional[Dict[str, Any]] = None
        self._cached_status_at = 0.0

    def get_status(self, *, force: bool = False) -> Dict[str, Any]:
        """Return a short-lived snapshot; failures always have zero speed."""
        now = self._clock()
        with self._status_lock:
            if (
                not force
                and self._cached_status is not None
                and now - self._cached_status_at < self.status_cache_seconds
            ):
                return _copy_status(self._cached_status)

        try:
            status = self._request_json(
                "GET", "/api/status", timeout=self.status_timeout
            )
            status = dict(status)
            status["online"] = True
            status.setdefault("backend", "tdl")
            status.setdefault("rolling_5s_bps", 0)
            status.setdefault("active_files", [])
            status.setdefault("queue_depth", 0)
            status.setdefault("pool", {})
            status.setdefault("last_error", "")
        except TDLUnavailable as error:
            status = _offline_status(str(error))

        with self._status_lock:
            self._cached_status = status
            self._cached_status_at = now
        return _copy_status(status)

    def submit_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self._raw_request(
            "POST", "/api/tasks", json=dict(payload), timeout=self.request_timeout
        )
        if response.status_code == 429:
            raise TDLQueueFull(_response_error(response, "tdl queue is full"))
        if response.status_code not in (200, 202):
            raise _response_exception(
                response, f"tdl submit returned {response.status_code}"
            )
        return _response_json(response)

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        response = self._raw_request(
            "GET",
            f"/api/tasks/{quote(str(task_id), safe='')}",
            timeout=self.request_timeout,
        )
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise _response_exception(
                response,
                f"tdl task lookup returned {response.status_code}",
            )
        return _response_json(response)

    def set_paused(self, paused: bool) -> Dict[str, Any]:
        return self._request_json(
            "POST",
            "/api/control",
            json={"action": "pause" if paused else "resume"},
            timeout=self.request_timeout,
        )

    async def wait_for_completion(
        self,
        payload: Dict[str, Any],
        *,
        should_continue: Callable[[], bool],
        poll_interval: float = 1.0,
    ) -> Dict[str, Any]:
        """Keep one task attached across brief outages and daemon restarts."""
        task_id = str(payload.get("id", ""))
        delay = max(float(poll_interval), 0.0)
        while True:
            if not should_continue():
                raise asyncio.CancelledError()
            try:
                task = await asyncio.to_thread(self.submit_task, payload)
            except (TDLUnavailable, TDLQueueFull):
                await asyncio.sleep(delay)
                continue
            if task.get("state") in self.TERMINAL_STATES:
                return task

            while True:
                if not should_continue():
                    raise asyncio.CancelledError()
                await asyncio.sleep(delay)
                try:
                    task = await asyncio.to_thread(self.get_task, task_id)
                except TDLUnavailable:
                    continue
                if task is None:
                    break
                if task.get("state") in self.TERMINAL_STATES:
                    return task

    def _request_json(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        response = self._raw_request(method, path, **kwargs)
        if response.status_code < 200 or response.status_code >= 300:
            raise _response_exception(response, f"tdl returned {response.status_code}")
        return _response_json(response)

    def _raw_request(self, method: str, path: str, **kwargs):
        try:
            return self._request(method, self.base_url + path, **kwargs)
        except Exception as error:
            raise TDLUnavailable(str(error)) from error

    def _direct_request(self, method: str, url: str, **kwargs):
        session = getattr(self._sessions, "session", None)
        if session is None:
            session = self._session_factory()
            session.trust_env = False
            self._sessions.session = session
        return session.request(method, url, **kwargs)


def _response_json(response) -> Dict[str, Any]:
    try:
        payload = response.json()
    except Exception as error:
        raise TDLUnavailable("tdl returned invalid JSON") from error
    if not isinstance(payload, dict):
        raise TDLUnavailable("tdl returned a non-object JSON response")
    return payload


def _response_error(response, fallback: str) -> str:
    try:
        payload = response.json()
        if isinstance(payload, dict) and payload.get("error"):
            return str(payload["error"])
    except Exception:
        pass
    return fallback


def _response_exception(response, fallback: str) -> TDLError:
    message = _response_error(response, fallback)
    if 400 <= int(response.status_code) < 500:
        return TDLRejected(message)
    return TDLUnavailable(message)


def _offline_status(error: str) -> Dict[str, Any]:
    return {
        "backend": "tdl",
        "online": False,
        "paused": False,
        "rolling_5s_bps": 0,
        "active_files": [],
        "queue_depth": 0,
        "pool": {"size": 0, "active_files": 0, "reconnects": 0},
        "last_error": error,
        "updated_at": 0,
    }


def _copy_status(status: Dict[str, Any]) -> Dict[str, Any]:
    copy = dict(status)
    copy["active_files"] = [dict(item) for item in status.get("active_files", [])]
    copy["pool"] = dict(status.get("pool", {}))
    return copy
