"""Process-wide, DC-aware reusable Kurigram media sessions."""

import asyncio
import contextlib
import logging
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from pyrogram.errors import AuthBytesInvalid

from module.download_stripe_scheduler import DownloadStripeScheduler

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MediaSessionPoolConfig:
    soft_sessions: int = 16
    max_sessions: int = 48
    pipeline_depth: int = 2
    idle_ttl: float = 600.0
    control_interval: float = 60.0
    adaptive: bool = True

    def __post_init__(self) -> None:
        if self.max_sessions > 48:
            raise ValueError("max_sessions must not exceed 48")
        if self.pipeline_depth > 2:
            raise ValueError("pipeline_depth must not exceed 2")


@dataclass(frozen=True)
class PoolWindow:
    pending: int
    utilization: float
    retry_rate: float
    unhealthy_fraction: float
    flood_wait: bool
    committed_bytes_per_second: float


@dataclass(frozen=True)
class ScaleDecision:
    target: int
    reason: str
    hold_until: float = 0.0


class AdaptivePoolController:
    """Pure adaptive target controller driven by complete metric windows."""

    _STEP = 8
    _PLATEAU_HOLD = 600.0

    def __init__(self, soft_target: int, hard_limit: int):
        self._hard_limit = min(48, max(0, hard_limit))
        self._soft_target = min(max(0, soft_target), self._hard_limit)
        self._target = self._soft_target
        self._pre_expansion_target: Optional[int] = None
        self._pre_expansion_goodput = 0.0
        self._post_expansion_windows = 0
        self._post_expansion_goodput = 0.0
        self._hold_until = 0.0
        self._idle = False

    def observe(self, window: PoolWindow, now: float) -> ScaleDecision:
        if window.pending <= 0:
            self._target = 0
            self._idle = True
            self._clear_expansion()
            return self._decision("idle")

        if self._idle:
            self._target = self._soft_target
            self._idle = False
            self._clear_expansion()
            return self._decision("demand")

        if window.flood_wait:
            return self._decision("flood_wait")

        if window.unhealthy_fraction > 0.10:
            self._target = max(0, self._target - self._STEP)
            self._clear_expansion()
            return self._decision("unhealthy")

        if self._pre_expansion_target is not None:
            self._post_expansion_windows += 1
            self._post_expansion_goodput += window.committed_bytes_per_second
            if self._post_expansion_windows < 2:
                return self._decision("evaluating")
            baseline = self._pre_expansion_goodput
            average_goodput = (
                self._post_expansion_goodput / self._post_expansion_windows
            )
            improved = (
                average_goodput >= baseline * 1.05
                if baseline > 0
                else average_goodput > 0
            )
            if improved:
                self._clear_expansion()
                return self._decision("goodput_growth")
            self._target = self._pre_expansion_target
            self._hold_until = now + self._PLATEAU_HOLD
            self._clear_expansion()
            return self._decision("plateau")

        if now < self._hold_until:
            return self._decision("plateau_hold")

        if window.retry_rate >= 0.02:
            return self._decision("retry_rate")
        if window.utilization < 0.8:
            return self._decision("underutilized")
        if self._target >= self._hard_limit:
            return self._decision("hard_limit")

        self._pre_expansion_target = self._target
        self._pre_expansion_goodput = window.committed_bytes_per_second
        self._post_expansion_windows = 0
        self._target = min(self._hard_limit, self._target + self._STEP)
        return self._decision("expand")

    def _decision(self, reason: str) -> ScaleDecision:
        return ScaleDecision(self._target, reason, self._hold_until)

    def _clear_expansion(self) -> None:
        self._pre_expansion_target = None
        self._pre_expansion_goodput = 0.0
        self._post_expansion_windows = 0
        self._post_expansion_goodput = 0.0


@dataclass(frozen=True)
class PoolSnapshot:
    desired: int
    hard_limit: int
    pipeline_depth: int
    live: int
    active_slots: int
    idle: int
    creating: int
    unhealthy: int
    pending: int
    active_files: int
    committed_bytes_per_second: float
    created: int
    evicted: int
    retries: int
    flood_waits: int
    fallbacks: int
    last_scale_reason: str
    by_dc: Dict[int, Dict[str, int]]


@dataclass(eq=False)
class _SessionEntry:
    session: Any
    dc_id: int
    last_used: float
    active_slots: int = 0
    consecutive_failures: int = 0
    force_unhealthy: bool = False
    retiring: bool = False
    stopping: bool = False
    replacement_dc_id: Optional[int] = None


@dataclass(eq=False)
class _LeaseWaiter:
    dc_id: int
    transfer_id: str
    future: asyncio.Future


class SessionLease:
    def __init__(self, pool, entry):
        self._pool = pool
        self._entry = entry
        self._released = False
        self._failure_kind = ""

    @property
    def session(self):
        return self._entry.session

    @property
    def dc_id(self) -> int:
        return self._entry.dc_id

    def mark_unhealthy(self) -> None:
        self._failure_kind = "fatal"

    def mark_transport_failure(self) -> None:
        if self._failure_kind != "fatal":
            self._failure_kind = "transport"

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._pool._release(self._entry, self._failure_kind)

    async def __aenter__(self):
        return self

    async def __aexit__(self, _type, _value, _traceback):
        await self.release()


class KurigramMediaSessionFactory:
    """Serialize Kurigram authorization imports and discard stale cache entries."""

    def __init__(self, client, attempts: int = 3):
        self.client = client
        self.attempts = attempts
        self.lock = client.sessions_lock

    async def __call__(self, dc_id: int):
        last_error = None
        async with self.lock:
            for _attempt in range(self.attempts):
                try:
                    return await self.client.get_session(
                        dc_id,
                        is_media=True,
                        export_authorization=False,
                        temporary=True,
                    )
                except AuthBytesInvalid as error:
                    last_error = error
                    stale_sessions = (
                        self.client.media_sessions.pop(dc_id, None),
                        getattr(self.client, "sessions", {}).pop(dc_id, None),
                    )
                    stopped_ids = set()
                    cancellation = None
                    for stale in stale_sessions:
                        if stale is None or id(stale) in stopped_ids:
                            continue
                        stopped_ids.add(id(stale))
                        stop_task = asyncio.create_task(stale.stop())
                        while not stop_task.done():
                            try:
                                await asyncio.shield(stop_task)
                            except asyncio.CancelledError as error:
                                cancellation = cancellation or error
                            except Exception:
                                pass
                        try:
                            stop_task.result()
                        except asyncio.CancelledError as error:
                            cancellation = cancellation or error
                        except Exception:
                            LOGGER.exception("Failed to stop stale media session")
                    if cancellation is not None:
                        raise cancellation
            if last_error is None:
                raise ValueError("Kurigram media session attempts must be positive")
            raise last_error


class GlobalMediaSessionPool:
    """Own and fairly lease temporary media sessions for one client process."""

    def __init__(
        self,
        factory: Callable[[int], Awaitable[Any]],
        config: MediaSessionPoolConfig,
        clock: Callable[[], float] = time.monotonic,
        tick: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self._factory = factory
        self._config = config
        self._clock = clock
        self._tick = tick
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._entries: List[_SessionEntry] = []
        self._waiters: DownloadStripeScheduler[_LeaseWaiter] = (
            DownloadStripeScheduler()
        )
        self._builders: Dict[int, asyncio.Task] = {}
        self._retired_by_builder: Dict[asyncio.Task, Any] = {}
        self._creating_by_dc: Dict[int, int] = {}
        self._creating = 0
        self._active_transfers: Set[Tuple[int, str]] = set()
        self._paused_until: Dict[int, float] = {}
        self._wake_by_dc: Dict[int, asyncio.Task] = {}
        self._wake_tasks: Set[asyncio.Task] = set()
        self._last_replacement_dc_id: Optional[int] = None
        self._closing = False
        self._closed = False
        self._close_task: Optional[asyncio.Task] = None
        self._control_task: Optional[asyncio.Task] = None

        self._desired = min(config.soft_sessions, config.max_sessions)
        self._controller = AdaptivePoolController(
            config.soft_sessions,
            config.max_sessions,
        )
        self._committed_bytes = 0
        self._committed_bytes_per_second = 0.0
        self._created = 0
        self._evicted = 0
        self._retries = 0
        self._stripe_attempts = 0
        self._flood_waits = 0
        self._fallbacks = 0
        self._last_scale_reason = "initial"
        self._window_started_at = self._clock()
        self._window_committed_bytes = 0
        self._window_retries = 0
        self._window_stripe_attempts = 0
        self._snapshot_lock = threading.Lock()
        self._snapshot = self._make_snapshot()

    def start(self) -> None:
        if self._closing or self._closed:
            return
        if not self._config.adaptive:
            self._desired = min(
                self._config.soft_sessions,
                self._config.max_sessions,
            )
            self._last_scale_reason = "fixed_target"
            self._refresh_snapshot()
            return
        if self._control_task is None or self._control_task.done():
            self._control_task = asyncio.create_task(self._control_loop())

    async def acquire(self, dc_id: int, transfer_id: str) -> SessionLease:
        loop = asyncio.get_running_loop()
        waiter = _LeaseWaiter(dc_id, transfer_id, loop.create_future())
        async with self._lock:
            if self._closing:
                raise asyncio.CancelledError()
            self._waiters.enqueue(dc_id, transfer_id, waiter)
            self._refresh_snapshot_locked()
        try:
            # Batch concurrently scheduled transfers before taking a fair turn.
            await asyncio.sleep(0)
            async with self._lock:
                if not self._closing and not waiter.future.done():
                    self._dispatch_locked(dc_id)
                    self._ensure_builder_locked(dc_id)
                    self._refresh_snapshot_locked()
            return await waiter.future
        except BaseException as acquire_error:
            orphaned_lease = None
            cancellation = (
                acquire_error
                if isinstance(acquire_error, asyncio.CancelledError)
                else None
            )
            while True:
                try:
                    async with self._lock:
                        self._waiters.cancel(waiter)
                        self._cancel_obsolete_replacements_locked()
                        if waiter.future.done() and not waiter.future.cancelled():
                            try:
                                orphaned_lease = waiter.future.result()
                            except BaseException:
                                pass
                        self._refresh_snapshot_locked()
                    break
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
            if orphaned_lease is not None:
                try:
                    await orphaned_lease.release()
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
            if cancellation is not None:
                raise cancellation
            raise

    @contextlib.asynccontextmanager
    async def transfer(self, dc_id: int, transfer_id: str):
        async with self._lock:
            if self._closing:
                raise asyncio.CancelledError()
            self._active_transfers.add((dc_id, transfer_id))
            self._refresh_snapshot_locked()
        try:
            yield
        finally:
            cancellation = None
            while True:
                try:
                    async with self._lock:
                        self._active_transfers.discard((dc_id, transfer_id))
                        removed = self._waiters.remove_transfer(dc_id, transfer_id)
                        for waiter in removed:
                            if not waiter.future.done():
                                waiter.future.cancel()
                        self._cancel_obsolete_replacements_locked()
                        self._refresh_snapshot_locked()
                    break
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
            if cancellation is not None:
                raise cancellation

    def record_committed(self, byte_count: int) -> None:
        self._committed_bytes += byte_count
        self._refresh_snapshot()

    def record_retry(self) -> None:
        self._retries += 1
        self._refresh_snapshot()

    def record_stripe_attempt(self) -> None:
        self._stripe_attempts += 1

    def record_fallback(self) -> None:
        self._fallbacks += 1
        self._refresh_snapshot()

    def pause_dc(self, dc_id: int, seconds: float) -> None:
        if self._closing or self._closed:
            return
        duration = max(0.0, seconds)
        now = self._clock()
        requested_until = now + duration
        effective_until = max(
            requested_until,
            self._paused_until.get(dc_id, requested_until),
        )
        self._paused_until[dc_id] = effective_until
        self._flood_waits += 1

        previous = self._wake_by_dc.get(dc_id)
        if previous is not None:
            previous.cancel()
        task = asyncio.create_task(
            self._wake_dc(dc_id, max(0.0, effective_until - now), effective_until)
        )
        self._wake_by_dc[dc_id] = task
        self._wake_tasks.add(task)
        task.add_done_callback(self._wake_tasks.discard)
        self._refresh_snapshot()

    def snapshot(self) -> PoolSnapshot:
        with self._snapshot_lock:
            return replace(
                self._snapshot,
                by_dc={
                    dc_id: dict(counts)
                    for dc_id, counts in self._snapshot.by_dc.items()
                },
            )

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            if self._close_task is None:
                self._closing = True
                self._cancel_all_waiters_locked()
                builders = list(self._builders.values())
                wake_tasks = list(self._wake_tasks)
                control_task = self._control_task
                tasks = builders + wake_tasks
                if control_task is not None:
                    tasks.append(control_task)
                for task in tasks:
                    task.cancel()
                self._refresh_snapshot_locked()
                self._close_task = asyncio.create_task(
                    self._finish_close(builders, wake_tasks, control_task)
                )
            close_task = self._close_task
        await asyncio.shield(close_task)

    async def _finish_close(
        self,
        builders: List[asyncio.Task],
        wake_tasks: List[asyncio.Task],
        control_task: Optional[asyncio.Task],
    ) -> None:
        tasks = builders + wake_tasks
        if control_task is not None:
            tasks.append(control_task)
        await asyncio.gather(*tasks, return_exceptions=True)
        async with self._condition:
            transferred_sessions = []
            for dc_id, task in list(self._builders.items()):
                if task in builders:
                    retired = self._finish_builder_reservation_locked(dc_id, task)
                    if retired is not None:
                        transferred_sessions.append(retired)
            await self._condition.wait_for(
                lambda: not any(
                    entry.active_slots or entry.stopping for entry in self._entries
                )
            )
            sessions = transferred_sessions + [
                entry.session for entry in self._entries
            ]
            self._entries.clear()
            self._paused_until.clear()
            self._refresh_snapshot_locked()

        await asyncio.gather(
            *(self._stop_session(session) for session in sessions),
            return_exceptions=True,
        )
        async with self._lock:
            self._closed = True
            self._refresh_snapshot_locked()

    async def _release(self, entry: _SessionEntry, failure_kind: str) -> None:
        stop_session = None
        cancellation = None
        while True:
            try:
                async with self._condition:
                    entry.active_slots -= 1
                    entry.last_used = self._clock()
                    if failure_kind == "fatal":
                        entry.force_unhealthy = True
                    if failure_kind == "transport":
                        entry.consecutive_failures += 1
                    elif not failure_kind:
                        entry.consecutive_failures = 0
                    if entry.consecutive_failures >= 2 or entry.force_unhealthy:
                        entry.retiring = True
                    if entry.retiring and entry.active_slots == 0:
                        if entry in self._entries and not entry.stopping:
                            entry.stopping = True
                            stop_session = entry.session
                    self._condition.notify_all()
                    if not self._closing and stop_session is None:
                        self._service_waiting_dcs_locked(entry.dc_id)
                    self._refresh_snapshot_locked()
                break
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
        if stop_session is not None:
            stop_task = asyncio.create_task(self._stop_session(stop_session))
            while not stop_task.done():
                try:
                    await asyncio.shield(stop_task)
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
            stop_task.result()
            while True:
                try:
                    async with self._condition:
                        if entry in self._entries:
                            self._entries.remove(entry)
                            self._evicted += 1
                        entry.stopping = False
                        self._condition.notify_all()
                        if not self._closing:
                            preferred_dc = (
                                entry.replacement_dc_id
                                if entry.replacement_dc_id is not None
                                else entry.dc_id
                            )
                            self._service_waiting_dcs_locked(preferred_dc)
                        self._refresh_snapshot_locked()
                    break
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
        if cancellation is not None:
            raise cancellation

    async def _control_loop(self) -> None:
        while True:
            await self._tick(self._config.control_interval)
            try:
                await self._control_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Media session pool control tick failed")

    async def _control_once(self) -> None:
        retire_now: List[_SessionEntry] = []
        now = self._clock()
        async with self._lock:
            for dc_id in list(self._paused_until):
                self._dc_is_paused_locked(dc_id)

            elapsed = now - self._window_started_at
            if elapsed <= 0:
                elapsed = self._config.control_interval
            committed = self._committed_bytes - self._window_committed_bytes
            retries = self._retries - self._window_retries
            attempts = self._stripe_attempts - self._window_stripe_attempts
            self._window_started_at = now
            self._window_committed_bytes = self._committed_bytes
            self._window_retries = self._retries
            self._window_stripe_attempts = self._stripe_attempts
            self._committed_bytes_per_second = committed / elapsed

            active_slots = sum(entry.active_slots for entry in self._entries)
            live_capacity = len(self._entries) * self._config.pipeline_depth
            pending_work = self._waiters.pending_count()
            pending_dcs = self._waiters.dc_ids()
            unhealthy = sum(
                entry.force_unhealthy or entry.consecutive_failures >= 2
                for entry in self._entries
            )
            window = PoolWindow(
                pending=pending_work,
                utilization=(
                    min(1.0, active_slots / live_capacity)
                    if live_capacity
                    else 0.0
                ),
                retry_rate=retries / attempts if attempts else 0.0,
                unhealthy_fraction=unhealthy / max(len(self._entries), 1),
                flood_wait=bool(self._paused_until),
                committed_bytes_per_second=self._committed_bytes_per_second,
            )
            decision = self._controller.observe(window, now)
            active_sessions = (
                active_slots + self._config.pipeline_depth - 1
            ) // self._config.pipeline_depth
            desired_floor = active_sessions if decision.reason == "idle" else 0
            if pending_work:
                desired_floor = max(desired_floor, len(pending_dcs))
            self._desired = min(
                self._config.max_sessions,
                max(decision.target, desired_floor),
            )
            self._last_scale_reason = decision.reason
            protected_entries = self._protected_pending_entries_locked()

            idle = sorted(
                (
                    entry
                    for entry in self._entries
                    if entry.active_slots == 0
                    and not entry.retiring
                    and not entry.stopping
                    and entry.dc_id not in self._paused_until
                ),
                key=lambda entry: entry.last_used,
            )
            expired = [
                entry
                for entry in idle
                if now - entry.last_used >= self._config.idle_ttl
                and entry not in protected_entries
            ]
            selected_for_retirement = list(expired)
            if decision.reason != "idle":
                already_retiring = sum(
                    entry.retiring or entry.stopping for entry in self._entries
                )
                excess = max(
                    0,
                    len(self._entries) - self._desired - already_retiring,
                )
                candidates = sorted(
                    (
                        entry
                        for entry in self._entries
                        if not entry.retiring
                        and not entry.stopping
                        and entry not in selected_for_retirement
                        and entry not in protected_entries
                        and entry.dc_id not in self._paused_until
                    ),
                    key=lambda entry: (entry.active_slots != 0, entry.last_used),
                )
                for entry in candidates:
                    if len(selected_for_retirement) >= excess:
                        break
                    selected_for_retirement.append(entry)
            for entry in selected_for_retirement:
                entry.retiring = True
                if entry.active_slots == 0:
                    entry.stopping = True
                    retire_now.append(entry)

            self._service_waiting_dcs_locked()
            self._refresh_snapshot_locked()

        cancellation = None
        for entry in retire_now:
            stopped_during_cancel = await self._stop_session_to_completion(
                entry.session
            )
            cancellation = cancellation or stopped_during_cancel

        if retire_now:
            while True:
                try:
                    async with self._condition:
                        for entry in retire_now:
                            if entry in self._entries:
                                self._entries.remove(entry)
                                self._evicted += 1
                            entry.stopping = False
                        self._condition.notify_all()
                        if not self._closing:
                            self._service_waiting_dcs_locked()
                        self._refresh_snapshot_locked()
                    break
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error

        snapshot = self.snapshot()
        LOGGER.info("Media session pool snapshot %s", snapshot)
        if cancellation is not None:
            raise cancellation

    def _dispatch_locked(self, dc_id: int) -> None:
        if self._closing or self._dc_is_paused_locked(dc_id):
            return
        while self._waiters.pending_count(dc_id):
            available = [
                entry
                for entry in self._entries
                if entry.dc_id == dc_id
                and not entry.retiring
                and not entry.force_unhealthy
                and entry.active_slots < self._config.pipeline_depth
            ]
            if not available:
                return
            entry = min(available, key=lambda item: (item.active_slots, item.last_used))
            waiter = self._waiters.pop_next(dc_id)
            if waiter is None:
                return
            if waiter.future.done():
                continue
            entry.active_slots += 1
            entry.last_used = self._clock()
            waiter.future.set_result(SessionLease(self, entry))

    def _ensure_builder_locked(self, dc_id: int) -> None:
        if (
            self._closing
            or dc_id in self._builders
            or not self._waiters.pending_count(dc_id)
            or self._dc_is_paused_locked(dc_id)
        ):
            return

        compatible = [
            entry
            for entry in self._entries
            if entry.dc_id == dc_id and not entry.retiring and not entry.force_unhealthy
        ]
        capacity_limit = min(self._desired, self._config.max_sessions)
        owned_or_creating = len(self._entries) + self._creating
        retired_session = None

        if owned_or_creating >= capacity_limit:
            if compatible:
                return
            protected_entries = self._protected_pending_entries_locked()
            idle_entries = [
                entry
                for entry in self._entries
                if entry.active_slots == 0
                and not entry.retiring
                and entry not in protected_entries
            ]
            if idle_entries:
                oldest = min(idle_entries, key=lambda entry: entry.last_used)
                self._entries.remove(oldest)
                self._evicted += 1
                retired_session = oldest.session
                self._last_replacement_dc_id = dc_id
            else:
                active_entries = [
                    entry
                    for entry in self._entries
                    if not entry.retiring and entry not in protected_entries
                ]
                replacement_in_progress = self._creating or any(
                    entry.retiring or entry.stopping for entry in self._entries
                )
                if active_entries and not replacement_in_progress:
                    oldest = min(active_entries, key=lambda entry: entry.last_used)
                    oldest.retiring = True
                    oldest.replacement_dc_id = dc_id
                    self._last_replacement_dc_id = dc_id
                return

        if len(self._entries) + self._creating >= self._config.max_sessions:
            return

        self._creating += 1
        self._creating_by_dc[dc_id] = self._creating_by_dc.get(dc_id, 0) + 1
        task = asyncio.create_task(self._build_session(dc_id, retired_session))
        self._builders[dc_id] = task
        if retired_session is not None:
            self._retired_by_builder[task] = retired_session

    async def _build_session(self, dc_id: int, retired_session: Any) -> None:
        current_task = asyncio.current_task()
        session = None
        reservation_active = True
        retired_needs_stop = retired_session is not None
        session_needs_stop = False
        try:
            if retired_session is not None:
                cancellation = await self._stop_session_to_completion(
                    retired_session
                )
                retired_needs_stop = False
                self._retired_by_builder.pop(current_task, None)
                if cancellation is not None:
                    raise cancellation
            session = await self._factory(dc_id)
            session_needs_stop = True
            async with self._lock:
                self._finish_builder_reservation_locked(dc_id, current_task)
                reservation_active = False
                if self._closing:
                    stop_created = session
                else:
                    self._entries.append(
                        _SessionEntry(session, dc_id, self._clock())
                    )
                    self._created += 1
                    stop_created = None
                    session_needs_stop = False
                    self._service_waiting_dcs_locked(dc_id)
                self._refresh_snapshot_locked()
            if stop_created is not None:
                cancellation = await self._stop_session_to_completion(stop_created)
                session_needs_stop = False
                if cancellation is not None:
                    raise cancellation
        except asyncio.CancelledError:
            if reservation_active:
                async with self._lock:
                    self._finish_builder_reservation_locked(dc_id, current_task)
                    if not self._closing:
                        self._service_waiting_dcs_locked()
                    self._refresh_snapshot_locked()
            if session is not None and session_needs_stop:
                await self._stop_session_to_completion(session)
            if retired_needs_stop:
                await self._stop_session_to_completion(retired_session)
                self._retired_by_builder.pop(current_task, None)
            raise
        except Exception as error:
            if reservation_active:
                async with self._lock:
                    self._finish_builder_reservation_locked(dc_id, current_task)
                    if not self._closing:
                        self._fail_dc_waiters_locked(dc_id, error)
                        self._service_waiting_dcs_locked()
                    self._refresh_snapshot_locked()
            if session is not None and session_needs_stop:
                await self._stop_session_to_completion(session)
            if retired_needs_stop:
                await self._stop_session_to_completion(retired_session)
                self._retired_by_builder.pop(current_task, None)

    def _finish_builder_reservation_locked(
        self,
        dc_id: int,
        current_task: Optional[asyncio.Task],
    ) -> Any:
        if self._builders.get(dc_id) is not current_task:
            return None
        self._creating -= 1
        creating_for_dc = self._creating_by_dc.get(dc_id, 0) - 1
        if creating_for_dc:
            self._creating_by_dc[dc_id] = creating_for_dc
        else:
            self._creating_by_dc.pop(dc_id, None)
        self._builders.pop(dc_id, None)
        return self._retired_by_builder.pop(current_task, None)

    def _fail_dc_waiters_locked(self, dc_id: int, error: BaseException) -> None:
        while self._waiters.pending_count(dc_id):
            waiter = self._waiters.pop_next(dc_id)
            if waiter is not None and not waiter.future.done():
                waiter.future.set_exception(error)
        self._cancel_obsolete_replacements_locked()

    def _service_waiting_dcs_locked(self, preferred_dc: Optional[int] = None) -> None:
        dc_ids = self._ordered_waiting_dcs_locked(preferred_dc)
        for dc_id in dc_ids:
            self._dispatch_locked(dc_id)
        dc_ids = self._ordered_waiting_dcs_locked(
            preferred_dc,
            rotate_replacements=True,
        )
        for dc_id in dc_ids:
            self._ensure_builder_locked(dc_id)

    def _ordered_waiting_dcs_locked(
        self,
        preferred_dc: Optional[int] = None,
        *,
        rotate_replacements: bool = False,
    ) -> List[int]:
        dc_ids = self._waiters.dc_ids()
        if (
            rotate_replacements
            and self._last_replacement_dc_id in dc_ids
        ):
            offset = dc_ids.index(self._last_replacement_dc_id) + 1
            dc_ids = dc_ids[offset:] + dc_ids[:offset]
        if preferred_dc in dc_ids:
            dc_ids.remove(preferred_dc)
            dc_ids.insert(0, preferred_dc)
        return dc_ids

    def _cancel_obsolete_replacements_locked(self) -> None:
        for entry in self._entries:
            replacement_dc_id = entry.replacement_dc_id
            if replacement_dc_id is None:
                continue
            if self._waiters.pending_count(replacement_dc_id):
                continue
            entry.replacement_dc_id = None
            if (
                not entry.stopping
                and not entry.force_unhealthy
                and entry.consecutive_failures < 2
            ):
                entry.retiring = False

    def _protected_pending_entries_locked(self) -> Set[_SessionEntry]:
        pending_dcs = [
            dc_id
            for dc_id in self._waiters.dc_ids()
            if not self._dc_is_paused_locked(dc_id)
        ]
        capacity_limit = min(self._desired, self._config.max_sessions)
        if len(pending_dcs) > capacity_limit:
            return set()

        protected = set()
        for dc_id in pending_dcs:
            compatible = [
                entry
                for entry in self._entries
                if entry.dc_id == dc_id
                and not entry.retiring
                and not entry.stopping
                and not entry.force_unhealthy
                and entry.consecutive_failures < 2
            ]
            if compatible:
                protected.add(
                    min(
                        compatible,
                        key=lambda entry: (
                            entry.consecutive_failures,
                            -entry.last_used,
                        ),
                    )
                )
        return protected

    def _cancel_all_waiters_locked(self) -> None:
        for dc_id in list(self._waiters.dc_ids()):
            while self._waiters.pending_count(dc_id):
                waiter = self._waiters.pop_next(dc_id)
                if waiter is not None and not waiter.future.done():
                    waiter.future.cancel()

    def _dc_is_paused_locked(self, dc_id: int) -> bool:
        paused_until = self._paused_until.get(dc_id)
        if paused_until is None:
            return False
        if paused_until <= self._clock():
            self._paused_until.pop(dc_id, None)
            return False
        return True

    async def _wake_dc(self, dc_id: int, seconds: float, expected_until: float) -> None:
        try:
            await self._tick(seconds)
            async with self._lock:
                if self._paused_until.get(dc_id) == expected_until:
                    self._paused_until.pop(dc_id, None)
                    self._dispatch_locked(dc_id)
                    self._ensure_builder_locked(dc_id)
                    self._refresh_snapshot_locked()
        finally:
            if self._wake_by_dc.get(dc_id) is asyncio.current_task():
                self._wake_by_dc.pop(dc_id, None)

    async def _stop_session(self, session: Any) -> None:
        try:
            await session.stop()
        except Exception:  # pragma: no cover - defensive logging for shutdown
            LOGGER.exception("Failed to stop pooled media session")

    async def _stop_session_to_completion(
        self,
        session: Any,
    ) -> Optional[asyncio.CancelledError]:
        cancellation = None
        stop_task = asyncio.create_task(self._stop_session(session))
        while not stop_task.done():
            try:
                await asyncio.shield(stop_task)
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
        stop_task.result()
        return cancellation

    def _refresh_snapshot_locked(self) -> None:
        self._refresh_snapshot()

    def _refresh_snapshot(self) -> None:
        snapshot = self._make_snapshot()
        with self._snapshot_lock:
            self._snapshot = snapshot

    def _make_snapshot(self) -> PoolSnapshot:
        by_dc: Dict[int, Dict[str, int]] = {}
        dc_ids = set(self._creating_by_dc)
        dc_ids.update(entry.dc_id for entry in self._entries)
        dc_ids.update(self._waiters.dc_ids())
        for dc_id in sorted(dc_ids):
            entries = [entry for entry in self._entries if entry.dc_id == dc_id]
            by_dc[dc_id] = {
                "live": len(entries),
                "active": sum(entry.active_slots for entry in entries),
                "idle": sum(
                    entry.active_slots == 0 and not entry.retiring
                    for entry in entries
                ),
                "creating": self._creating_by_dc.get(dc_id, 0),
                "unhealthy": sum(
                    entry.force_unhealthy or entry.consecutive_failures >= 2
                    for entry in entries
                ),
                "pending": self._waiters.pending_count(dc_id),
            }
        return PoolSnapshot(
            desired=self._desired,
            hard_limit=self._config.max_sessions,
            pipeline_depth=self._config.pipeline_depth,
            live=len(self._entries),
            active_slots=sum(entry.active_slots for entry in self._entries),
            idle=sum(
                entry.active_slots == 0 and not entry.retiring
                for entry in self._entries
            ),
            creating=self._creating,
            unhealthy=sum(
                entry.force_unhealthy or entry.consecutive_failures >= 2
                for entry in self._entries
            ),
            pending=self._waiters.pending_count(),
            active_files=len(self._active_transfers),
            committed_bytes_per_second=self._committed_bytes_per_second,
            created=self._created,
            evicted=self._evicted,
            retries=self._retries,
            flood_waits=self._flood_waits,
            fallbacks=self._fallbacks,
            last_scale_reason=self._last_scale_reason,
            by_dc=by_dc,
        )
