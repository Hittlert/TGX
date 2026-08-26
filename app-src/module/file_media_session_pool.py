"""Per-file media session control and process-wide session coordination."""

from __future__ import annotations

import asyncio
import copy
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, is_dataclass, replace
from types import MappingProxyType
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

LOGGER = logging.getLogger(__name__)


def _freeze(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return copy.deepcopy(value)


def _percentile(values: List[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


async def _join_shielded(task: asyncio.Task) -> Optional[asyncio.CancelledError]:
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = cancellation or error
        except BaseException:
            if not task.done():
                raise
    return cancellation


async def _join_factory_task(task: asyncio.Task) -> Optional[asyncio.CancelledError]:
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = cancellation or error
            if not task.done():
                task.cancel()
        except BaseException:
            if not task.done():
                raise
    return cancellation


@dataclass(frozen=True)
class CoordinatorConfig:
    max_sessions: int = 60
    expansion_interval: float = 10.0
    warm_session_handoff: bool = False
    warm_session_limit: int = 20
    warm_session_ttl: float = 120.0

    def __post_init__(self) -> None:
        if not 1 <= self.max_sessions <= 60:
            raise ValueError("max_sessions must be between 1 and 60")
        if self.expansion_interval <= 0:
            raise ValueError("expansion_interval must be positive")
        if not 0 <= self.warm_session_limit <= 60:
            raise ValueError("warm_session_limit must be between 0 and 60")
        if self.warm_session_ttl <= 0:
            raise ValueError("warm_session_ttl must be positive")


@dataclass(frozen=True)
class CoordinatorSnapshot:
    used: int
    hard_limit: int
    creating: int
    live: int
    idle: int
    created: int
    reused: int
    draining: int
    active_files: int
    committed_bytes_per_second: float
    raw_bps: float
    rolling_5s_bps: float
    p10_5s_bps: float
    mean_5s_bps: float
    stddev_5s_bps: float
    cv: float
    sample_count: int
    raw_samples: Tuple[float, ...]
    rolling_5s_samples: Tuple[float, ...]
    fallbacks: int
    expansion_queue: int
    dc_cooldowns: Dict[int, float]
    pools: Dict[str, Dict[str, object]]


class OwnedMediaSession:
    """A physical media session and its single coordinator permit."""

    def __init__(self, coordinator, pool_id: str, dc_id: int, session: Any):
        self._coordinator = coordinator
        self._pool_id = pool_id
        self._dc_id = dc_id
        self._session = session
        self._state = "live"
        self._idle_since: Optional[float] = None
        self._release_task: Optional[asyncio.Task] = None
        self._close_task: Optional[asyncio.Task] = None

    @property
    def session(self):
        return self._session

    @property
    def dc_id(self) -> int:
        return self._dc_id

    def release(self) -> Coroutine[Any, Any, None]:
        if (
            not self._coordinator._config.warm_session_handoff
            or self._coordinator._config.warm_session_limit == 0
        ):
            return self.close()
        if self._release_task is None:
            self._release_task = asyncio.create_task(
                self._coordinator._release_owned(self)
            )
        return self._wait_released()

    async def _wait_released(self) -> None:
        release_task = self._release_task
        if release_task is None:
            raise AssertionError("owned session release task was not started")
        cancellation = await _join_shielded(release_task)
        release_task.result()
        if cancellation is not None:
            raise cancellation

    def close(self) -> Coroutine[Any, Any, None]:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._coordinator._stop_owned(self))
        return self._wait_closed()

    async def _wait_closed(self) -> None:
        close_task = self._close_task
        if close_task is None:
            raise AssertionError("owned session close task was not started")
        cancellation = await _join_shielded(close_task)
        close_task.result()
        if cancellation is not None:
            raise cancellation


class MediaTransferCoordinator:
    """Coordinate the process-wide media session budget and DC cooldowns."""

    def __init__(
        self,
        factory: Callable[[int], Coroutine[Any, Any, Any]],
        config: CoordinatorConfig,
        clock: Callable[[], float] = time.monotonic,
        tick: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self._factory = factory
        self._config = config
        self._clock = clock
        self._tick = tick
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._snapshot_lock = threading.Lock()
        self._creating = 0
        self._live = 0
        self._draining = 0
        self._created = 0
        self._reused = 0
        self._pool_used: Dict[str, int] = {}
        self._pool_committed: Dict[str, int] = {}
        self._pool_snapshots: Dict[str, Dict[str, object]] = {}
        self._owned: Set[OwnedMediaSession] = set()
        self._idle_by_dc: Dict[int, Deque[OwnedMediaSession]] = {}
        self._creation_tasks: Set[asyncio.Task] = set()
        self._factory_tasks: Set[asyncio.Task] = set()
        self._last_expansion: Optional[float] = None
        self._dc_cooldowns: Dict[int, float] = {}
        self._dc_events: Dict[int, asyncio.Event] = {}
        self._wake_tasks: Set[asyncio.Task] = set()
        self._metrics_task: Optional[asyncio.Task] = None
        self._metric_bytes: Dict[int, int] = {}
        self._pool_metric_bytes: Dict[str, Dict[int, int]] = {}
        self._raw_samples: Deque[float] = deque(maxlen=600)
        self._rolling_5s_samples: Deque[float] = deque(maxlen=600)
        self._pool_raw_samples: Dict[str, Deque[float]] = {}
        self._pool_raw_bps: Dict[str, float] = {}
        self._pool_rolling_5s_bps: Dict[str, float] = {}
        self._pool_idle_metric_windows: Dict[str, int] = {}
        self._next_metric_second = int(self._clock())
        self._raw_bps = 0.0
        self._rolling_5s_bps = 0.0
        self._p10_5s_bps = 0.0
        self._mean_5s_bps = 0.0
        self._stddev_5s_bps = 0.0
        self._cv = 0.0
        self._metric_sample_count = 0
        self._fallbacks = 0
        self._closing = False
        self._closed = False
        self._close_task: Optional[asyncio.Task] = None
        self._snapshot = self._make_snapshot()

    def start(self) -> None:
        """Start the one-second committed-byte sampler on the app loop."""
        if self._closing:
            raise RuntimeError("media transfer coordinator is closing")
        if self._metrics_task is None:
            self._metrics_task = asyncio.create_task(self._metrics_loop())

    async def create_sessions(
        self,
        pool_id: str,
        dc_id: int,
        count: int,
        expansion: bool = False,
        on_session: Optional[Callable[[OwnedMediaSession], None]] = None,
    ) -> list[OwnedMediaSession]:
        if count < 0:
            raise ValueError("count must not be negative")
        if count == 0:
            return []

        creator = asyncio.current_task()
        if creator is None:
            raise RuntimeError("session creation requires an asyncio task")
        reused: List[OwnedMediaSession] = []
        remaining = count
        registered_creator = False
        while True:
            await self.wait_for_dc(dc_id)
            retire: List[OwnedMediaSession] = []
            async with self._condition:
                if self._closing:
                    raise RuntimeError("media transfer coordinator is closing")
                if self._dc_is_paused(dc_id):
                    continue
                now = self._clock()
                if expansion and self._last_expansion is not None:
                    if now - self._last_expansion < self._config.expansion_interval:
                        return []

                expired = self._expired_idle_locked(now)
                if expired:
                    retire = expired
                    self._mark_idle_retiring_locked(retire)
                else:
                    reusable = list(self._idle_by_dc.get(dc_id, ()))[:count]
                    remaining = count - len(reusable)
                    used = self._creating + self._live + self._draining
                    over_budget = max(
                        0,
                        used + remaining - self._config.max_sessions,
                    )
                    if over_budget:
                        excluded = set(reusable)
                        candidates = [
                            owned
                            for owned in self._idle_sessions_locked()
                            if owned not in excluded
                        ]
                        if len(candidates) < over_budget:
                            return []
                        retire = candidates[:over_budget]
                        self._mark_idle_retiring_locked(retire)
                    else:
                        reused = self._activate_idle_locked(
                            reusable,
                            pool_id,
                        )
                        self._creating += remaining
                        self._pool_used[pool_id] = (
                            self._pool_used.get(pool_id, 0) + count
                        )
                        if expansion:
                            self._last_expansion = now
                        if remaining:
                            self._creation_tasks.add(creator)
                            registered_creator = True
                        self._publish_locked()
                        self._condition.notify_all()
                        break

            if retire:
                retirement = asyncio.create_task(self._retire_group(retire))
                cancellation = await _join_shielded(retirement)
                retirement.result()
                if cancellation is not None:
                    raise cancellation

        created: List[OwnedMediaSession] = list(reused)
        delivered: Set[OwnedMediaSession] = set()
        failure = None
        failure_traceback = None
        cleaned = False
        try:
            if on_session is not None:
                for owned in reused:
                    on_session(owned)
                    delivered.add(owned)
            for _ in range(remaining):
                factory_task: asyncio.Task = asyncio.create_task(self._factory(dc_id))
                self._factory_tasks.add(factory_task)
                factory_cancellation = await _join_factory_task(factory_task)
                try:
                    session = factory_task.result()
                finally:
                    self._factory_tasks.discard(factory_task)
                registration = asyncio.create_task(
                    self._register_session(pool_id, dc_id, session)
                )
                cancellation = await _join_shielded(registration)
                owned, closing = registration.result()
                created.append(owned)
                remaining -= 1
                if on_session is not None:
                    on_session(owned)
                    delivered.add(owned)
                if factory_cancellation is not None:
                    raise factory_cancellation
                if cancellation is not None:
                    raise cancellation
                if closing:
                    raise RuntimeError("media transfer coordinator is closing")
        except BaseException as error:
            failure = error
            failure_traceback = error.__traceback__

        if failure is not None:
            cleanup = asyncio.create_task(
                self._cleanup_failed_group(
                    pool_id,
                    remaining,
                    [owned for owned in created if owned not in delivered],
                )
            )
            await _join_shielded(cleanup)
            cleanup.result()
            cleaned = True

        if registered_creator:
            unregister = asyncio.create_task(self._unregister_creator(creator))
            unregister_cancellation = await _join_shielded(unregister)
            try:
                unregister.result()
            except BaseException as error:
                if failure is None:
                    failure = error
                    failure_traceback = error.__traceback__
            if unregister_cancellation is not None and failure is None:
                failure = unregister_cancellation
                failure_traceback = unregister_cancellation.__traceback__

        if failure is not None and not cleaned:
            cleanup = asyncio.create_task(
                self._cleanup_failed_group(
                    pool_id,
                    remaining,
                    [owned for owned in created if owned not in delivered],
                )
            )
            await _join_shielded(cleanup)
            cleanup.result()

        if failure is not None:
            raise failure.with_traceback(failure_traceback)
        return created

    def pause_dc(self, dc_id: int, seconds: float) -> None:
        if self._closing:
            return
        now = self._clock()
        requested = now + max(0.0, seconds)
        current = self._dc_cooldowns.get(dc_id, now)
        deadline = max(current, requested)
        if deadline <= now:
            return
        if deadline == current:
            return
        self._dc_cooldowns[dc_id] = deadline
        event = self._dc_events.get(dc_id)
        if event is None or event.is_set():
            event = asyncio.Event()
            self._dc_events[dc_id] = event
        task = asyncio.create_task(self._wake_dc(dc_id, deadline, deadline - now))
        self._wake_tasks.add(task)
        task.add_done_callback(self._wake_tasks.discard)
        self._publish_current()

    async def wait_for_dc(self, dc_id: int) -> None:
        while self.dc_is_paused(dc_id):
            if self._closing:
                raise RuntimeError("media transfer coordinator is closing")
            event = self._dc_events[dc_id]
            await asyncio.shield(event.wait())

    def dc_is_paused(self, dc_id: int) -> bool:
        paused = self._dc_is_paused(dc_id)
        self._publish_current()
        return paused

    def record_committed(self, pool_id: str, byte_count: int) -> None:
        self._pool_committed[pool_id] = (
            self._pool_committed.get(pool_id, 0) + byte_count
        )
        second = int(self._clock())
        self._metric_bytes[second] = self._metric_bytes.get(second, 0) + byte_count
        pool_buckets = self._pool_metric_bytes.setdefault(pool_id, {})
        pool_buckets[second] = pool_buckets.get(second, 0) + byte_count
        self._publish_current()

    def record_fallback(self) -> None:
        self._fallbacks += 1
        self._publish_current()

    def update_pool(self, snapshot) -> None:
        if is_dataclass(snapshot):
            values = asdict(snapshot)
        elif isinstance(snapshot, Mapping):
            values = copy.deepcopy(dict(snapshot))
        else:
            values = copy.deepcopy(vars(snapshot))
        pool_id = values.pop("pool_id")
        self._pool_snapshots[pool_id] = values
        self._publish_current()

    def snapshot(self) -> CoordinatorSnapshot:
        with self._snapshot_lock:
            snapshot = self._snapshot
            return replace(
                snapshot,
                dc_cooldowns=_freeze(snapshot.dc_cooldowns),
                pools=_freeze(snapshot.pools),
            )

    def close(self) -> Coroutine[Any, Any, None]:
        if self._close_task is None:
            self._closing = True
            for event in self._dc_events.values():
                event.set()
            for task in list(self._wake_tasks):
                task.cancel()
            for task in list(self._factory_tasks):
                task.cancel()
            if self._metrics_task is not None:
                self._metrics_task.cancel()
            self._publish_current()
            self._close_task = asyncio.create_task(self._close_all())
        return self._wait_closed()

    async def _wait_closed(self) -> None:
        close_task = self._close_task
        if close_task is None:
            raise AssertionError("coordinator close task was not started")
        cancellation = await _join_shielded(close_task)
        close_task.result()
        if cancellation is not None:
            raise cancellation

    def _idle_sessions_locked(self) -> List[OwnedMediaSession]:
        return [owned for sessions in self._idle_by_dc.values() for owned in sessions]

    def _expired_idle_locked(self, now: float) -> List[OwnedMediaSession]:
        return [
            owned
            for owned in self._idle_sessions_locked()
            if owned._idle_since is not None
            and now - owned._idle_since >= self._config.warm_session_ttl
        ]

    def _remove_idle_locked(self, owned: OwnedMediaSession) -> None:
        sessions = self._idle_by_dc.get(owned.dc_id)
        if sessions is None:
            return
        try:
            sessions.remove(owned)
        except ValueError:
            return
        if not sessions:
            self._idle_by_dc.pop(owned.dc_id, None)

    def _mark_idle_retiring_locked(
        self,
        sessions: List[OwnedMediaSession],
    ) -> None:
        for owned in sessions:
            self._remove_idle_locked(owned)
            owned._state = "retiring"

    def _activate_idle_locked(
        self,
        sessions: List[OwnedMediaSession],
        pool_id: str,
    ) -> List[OwnedMediaSession]:
        for owned in sessions:
            self._remove_idle_locked(owned)
            owned._pool_id = pool_id
            owned._idle_since = None
            owned._release_task = None
            owned._state = "live"
        self._reused += len(sessions)
        return sessions

    async def _retire_group(self, sessions: List[OwnedMediaSession]) -> None:
        results = await asyncio.gather(
            *(owned.close() for owned in sessions),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise errors[0]

    async def _register_session(self, pool_id: str, dc_id: int, session: Any):
        async with self._condition:
            self._creating -= 1
            self._live += 1
            self._created += 1
            owned = OwnedMediaSession(self, pool_id, dc_id, session)
            self._owned.add(owned)
            self._publish_locked()
            self._condition.notify_all()
            return owned, self._closing

    async def _release_reservations(self, pool_id: str, count: int) -> None:
        if not count:
            return
        async with self._condition:
            self._creating -= count
            self._release_pool_slots(pool_id, count)
            self._publish_locked()
            self._condition.notify_all()

    async def _cleanup_failed_group(
        self,
        pool_id: str,
        remaining: int,
        created: list[OwnedMediaSession],
    ) -> None:
        await self._release_reservations(pool_id, remaining)
        if created:
            await asyncio.gather(
                *(owned.close() for owned in created),
                return_exceptions=True,
            )

    async def _unregister_creator(self, creator: asyncio.Task) -> None:
        async with self._condition:
            self._creation_tasks.discard(creator)
            self._condition.notify_all()

    async def _release_owned(self, owned: OwnedMediaSession) -> None:
        should_stop = False
        async with self._condition:
            if owned._state != "live":
                return
            idle_limit = min(
                self._config.warm_session_limit,
                self._config.max_sessions,
            )
            should_stop = (
                self._closing
                or not self._config.warm_session_handoff
                or len(self._idle_sessions_locked()) >= idle_limit
            )
            if not should_stop:
                pool_id = owned._pool_id
                owned._pool_id = None
                owned._idle_since = self._clock()
                owned._state = "idle"
                self._idle_by_dc.setdefault(owned.dc_id, deque()).append(owned)
                self._release_pool_slots(pool_id, 1)
                self._publish_locked()
                self._condition.notify_all()
                return

        if should_stop:
            await self._stop_owned(owned)

    async def _stop_owned(self, owned: OwnedMediaSession) -> None:
        pool_id = None
        async with self._condition:
            while owned._state == "draining":
                await self._condition.wait()
            if owned._state == "released":
                return
            if owned._state == "idle":
                self._remove_idle_locked(owned)
            elif owned._state == "live":
                pool_id = owned._pool_id
            owned._state = "draining"
            self._live -= 1
            self._draining += 1
            self._publish_locked()
            self._condition.notify_all()

        stop_task = asyncio.create_task(owned.session.stop())
        cancellation = await _join_shielded(stop_task)
        async with self._condition:
            owned._state = "released"
            owned._pool_id = None
            owned._idle_since = None
            self._draining -= 1
            self._owned.discard(owned)
            self._release_pool_slots(pool_id, 1)
            self._publish_locked()
            self._condition.notify_all()
        stop_task.result()
        if cancellation is not None:
            raise cancellation

    async def _wake_dc(self, dc_id: int, deadline: float, delay: float) -> None:
        try:
            remaining = delay
            while self._dc_cooldowns.get(dc_id) == deadline and remaining > 0:
                await self._tick(remaining)
                remaining = deadline - self._clock()
            self._finish_dc_cooldown(dc_id, deadline)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("DC %s cooldown wake failed; failing open", dc_id)
            self._finish_dc_cooldown(dc_id, deadline)

    async def _metrics_loop(self) -> None:
        try:
            while not self._closing:
                deadline = self._next_metric_second + 1
                now = self._clock()
                if now < deadline:
                    await self._tick(deadline - now)
                    now = self._clock()
                if now < deadline:
                    await asyncio.sleep(0)
                    continue
                self._finalize_metric_buckets(now)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Committed-byte metrics loop stopped unexpectedly")

    def _finalize_metric_buckets(self, now: float) -> None:
        cutoff = int(now)
        changed = False
        while self._next_metric_second < cutoff:
            second = self._next_metric_second
            self._next_metric_second += 1
            raw_bps = float(self._metric_bytes.pop(second, 0))
            self._raw_samples.append(raw_bps)
            self._metric_sample_count += 1
            self._raw_bps = raw_bps
            recent = list(self._raw_samples)[-5:]
            self._rolling_5s_bps = sum(recent) / len(recent)
            if len(recent) == 5:
                self._rolling_5s_samples.append(self._rolling_5s_bps)

            pool_ids = (
                set(self._pool_raw_samples)
                | set(self._pool_metric_bytes)
                | set(self._pool_used)
            )
            for pool_id in pool_ids:
                pool_buckets = self._pool_metric_bytes.get(pool_id, {})
                pool_raw_bps = float(pool_buckets.pop(second, 0))
                if not pool_buckets:
                    self._pool_metric_bytes.pop(pool_id, None)
                history = self._pool_raw_samples.setdefault(pool_id, deque(maxlen=5))
                history.append(pool_raw_bps)
                self._pool_raw_bps[pool_id] = pool_raw_bps
                self._pool_rolling_5s_bps[pool_id] = sum(history) / len(history)

                if self._pool_used.get(pool_id, 0) or pool_raw_bps:
                    self._pool_idle_metric_windows[pool_id] = 0
                else:
                    idle_windows = self._pool_idle_metric_windows.get(pool_id, 0) + 1
                    self._pool_idle_metric_windows[pool_id] = idle_windows
                    if idle_windows >= 5 and not any(history):
                        self._pool_raw_samples.pop(pool_id, None)
                        self._pool_raw_bps.pop(pool_id, None)
                        self._pool_rolling_5s_bps.pop(pool_id, None)
                        self._pool_idle_metric_windows.pop(pool_id, None)
                        self._pool_snapshots.pop(pool_id, None)
                        self._pool_committed.pop(pool_id, None)
            changed = True

        if not changed:
            return
        rolling_samples = list(self._rolling_5s_samples)
        self._p10_5s_bps = _percentile(rolling_samples, 0.10)
        if rolling_samples:
            self._mean_5s_bps = sum(rolling_samples) / len(rolling_samples)
            variance = sum(
                (sample - self._mean_5s_bps) ** 2 for sample in rolling_samples
            ) / len(rolling_samples)
            self._stddev_5s_bps = math.sqrt(variance)
            self._cv = (
                self._stddev_5s_bps / self._mean_5s_bps
                if self._mean_5s_bps > 0
                else 0.0
            )
        else:
            self._mean_5s_bps = 0.0
            self._stddev_5s_bps = 0.0
            self._cv = 0.0
        self._publish_current()

    async def _close_all(self) -> None:
        metrics_task = self._metrics_task
        if metrics_task is not None:
            metrics_task.cancel()
            await asyncio.gather(metrics_task, return_exceptions=True)

        wake_tasks = list(self._wake_tasks)
        for task in wake_tasks:
            task.cancel()
        if wake_tasks:
            await asyncio.gather(*wake_tasks, return_exceptions=True)

        errors: List[BaseException] = []
        while True:
            async with self._condition:
                creators = [task for task in self._creation_tasks if task is not None]
                for task in creators:
                    task.cancel()
                owned = list(self._owned)
                used = self._creating + self._live + self._draining
                if used == 0 and not creators:
                    self._closed = True
                    self._publish_locked()
                    self._condition.notify_all()
                    break
                if not owned:
                    await self._condition.wait()
                    continue
            results = await asyncio.gather(
                *(session.close() for session in owned),
                return_exceptions=True,
            )
            errors.extend(
                result for result in results if isinstance(result, BaseException)
            )
        if errors:
            raise errors[0]

    def _dc_is_paused(self, dc_id: int) -> bool:
        deadline = self._dc_cooldowns.get(dc_id)
        if deadline is None:
            return False
        if deadline > self._clock():
            return True
        self._dc_cooldowns.pop(dc_id, None)
        event = self._dc_events.get(dc_id)
        if event is not None:
            event.set()
        return False

    def _finish_dc_cooldown(self, dc_id: int, deadline: float) -> None:
        if self._dc_cooldowns.get(dc_id) != deadline:
            return
        self._dc_cooldowns.pop(dc_id, None)
        event = self._dc_events.get(dc_id)
        if event is not None:
            event.set()
        self._publish_current()

    def _release_pool_slots(self, pool_id: str, count: int) -> None:
        remaining = self._pool_used.get(pool_id, 0) - count
        if remaining > 0:
            self._pool_used[pool_id] = remaining
        else:
            self._pool_used.pop(pool_id, None)

    def _publish_current(self) -> None:
        snapshot = self._make_snapshot()
        with self._snapshot_lock:
            self._snapshot = snapshot

    def _publish_locked(self) -> None:
        self._publish_current()

    def _make_snapshot(self) -> CoordinatorSnapshot:
        used = self._creating + self._live + self._draining
        if min(self._creating, self._live, self._draining) < 0:
            raise AssertionError("coordinator budget counters diverged")
        if used > self._config.max_sessions:
            raise AssertionError("coordinator session budget exceeded")
        pool_ids = (
            set(self._pool_used)
            | set(self._pool_metric_bytes)
            | set(self._pool_raw_samples)
        )
        pools = {}
        for pool_id in pool_ids:
            values = copy.deepcopy(self._pool_snapshots.get(pool_id, {}))
            values["used"] = self._pool_used.get(pool_id, 0)
            values["committed_bytes"] = self._pool_committed.get(pool_id, 0)
            values["raw_bps"] = self._pool_raw_bps.get(pool_id, 0.0)
            values["rolling_5s_bps"] = self._pool_rolling_5s_bps.get(pool_id, 0.0)
            pools[pool_id] = values
        return CoordinatorSnapshot(
            used=used,
            hard_limit=self._config.max_sessions,
            creating=self._creating,
            live=self._live,
            idle=len(self._idle_sessions_locked()),
            created=self._created,
            reused=self._reused,
            draining=self._draining,
            active_files=sum(count > 0 for count in self._pool_used.values()),
            committed_bytes_per_second=self._rolling_5s_bps,
            raw_bps=self._raw_bps,
            rolling_5s_bps=self._rolling_5s_bps,
            p10_5s_bps=self._p10_5s_bps,
            mean_5s_bps=self._mean_5s_bps,
            stddev_5s_bps=self._stddev_5s_bps,
            cv=self._cv,
            sample_count=self._metric_sample_count,
            raw_samples=tuple(self._raw_samples),
            rolling_5s_samples=tuple(self._rolling_5s_samples),
            fallbacks=self._fallbacks,
            expansion_queue=0,
            dc_cooldowns=_freeze(self._dc_cooldowns),
            pools=_freeze(pools),
        )


@dataclass(frozen=True)
class FilePoolConfig:
    initial_sessions: int = 4
    max_sessions: int = 12
    control_interval: float = 10.0
    growth_hold: float = 120.0
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if self.initial_sessions != 4:
            raise ValueError("initial_sessions must be 4")
        if self.max_sessions not in (4, 8, 12):
            raise ValueError("max_sessions must be one of 4, 8, or 12")
        if self.control_interval <= 0:
            raise ValueError("control_interval must be positive")
        if self.growth_hold <= 0:
            raise ValueError("growth_hold must be positive")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")


@dataclass(frozen=True)
class FilePoolWindow:
    pending: int
    utilization: float
    retry_rate: float
    unhealthy_fraction: float
    flood_wait: bool
    committed_bytes_per_second: float
    stable_windows: int


@dataclass(frozen=True)
class FileScaleDecision:
    target: int
    reason: str
    hold_until: float = 0.0


class FilePoolController:
    _TIERS = (4, 8, 12)

    def __init__(self, config: FilePoolConfig):
        self._config = config
        self._target = config.initial_sessions
        self._pre_growth_target: Optional[int] = None
        self._pre_growth_goodput = 0.0
        self._evaluation: List[float] = []
        self._hold_until = 0.0

    @property
    def target(self) -> int:
        return self._target

    def observe(self, window: FilePoolWindow, now: float) -> FileScaleDecision:
        if window.pending <= 0 or window.pending < self._target:
            if self._pre_growth_target is not None:
                self._target = self._pre_growth_target
                self._pre_growth_target = None
                self._evaluation.clear()
            return FileScaleDecision(
                min(self._target, max(window.pending, 0)),
                "tail",
                self._hold_until,
            )
        if window.flood_wait:
            return FileScaleDecision(self._target, "flood_wait", self._hold_until)
        if window.retry_rate >= 0.02 or window.unhealthy_fraction > 0.10:
            tier = self._TIERS.index(self._target)
            self._target = self._TIERS[max(0, tier - 1)]
            self._pre_growth_target = None
            self._evaluation.clear()
            self._hold_until = now + self._config.growth_hold
            return FileScaleDecision(self._target, "unhealthy", self._hold_until)
        if self._pre_growth_target is not None:
            self._evaluation.append(window.committed_bytes_per_second)
            if len(self._evaluation) < 2:
                return FileScaleDecision(self._target, "evaluating")
            average = sum(self._evaluation) / len(self._evaluation)
            if self._pre_growth_goodput == 0:
                improved = average > 0
            else:
                improved = average >= self._pre_growth_goodput * 1.05
            if not improved:
                self._target = self._pre_growth_target
                self._hold_until = now + self._config.growth_hold
                reason = "plateau"
            else:
                reason = "goodput_growth"
            self._pre_growth_target = None
            self._evaluation.clear()
            return FileScaleDecision(self._target, reason, self._hold_until)
        if now < self._hold_until:
            return FileScaleDecision(self._target, "growth_hold", self._hold_until)
        if window.stable_windows < 2 or window.utilization < 0.80:
            return FileScaleDecision(self._target, "not_ready", self._hold_until)
        tier = self._TIERS.index(self._target)
        if tier == len(self._TIERS) - 1:
            return FileScaleDecision(self._target, "max_tier", self._hold_until)
        next_target = self._TIERS[tier + 1]
        if next_target > self._config.max_sessions:
            return FileScaleDecision(self._target, "max_tier", self._hold_until)
        if self._target <= window.pending < next_target:
            return FileScaleDecision(window.pending, "tail", self._hold_until)
        previous_target = self._target
        self._target = next_target
        self._pre_growth_target = previous_target
        self._pre_growth_goodput = window.committed_bytes_per_second
        return FileScaleDecision(self._target, "expand", self._hold_until)


class StripeAttemptError(Exception):
    """Describe the outcome of one file-stripe callback attempt."""

    _KINDS = frozenset(("transport", "flood_wait", "fatal"))

    def __init__(
        self,
        error: BaseException,
        kind: str,
        wait_seconds: float = 0.0,
        completed: bool = False,
    ):
        if kind not in self._KINDS:
            raise ValueError("kind must be transport, flood_wait, or fatal")
        if wait_seconds < 0:
            raise ValueError("wait_seconds must not be negative")
        super().__init__(str(error))
        self.error = error
        self.kind = kind
        self.wait_seconds = float(wait_seconds)
        self.completed = completed


@dataclass(frozen=True)
class FilePoolSnapshot:
    pool_id: str
    target: int
    live: int
    active: int
    draining: int
    pending: int
    committed_bytes_per_second: float
    retries: int
    resets: int
    unhealthy: int
    tier: int
    last_scale_reason: str


@dataclass(eq=False)
class _FileWorker:
    owned_session: OwnedMediaSession
    task: Optional[asyncio.Task] = None
    current: Any = None
    draining: bool = False
    closed: bool = False
    consecutive_transport_failures: int = 0
    unhealthy: bool = False
    reusable: bool = False


class FileMediaSessionPool:
    """Own dedicated media-session workers for one file download."""

    def __init__(
        self,
        coordinator: MediaTransferCoordinator,
        config: FilePoolConfig,
        pool_id: str,
        dc_id: int,
        clock: Callable[[], float] = time.monotonic,
        tick: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self._coordinator = coordinator
        self._config = config
        self.pool_id = pool_id
        self.dc_id = dc_id
        self._clock = clock
        self._tick = tick
        self._controller = FilePoolController(config)
        self._queue: Deque[Any] = deque()
        self._workers: List[_FileWorker] = []
        self._download_stripe: Optional[Callable[[Any, Any], Awaitable[None]]] = None
        self._completion: Optional[asyncio.Future] = None
        self._fatal_error: Optional[BaseException] = None
        self._control_task: Optional[asyncio.Task] = None
        self._target = 0
        self._committed_bytes = 0
        self._committed_bytes_per_second = 0.0
        self._retries = 0
        self._resets = 0
        self._window_started_at = 0.0
        self._window_committed_bytes = 0
        self._window_retries = 0
        self._window_attempts = 0
        self._window_unhealthy: Set[_FileWorker] = set()
        self._window_peak_live = 0
        self._stable_windows = 0
        self._window_active_area = 0.0
        self._window_live_area = 0.0
        self._utilization_sampled_at = 0.0
        self._last_scale_reason = "initial"
        self._started = False
        self._closing = False
        self._closed = False
        self._run_task: Optional[asyncio.Task] = None
        self._close_task: Optional[asyncio.Task] = None
        self._snapshot_lock = threading.Lock()
        self._snapshot = self._make_snapshot()
        self._publish()

    async def run(self, stripes, download_stripe) -> None:
        if self._closing:
            raise RuntimeError("file media session pool is already closed")
        if self._started:
            raise RuntimeError("file media session pool can only run once")
        self._started = True
        self._run_task = asyncio.current_task()
        self._download_stripe = download_stripe
        self._queue.extend(stripes)
        self._target = min(self._config.initial_sessions, len(self._queue))
        self._window_started_at = self._clock()
        self._utilization_sampled_at = self._window_started_at
        self._completion = asyncio.get_running_loop().create_future()
        self._publish()

        failure = None
        failure_traceback = None
        try:
            await self._start_workers(self._target, expansion=False)
            self._control_task = asyncio.create_task(self._control_loop())
            self._check_completion()
            await self._completion
            if self._fatal_error is not None:
                raise self._fatal_error
        except BaseException as error:
            failure = error
            failure_traceback = error.__traceback__

        try:
            await self.close()
        except BaseException as error:
            if failure is None:
                failure = error
                failure_traceback = error.__traceback__
        if failure is None and self._fatal_error is not None:
            failure = self._fatal_error
            failure_traceback = failure.__traceback__

        if failure is not None:
            raise failure.with_traceback(failure_traceback)

    def record_committed(self, byte_count: int) -> None:
        self._committed_bytes += byte_count
        self._coordinator.record_committed(self.pool_id, byte_count)
        self._publish()

    def snapshot(self) -> FilePoolSnapshot:
        with self._snapshot_lock:
            return replace(self._snapshot)

    def close(self) -> Coroutine[Any, Any, None]:
        if self._close_task is None:
            self._closing = True
            run_task = self._run_task
            if (
                run_task is not None
                and run_task is not asyncio.current_task()
                and not run_task.done()
            ):
                run_task.cancel()
            if self._control_task is not None and not self._control_task.done():
                self._control_task.cancel()
            for worker in self._workers:
                worker.draining = True
                if worker.task is not None and not worker.task.done():
                    worker.task.cancel()
            self._publish()
            self._close_task = asyncio.create_task(self._finish_close())
        return self._wait_closed()

    async def _wait_closed(self) -> None:
        close_task = self._close_task
        if close_task is None:
            raise AssertionError("file pool close task was not started")
        cancellation = await _join_shielded(close_task)
        close_task.result()
        if cancellation is not None:
            raise cancellation

    async def _start_workers(self, count: int, expansion: bool) -> int:
        def start_worker(owned_session: OwnedMediaSession) -> None:
            self._accrue_utilization()
            worker = _FileWorker(owned_session)
            self._workers.append(worker)
            worker.task = asyncio.create_task(self._worker(worker))
            self._window_peak_live = max(self._window_peak_live, self._live())
            self._publish()

        sessions = await self._coordinator.create_sessions(
            self.pool_id,
            self.dc_id,
            count,
            expansion=expansion,
            on_session=start_worker,
        )
        return len(sessions)

    async def _worker(self, worker: _FileWorker) -> None:
        try:
            while not self._closing and not worker.draining:
                try:
                    stripe = self._queue.popleft()
                except IndexError:
                    worker.reusable = True
                    return
                self._accrue_utilization()
                worker.current = stripe
                worker.reusable = False
                self._publish()
                callback = self._download_stripe
                if callback is None:
                    raise AssertionError("file pool callback is not configured")
                while True:
                    await self._coordinator.wait_for_dc(self.dc_id)
                    self._window_attempts += 1
                    try:
                        await callback(worker.owned_session.session, stripe)
                    except StripeAttemptError as failure:
                        if failure.completed:
                            break
                        if failure.kind == "fatal":
                            raise failure.error
                        if failure.kind == "flood_wait":
                            stripe.attempts += 1
                            self._retries += 1
                            self._window_retries += 1
                            self._coordinator.pause_dc(
                                self.dc_id,
                                failure.wait_seconds,
                            )
                            self._publish()
                            if stripe.attempts >= self._config.max_attempts:
                                raise failure.error
                            self._accrue_utilization()
                            self._queue.appendleft(stripe)
                            worker.current = None
                            self._publish()
                            await self._coordinator.wait_for_dc(self.dc_id)
                            if self._closing or worker.draining:
                                return
                            try:
                                stripe = self._queue.popleft()
                            except IndexError:
                                return
                            self._accrue_utilization()
                            worker.current = stripe
                            self._publish()
                            continue
                        if failure.kind != "transport":
                            raise
                        stripe.attempts += 1
                        self._retries += 1
                        self._window_retries += 1
                        worker.consecutive_transport_failures += 1
                        self._publish()
                        if stripe.attempts >= self._config.max_attempts:
                            raise failure.error
                        if worker.consecutive_transport_failures >= 2:
                            self._accrue_utilization()
                            worker.unhealthy = True
                            worker.draining = True
                            self._resets += 1
                            self._window_unhealthy.add(worker)
                            self._queue.appendleft(stripe)
                            worker.current = None
                            self._publish()
                            return
                        await self._tick(min(2 ** (stripe.attempts - 1), 8.0))
                        continue
                    break
                self._accrue_utilization()
                worker.current = None
                worker.reusable = True
                worker.consecutive_transport_failures = 0
                self._target = min(self._target, self._pending())
                self._publish()
                self._check_completion()
        except asyncio.CancelledError as error:
            self._clear_current(worker)
            if not self._closing:
                self._set_fatal(error)
        except BaseException as error:
            self._clear_current(worker)
            self._set_fatal(error)
        finally:
            self._accrue_utilization()
            worker.draining = True
            self._publish()
            try:
                if (
                    worker.reusable
                    and not worker.unhealthy
                    and self._fatal_error is None
                ):
                    await worker.owned_session.release()
                else:
                    await worker.owned_session.close()
            except asyncio.CancelledError as error:
                if not self._closing:
                    self._set_fatal(error)
            except BaseException as error:
                self._set_fatal(error)
            self._accrue_utilization()
            worker.closed = True
            self._target = min(self._target, self._pending())
            self._publish()
            self._check_completion()

    async def _control_loop(self) -> None:
        try:
            while not self._closing:
                deadline = self._window_started_at + self._config.control_interval
                while not self._closing and self._clock() < deadline:
                    await self._tick(deadline - self._clock())
                    if self._clock() < deadline:
                        await asyncio.sleep(0)
                if self._closing:
                    return
                await self._complete_window(self._clock())
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._set_fatal(error)

    async def _complete_window(self, now: float) -> None:
        self._accrue_utilization(now)
        elapsed = max(now - self._window_started_at, self._config.control_interval)
        pending = self._pending()
        live = self._live()
        retry_rate = self._window_retries / max(self._window_attempts, 1)
        unhealthy_fraction = len(self._window_unhealthy) / max(
            self._window_peak_live,
            1,
        )
        flood_wait = self._coordinator.dc_is_paused(self.dc_id)
        utilization = (
            self._window_active_area / self._window_live_area
            if self._window_live_area > 0
            else 0.0
        )
        self._committed_bytes_per_second = (
            self._committed_bytes - self._window_committed_bytes
        ) / elapsed
        stable = (
            pending > 0
            and utilization >= 0.80
            and retry_rate < 0.02
            and not self._window_unhealthy
            and not flood_wait
        )
        self._stable_windows = self._stable_windows + 1 if stable else 0
        window = FilePoolWindow(
            pending=pending,
            utilization=utilization,
            retry_rate=retry_rate,
            unhealthy_fraction=unhealthy_fraction,
            flood_wait=flood_wait,
            committed_bytes_per_second=self._committed_bytes_per_second,
            stable_windows=self._stable_windows,
        )
        controller_state = copy.deepcopy(self._controller.__dict__)
        decision = self._controller.observe(window, now)
        self._last_scale_reason = decision.reason
        self._window_started_at = now
        self._window_committed_bytes = self._committed_bytes
        self._window_retries = 0
        self._window_attempts = 0
        self._window_unhealthy.clear()
        self._window_peak_live = live
        self._window_active_area = 0.0
        self._window_live_area = 0.0
        self._utilization_sampled_at = now
        await self._apply_decision(decision, controller_state)
        self._publish()

    async def _apply_decision(
        self,
        decision: FileScaleDecision,
        controller_state: Dict[str, Any],
    ) -> None:
        desired = min(decision.target, self._pending(), self._config.max_sessions)
        if desired < self._target:
            self._target = desired
            self._mark_excess_draining(max(0, self._available_workers() - desired))
        if self._coordinator.dc_is_paused(self.dc_id):
            return
        missing = max(0, desired - self._available_workers())
        if missing:
            expansion = desired > self._target
            granted = await self._start_workers(missing, expansion=expansion)
            if granted:
                self._target = desired
            elif expansion:
                self._controller.__dict__.clear()
                self._controller.__dict__.update(controller_state)
                self._last_scale_reason = "growth_denied"

    def _mark_excess_draining(self, count: int) -> None:
        candidates = [
            worker
            for worker in self._workers
            if not worker.closed and not worker.draining
        ]
        candidates.sort(key=lambda worker: worker.current is not None)
        for worker in candidates[:count]:
            worker.draining = True
        self._publish()

    async def _finish_close(self) -> None:
        tasks = [worker.task for worker in self._workers if worker.task is not None]
        if self._control_task is not None:
            tasks.append(self._control_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        remaining = [worker for worker in self._workers if not worker.closed]
        if remaining:
            await asyncio.gather(
                *(worker.owned_session.close() for worker in remaining),
                return_exceptions=True,
            )
        self._accrue_utilization()
        for worker in self._workers:
            worker.closed = True
        self._closed = True
        self._target = 0
        self._publish()

    def _set_fatal(self, error: BaseException) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
        completion = self._completion
        if completion is not None and not completion.done():
            completion.set_result(None)
        self._publish()

    def _check_completion(self) -> None:
        completion = self._completion
        if (
            completion is not None
            and not completion.done()
            and (self._fatal_error is not None or self._pending() == 0)
        ):
            completion.set_result(None)

    def _clear_current(self, worker: _FileWorker) -> None:
        if worker.current is None:
            return
        self._accrue_utilization()
        worker.current = None
        self._publish()

    def _accrue_utilization(self, now: Optional[float] = None) -> None:
        sampled_at = self._clock() if now is None else now
        elapsed = max(0.0, sampled_at - self._utilization_sampled_at)
        if elapsed:
            live = self._live()
            active = sum(
                worker.current is not None
                for worker in self._workers
                if not worker.closed
            )
            self._window_active_area += active * elapsed
            self._window_live_area += live * elapsed
        self._utilization_sampled_at = sampled_at

    def _live(self) -> int:
        return sum(not worker.closed for worker in self._workers)

    def _available_workers(self) -> int:
        return sum(
            not worker.closed and not worker.draining for worker in self._workers
        )

    def _pending(self) -> int:
        return len(self._queue) + sum(
            worker.current is not None for worker in self._workers
        )

    def _make_snapshot(self) -> FilePoolSnapshot:
        live_workers = [worker for worker in self._workers if not worker.closed]
        return FilePoolSnapshot(
            pool_id=self.pool_id,
            target=self._target,
            live=len(live_workers),
            active=sum(worker.current is not None for worker in live_workers),
            draining=sum(worker.draining for worker in live_workers),
            pending=self._pending(),
            committed_bytes_per_second=self._committed_bytes_per_second,
            retries=self._retries,
            resets=self._resets,
            unhealthy=sum(worker.unhealthy for worker in live_workers),
            tier=self._controller.target,
            last_scale_reason=self._last_scale_reason,
        )

    def _publish(self) -> None:
        snapshot = self._make_snapshot()
        with self._snapshot_lock:
            self._snapshot = snapshot
        self._coordinator.update_pool(snapshot)
