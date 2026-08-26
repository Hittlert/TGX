# Global Adaptive Media Session Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable, DC-aware Telegram media-session pool that schedules 5 MiB logical stripes fairly, scales from 16 to at most 48 sessions, and preserves the existing SQLite resume and SHA-256 integrity chain.

**Architecture:** A pure deficit-round-robin scheduler orders stripe lease requests by file and DC. A process-wide `GlobalMediaSessionPool` owns Kurigram temporary media sessions, grants up to two concurrent leases per session, replaces unhealthy sessions, and uses an adaptive controller to change the desired pool size. The existing `ParallelDownloader` keeps file planning, positional writes, manifests, and final verification, but obtains a pool lease for each 5 MiB stripe instead of owning temporary sessions per file.

**Tech Stack:** Python 3.11, asyncio, Kurigram 2.2.24 (`pyrogram` namespace), SQLite, unittest, Flask, Docker Compose.

## Global Constraints

- Modify NAS files only below `/volume2/docker/telegram_media_downloader_us`.
- Keep `max_download_task` at 5.
- Keep Telegram protocol chunks at 1 MiB and logical scheduler stripes at 5 MiB.
- Keep `parallel_download_workers: 2` and `parallel_download_min_size: 268435456` unchanged for rollback compatibility.
- New pool defaults stay disabled in repository code until NAS validation passes.
- Global live plus creating media sessions never exceed 48.
- A session remains bound to one DC; CDN sessions remain Kurigram-owned and outside the pool.
- Preserve SQLite records, per-file manifests, SSD positional writes, final readback, whole-file SHA-256, and sequential fallback.
- Do not stage or revert unrelated dirty worktree changes.
- Run dependency-bearing tests in the NAS Docker image because the local Python environment does not contain the application dependencies.

For each task, sync only that task's changed files into the NAS `dev-test`
tree before running its `python -m unittest` command inside
`telegram_media_downloader_us:parallel-a9452ad`. Pure scheduler tests may also
run locally. Do not mount an incomplete `module` directory over `/app/module`;
mount each changed module file individually and mount `dev-test/tests` over
`/app/tests`.

---

### Task 1: Add Pool Configuration Without Changing Rollback Keys

**Files:**
- Modify: `module/app.py:449-455`
- Modify: `module/app.py:860-895`
- Test: `tests/module/test_app.py`

**Interfaces:**
- Produces: `Application.parallel_session_pool_enabled: bool`
- Produces: `Application.parallel_pool_file_threshold: int`
- Produces: `Application.parallel_pool_stripe_size: int`
- Produces: `Application.parallel_pool_soft_sessions: int`
- Produces: `Application.parallel_pool_max_sessions: int`
- Produces: `Application.parallel_pool_pipeline_depth: int`
- Produces: `Application.parallel_pool_idle_ttl: int`
- Produces: `Application.parallel_pool_control_interval: int`

- [ ] **Step 1: Write failing default and assignment tests**

Add these assertions to `tests/module/test_app.py`:

```python
def test_global_pool_defaults_are_disabled_and_bounded(self):
    app = Application("", "")

    self.assertFalse(app.parallel_session_pool_enabled)
    self.assertEqual(5 * 1024 * 1024, app.parallel_pool_file_threshold)
    self.assertEqual(5 * 1024 * 1024, app.parallel_pool_stripe_size)
    self.assertEqual(16, app.parallel_pool_soft_sessions)
    self.assertEqual(48, app.parallel_pool_max_sessions)
    self.assertEqual(2, app.parallel_pool_pipeline_depth)
    self.assertEqual(600, app.parallel_pool_idle_ttl)
    self.assertEqual(60, app.parallel_pool_control_interval)

def test_assign_config_reads_global_pool_values(self):
    app = Application("", "")
    app.assign_config({
        "api_id": 123,
        "api_hash": "hash",
        "media_types": [],
        "file_formats": {},
        "parallel_session_pool_enabled": True,
        "parallel_pool_file_threshold": 6 * 1024 * 1024,
        "parallel_pool_stripe_size": 10 * 1024 * 1024,
        "parallel_pool_soft_sessions": 24,
        "parallel_pool_max_sessions": 40,
        "parallel_pool_pipeline_depth": 1,
        "parallel_pool_idle_ttl": 900,
        "parallel_pool_control_interval": 30,
    })

    self.assertTrue(app.parallel_session_pool_enabled)
    self.assertEqual(6 * 1024 * 1024, app.parallel_pool_file_threshold)
    self.assertEqual(10 * 1024 * 1024, app.parallel_pool_stripe_size)
    self.assertEqual(24, app.parallel_pool_soft_sessions)
    self.assertEqual(40, app.parallel_pool_max_sessions)
    self.assertEqual(1, app.parallel_pool_pipeline_depth)
    self.assertEqual(900, app.parallel_pool_idle_ttl)
    self.assertEqual(30, app.parallel_pool_control_interval)

def test_invalid_global_pool_values_fall_back_to_defaults(self):
    app = Application("", "")
    app.assign_config({
        "api_id": 123,
        "api_hash": "hash",
        "media_types": [],
        "file_formats": {},
        "parallel_pool_file_threshold": 0,
        "parallel_pool_stripe_size": 3 * 1024 * 1024 + 1,
        "parallel_pool_soft_sessions": 49,
        "parallel_pool_max_sessions": 99,
        "parallel_pool_pipeline_depth": 9,
        "parallel_pool_idle_ttl": 0,
        "parallel_pool_control_interval": 0,
    })

    self.assertEqual(5 * 1024 * 1024, app.parallel_pool_file_threshold)
    self.assertEqual(5 * 1024 * 1024, app.parallel_pool_stripe_size)
    self.assertEqual(16, app.parallel_pool_soft_sessions)
    self.assertEqual(48, app.parallel_pool_max_sessions)
    self.assertEqual(2, app.parallel_pool_pipeline_depth)
    self.assertEqual(600, app.parallel_pool_idle_ttl)
    self.assertEqual(60, app.parallel_pool_control_interval)
```

- [ ] **Step 2: Run the tests and verify RED**

Sync `module/app.py` and `tests/module/test_app.py` into the NAS `dev-test`
tree, then run:

```bash
source ~/.zshrc
printf '%s\n' "$tmp_pwd2" | ssh de1ta@192.168.79.37 \
  "sudo -S docker run --rm \
   -v /volume2/docker/telegram_media_downloader_us/dev-test/module/app.py:/app/module/app.py:ro \
   -v /volume2/docker/telegram_media_downloader_us/dev-test/tests:/app/tests:ro \
   --entrypoint python telegram_media_downloader_us:parallel-a9452ad \
   -m unittest tests.module.test_app.AppTestCase.test_global_pool_defaults_are_disabled_and_bounded \
   tests.module.test_app.AppTestCase.test_assign_config_reads_global_pool_values \
   tests.module.test_app.AppTestCase.test_invalid_global_pool_values_fall_back_to_defaults"
```

Expected: three failures because the new attributes do not exist.

- [ ] **Step 3: Add defaults and validated config assignment**

Add these defaults beside the existing parallel-download fields:

```python
self.parallel_session_pool_enabled: bool = False
self.parallel_pool_file_threshold: int = 5 * 1024 * 1024
self.parallel_pool_stripe_size: int = 5 * 1024 * 1024
self.parallel_pool_soft_sessions: int = 16
self.parallel_pool_max_sessions: int = 48
self.parallel_pool_pipeline_depth: int = 2
self.parallel_pool_idle_ttl: int = 600
self.parallel_pool_control_interval: int = 60
self.media_session_pool = None
```

Read the keys in `assign_config()` and enforce these exact ranges:

```python
self.parallel_session_pool_enabled = get_config(
    _config,
    "parallel_session_pool_enabled",
    self.parallel_session_pool_enabled,
    bool,
)

def bounded_int(name, default, minimum, maximum):
    value = get_config(_config, name, default, int)
    return value if minimum <= value <= maximum else default

self.parallel_pool_file_threshold = bounded_int(
    "parallel_pool_file_threshold", 5 * 1024 * 1024, 1024 * 1024, 1024**3
)
self.parallel_pool_stripe_size = bounded_int(
    "parallel_pool_stripe_size", 5 * 1024 * 1024, 1024 * 1024, 64 * 1024 * 1024
)
if self.parallel_pool_stripe_size % (1024 * 1024):
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
```

Keep the existing `parallel_download_workers` and
`parallel_download_min_size` parsing unchanged.

- [ ] **Step 4: Run the focused and existing app tests**

Run the three tests from Step 2 plus:

```bash
python -m unittest \
  tests.module.test_app.AppTestCase.test_parallel_download_defaults_are_conservative \
  tests.module.test_app.AppTestCase.test_assign_config_reads_parallel_canary_values
```

Expected: all five tests pass.

- [ ] **Step 5: Commit only the configuration change**

```bash
git add module/app.py tests/module/test_app.py
git commit -m "feat: configure adaptive media session pool"
```

---

### Task 2: Build The Pure Deficit-Round-Robin Scheduler

**Files:**
- Create: `module/download_stripe_scheduler.py`
- Create: `tests/module/test_download_stripe_scheduler.py`

**Interfaces:**
- Produces: `DownloadStripeScheduler[T].enqueue(dc_id: int, transfer_id: str, item: T) -> None`
- Produces: `DownloadStripeScheduler[T].pop_next(dc_id: int) -> Optional[T]`
- Produces: `DownloadStripeScheduler[T].cancel(item: T) -> bool`
- Produces: `DownloadStripeScheduler[T].remove_transfer(dc_id: int, transfer_id: str) -> List[T]`
- Produces: `pending_count(dc_id: Optional[int] = None) -> int`
- Produces: `active_transfer_count(dc_id: int) -> int`

- [ ] **Step 1: Write failing fairness, cancellation, and work-conservation tests**

Create `tests/module/test_download_stripe_scheduler.py`:

```python
import unittest

from module.download_stripe_scheduler import DownloadStripeScheduler


class DownloadStripeSchedulerTest(unittest.TestCase):
    def test_round_robins_files_within_one_dc(self):
        scheduler = DownloadStripeScheduler()
        scheduler.enqueue(4, "a", "a1")
        scheduler.enqueue(4, "a", "a2")
        scheduler.enqueue(4, "b", "b1")
        scheduler.enqueue(4, "b", "b2")

        self.assertEqual(
            ["a1", "b1", "a2", "b2"],
            [scheduler.pop_next(4) for _ in range(4)],
        )

    def test_single_file_consumes_all_available_turns(self):
        scheduler = DownloadStripeScheduler()
        for number in range(4):
            scheduler.enqueue(2, "only", number)

        self.assertEqual([0, 1, 2, 3], [scheduler.pop_next(2) for _ in range(4)])

    def test_never_pops_another_dc(self):
        scheduler = DownloadStripeScheduler()
        scheduler.enqueue(2, "a", "dc2")
        scheduler.enqueue(5, "b", "dc5")

        self.assertEqual("dc2", scheduler.pop_next(2))
        self.assertIsNone(scheduler.pop_next(2))
        self.assertEqual("dc5", scheduler.pop_next(5))

    def test_cancel_and_remove_transfer_preserve_rotation(self):
        scheduler = DownloadStripeScheduler()
        scheduler.enqueue(4, "a", "a1")
        scheduler.enqueue(4, "a", "a2")
        scheduler.enqueue(4, "b", "b1")

        self.assertTrue(scheduler.cancel("a1"))
        self.assertEqual(["a2"], scheduler.remove_transfer(4, "a"))
        self.assertEqual("b1", scheduler.pop_next(4))
        self.assertEqual(0, scheduler.pending_count())
```

- [ ] **Step 2: Run the scheduler tests and verify RED**

```bash
python -m unittest tests.module.test_download_stripe_scheduler
```

Expected: import failure for `module.download_stripe_scheduler`.

- [ ] **Step 3: Implement the deterministic scheduler**

Create a generic scheduler backed by an owner queue per DC:

```python
"""Fair logical-stripe scheduling by Telegram DC and transfer."""

from collections import OrderedDict, deque
from typing import Deque, Dict, Generic, List, Optional, TypeVar

T = TypeVar("T")


class DownloadStripeScheduler(Generic[T]):
    def __init__(self):
        self._items: Dict[int, OrderedDict[str, Deque[T]]] = {}
        self._rotation: Dict[int, Deque[str]] = {}

    def enqueue(self, dc_id: int, transfer_id: str, item: T) -> None:
        owners = self._items.setdefault(dc_id, OrderedDict())
        rotation = self._rotation.setdefault(dc_id, deque())
        if transfer_id not in owners:
            owners[transfer_id] = deque()
            rotation.append(transfer_id)
        owners[transfer_id].append(item)

    def pop_next(self, dc_id: int) -> Optional[T]:
        owners = self._items.get(dc_id)
        rotation = self._rotation.get(dc_id)
        if not owners or not rotation:
            return None
        transfer_id = rotation.popleft()
        queue = owners[transfer_id]
        item = queue.popleft()
        if queue:
            rotation.append(transfer_id)
        else:
            del owners[transfer_id]
        self._prune(dc_id)
        return item

    def cancel(self, item: T) -> bool:
        for dc_id, owners in list(self._items.items()):
            for transfer_id, queue in list(owners.items()):
                try:
                    queue.remove(item)
                except ValueError:
                    continue
                if not queue:
                    self._drop_owner(dc_id, transfer_id)
                self._prune(dc_id)
                return True
        return False

    def remove_transfer(self, dc_id: int, transfer_id: str) -> List[T]:
        owners = self._items.get(dc_id, OrderedDict())
        removed = list(owners.get(transfer_id, ()))
        self._drop_owner(dc_id, transfer_id)
        self._prune(dc_id)
        return removed

    def pending_count(self, dc_id: Optional[int] = None) -> int:
        if dc_id is not None:
            return sum(len(queue) for queue in self._items.get(dc_id, {}).values())
        return sum(
            len(queue)
            for owners in self._items.values()
            for queue in owners.values()
        )

    def active_transfer_count(self, dc_id: int) -> int:
        return len(self._items.get(dc_id, {}))

    def dc_ids(self) -> List[int]:
        return [dc_id for dc_id in self._items if self.pending_count(dc_id)]

    def _drop_owner(self, dc_id: int, transfer_id: str) -> None:
        owners = self._items.get(dc_id)
        rotation = self._rotation.get(dc_id)
        if owners is not None:
            owners.pop(transfer_id, None)
        if rotation is not None:
            self._rotation[dc_id] = deque(
                owner for owner in rotation if owner != transfer_id
            )

    def _prune(self, dc_id: int) -> None:
        if not self._items.get(dc_id):
            self._items.pop(dc_id, None)
            self._rotation.pop(dc_id, None)
```

- [ ] **Step 4: Run the tests and verify GREEN**

```bash
python -m unittest tests.module.test_download_stripe_scheduler
```

Expected: four tests pass.

- [ ] **Step 5: Commit the scheduler**

```bash
git add module/download_stripe_scheduler.py tests/module/test_download_stripe_scheduler.py
git commit -m "feat: add fair download stripe scheduler"
```

---

### Task 3: Implement The DC-Aware Reusable Session Pool

**Files:**
- Create: `module/media_session_pool.py`
- Create: `tests/module/test_media_session_pool.py`
- Modify: `module/pyrogram_extension.py:1429-1442`

**Interfaces:**
- Consumes: `DownloadStripeScheduler`
- Produces: `MediaSessionPoolConfig`
- Produces: `GlobalMediaSessionPool.acquire(dc_id: int, transfer_id: str) -> SessionLease`
- Produces: `GlobalMediaSessionPool.transfer(dc_id: int, transfer_id: str)` async context manager
- Produces: `GlobalMediaSessionPool.close() -> None`
- Produces: `GlobalMediaSessionPool.record_committed(byte_count: int) -> None`
- Produces: `GlobalMediaSessionPool.record_retry() -> None`
- Produces: `GlobalMediaSessionPool.record_fallback() -> None`
- Produces: `GlobalMediaSessionPool.pause_dc(dc_id: int, seconds: float) -> None`
- Produces: `SessionLease.mark_transport_failure() -> None`
- Produces: `SessionLease.mark_unhealthy() -> None`
- Produces: `GlobalMediaSessionPool.snapshot() -> PoolSnapshot`

- [ ] **Step 1: Write failing pool lifecycle tests**

Create fakes with observable session identity and `stop()` calls, then add tests
covering these exact cases:

```python
import asyncio
import collections
import dataclasses
import unittest

from module.media_session_pool import (
    GlobalMediaSessionPool,
    MediaSessionPoolConfig,
)


class FakeSession:
    def __init__(self, dc_id, number):
        self.dc_id = dc_id
        self.number = number
        self.stop_calls = 0

    async def stop(self):
        self.stop_calls += 1


class FakeFactory:
    def __init__(self):
        self.sessions = []

    async def __call__(self, dc_id):
        session = FakeSession(dc_id, len(self.sessions))
        self.sessions.append(session)
        return session


class BlockingFactory(FakeFactory):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, dc_id):
        self.started.set()
        await self.release.wait()
        return await super().__call__(dc_id)


class TrackingFactory(FakeFactory):
    def __init__(self):
        super().__init__()
        self.active_by_dc = collections.Counter()
        self.max_active_by_dc = collections.Counter()

    async def __call__(self, dc_id):
        self.active_by_dc[dc_id] += 1
        self.max_active_by_dc[dc_id] = max(
            self.max_active_by_dc[dc_id],
            self.active_by_dc[dc_id],
        )
        await asyncio.sleep(0)
        try:
            return await super().__call__(dc_id)
        finally:
            self.active_by_dc[dc_id] -= 1


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class FakeKurigramClient:
    def __init__(self, error):
        self.sessions_lock = asyncio.Lock()
        self.cached = FakeSession(4, "cached")
        self.media_sessions = {4: self.cached}
        self.error = error
        self.calls = 0

    async def get_session(self, dc_id, **kwargs):
        self.calls += 1
        raise self.error


class GlobalMediaSessionPoolTest(unittest.IsolatedAsyncioTestCase):
    async def test_never_exceeds_live_plus_creating_hard_limit(self):
        factory = BlockingFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=4, max_sessions=4, pipeline_depth=1),
        )
        tasks = [asyncio.create_task(pool.acquire(4, f"file-{i}")) for i in range(8)]
        await factory.started.wait()
        self.assertLessEqual(pool.snapshot().live + pool.snapshot().creating, 4)
        factory.release.set()
        leases = await asyncio.gather(*tasks[:4])
        for lease in leases:
            await lease.release()
        for task in tasks[4:]:
            task.cancel()
        await asyncio.gather(*tasks[4:], return_exceptions=True)
        await pool.close()

    async def test_sessions_never_cross_dc_boundaries(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=2, max_sessions=2, pipeline_depth=1),
        )
        lease2, lease5 = await asyncio.gather(
            pool.acquire(2, "dc2-file"),
            pool.acquire(5, "dc5-file"),
        )
        self.assertEqual(2, lease2.dc_id)
        self.assertEqual(5, lease5.dc_id)
        self.assertEqual(2, lease2.session.dc_id)
        self.assertEqual(5, lease5.session.dc_id)
        await lease2.release()
        await lease5.release()
        await pool.close()

    async def test_pipeline_depth_allows_two_leases_per_session(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=2),
        )
        first, second = await asyncio.gather(
            pool.acquire(4, "a"),
            pool.acquire(4, "b"),
        )
        self.assertIs(first.session, second.session)
        self.assertEqual(2, pool.snapshot().pipeline_depth)
        self.assertEqual(2, pool.snapshot().active_slots)
        await first.release()
        await second.release()
        await pool.close()

    async def test_each_waiting_dc_receives_capacity(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=3, max_sessions=3, pipeline_depth=1),
        )
        leases = await asyncio.gather(
            pool.acquire(2, "dc2-a"),
            pool.acquire(4, "dc4-a"),
            pool.acquire(5, "dc5-a"),
        )
        self.assertEqual({2, 4, 5}, {lease.dc_id for lease in leases})
        for lease in leases:
            await lease.release()
        await pool.close()

    async def test_session_creation_is_serial_within_each_dc(self):
        factory = TrackingFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=4, max_sessions=4, pipeline_depth=1),
        )
        leases = await asyncio.gather(
            *(pool.acquire(4, f"file-{number}") for number in range(4))
        )
        self.assertEqual(1, factory.max_active_by_dc[4])
        for lease in leases:
            await lease.release()
        await pool.close()

    async def test_lru_idle_session_is_evicted_for_waiting_dc(self):
        factory = FakeFactory()
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=2, max_sessions=2, pipeline_depth=1),
            clock=clock,
        )
        old = await pool.acquire(2, "old")
        await old.release()
        clock.advance(1)
        recent = await pool.acquire(4, "recent")
        await recent.release()

        dc5 = await pool.acquire(5, "new-dc")

        self.assertEqual(5, dc5.dc_id)
        self.assertEqual(1, old.session.stop_calls)
        self.assertEqual(0, recent.session.stop_calls)
        await dc5.release()
        await pool.close()

    async def test_waiting_files_receive_round_robin_leases(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        held = await pool.acquire(4, "held")
        order = []

        async def waiter(owner):
            lease = await pool.acquire(4, owner)
            order.append(owner)
            await lease.release()

        tasks = [
            asyncio.create_task(waiter("a")),
            asyncio.create_task(waiter("a")),
            asyncio.create_task(waiter("b")),
            asyncio.create_task(waiter("b")),
        ]
        await held.release()
        await asyncio.gather(*tasks)
        self.assertEqual(["a", "b", "a", "b"], order)
        await pool.close()

    async def test_unhealthy_session_is_stopped_and_replaced(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        bad = await pool.acquire(4, "a")
        bad_session = bad.session
        bad.mark_unhealthy()
        await bad.release()
        replacement = await pool.acquire(4, "a")
        self.assertIsNot(bad_session, replacement.session)
        self.assertEqual(1, bad_session.stop_calls)
        await replacement.release()
        await pool.close()

    async def test_two_transport_failures_retire_a_session(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        first = await pool.acquire(4, "a")
        original = first.session
        first.mark_transport_failure()
        await first.release()
        second = await pool.acquire(4, "a")
        self.assertIs(original, second.session)
        second.mark_transport_failure()
        await second.release()
        replacement = await pool.acquire(4, "a")
        self.assertIsNot(original, replacement.session)
        await replacement.release()
        await pool.close()

    async def test_close_stops_every_owned_session_once_and_cancels_waiters(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        lease = await pool.acquire(4, "a")
        waiter = asyncio.create_task(pool.acquire(4, "b"))
        close_task = asyncio.create_task(pool.close())
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertFalse(close_task.done())
        await lease.release()
        await close_task
        self.assertEqual(1, lease.session.stop_calls)
```

- [ ] **Step 2: Run the pool tests and verify RED**

```bash
python -m unittest tests.module.test_media_session_pool.GlobalMediaSessionPoolTest
```

Expected: import failure for `module.media_session_pool`.

- [ ] **Step 3: Implement pool types and lease accounting**

Define these public data types exactly:

```python
@dataclass(frozen=True)
class MediaSessionPoolConfig:
    soft_sessions: int = 16
    max_sessions: int = 48
    pipeline_depth: int = 2
    idle_ttl: float = 600.0
    control_interval: float = 60.0
    adaptive: bool = True


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
```

Implement `GlobalMediaSessionPool` around one `asyncio.Lock`, a list of session
entries, `DownloadStripeScheduler` waiters, per-DC builder tasks, and these
invariants:

```python
async def acquire(self, dc_id: int, transfer_id: str) -> SessionLease:
    loop = asyncio.get_running_loop()
    waiter = _LeaseWaiter(dc_id, transfer_id, loop.create_future())
    async with self._lock:
        if self._closing:
            raise asyncio.CancelledError()
        self._waiters.enqueue(dc_id, transfer_id, waiter)
        self._dispatch_locked(dc_id)
        self._ensure_builder_locked(dc_id)
    try:
        return await waiter.future
    except BaseException:
        async with self._lock:
            self._waiters.cancel(waiter)
        raise

async def _release(self, entry, failure_kind: str) -> None:
    stop_session = None
    async with self._lock:
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
            self._entries.remove(entry)
            stop_session = entry.session
        self._dispatch_locked(entry.dc_id)
        self._ensure_builder_locked(entry.dc_id)
    if stop_session is not None:
        await stop_session.stop()
```

Register active files with an async context manager:

```python
@contextlib.asynccontextmanager
async def transfer(self, dc_id: int, transfer_id: str):
    async with self._lock:
        self._active_transfers.add((dc_id, transfer_id))
        self._refresh_snapshot_locked()
    try:
        yield
    finally:
        async with self._lock:
            self._active_transfers.discard((dc_id, transfer_id))
            removed = self._waiters.remove_transfer(dc_id, transfer_id)
            for waiter in removed:
                if not waiter.future.done():
                    waiter.future.cancel()
            self._refresh_snapshot_locked()
```

`_dispatch_locked(dc_id)` repeatedly selects an entry with
`active_slots < pipeline_depth`, pops the next fair waiter for that DC,
increments `active_slots`, and resolves the waiter with a `SessionLease`.

The builder increments `_creating` before leaving the lock, invokes the
factory outside the lock, decrements `_creating`, appends a healthy entry, and
dispatches waiters. Count `_entries + _creating` before every build. Build one
session at a time per DC.

When the hard limit is full and a waiting DC has no compatible slot, select
the globally oldest entry with `active_slots == 0`, remove it under the lock,
and stop it outside the lock before building the replacement. Mark active
entries for retirement only when no idle entry exists. `close()` cancels all
waiters, rejects new acquisitions, and waits on an `asyncio.Condition` until
active slots reach zero before stopping sessions. `_release()` notifies that
condition after decrementing `active_slots`, and neither release nor a builder
may create replacement sessions once `_closing` is true. `close()` also
cancels and awaits every builder and delayed DC wake-up task before returning.

- [ ] **Step 4: Add the Kurigram session factory and explicit shared lock**

In `HookClient.__init__`, create an explicit lock after `super().__init__`:

```python
if not hasattr(self, "sessions_lock"):
    self.sessions_lock = asyncio.Lock()
```

Add `KurigramMediaSessionFactory` in `module/media_session_pool.py`:

```python
class KurigramMediaSessionFactory:
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
                        temporary=True,
                    )
                except AuthBytesInvalid as error:
                    last_error = error
                    stale = self.client.media_sessions.pop(dc_id, None)
                    if stale is not None:
                        await stale.stop()
            raise last_error
```

Import `AuthBytesInvalid` from `pyrogram.errors`. Tests must assert three
failed attempts, cache eviction, and stopping the cached object with this
exact case:

```python
async def test_kurigram_factory_retries_auth_and_evicts_stale_cache(self):
    error = AuthBytesInvalid()
    client = FakeKurigramClient(error)
    factory = KurigramMediaSessionFactory(client, attempts=3)

    with self.assertRaises(AuthBytesInvalid):
        await factory(4)

    self.assertEqual(3, client.calls)
    self.assertNotIn(4, client.media_sessions)
    self.assertEqual(1, client.cached.stop_calls)
```

- [ ] **Step 5: Run pool, scheduler, and existing temporary-session tests**

```bash
python -m unittest \
  tests.module.test_download_stripe_scheduler \
  tests.module.test_media_session_pool \
  tests.module.test_parallel_downloader.KurigramSessionPoolTest \
  tests.module.test_parallel_downloader.SessionBoundClientTest
```

Expected: all tests pass. Existing per-file session behavior remains available
for rollback mode.

- [ ] **Step 6: Commit the reusable pool core**

```bash
git add \
  module/download_stripe_scheduler.py \
  module/media_session_pool.py \
  tests/module/test_download_stripe_scheduler.py \
  tests/module/test_media_session_pool.py
git add -p module/pyrogram_extension.py
git commit -m "feat: add reusable DC media session pool"
```

---

### Task 4: Add Adaptive Scaling And Pool Snapshots

**Files:**
- Modify: `module/media_session_pool.py`
- Modify: `tests/module/test_media_session_pool.py`

**Interfaces:**
- Produces: `PoolWindow`
- Produces: `ScaleDecision`
- Produces: `AdaptivePoolController.observe(window: PoolWindow, now: float) -> ScaleDecision`
- Extends: `GlobalMediaSessionPool.start() -> None`
- Extends: `GlobalMediaSessionPool.snapshot() -> PoolSnapshot`

- [ ] **Step 1: Write failing controller tests**

Add pure tests for the exact state machine:

```python
class AdaptivePoolControllerTest(unittest.TestCase):
    def stable(self, goodput=10.0):
        return PoolWindow(
            pending=100,
            utilization=0.9,
            retry_rate=0.0,
            unhealthy_fraction=0.0,
            flood_wait=False,
            committed_bytes_per_second=goodput,
        )

    def test_scales_16_to_48_in_steps_of_eight(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        targets = []
        now = 0.0
        for goodput in (10, 12, 12, 15, 15, 18, 18, 21, 21, 24):
            decision = controller.observe(self.stable(goodput), now)
            targets.append(decision.target)
            now += 60
        self.assertEqual(48, max(targets))
        self.assertTrue(all(target <= 48 for target in targets))

    def test_reverts_expansion_after_two_plateau_windows(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        self.assertEqual(24, controller.observe(self.stable(10), 0).target)
        controller.observe(self.stable(10.1), 60)
        decision = controller.observe(self.stable(10.2), 120)
        self.assertEqual(16, decision.target)
        self.assertEqual("plateau", decision.reason)
        self.assertGreater(decision.hold_until, 120)

    def test_flood_wait_freezes_growth(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        window = dataclasses.replace(self.stable(), flood_wait=True)
        decision = controller.observe(window, 0)
        self.assertEqual(16, decision.target)
        self.assertEqual("flood_wait", decision.reason)

    def test_unhealthy_window_reduces_target_by_eight(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        controller._target = 32
        window = dataclasses.replace(self.stable(), unhealthy_fraction=0.2)
        self.assertEqual(24, controller.observe(window, 0).target)

    def test_no_pending_work_sets_demand_target_to_zero(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        window = dataclasses.replace(self.stable(), pending=0, utilization=0.0)
        self.assertEqual(0, controller.observe(window, 0).target)
```

- [ ] **Step 2: Run controller tests and verify RED**

```bash
python -m unittest tests.module.test_media_session_pool.AdaptivePoolControllerTest
```

Expected: import or attribute failures for the new controller types.

- [ ] **Step 3: Implement the pure controller**

Use immutable inputs and outputs:

```python
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
```

`AdaptivePoolController` starts at 16 when demand first appears, expands by 8
only when utilization is at least 0.8 and retry rate is below 0.02, evaluates
two post-expansion windows, accepts at least 5 percent goodput improvement,
reverts and holds for 600 seconds on plateau, freezes on FloodWait, reduces by
8 when unhealthy fraction exceeds 0.10, and never exceeds 48.

- [ ] **Step 4: Integrate the control loop and thread-readable snapshot**

`GlobalMediaSessionPool.start()` creates one control task. Every configured
interval it computes committed-byte goodput from `record_committed()`, derives
utilization from active slots divided by live capacity, calls `observe()`,
updates `_desired`, retires excess idle entries by LRU order, and starts
builders for waiting DCs.

When `config.adaptive` is false, `start()` records a fixed-target snapshot but
does not create the control task. Acquire and release still build, dispatch,
replace, and close sessions normally at the configured soft target.

Store the latest immutable `PoolSnapshot` behind a `threading.Lock` so Flask
can read it without touching asyncio state. `close()` cancels and awaits the
control task before stopping sessions.

Log the same snapshot once per control window, including pipeline depth,
per-DC counts, committed-byte goodput, creation/eviction/retry/FloodWait/
fallback counters, and the latest scale reason. Do not log one line per stripe.

Add a per-DC `paused_until` map. `pause_dc(dc_id, seconds)` increments the
FloodWait counter, records `clock() + seconds`, prevents `_dispatch_locked`
from assigning new leases for that DC, and schedules a wake-up at expiry.
Healthy sessions and waiters remain in the pool. The control window reports
FloodWait while any DC pause is active and therefore freezes expansion.

Add this idle-reaping test with the fake clock from Task 3:

```python
async def test_control_tick_reaps_only_expired_idle_sessions(self):
    factory = FakeFactory()
    clock = FakeClock()
    pool = GlobalMediaSessionPool(
        factory,
        MediaSessionPoolConfig(
            soft_sessions=1,
            max_sessions=1,
            pipeline_depth=1,
            idle_ttl=600,
        ),
        clock=clock,
    )
    lease = await pool.acquire(4, "idle")
    session = lease.session
    await lease.release()

    clock.advance(599)
    await pool._control_once()
    self.assertEqual(0, session.stop_calls)

    clock.advance(2)
    await pool._control_once()
    self.assertEqual(1, session.stop_calls)
    self.assertEqual(0, pool.snapshot().live)
    await pool.close()
```

- [ ] **Step 5: Run controller and lifecycle tests**

```bash
python -m unittest tests.module.test_media_session_pool
```

Expected: all controller, cap, DC, pipeline, replacement, and close tests pass.

- [ ] **Step 6: Commit adaptive control**

```bash
git add module/media_session_pool.py tests/module/test_media_session_pool.py
git commit -m "feat: adapt media pool to measured goodput"
```

---

### Task 5: Route 5 MiB Stripes Through Global Pool Leases

**Files:**
- Modify: `module/parallel_downloader.py`
- Modify: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Consumes: `GlobalMediaSessionPool.acquire()` and `.transfer()`
- Produces: `plan_missing_stripes(chunks, completed_offsets, stripe_size)`
- Produces: `KurigramRangeSource.iter_range_on_session(session, start_offset, expected_length)`
- Extends: `ParallelDownloader(..., pool=None, stripe_size=5 * CHUNK_SIZE, transfer_id=None)`

- [ ] **Step 1: Write failing stripe-planning tests**

```python
class StripePlanningTest(unittest.TestCase):
    def test_groups_five_one_mib_chunks_per_stripe(self):
        chunks = plan_chunks(12 * CHUNK_SIZE)
        stripes = plan_missing_stripes(chunks, set(), 5 * CHUNK_SIZE)
        self.assertEqual([5, 5, 2], [len(stripe) for stripe in stripes])

    def test_resume_never_bridges_a_completed_chunk(self):
        chunks = plan_chunks(12 * CHUNK_SIZE)
        stripes = plan_missing_stripes(
            chunks,
            {5 * CHUNK_SIZE},
            5 * CHUNK_SIZE,
        )
        self.assertEqual(
            [[0, 1, 2, 3, 4], [6, 7, 8, 9, 10], [11]],
            [[chunk.offset // CHUNK_SIZE for chunk in stripe] for stripe in stripes],
        )

    def test_final_short_protocol_chunk_stays_in_final_stripe(self):
        chunks = plan_chunks(5 * CHUNK_SIZE + 123)
        stripes = plan_missing_stripes(chunks, set(), 5 * CHUNK_SIZE)
        self.assertEqual([5 * CHUNK_SIZE, 123], [sum(c.length for c in s) for s in stripes])
```

- [ ] **Step 2: Write failing pooled-download behavior tests**

Use a fake pool whose leases contain distinct fake sessions and a source that
records each session passed to `iter_range_on_session`:

```python
import collections
import contextlib

class FakeLease:
    def __init__(self, pool, session):
        self.pool = pool
        self.session = session
        self.failure = ""

    def mark_transport_failure(self):
        self.failure = "transport"

    def mark_unhealthy(self):
        self.failure = "fatal"

    async def release(self):
        if self.failure:
            self.pool.unhealthy_sessions.append(self.session)
        self.pool.active_leases -= 1

    async def __aenter__(self):
        return self

    async def __aexit__(self, _type, _value, _traceback):
        await self.release()


class FakeLeasePool:
    def __init__(self, sessions):
        self.sessions = collections.deque(sessions)
        self.transfer_ids = []
        self.closed_transfers = []
        self.unhealthy_sessions = []
        self.active_leases = 0
        self.committed = 0
        self.retries = 0

    @contextlib.asynccontextmanager
    async def transfer(self, _dc_id, transfer_id):
        self.transfer_ids.append(transfer_id)
        try:
            yield
        finally:
            self.closed_transfers.append(transfer_id)

    async def acquire(self, _dc_id, _transfer_id):
        session = self.sessions.popleft()
        self.sessions.append(session)
        self.active_leases += 1
        return FakeLease(self, session)

    def record_committed(self, byte_count):
        self.committed += byte_count

    def record_retry(self):
        self.retries += 1

    def pause_dc(self, _dc_id, _seconds):
        return None


class SessionAwareMemorySource(MemoryRangeSource):
    def __init__(self, data, chunk_size=CHUNK_SIZE):
        super().__init__(data, chunk_size)
        self.sessions_seen = []

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        async for chunk in super().iter_range(start_offset, expected_length):
            yield chunk


class FailingSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data, bad_session):
        super().__init__(data)
        self.bad_session = bad_session
        self.failed = False

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        if session == self.bad_session and not self.failed:
            self.failed = True
            raise asyncio.TimeoutError()
        async for chunk in MemoryRangeSource.iter_range(
            self, start_offset, expected_length
        ):
            yield chunk


class BlockingSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data):
        super().__init__(data)
        self.started = asyncio.Event()

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.started.set()
        await asyncio.Event().wait()
        if False:
            yield b""


class PooledParallelDownloaderTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.part_path = Path(self.temp_dir.name) / "pooled.part"
        self.data = b"x" * (6 * CHUNK_SIZE)

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    async def test_each_stripe_uses_a_fair_pool_lease(self):
        data = bytes(range(251)) * 50000
        source = SessionAwareMemorySource(data, chunk_size=CHUNK_SIZE)
        pool = FakeLeasePool(["session-a", "session-b"])
        downloader = ParallelDownloader(
            source,
            workers=2,
            pool=pool,
            stripe_size=5 * CHUNK_SIZE,
            transfer_id="file-a",
        )

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertGreaterEqual(len(source.sessions_seen), 2)
        self.assertEqual({"file-a"}, set(pool.transfer_ids))
        self.assertEqual(data, self.part_path.read_bytes())

    async def test_retryable_failure_releases_bad_session_and_resumes_elsewhere(self):
        source = FailingSessionMemorySource(self.data, bad_session="bad")
        pool = FakeLeasePool(["bad", "good"])
        downloader = ParallelDownloader(
            source,
            pool=pool,
            stripe_size=5 * CHUNK_SIZE,
            transfer_id="retry-file",
            sleep=no_sleep,
        )

        result = await downloader.download(identity_for(len(self.data)), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertIn("bad", pool.unhealthy_sessions)
        self.assertIn("good", source.sessions_seen)

    async def test_cancellation_unregisters_transfer_and_releases_leases(self):
        source = BlockingSessionMemorySource(self.data)
        pool = FakeLeasePool(["session"])
        task = asyncio.create_task(
            ParallelDownloader(source, pool=pool, transfer_id="cancel-file").download(
                identity_for(len(self.data)), self.part_path
            )
        )
        await source.started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(["cancel-file"], pool.closed_transfers)
        self.assertEqual(0, pool.active_leases)
```

- [ ] **Step 3: Run focused tests and verify RED**

```bash
python -m unittest \
  tests.module.test_parallel_downloader.StripePlanningTest \
  tests.module.test_parallel_downloader.PooledParallelDownloaderTest
```

Expected: failures for missing stripe planner, pooled constructor arguments,
and session-specific iterator.

- [ ] **Step 4: Implement exact missing-stripe grouping**

```python
def plan_missing_stripes(chunks, completed_offsets, stripe_size):
    if stripe_size < CHUNK_SIZE or stripe_size % CHUNK_SIZE:
        raise ValueError("stripe_size must be a positive 1 MiB multiple")
    completed = set(completed_offsets)
    stripes = []
    current = []
    current_bytes = 0
    previous_end = None
    for chunk in sorted(chunks, key=lambda item: item.offset):
        if chunk.offset in completed:
            if current:
                stripes.append(current)
                current = []
                current_bytes = 0
            previous_end = None
            continue
        contiguous = previous_end is None or previous_end == chunk.offset
        if current and (not contiguous or current_bytes + chunk.length > stripe_size):
            stripes.append(current)
            current = []
            current_bytes = 0
        current.append(chunk)
        current_bytes += chunk.length
        previous_end = chunk.offset + chunk.length
    if current:
        stripes.append(current)
    return stripes
```

- [ ] **Step 5: Refactor the range source to accept a leased session**

Extract the validation and stream-consumption body from `iter_range()` so both
paths share it. Add:

```python
async def iter_range_on_session(self, session, start_offset, expected_length):
    bound_client = _SessionBoundClient(self.client, session)
    stream = self.client.get_file.__func__(
        bound_client,
        self.file_id,
        self.file_size,
        limit=(expected_length + CHUNK_SIZE - 1) // CHUNK_SIZE,
        offset=start_offset // CHUNK_SIZE,
    )
    async for chunk in self._validated_stream(
        stream,
        start_offset,
        expected_length,
    ):
        yield chunk
```

Keep existing `iter_range()` behavior unchanged for per-file rollback mode.

- [ ] **Step 6: Add pooled execution to ParallelDownloader**

Add optional `pool`, `stripe_size`, and `transfer_id` constructor fields. In
pooled mode, skip source `prepare()` and `close()`, register one transfer
context, create one task per missing stripe, and acquire a lease inside every
retry attempt:

```python
async with self.pool.transfer(identity.dc_id, self.transfer_id):
    tasks = [
        asyncio.create_task(
            self._download_group(
                fd,
                manifest,
                stripe,
                identity.file_size,
                progress,
                dc_id=identity.dc_id,
                transfer_id=self.transfer_id,
            )
        )
        for stripe in plan_missing_stripes(
            chunks,
            completed.keys(),
            self.stripe_size,
        )
    ]
    await self._gather_or_cancel(tasks)
```

Within `_download_group`, acquire a fresh lease for each attempt and call
`iter_range_on_session`. Call `mark_transport_failure()` for timeouts and
connection resets; call `mark_unhealthy()` for authorization loss or a session
protocol failure that cannot be retried safely on the same connection. Release
the lease in `finally`, and call `pool.record_committed(chunk.length)` after
each durable manifest commit. Preserve existing retry counters, FloodWait
sleeps, injected aborts, and final verification.

For FloodWait, call `pool.pause_dc(dc_id, flood_wait.value)` before sleeping.
For every retried stripe call `pool.record_retry()`. The production fallback
path in Task 6 calls `pool.record_fallback()` once per affected file.

- [ ] **Step 7: Run all parallel downloader tests**

```bash
python -m unittest tests.module.test_parallel_downloader
```

Expected: existing per-file tests and all new pooled-stripe tests pass.

- [ ] **Step 8: Commit pooled stripe integration**

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "feat: schedule parallel downloads through global pool"
```

---

### Task 6: Own The Pool In Application Runtime And Route Files At 5 MiB

**Files:**
- Modify: `media_downloader.py:236-305`
- Modify: `media_downloader.py:959-1025`
- Modify: `tests/test_media_downloader.py:360-505`

**Interfaces:**
- Consumes: `MediaSessionPoolConfig`, `KurigramMediaSessionFactory`, `GlobalMediaSessionPool`
- Produces: `_should_use_global_pool(media_size: int) -> bool`
- Produces: `_shutdown_runtime(client, tasks, pool) -> None`
- Assigns: `app.media_session_pool`

- [ ] **Step 1: Write failing routing tests**

At the end of the existing `DownloadRoutingTest.setUp`, save the new fields:

```python
self.original_pool_values = {
    "parallel_session_pool_enabled": app.parallel_session_pool_enabled,
    "parallel_pool_file_threshold": app.parallel_pool_file_threshold,
    "parallel_pool_stripe_size": app.parallel_pool_stripe_size,
    "media_session_pool": app.media_session_pool,
}
```

At the end of the existing `tearDown`, restore them:

```python
for name, value in self.original_pool_values.items():
    setattr(app, name, value)
```

Then add these tests:

```python
def test_global_pool_threshold_is_strictly_greater_than_five_mib(self):
    app.parallel_session_pool_enabled = True
    app.parallel_pool_file_threshold = 5 * 1024 * 1024
    app.media_session_pool = mock.Mock()

    self.assertFalse(_should_use_global_pool(5 * 1024 * 1024))
    self.assertTrue(_should_use_global_pool(5 * 1024 * 1024 + 1))

@mock.patch("media_downloader.ParallelDownloader")
@mock.patch("media_downloader.KurigramRangeSource")
async def test_large_file_uses_process_pool(self, source_factory, downloader_factory):
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

async def test_five_mib_file_stays_on_sequential_path(self):
    app.parallel_session_pool_enabled = True
    app.parallel_pool_file_threshold = 5 * 1024 * 1024
    app.media_session_pool = mock.Mock()
    self.media.file_size = 5 * 1024 * 1024

    result = await _download_to_temp(
        self.client, self.message, self.media, "/tmp/candidate", -100123, ()
    )

    self.assertEqual("/tmp/sequential", result)
    self.client.download_media.assert_awaited_once()
```

- [ ] **Step 2: Write a failing shutdown-order test**

```python
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
    pool = SimpleNamespace(close=mock.AsyncMock(side_effect=lambda: events.append("pool")))
    client = SimpleNamespace(stop=mock.AsyncMock(side_effect=lambda: events.append("client")))

    await _shutdown_runtime(client, [task], pool)

    self.assertEqual(["task", "pool", "client"], events)
```

- [ ] **Step 3: Run routing and shutdown tests and verify RED**

```bash
python -m unittest tests.test_media_downloader.DownloadRoutingTest
```

Expected: missing helper and pooled constructor assertion failures.

- [ ] **Step 4: Implement global routing with old canary fallback preserved**

```python
def _should_use_global_pool(media_size: int) -> bool:
    return bool(
        app.parallel_session_pool_enabled
        and app.media_session_pool is not None
        and media_size > app.parallel_pool_file_threshold
    )
```

In `_download_to_temp`, check the global route first. Construct
`ParallelDownloader` with `pool=app.media_session_pool`,
`stripe_size=app.parallel_pool_stripe_size`, and a deterministic transfer ID
containing chat ID, message ID, and file unique ID. If global mode is disabled,
retain the current `parallel_download_enabled` canary branch byte-for-byte.
Track which branch was selected so only a failed global-pool attempt calls
`app.media_session_pool.record_fallback()` before the per-file sequential
fallback.

- [ ] **Step 5: Initialize and close the process pool in main**

After `client.start()` and before consumer tasks:

```python
if app.parallel_session_pool_enabled:
    pool_config = MediaSessionPoolConfig(
        soft_sessions=app.parallel_pool_soft_sessions,
        max_sessions=app.parallel_pool_max_sessions,
        pipeline_depth=app.parallel_pool_pipeline_depth,
        idle_ttl=app.parallel_pool_idle_ttl,
        control_interval=app.parallel_pool_control_interval,
    )
    app.media_session_pool = GlobalMediaSessionPool(
        KurigramMediaSessionFactory(client),
        pool_config,
    )
    app.media_session_pool.start()
```

Extract `_shutdown_runtime` so all consumer tasks are cancelled and awaited,
then the pool closes, then `client.stop()` runs. Set
`app.media_session_pool = None` after close. This order prevents leased
sessions from outliving the client or being stopped under active stripe tasks.

- [ ] **Step 6: Run routing plus worker recovery tests**

```bash
python -m unittest \
  tests.test_media_downloader.DownloadRoutingTest \
  tests.test_media_downloader.DatabaseConsumerTest \
  tests.module.test_parallel_downloader.ParallelSourceLifecycleTest
```

Expected: pooled routing, old canary routing, cancellation, fallback, database
consumer behavior, and source cleanup all pass.

- [ ] **Step 7: Commit runtime ownership**

```bash
git add media_downloader.py tests/test_media_downloader.py
git commit -m "feat: own global media pool for downloader runtime"
```

---

### Task 7: Expose Compact Pool Health In The Mobile Web Status

**Files:**
- Modify: `module/app.py`
- Modify: `module/web.py:510-531`
- Modify: `module/templates/index.html:65-85`
- Modify: `module/templates/index.html:651-662`
- Modify: `module/static/css/index.css`
- Create: `tests/module/test_media_pool_web.py`

**Interfaces:**
- Consumes: `GlobalMediaSessionPool.snapshot()`
- Produces: `Application.get_media_pool_status() -> dict`
- Extends: `GET /get_download_status` JSON with `media_pool`

- [ ] **Step 1: Write failing status endpoint tests**

Create `tests/module/test_media_pool_web.py` with:

```python
import unittest
from types import SimpleNamespace

from module import web


class MediaPoolStatusTestCase(unittest.TestCase):
    def setUp(self):
        self.original_app = web._web_app
        web._flask_app.config["LOGIN_DISABLED"] = True

    def tearDown(self):
        web._web_app = self.original_app

    def test_status_returns_disabled_pool_shape(self):
        web._web_app = SimpleNamespace(get_media_pool_status=lambda: {
            "enabled": False,
            "desired": 0,
            "live": 0,
            "hard_limit": 48,
        })
        response = web.get_flask_app().test_client().get("/get_download_status")
        payload = response.get_json()
        self.assertEqual(200, response.status_code)
        self.assertFalse(payload["media_pool"]["enabled"])

    def test_status_exposes_compact_live_pool_counts(self):
        status = {
            "enabled": True,
            "desired": 24,
            "live": 22,
            "active_slots": 31,
            "hard_limit": 48,
            "pipeline_depth": 2,
            "last_scale_reason": "goodput_growth",
        }
        web._web_app = SimpleNamespace(get_media_pool_status=lambda: status)
        payload = web.get_flask_app().test_client().get(
            "/get_download_status"
        ).get_json()
        self.assertEqual(status, payload["media_pool"])
```

- [ ] **Step 2: Run web tests and verify RED**

```bash
python -m unittest tests.module.test_media_pool_web.MediaPoolStatusTestCase
```

Expected: `get_download_status` has no `media_pool` object.

- [ ] **Step 3: Add a thread-safe application status adapter**

```python
def get_media_pool_status(self) -> dict:
    pool = self.media_session_pool
    if pool is None:
        return {
            "enabled": False,
            "desired": 0,
            "live": 0,
            "active_slots": 0,
            "hard_limit": self.parallel_pool_max_sessions,
            "pipeline_depth": self.parallel_pool_pipeline_depth,
            "last_scale_reason": "disabled",
        }
    payload = asdict(pool.snapshot())
    payload["enabled"] = True
    return payload
```

Import `asdict` from `dataclasses`.

- [ ] **Step 4: Return structured JSON and render a compact status row**

Replace the hand-built status string with:

```python
return jsonify({
    "download_speed": format_byte(get_total_download_speed()) + "/s",
    "upload_speed": "0.00 B/s",
    "media_pool": (
        _web_app.get_media_pool_status()
        if _web_app is not None
        else {
            "enabled": False,
            "live": 0,
            "active_slots": 0,
            "desired": 0,
            "hard_limit": 48,
            "pipeline_depth": 2,
        }
    ),
})
```

Add one compact element beside the existing speed text:

```html
<i id="media_pool_status" class="media-pool-status">Pool off</i>
```

Update it in the existing one-second status callback:

```javascript
var pool = result.media_pool || {};
var poolText = pool.enabled
  ? 'Pool ' + (pool.live || 0) + '/' + (pool.hard_limit || 48) +
    ' · ' + (pool.active_slots || 0) + ' active'
  : 'Pool off';
$('#media_pool_status').text(poolText);
```

Use the existing neutral status colors, no card, no nested panel, and allow the
line to wrap on narrow portrait screens.

- [ ] **Step 5: Run web and configuration tests**

```bash
python -m unittest \
  tests.module.test_media_pool_web \
  tests.module.test_web \
  tests.module.test_app.AppTestCase.test_global_pool_defaults_are_disabled_and_bounded
```

Expected: all tests pass.

- [ ] **Step 6: Commit observability without staging unrelated web changes**

Inspect all pre-existing dirty hunks first. Stage only the pool status hunks:

```bash
git add -p module/app.py module/web.py module/templates/index.html module/static/css/index.css
git add tests/module/test_media_pool_web.py
git commit -m "feat: expose adaptive media pool status"
```

---

### Task 8: Add A Reproducible Pool Benchmark And Integrity Sweep

**Files:**
- Create: `tools/benchmark_global_media_pool.py`
- Create: `tests/tools/test_benchmark_global_media_pool.py`

**Interfaces:**
- Consumes: production config, read-only download records, HDD baseline, and global pool classes
- Produces: one JSON report per target and pipeline depth
- Does not mutate: producer cursors, download records, retry state, or HDD baseline

- [ ] **Step 1: Write failing CLI and report tests**

Tests patch the client and pool factories and assert:

```python
class BenchmarkGlobalMediaPoolTest(unittest.TestCase):
    def test_parser_requires_exact_sample_identity_and_output(self):
        args = build_parser().parse_args([
            "--chat-id", "-1002313319912",
            "--message-id", "10341",
            "--output", "/app/temp/pool-gate",
            "--session-target", "24",
            "--pipeline-depth", "2",
        ])
        self.assertEqual(24, args.session_target)
        self.assertEqual(2, args.pipeline_depth)

    def test_report_blocks_when_sha_differs(self):
        report = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="b" * 64,
            file_size=100,
            elapsed_seconds=1.0,
            snapshot={"live": 8},
            retries=0,
        )
        self.assertFalse(report["eligible"])
        self.assertFalse(report["same_sha256"])

    def test_report_uses_committed_bytes_for_goodput(self):
        report = build_report(
            baseline_sha256="a" * 64,
            candidate_sha256="a" * 64,
            file_size=20 * 1024 * 1024,
            elapsed_seconds=2.0,
            snapshot={"live": 16},
            retries=0,
        )
        self.assertEqual(10 * 1024 * 1024, report["goodput_bytes_per_second"])
        self.assertTrue(report["eligible"])
```

- [ ] **Step 2: Run benchmark tests and verify RED**

```bash
python -m unittest tests.tools.test_benchmark_global_media_pool
```

Expected: import failure for the new tool.

- [ ] **Step 3: Implement the read-only benchmark**

The CLI must:

1. Open `download_records.sqlite3` with `mode=ro&immutable=1`.
2. Resolve the exact successful row by chat and message ID.
3. Verify the HDD baseline exists and has the recorded size.
4. Start one `HookClient` with production config and sessions.
5. Construct a pool with soft and max sessions both equal to
   `--session-target`, the requested pipeline depth, and `adaptive=False`.
6. Download through `ParallelDownloader` into a unique `/app/temp` path.
7. Close pool before client stop.
8. Hash baseline and candidate independently.
9. Atomically write JSON containing size, both hashes, eligibility, elapsed
   time, goodput, retries, pool peak, DC counts, and errors.

Return exit code 0 only when size and SHA match and integrity is verified.

- [ ] **Step 4: Run tool tests and the complete local-style suite in Docker**

```bash
python -m unittest \
  tests.tools.test_benchmark_global_media_pool \
  tests.module.test_download_stripe_scheduler \
  tests.module.test_media_session_pool \
  tests.module.test_parallel_downloader \
  tests.module.test_app \
  tests.module.test_media_pool_web \
  tests.module.test_web \
  tests.test_media_downloader.DownloadRoutingTest
```

Expected: all tests pass.

- [ ] **Step 5: Commit the benchmark**

```bash
git add tools/benchmark_global_media_pool.py tests/tools/test_benchmark_global_media_pool.py
git commit -m "test: benchmark adaptive global media pool"
```

---

### Task 9: Build, Gate, Deploy, And Observe On The NAS

**Files:**
- Verify: all files changed in Tasks 1-8
- Modify on NAS after gate: `/volume2/docker/telegram_media_downloader_us/config.yaml`
- Modify on NAS after gate: `/volume2/docker/telegram_media_downloader_us/docker-compose.yaml`
- Back up under: `/volume2/docker/telegram_media_downloader_us/backups/`

**Interfaces:**
- Candidate image: `telegram_media_downloader_us:pool-<short-commit>`
- Rollback image: `telegram_media_downloader_us:parallel-a9452ad`

- [ ] **Step 1: Run final static and regression checks**

```bash
git diff --check
git status --short
```

Run the full relevant Docker suite:

```bash
python -m unittest \
  tests.module.test_download_stripe_scheduler \
  tests.module.test_media_session_pool \
  tests.module.test_parallel_downloader \
  tests.tools.test_benchmark_global_media_pool \
  tests.tools.test_validate_parallel_downloads \
  tests.module.test_parallel_validation \
  tests.module.test_app \
  tests.module.test_media_pool_web \
  tests.module.test_web \
  tests.test_media_downloader.DownloadRoutingTest
```

Expected: zero failures and zero errors.

- [ ] **Step 2: Back up and sync only project files**

Create a timestamped backup containing the current Compose file, config, and
every application file that will be replaced. Sync only into
`/volume2/docker/telegram_media_downloader_us/app-src` and `dev-test`. Verify
local and NAS SHA-256 values for every synced source file.

- [ ] **Step 3: Build the candidate without touching production Compose**

```bash
source ~/.zshrc
printf '%s\n' "$tmp_pwd2" | ssh de1ta@192.168.79.37 \
  "sudo -S docker build \
   -f /volume2/docker/telegram_media_downloader_us/app-src/Dockerfile.local \
   -t telegram_media_downloader_us:pool-$(git rev-parse --short HEAD) \
   /volume2/docker/telegram_media_downloader_us/app-src"
```

Run the full suite from Step 1 against the image-internal source.

- [ ] **Step 4: Run the SHA gate across file-size buckets**

Stop production with a remote shell trap that always restarts the current
Compose service. Run immutable samples below 5 MiB, 5-20 MiB, 50-200 MiB,
200 MiB-1 GiB, and above 1 GiB. Require exact size, candidate SHA equal to HDD
SHA, zero unexplained integrity errors, and no destination replacement.

- [ ] **Step 5: Run the session and pipeline sweep**

For the same 286,168,400-byte message 10341 and one sample above 1 GiB, run:

```text
targets:        8, 16, 24, 32, 40, 48
pipeline depth: 1, 2
```

Record committed-byte goodput, total elapsed time, SHA, retries,
`AUTH_BYTES_INVALID`, connection resets, FloodWait, CPU, and memory. Abort the
sweep on any SHA mismatch, CDN hash error, or repeated FloodWait. Preserve JSON
reports under `/app/temp/parallel_validation/global-pool-<commit>/` and remove
candidate media files after reports are verified.

Select the highest stable target that still improves committed-byte goodput by
at least 5 percent over the preceding stable target without materially raising
retry, FloodWait, CPU, or memory pressure. Seed production with that measured
target, never lower than 16 and never higher than 48; the adaptive controller
may still move below or above it at runtime within its safety rules.

- [ ] **Step 6: Inject interruption and verify resume**

Abort a candidate after durable manifest commits, restart it with the same
identity and partial path, and require nonzero recovered chunks plus a final
SHA equal to the HDD baseline. Terminate one leased connection during a stripe
and require the stripe to complete through another session without container
restart or whole-file fallback.

- [ ] **Step 7: Enable the pool only after all gates pass**

Add these NAS config keys while leaving old canary keys unchanged:

```yaml
parallel_session_pool_enabled: true
parallel_pool_file_threshold: 5242880
parallel_pool_stripe_size: 5242880
parallel_pool_soft_sessions: 16
parallel_pool_max_sessions: 48
parallel_pool_pipeline_depth: 2
parallel_pool_idle_ttl: 600
parallel_pool_control_interval: 60
```

Before writing the production config, replace the shown soft-session value
with the numeric target selected in Step 5. Keep `16` only when the sweep does
not justify a higher stable starting point.

Switch only the Compose image tag to the candidate and recreate only
`telegram_media_downloader_us`.

- [ ] **Step 8: Observe real production behavior**

Require all of the following before declaring completion:

- Web returns HTTP 200 and the mobile status shows pool live/limit counts.
- Container image is the candidate, state is running, and restart count is 0.
- At least two real files above 1 GiB reach SQLite `success`.
- Both final HDD SHA values equal the downloader's verified candidate hashes.
- At least one proxy reset or injected connection reset recovers without a
  container restart.
- The live pool does not exceed 48 sessions or pipeline depth 2.
- Multiple active files receive stripe turns; no active file remains unchanged
  while compatible sessions repeatedly finish other stripes.
- The controller records a reason for every expansion, plateau, or contraction.

- [ ] **Step 9: Roll back immediately on a failed production gate**

Restore `telegram_media_downloader_us:parallel-a9452ad`, restore the backed-up
config and Compose files, and recreate only this service. Do not delete SQLite
records, manifests, partial files, cursors, or Telegram account sessions.

- [ ] **Step 10: Record final evidence**

Commit any source-only corrections discovered during the gate, rerun the full
suite, and report the deployed image tag, test count, selected stable target,
pipeline depth, benchmark goodput, SHA evidence, pool snapshot, Web health,
restart count, and rollback backup path.
