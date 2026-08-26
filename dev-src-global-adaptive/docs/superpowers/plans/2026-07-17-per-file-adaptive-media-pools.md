# Per-File Adaptive Media Pools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the shared Telegram media-session data plane with up to five isolated per-file pools that keep one session bound to one worker, adapt through 4/8/12 sessions, never exceed 60 feature-owned sessions, and pass sustained verified-goodput and integrity gates.

**Architecture:** A process-wide `MediaTransferCoordinator` owns only permits, DC cooldowns, expansion arbitration, aggregate metrics, and the serialized Kurigram session factory. Each eligible `ParallelDownloader` creates a short-lived `FileMediaSessionPool`; its workers keep one physical session for the file lifetime and immediately claim the next stripe from that same file. Existing one-MiB manifest commits, positional writes, restart recovery, final readback, whole-file SHA-256, and sequential fallback remain authoritative.

**Tech Stack:** Python 3.11, asyncio, Kurigram 2.2.24 (`pyrogram` namespace), SQLite, unittest, Flask, Docker Compose, Synology-style NAS Linux.

## Global Constraints

- Modify NAS files only below `/volume2/docker/telegram_media_downloader_us` and touch only this project's Docker resources.
- Keep `max_download_task: 5`; do not increase file-consumer concurrency.
- Keep Telegram protocol chunks at 1 MiB and use 5 MiB logical stripes for the first canary.
- Bind one physical media session to one file worker; never lease or move it to another file or DC.
- Use pipeline depth one, at most 12 sessions per file, and at most 60 creating/live/draining feature-owned sessions process-wide.
- Preserve Kurigram handling for authorization, file references, DC migration, CDN redirects, CDN decryption, Telegram RPC errors, and reconnects; retain each worker's lazily created CDN transport across logical stripes and close it with the worker media session.
- Preserve SQLite task records, per-file manifests, SSD positional writes, final SSD readback, whole-file SHA-256, and atomic HDD publication.
- Keep the existing `global` pool implementation compiled as an immediate rollback mode.
- Keep the active proxy at `http://192.168.79.22:6152` unless production evidence proves that endpoint unavailable.
- Do not alter production Compose until the exact candidate image passes automated tests and controlled hash comparisons.
- Production acceptance is ten post-warmup minutes with five-second committed-byte P10 at least 8 MiB/s, CV at most 25%, stripe retries below 2%, no more than ten reset logs, 100% hash agreement, restart count zero, and OOM false.
- Run dependency-bearing tests in Docker because the local macOS Python environment is not the production runtime.

---

### Task 1: Add Pure Per-File Tier Control

**Files:**
- Create: `module/file_media_session_pool.py`
- Create: `tests/module/test_file_media_session_pool.py`

**Interfaces:**
- Produces: `FilePoolConfig(initial_sessions=4, max_sessions=12, control_interval=10.0, growth_hold=120.0)`
- Produces: `FilePoolWindow(pending, utilization, retry_rate, unhealthy_fraction, flood_wait, committed_bytes_per_second, stable_windows)`
- Produces: `FilePoolController.observe(window: FilePoolWindow, now: float) -> FileScaleDecision`
- Produces: tiers restricted to `(4, 8, 12)` and bounded by remaining stripes.

- [ ] **Step 1: Write failing controller tests**

Create deterministic tests covering initial target, two stable windows before growth, 5% post-growth retention, plateau rollback, two-minute hold, retry/unhealthy contraction, FloodWait freeze, and file-tail contraction. Use this test shape:

```python
def window(**overrides):
    values = {
        "pending": 100,
        "utilization": 1.0,
        "retry_rate": 0.0,
        "unhealthy_fraction": 0.0,
        "flood_wait": False,
        "committed_bytes_per_second": 8 * 1024 * 1024,
        "stable_windows": 2,
    }
    values.update(overrides)
    return FilePoolWindow(**values)

def test_grows_four_eight_twelve_only_after_stable_windows(self):
    controller = FilePoolController(FilePoolConfig())
    self.assertEqual(4, controller.target)
    self.assertEqual(8, controller.observe(window(), 10).target)
    self.assertEqual("evaluating", controller.observe(window(), 20).reason)
    self.assertEqual(8, controller.observe(
        window(committed_bytes_per_second=9 * 1024 * 1024), 30
    ).target)
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
python -m unittest tests.module.test_file_media_session_pool -v
```

Expected: import failure because `module.file_media_session_pool` does not exist.

- [ ] **Step 3: Implement immutable config/window/decision types and pure controller**

Implement the tier state machine with these public shapes:

```python
@dataclass(frozen=True)
class FilePoolConfig:
    initial_sessions: int = 4
    max_sessions: int = 12
    control_interval: float = 10.0
    growth_hold: float = 120.0
    max_attempts: int = 3

@dataclass(frozen=True)
class FileScaleDecision:
    target: int
    reason: str
    hold_until: float = 0.0

class FilePoolController:
    _TIERS = (4, 8, 12)

    def __init__(self, config):
        self._config = config
        self._target = config.initial_sessions
        self._pre_growth_target = None
        self._pre_growth_goodput = 0.0
        self._evaluation = []
        self._hold_until = 0.0

    @property
    def target(self) -> int:
        return self._target

    def observe(self, window: FilePoolWindow, now: float) -> FileScaleDecision:
        if window.pending <= 0:
            return FileScaleDecision(self._target, "tail", self._hold_until)
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
            if average < self._pre_growth_goodput * 1.05:
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
        self._pre_growth_target = self._target
        self._pre_growth_goodput = window.committed_bytes_per_second
        self._target = min(self._TIERS[tier + 1], self._config.max_sessions)
        return FileScaleDecision(self._target, "expand", self._hold_until)
```

Reject `initial_sessions != 4`, `max_sessions` outside `(4, 8, 12)`, non-positive timing, and `max_attempts < 1` in `FilePoolConfig.__post_init__`.

- [ ] **Step 4: Run controller tests and verify GREEN**

Run the command from Step 2. Expected: all controller tests pass without network access.

- [ ] **Step 5: Commit the pure controller**

```bash
git add module/file_media_session_pool.py tests/module/test_file_media_session_pool.py
git commit -m "feat: add per-file media pool controller"
```

---

### Task 2: Build The Process-Wide Session Budget Coordinator

**Files:**
- Modify: `module/file_media_session_pool.py`
- Modify: `tests/module/test_file_media_session_pool.py`
- Reuse: `module/media_session_pool.py:KurigramMediaSessionFactory`

**Interfaces:**
- Produces: `CoordinatorConfig(max_sessions=60, expansion_interval=10.0)`
- Produces: `MediaTransferCoordinator.create_sessions(pool_id, dc_id, count, expansion=False) -> list[OwnedMediaSession]`
- Produces: `OwnedMediaSession.session`, `OwnedMediaSession.close()`
- Produces: `pause_dc(dc_id, seconds)`, `wait_for_dc(dc_id)`, `dc_is_paused(dc_id)`
- Produces: `record_committed(pool_id, bytes)`, `update_pool(snapshot)`, `snapshot()` and `close()`.

- [ ] **Step 1: Write failing budget, arbitration, cooldown, and cancellation tests**

Add fakes whose `stop()` call can block and prove all of these invariants:

```python
async def test_creating_live_and_draining_share_one_sixty_slot_budget(self):
    coordinator = MediaTransferCoordinator(factory, CoordinatorConfig(max_sessions=60))
    first = await coordinator.create_sessions("a", 4, 60)
    self.assertEqual(60, coordinator.snapshot().used)
    self.assertEqual([], await coordinator.create_sessions("b", 4, 4))
    close_task = asyncio.create_task(first[0].close())
    await stop_started.wait()
    self.assertEqual(60, coordinator.snapshot().used)
    allow_stop.set()
    await close_task
    self.assertEqual(59, coordinator.snapshot().used)

async def test_only_one_pool_receives_expansion_grant_per_interval(self):
    a = await coordinator.create_sessions("a", 2, 4, expansion=True)
    b = await coordinator.create_sessions("b", 2, 4, expansion=True)
    self.assertEqual(4, len(a))
    self.assertEqual([], b)
```

Also test factory failure releases every reservation, repeated close stops once, cancellation completes stop before releasing the permit, FloodWait affects only the same DC, shorter repeated waits never shorten a cooldown, and `close()` rejects new sessions while waiting for all owned sessions to close.

- [ ] **Step 2: Run coordinator tests and verify RED**

Run:

```bash
python -m unittest \
  tests.module.test_file_media_session_pool.MediaTransferCoordinatorTest -v
```

Expected: missing coordinator symbols.

- [ ] **Step 3: Implement coordinator ownership and snapshots**

Use one `asyncio.Lock`, a condition/event per DC wakeup, and a thread-safe immutable snapshot. Reserve slots before invoking the factory; count a reserved slot as `creating`; transition it to `live` only after factory success; transition `live -> draining -> released` around a cancellation-shielded `session.stop()`.

```python
@dataclass(frozen=True)
class CoordinatorSnapshot:
    used: int
    hard_limit: int
    creating: int
    live: int
    draining: int
    active_files: int
    committed_bytes_per_second: float
    expansion_queue: int
    dc_cooldowns: Dict[int, float]
    pools: Dict[str, Dict[str, object]]

class OwnedMediaSession:
    @property
    def session(self):
        return self._session

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._coordinator._stop_owned(self)
            )
        cancellation = None
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
        self._close_task.result()
        if cancellation is not None:
            raise cancellation
```

`create_sessions(pool_id, dc_id, count, expansion=True)` grants either the full requested tier step or none. Initial creation may return fewer only when fewer slots were explicitly requested by the file tail. Keep the existing `KurigramMediaSessionFactory` as the only construction path.

- [ ] **Step 4: Run focused and legacy factory tests**

```bash
python -m unittest \
  tests.module.test_file_media_session_pool.MediaTransferCoordinatorTest \
  tests.module.test_media_session_pool.KurigramMediaSessionFactoryTest -v
```

Expected: all tests pass; no session permit leaks under cancellation.

- [ ] **Step 5: Commit the coordinator**

```bash
git add module/file_media_session_pool.py tests/module/test_file_media_session_pool.py
git commit -m "feat: coordinate per-file media session budgets"
```

---

### Task 3: Run Dedicated Session Workers Inside Each File Pool

**Files:**
- Modify: `module/file_media_session_pool.py`
- Modify: `tests/module/test_file_media_session_pool.py`

**Interfaces:**
- Produces: `StripeAttemptError(error, kind, wait_seconds=0.0, completed=False)` where kind is `transport`, `flood_wait`, or `fatal`.
- Produces: `FileMediaSessionPool.run(stripes, download_stripe) -> None`.
- Consumes callback: `download_stripe(session, stripe) -> Awaitable[None]`.
- Produces per-file `FilePoolSnapshot` fields: target/live/active/draining/pending/committed Bps/retries/resets/unhealthy/tier/reason.

- [ ] **Step 1: Write failing affinity and lifecycle tests**

Use hashable fake stripe objects and a callback that records `(session_id, stripe_id)`. Prove:

```python
async def test_worker_keeps_same_session_for_consecutive_file_stripes(self):
    await pool.run(list(range(12)), download)
    by_worker = group_calls_by_session(calls)
    self.assertTrue(any(len(stripes) > 1 for stripes in by_worker.values()))
    self.assertEqual({"file-a"}, {owner for owner, _ in factory.created})

async def test_one_session_never_executes_two_stripes_concurrently(self):
    await pool.run(list(range(40)), overlap_detecting_download)
    self.assertEqual(1, max_active_by_session)
```

Also prove: initial 4 sessions, expansion to 8 then 12 only through controller grants, a worker claims only its file queue, draining workers finish their active stripe but claim no next stripe, file tail closes surplus sessions, first transport error retries unfinished work on the same session, second transport error replaces only that session and contracts one tier, FloodWait requeues unfinished work and pauses the DC, fatal error cancels siblings and closes every session, and cancellation releases all 60-budget permits exactly once.

- [ ] **Step 2: Run file-pool tests and verify RED**

```bash
python -m unittest \
  tests.module.test_file_media_session_pool.FileMediaSessionPoolTest -v
```

Expected: missing `FileMediaSessionPool` and `StripeAttemptError`.

- [ ] **Step 3: Implement the file-owned work queue and worker manager**

Implement a queue local to one pool. Pair each `OwnedMediaSession` with exactly one `_FileWorker`; the worker repeatedly takes another stripe only from that queue. The control loop samples complete ten-second windows, asks the pure controller for a tier, obtains an atomic four-session expansion grant, and marks excess workers draining.

```python
class FileMediaSessionPool:
    async def run(self, stripes, download_stripe):
        self._enqueue(stripes)
        await self._start_workers(min(4, len(stripes)), expansion=False)
        control = asyncio.create_task(self._control_loop())
        try:
            await self._completion
            if self._fatal_error is not None:
                raise self._fatal_error
        finally:
            control.cancel()
            await asyncio.gather(control, return_exceptions=True)
            await self.close()

    async def _worker(self, owned_session):
        while not self._closing and not worker.draining:
            stripe = self._claim_nowait()
            if stripe is None:
                return
            await self._coordinator.wait_for_dc(self.dc_id)
            try:
                await self._download_stripe(owned_session.session, stripe)
            except StripeAttemptError as failure:
                if failure.completed:
                    self._record_success(worker)
                    continue
                stripe.attempts += 1
                if stripe.attempts >= self._config.max_attempts:
                    raise failure.error
                self._requeue(stripe)
                if failure.kind == "flood_wait":
                    self._coordinator.pause_dc(self.dc_id, failure.wait_seconds)
                elif failure.kind == "transport":
                    worker.consecutive_transport_failures += 1
                    self._record_retry(worker)
                    if worker.consecutive_transport_failures >= 2:
                        worker.unhealthy = True
                        worker.draining = True
                        return
                else:
                    raise failure.error
```

Never cancel an in-flight stripe to contract. On fatal/caller cancellation, cancel work but shield session and iterator cleanup to completion. Publish a fresh immutable pool snapshot after every state transition.

- [ ] **Step 4: Run file-pool and coordinator suites**

```bash
python -m unittest tests.module.test_file_media_session_pool -v
```

Expected: every pure, coordinator, and lifecycle test passes.

- [ ] **Step 5: Commit file pool lifecycle**

```bash
git add module/file_media_session_pool.py tests/module/test_file_media_session_pool.py
git commit -m "feat: bind media sessions to file workers"
```

---

### Task 4: Connect File Pools To The Integrity-Checked Downloader

**Files:**
- Modify: `module/parallel_downloader.py`
- Modify: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Adds constructor argument: `coordinator: Optional[MediaTransferCoordinator] = None`.
- Keeps existing `pool=` argument exclusively for rollback `global` mode.
- Adds mutable internal `_FileStripeWork(chunks, next_index=0, attempts=0)`.
- Uses existing `KurigramRangeSource.iter_range_on_session()` and all existing manifest/final verification code.

- [ ] **Step 1: Write failing session-affinity, retry, resume, and hash tests**

Add tests proving the real downloader callback advances durable one-MiB chunks inside a five-MiB work item, then returns the same worker/session to another stripe:

```python
async def test_per_file_mode_keeps_session_across_stripes_and_hashes_exactly(self):
    downloader = ParallelDownloader(
        source,
        coordinator=coordinator,
        stripe_size=5 * CHUNK_SIZE,
        transfer_id="file-a",
    )
    result = await downloader.download(identity, part_path)
    self.assertEqual(hashlib.sha256(payload).hexdigest(), result.sha256)
    self.assertTrue(source.one_session_read_multiple_stripes)
```

Also cover a timeout after two committed chunks, timeout after the final chunk, FloodWait, fatal session error, injected cancellation/resume, source lifecycle ownership, out-of-order positional writes, and rejection of simultaneous `pool` plus `coordinator` arguments.

- [ ] **Step 2: Run focused downloader tests and verify RED**

```bash
python -m unittest \
  tests.module.test_parallel_downloader.PerFileParallelDownloaderTest -v
```

Expected: constructor rejects `coordinator` and the new test class fails.

- [ ] **Step 3: Extract one-attempt stripe download without changing integrity gates**

Create `_download_file_stripe()` by adapting `_download_pooled_group()` so it performs exactly one attempt on the supplied bound session. Advance `_FileStripeWork.next_index` only after positional write plus manifest SHA-256 commit. Translate only recognized failures:

```python
except Exception as error:
    if work.done and (_find_flood_wait(error) or is_retryable_timeout(error)):
        return
    if flood_wait := _find_flood_wait(error):
        raise StripeAttemptError(
            error, "flood_wait", wait_seconds=max(float(flood_wait.value), 0)
        ) from error
    if _is_fatal_session_error(error):
        raise StripeAttemptError(error, "fatal") from error
    if is_retryable_timeout(error):
        raise StripeAttemptError(error, "transport") from error
    raise
```

In `_download_prepared()`, retain the old fixed and global branches unchanged and add a coordinator branch that builds `_FileStripeWork` objects and calls `FileMediaSessionPool.run()`. Leave manifest coverage comparison, `os.fsync`, final readback, whole-file SHA-256, optional Telegram hashes, and result creation byte-for-byte behaviorally unchanged.

- [ ] **Step 4: Run focused plus all existing downloader tests**

```bash
python -m unittest \
  tests.module.test_parallel_downloader \
  tests.module.test_file_media_session_pool -v
```

Expected: new per-file tests and all legacy fixed/global/integrity/recovery tests pass.

- [ ] **Step 5: Commit downloader integration**

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "feat: download stripes through file-owned sessions"
```

---

### Task 5: Add Mode Configuration, Routing, And Runtime Lifecycle

**Files:**
- Modify: `module/app.py`
- Modify: `media_downloader.py`
- Modify: `tests/module/test_app.py`
- Modify: `tests/test_media_downloader.py`

**Interfaces:**
- Produces config key `parallel_pool_mode: off|global|per_file`.
- Produces `Application.media_transfer_coordinator` alongside legacy `media_session_pool`.
- Routes files `<= 5 MiB` to sequential Kurigram and larger files to the configured mode.
- Starts coordinator after the Telegram client and closes it after download consumers stop but before the client stops.

- [ ] **Step 1: Write failing config, routing, and shutdown tests**

Test repository defaults as `off`; validate exactly these per-file defaults and bounds:

```python
self.assertEqual("off", app.parallel_pool_mode)
self.assertEqual(5 * 1024 * 1024, app.parallel_file_pool_threshold)
self.assertEqual(5 * 1024 * 1024, app.parallel_file_pool_stripe_size)
self.assertEqual(4, app.parallel_file_pool_initial_sessions)
self.assertEqual(12, app.parallel_file_pool_max_sessions)
self.assertEqual(10, app.parallel_file_pool_control_interval)
self.assertEqual(120, app.parallel_file_pool_growth_hold)
self.assertEqual(60, app.parallel_media_session_budget)
self.assertEqual(1, app.parallel_file_pool_pipeline_depth)
```

Routing tests must prove strict `> 5 MiB`, correct `coordinator=` injection in `per_file`, unchanged `pool=` injection in `global`, sequential fallback on candidate failure, no fallback on cancellation, and coordinator cleanup on startup failure/repeated shutdown cancellation.

- [ ] **Step 2: Run app/runtime tests and verify RED**

```bash
python -m unittest \
  tests.module.test_app \
  tests.test_media_downloader.DownloadRoutingTest \
  tests.test_media_downloader.RuntimeShutdownTest \
  tests.test_media_downloader.RuntimeMainTest -v
```

Expected: missing mode/config/coordinator fields.

- [ ] **Step 3: Implement validated mode and lifecycle**

Keep `parallel_session_pool_enabled` as a legacy input: when `parallel_pool_mode` is absent, map true to `global` and false to `off`. Explicit `parallel_pool_mode` wins. Reject unknown modes to `off`; force threshold and first-canary stripe to exactly 5 MiB; force initial 4, max in `(4, 8, 12)`, budget at most 60, and pipeline depth one.

```python
if app.parallel_pool_mode == "per_file":
    coordinator = MediaTransferCoordinator(
        KurigramMediaSessionFactory(client),
        CoordinatorConfig(
            max_sessions=app.parallel_media_session_budget,
            expansion_interval=app.parallel_file_pool_control_interval,
        ),
    )
    coordinator.start()
    app.media_transfer_coordinator = coordinator
elif app.parallel_pool_mode == "global":
    pool = GlobalMediaSessionPool(
        KurigramMediaSessionFactory(client),
        MediaSessionPoolConfig(
            soft_sessions=app.parallel_pool_soft_sessions,
            max_sessions=app.parallel_pool_max_sessions,
            pipeline_depth=app.parallel_pool_pipeline_depth,
            idle_ttl=app.parallel_pool_idle_ttl,
            control_interval=app.parallel_pool_control_interval,
        ),
    )
    pool.start()
    app.media_session_pool = pool
```

Build `FilePoolConfig` in `_download_to_temp()` and pass it with the coordinator. On per-file failure call `coordinator.record_fallback()` before the existing sequential fallback. Shutdown order is consumers/bot, legacy pool or coordinator, then Telegram client.

- [ ] **Step 4: Run app/runtime and downloader suites**

```bash
python -m unittest \
  tests.module.test_app \
  tests.test_media_downloader \
  tests.module.test_parallel_downloader \
  tests.module.test_file_media_session_pool -v
```

Expected: all tests pass with legacy global behavior still covered.

- [ ] **Step 5: Commit runtime mode support**

```bash
git add module/app.py media_downloader.py tests/module/test_app.py tests/test_media_downloader.py
git commit -m "feat: run configurable per-file media pools"
```

---

### Task 6: Expose Raw And Smoothed Pool Metrics In The Mobile Web UI

**Files:**
- Modify: `module/app.py`
- Modify: `module/web.py`
- Modify: `module/templates/index.html`
- Modify: `module/static/css/index.css`
- Modify: `tests/module/test_media_pool_web.py`
- Modify: `tests/module/test_web.py`

**Interfaces:**
- `/get_download_status.media_pool` remains backward compatible and gains `mode`, `used`, `hard_limit`, `creating`, `live`, `draining`, `active_files`, `raw_bps`, `rolling_5s_bps`, `p10_5s_bps`, `cv`, and `files`.
- Footer renders `Pools N/5 - Sessions X/60` and five-second rolling committed speed.
- Per-file download rows render the coordinator's matching file-pool rolling speed, not the legacy mixed clock.

- [ ] **Step 1: Write failing status serialization and rendering tests**

Build an immutable coordinator snapshot with two file pools and assert exact JSON-safe dictionaries. Add HTML assertions that mobile portrait layout keeps the aggregate status on one wrapping line and does not hide the file name, progress, or speed.

```python
self.assertEqual("per_file", payload["media_pool"]["mode"])
self.assertEqual(8, payload["media_pool"]["used"])
self.assertEqual(60, payload["media_pool"]["hard_limit"])
self.assertEqual(2, payload["media_pool"]["active_files"])
self.assertEqual(8 * 1024 * 1024, payload["media_pool"]["rolling_5s_bps"])
```

- [ ] **Step 2: Run Web tests and verify RED**

```bash
python -m unittest \
  tests.module.test_media_pool_web \
  tests.module.test_web -v
```

Expected: status lacks per-file mode fields and template rendering assertions fail.

- [ ] **Step 3: Add thread-safe adapter and compact mobile rendering**

Make `Application.get_media_pool_status()` select coordinator snapshots for `per_file`, legacy pool snapshots for `global`, and a stable disabled shape otherwise. Do not calculate validation metrics from UI polling. Coordinator records one-second committed-byte buckets independently and derives raw, rolling five-second, P10, mean, standard deviation, and CV from those buckets.

Update the existing footer only; do not add nested cards or a desktop-only dashboard. Use the existing mobile-first typography and allow status text to wrap without overlap.

- [ ] **Step 4: Run Web and broad app tests**

```bash
python -m unittest \
  tests.module.test_media_pool_web \
  tests.module.test_web \
  tests.module.test_app -v
```

Expected: all tests pass and old global disabled/status shapes remain accepted.

- [ ] **Step 5: Commit observability**

```bash
git add module/app.py module/web.py module/templates/index.html \
  module/static/css/index.css tests/module/test_media_pool_web.py \
  tests/module/test_web.py
git commit -m "feat: report per-file media pool performance"
```

---

### Task 7: Add Deterministic Integrity And Performance Validation

**Files:**
- Create: `tools/benchmark_file_media_pools.py`
- Create: `tests/tools/test_benchmark_file_media_pools.py`
- Modify: `tools/validate_parallel_downloads.py`
- Modify: `tests/tools/test_validate_parallel_downloads.py`

**Interfaces:**
- Produces JSON/JSONL samples containing timestamp, committed bytes, one-second Bps, five-second Bps, P10, CV, retries, resets, files, sessions, DCs, restart count, and OOM state.
- Supports fixed per-file tiers 4/8/12 and stripe sizes 5/10/20 MiB without changing production records.
- Reuses immutable source files and compares size plus whole-file SHA-256 on every success/resume/failure run.

- [ ] **Step 1: Write failing metric and validation tests**

Test five-second rolling calculations, percentile interpolation, CV for zero/constant/mixed windows, exclusion annotation, hard failure on any hash mismatch, and non-zero exit when the acceptance gate fails.

```python
result = summarize([8, 8, 8, 8, 8], mib=True)
self.assertEqual(8.0, result.p10_mib_s)
self.assertEqual(0.0, result.cv)
self.assertTrue(evaluate(result, min_p10=8.0, max_cv=0.25).passed)
```

- [ ] **Step 2: Run tool tests and verify RED**

```bash
python -m unittest \
  tests.tools.test_benchmark_file_media_pools \
  tests.tools.test_validate_parallel_downloads -v
```

Expected: benchmark module is absent and per-file validation modes are unsupported.

- [ ] **Step 3: Implement offline metric math and controlled runner**

Keep sample math pure and unit-testable. The live runner must write only beneath a caller-supplied project validation directory, never modify task records, stop the production Compose service only after candidate image tests pass, and restore it through a shell trap. Every report lists warmup/tail/outage exclusions explicitly.

- [ ] **Step 4: Run tool tests and dry-run CLI help**

```bash
python -m unittest \
  tests.tools.test_benchmark_file_media_pools \
  tests.tools.test_validate_parallel_downloads -v
python tools/benchmark_file_media_pools.py --help
```

Expected: tests pass and help exits zero without network access.

- [ ] **Step 5: Commit validation tooling**

```bash
git add tools/benchmark_file_media_pools.py \
  tests/tools/test_benchmark_file_media_pools.py \
  tools/validate_parallel_downloads.py \
  tests/tools/test_validate_parallel_downloads.py
git commit -m "test: validate per-file pool integrity and goodput"
```

---

### Task 8: Review, Test, And Build The Exact Candidate Image

**Files:**
- Modify only files found by review failures.
- Record evidence in: `.superpowers/sdd/task-per-file-pools-report.md`

**Interfaces:**
- Produces one immutable Docker image tag `telegram_media_downloader_us:per-file-<commit>`.
- Produces image source SHA-256 and complete test output tied to that image ID.

- [ ] **Step 1: Run formatting/static checks and the broad suite**

```bash
python -m compileall -q module media_downloader.py tools
python -m unittest discover -s tests -v
git diff --check
```

Expected: compile and diff checks exit zero; all tests pass. Dependency failures on macOS are rerun inside the candidate Docker image and documented rather than ignored.

- [ ] **Step 2: Request independent code review**

Review specifically for session ownership crossing files/DCs, budget leaks, repeated cancellation, session stop ordering, FloodWait scope, file-tail deadlock, retry amplification, manifest corruption, fallback double-download, status-thread races, and legacy global rollback regression. Fix every Critical/Important finding with a failing regression test and a focused commit.

- [ ] **Step 3: Build on the NAS without changing Compose**

Sync the committed tree only into a timestamped build directory below `/volume2/docker/telegram_media_downloader_us`, build:

```bash
sudo docker build \
  -t telegram_media_downloader_us:per-file-<commit> \
  /volume2/docker/telegram_media_downloader_us/build-per-file-<timestamp>
```

Do not alter another NAS directory, daemon setting, network, or container.

- [ ] **Step 4: Run the exact image's full tests without source mounts**

```bash
sudo docker run --rm --entrypoint python \
  telegram_media_downloader_us:per-file-<commit> \
  -m unittest discover -s tests -v
```

Expected: all tests pass from files baked into the image. Record image ID, source hash, test count, and elapsed time.

- [ ] **Step 5: Commit review evidence**

```bash
git add .superpowers/sdd/task-per-file-pools-report.md
git commit -m "docs: record per-file pool candidate verification"
```

---

### Task 9: Validate Hashes, Deploy Safely, And Tune For The Goal

**Files:**
- Modify NAS only under `/volume2/docker/telegram_media_downloader_us`.
- Update local evidence: `.superpowers/sdd/task-per-file-pools-report.md`

**Interfaces:**
- Produces a stopped-state backup of Compose/config/image metadata and a tested rollback command.
- Produces fixed-tier 4/8/12 and stripe 5/10/20 reports using the same files, proxy, and DC mix.
- Produces the ten-minute acceptance result from raw committed bytes.

- [ ] **Step 1: Perform isolated immutable-file hash validation**

With production stopped only for the validation window, download already-complete files from multiple size bands into a project validation directory using success, cancellation/resume, forced reset, proxy interruption, and container restart scenarios. Compare candidate size and SHA-256 with the existing HDD file every time. Expected: 100% agreement and no source modification.

- [ ] **Step 2: Create stopped-state backup and deploy behind explicit mode**

Back up Compose, config, image ID, container inspect, SQLite metadata, and manifests beneath a timestamped project backup. Set:

```yaml
parallel_pool_mode: per_file
parallel_file_pool_threshold: 5242880
parallel_file_pool_stripe_size: 5242880
parallel_file_pool_initial_sessions: 4
parallel_file_pool_max_sessions: 12
parallel_file_pool_control_interval: 10
parallel_file_pool_growth_hold: 120
parallel_media_session_budget: 60
parallel_file_pool_pipeline_depth: 1
max_download_task: 5
```

Install a shell rollback trap before `docker compose up -d`. Verify Web HTTP 200, restart count zero, OOM false, proxy connectivity, database consumers, and session counts.

- [ ] **Step 3: Run controlled tier and stripe sweeps**

Use identical immutable files and compare fixed 4, 8, 12 sessions at 5 MiB stripes. Select the best stable tier policy, then compare 5, 10, and 20 MiB stripes. Reject any setting with hash mismatch, materially worse P10/CV, retry amplification, reset growth, pool stall, or manual recovery requirement.

- [ ] **Step 4: Run the ten-minute production acceptance window**

Exclude only the first 30 seconds, explicit logged proxy/DC outage, and a file tail with fewer than eight runnable workers; report every exclusion. Require:

```text
P10 five-second committed goodput >= 8 MiB/s
coefficient of variation <= 0.25
stripe retry rate < 0.02
transport reset logs <= 10
hash agreement = 100%
container restarts = 0
OOMKilled = false
Web and database consumers responsive
```

- [ ] **Step 5: Keep the winner or roll back from evidence**

If every gate passes, retain the candidate and commit final measurements. If a correctness/stability gate fails, immediately restore the prior image/config. If only the speed target is constrained by DC/proxy/account limits, keep the best hash-safe stable candidate only when it is no worse than baseline, and report the controlled evidence without claiming success.

- [ ] **Step 6: Commit final operational evidence**

```bash
git add .superpowers/sdd/task-per-file-pools-report.md
git commit -m "docs: record per-file pool production validation"
```

---

## Completion Definition

The work is complete only when all automated suites pass from the exact image, immutable-file success/failure/resume candidates match existing SHA-256 values, the deployed service remains healthy without intervention, and the ten-minute raw committed-byte window meets the stated P10/CV/retry/reset gates. A high UI peak, smoothed display, or short benchmark does not satisfy the goal.
