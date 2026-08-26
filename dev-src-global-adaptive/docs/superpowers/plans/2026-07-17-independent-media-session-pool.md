# Independent Media Session Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give each large-file range worker an independent Kurigram media session while preserving Kurigram's file/CDN protocol implementation and the existing resumable integrity pipeline.

**Architecture:** `KurigramRangeSource` will prepare a bounded queue of temporary media sessions sequentially, lease one session per contiguous range, and invoke the existing Kurigram `get_file` method through a session-bound client adapter. `ParallelDownloader` will own source preparation and cleanup around its existing chunk download lifecycle.

**Tech Stack:** Python 3.11, Kurigram 2.2.24, asyncio, SQLite, unittest, Docker Compose.

## Global Constraints

- NAS changes are limited to `/volume2/docker/telegram_media_downloader_us`.
- Keep `max_download_task` at 5 and start with `parallel_download_workers: 2`.
- Do not implement raw MTProto file requests, CDN redirects, decryption, or CDN hash verification.
- Preserve SQLite resume manifests, positional SSD writes, final chunk readback, whole-file SHA-256, and sequential fallback.
- Create temporary media sessions sequentially and close them on success, failure, and cancellation.
- Deploy only after an already-downloaded large file matches the HDD baseline SHA-256 and outperforms the same-session baseline.

---

### Task 1: Session-Bound Kurigram Adapter

**Files:**
- Modify: `module/parallel_downloader.py`
- Test: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Produces: `_SessionBoundClient(client, media_session)` with delegated attributes and `get_session(...)` routing.
- Consumes: Kurigram's existing bound `client.get_file.__func__` implementation.

- [ ] **Step 1: Write failing adapter routing tests**

Add tests that construct a real-client double with distinct media and CDN session objects. Verify normal media requests return the bound media session and CDN requests delegate to the real client.

```python
async def test_bound_client_uses_leased_media_session(self):
    real_client = FakeKurigramClient()
    leased = object()
    bound = _SessionBoundClient(real_client, leased)

    result = await bound.get_session(4, is_media=True)

    self.assertIs(leased, result)
    self.assertEqual([], real_client.get_session_calls)

async def test_bound_client_delegates_cdn_session_requests(self):
    real_client = FakeKurigramClient()
    leased = object()
    bound = _SessionBoundClient(real_client, leased)

    result = await bound.get_session(4, is_cdn=True, temporary=True)

    self.assertIs(real_client, result)
    self.assertEqual([(4, False, True, True)], real_client.get_session_calls)
```

- [ ] **Step 2: Run the adapter tests and verify RED**

Run in the NAS test image:

```bash
python -m unittest \
  tests.module.test_parallel_downloader.SessionBoundClientTest
```

Expected: import or name failure because `_SessionBoundClient` does not exist.

- [ ] **Step 3: Implement the minimal adapter**

Add an internal adapter that delegates all non-session attributes and preserves the complete `get_session` call signature through keyword forwarding.

```python
class _SessionBoundClient:
    def __init__(self, client, media_session):
        self._client = client
        self._media_session = media_session

    def __getattr__(self, name):
        return getattr(self._client, name)

    async def get_session(
        self,
        dc_id=None,
        is_media=False,
        is_cdn=False,
        **kwargs,
    ):
        if is_media and not is_cdn:
            return self._media_session
        return await self._client.get_session(
            dc_id,
            is_media=is_media,
            is_cdn=is_cdn,
            **kwargs,
        )
```

- [ ] **Step 4: Run the adapter tests and verify GREEN**

Run the command from Step 2. Expected: all adapter tests pass.

- [ ] **Step 5: Commit the adapter**

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "feat: bind range downloads to media sessions"
```

---

### Task 2: Temporary Media Session Pool

**Files:**
- Modify: `module/parallel_downloader.py`
- Test: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Produces: `KurigramRangeSource.prepare(worker_count: int) -> Awaitable[None]`.
- Produces: `KurigramRangeSource.close() -> Awaitable[None]`.
- Changes: `KurigramRangeSource.iter_range(...)` leases a prepared session for the full range.
- Consumes: `_SessionBoundClient` from Task 1.

- [ ] **Step 1: Extend the fake client with temporary sessions**

Add `FakeMediaSession` and update `FakeKurigramClient.get_session` to record `(dc_id, is_media, is_cdn, temporary)`, return the cached fake for normal calls, and create a new stoppable session for each temporary media call.

```python
class FakeMediaSession:
    def __init__(self, number):
        self.number = number
        self.stop_calls = 0

    async def stop(self):
        self.stop_calls += 1
```

- [ ] **Step 2: Write failing pool behavior tests**

Add tests proving sequential creation, distinct leases, lease return on errors, cleanup after partial preparation, and idempotent close.

```python
async def test_prepare_creates_sessions_sequentially(self):
    await source.prepare(2)
    self.assertEqual(2, len(client.temporary_sessions))

async def test_concurrent_ranges_use_distinct_sessions(self):
    await source.prepare(2)
    await asyncio.gather(consume(0), consume(CHUNK_SIZE))
    self.assertEqual(2, len(set(client.range_session_numbers)))

async def test_range_error_returns_session_to_pool(self):
    await source.prepare(2)
    with self.assertRaises(RuntimeError):
        await consume_failing_range()
    await consume_working_range()
    self.assertEqual(2, source.available_session_count)

async def test_prepare_failure_closes_created_sessions(self):
    client.fail_temporary_session_number = 2
    with self.assertRaises(RuntimeError):
        await source.prepare(2)
    self.assertEqual(1, client.temporary_sessions[0].stop_calls)

async def test_close_stops_all_temporary_sessions_once(self):
    await source.prepare(2)
    await source.close()
    await source.close()
    self.assertTrue(all(item.stop_calls == 1 for item in client.temporary_sessions))
```

- [ ] **Step 3: Run pool tests and verify RED**

```bash
python -m unittest \
  tests.module.test_parallel_downloader.KurigramSessionPoolTest
```

Expected: failures because `prepare`, pooled leasing, and `close` are absent.

- [ ] **Step 4: Implement sequential preparation and cleanup**

Add pool state to `KurigramRangeSource` and create temporary sessions one at a time after `_get_media_session()` warms the cached authorization path.

```python
async def prepare(self, worker_count):
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    if self._session_queue is not None:
        if worker_count != len(self._temporary_sessions):
            raise RuntimeError("range source already prepared")
        return

    await self._get_media_session()
    sessions = []
    try:
        for _ in range(worker_count):
            session = await self.client.get_session(
                self.file_id.dc_id,
                is_media=True,
                temporary=True,
            )
            sessions.append(session)
    except BaseException:
        await asyncio.gather(
            *(session.stop() for session in sessions),
            return_exceptions=True,
        )
        raise

    queue = asyncio.Queue(maxsize=worker_count)
    for session in sessions:
        queue.put_nowait(session)
    self._temporary_sessions = sessions
    self._session_queue = queue
```

`iter_range` leases from `_session_queue`, calls `self.client.get_file.__func__` with `_SessionBoundClient`, and returns the lease in `finally`. If no pool is prepared, retain the current cached-session behavior for direct adapter tests and compatibility.

- [ ] **Step 5: Implement idempotent close**

Detach the queue and session list before awaiting stops so cancellation or repeated calls cannot stop the same session twice.

- [ ] **Step 6: Run pool and existing range-source tests**

```bash
python -m unittest \
  tests.module.test_parallel_downloader.KurigramSessionPoolTest \
  tests.module.test_parallel_downloader.KurigramRangeSourceTest
```

Expected: all tests pass.

- [ ] **Step 7: Commit the session pool**

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "feat: add temporary media session pool"
```

---

### Task 3: Parallel Downloader Source Lifecycle

**Files:**
- Modify: `module/parallel_downloader.py`
- Test: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Consumes: optional source methods `prepare(worker_count)` and `close()`.
- Preserves: `ParallelDownloader.download(...) -> ParallelDownloadResult`.

- [ ] **Step 1: Write failing lifecycle tests**

Use a source double that records lifecycle calls and can fail or block inside `iter_range`.

```python
async def test_download_prepares_and_closes_source_on_success(self):
    result = await downloader.download(identity, part_path)
    self.assertTrue(result.integrity.verified)
    self.assertEqual([("prepare", 2), ("close",)], source.lifecycle)

async def test_download_closes_source_after_range_failure(self):
    with self.assertRaises(RuntimeError):
        await downloader.download(identity, part_path)
    self.assertEqual(("close",), source.lifecycle[-1])

async def test_download_closes_source_after_cancellation(self):
    task = asyncio.create_task(downloader.download(identity, part_path))
    await source.started.wait()
    task.cancel()
    with self.assertRaises(asyncio.CancelledError):
        await task
    self.assertEqual(("close",), source.lifecycle[-1])

async def test_cleanup_error_does_not_hide_download_error(self):
    source.download_error = RuntimeError("download failed")
    source.close_error = RuntimeError("close failed")
    with self.assertRaisesRegex(RuntimeError, "download failed"):
        await downloader.download(identity, part_path)
```

- [ ] **Step 2: Run lifecycle tests and verify RED**

```bash
python -m unittest \
  tests.module.test_parallel_downloader.ParallelSourceLifecycleTest
```

Expected: lifecycle methods are never called.

- [ ] **Step 3: Wrap the existing download body with source lifecycle**

Extract the current implementation into `_download_prepared(...)`. Keep the public `download(...)` method responsible for optional preparation and cleanup.

Import `logger` from `loguru` so cleanup failures can be recorded without
replacing an earlier download exception.

```python
async def download(self, identity, part_path, progress=None):
    prepare = getattr(self.source, "prepare", None)
    close = getattr(self.source, "close", None)
    primary_error = None
    if prepare is not None:
        await prepare(self.workers)
    try:
        return await self._download_prepared(identity, part_path, progress)
    except BaseException as error:
        primary_error = error
        raise
    finally:
        if close is not None:
            try:
                await close()
            except Exception:
                if primary_error is None:
                    raise
                logger.exception("Failed to close parallel range source")
```

- [ ] **Step 4: Run lifecycle and recovery tests**

```bash
python -m unittest \
  tests.module.test_parallel_downloader.ParallelSourceLifecycleTest \
  tests.module.test_parallel_downloader.ParallelRecoveryTest \
  tests.test_media_downloader.DownloadRoutingTest
```

Expected: all tests pass; cancellation still does not invoke sequential fallback.

- [ ] **Step 5: Commit lifecycle ownership**

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "fix: close parallel media sessions reliably"
```

---

### Task 4: Candidate Image Verification

**Files:**
- Verify: `module/parallel_downloader.py`
- Verify: `tests/module/test_parallel_downloader.py`
- Verify: `tools/validate_parallel_downloads.py`

**Interfaces:**
- Consumes the existing Docker base image `telegram_media_downloader_base:kurigram-2.2.24`.
- Produces a commit-tagged candidate image without changing the running Compose service.

- [ ] **Step 1: Run the core test suite**

```bash
python -m unittest \
  tests.module.test_parallel_downloader \
  tests.tools.test_validate_parallel_downloads \
  tests.module.test_parallel_validation \
  tests.test_media_downloader.DownloadRoutingTest
```

Expected: zero failures and zero errors.

- [ ] **Step 2: Run syntax and diff checks**

```bash
python -m py_compile \
  module/parallel_downloader.py \
  tools/validate_parallel_downloads.py \
  media_downloader.py
git diff --check
```

Expected: both commands exit 0.

- [ ] **Step 3: Back up and sync only changed application files**

Create a timestamped backup below `/volume2/docker/telegram_media_downloader_us/backups`, then sync only files changed by this implementation into the NAS `app-src` directory.

- [ ] **Step 4: Build a commit-tagged candidate image**

```bash
commit=$(git rev-parse --short HEAD)
docker build \
  -f /volume2/docker/telegram_media_downloader_us/app-src/Dockerfile.local \
  -t "telegram_media_downloader_us:parallel-${commit}" \
  /volume2/docker/telegram_media_downloader_us/app-src
```

Expected: Docker build exits 0 and the running service still uses `telegram_media_downloader_us:parallel-7632312`.

- [ ] **Step 5: Run the core tests inside the built image**

Mount the test directory read-only and rerun Step 1 against the candidate image. Expected: zero failures and zero errors.

---

### Task 5: NAS Integrity and Throughput Gate

**Files:**
- Modify after successful gate: `/volume2/docker/telegram_media_downloader_us/docker-compose.yaml`
- Verify: `/volume2/docker/telegram_media_downloader_us/config.yaml`

**Interfaces:**
- Input baseline: an existing successful large download under `/app/downloads`.
- Output evidence: candidate SHA-256, baseline SHA-256, session count, elapsed seconds, and bytes per second.

- [ ] **Step 1: Record current production health**

Capture image, restart count, Web HTTP status, active task counts, and the last five minutes of logs. Confirm the production config remains enabled with two workers.

- [ ] **Step 2: Run a controlled two-session download**

Briefly stop only this Compose service, run the candidate image with the existing config and session mounts, and use a trap to restore the service on every exit path. Download message `10341` from chat `-1002313319912` into a fresh validation directory.

- [ ] **Step 3: Compare integrity and speed**

Require all of the following:

```text
candidate_size == 286168400
candidate_sha256 == baseline_sha256
workers == 2
temporary_media_sessions == 2
AUTH_BYTES_INVALID count == 0
CDNFileHashMismatch count == 0
throughput > measured same-session throughput
```

If any condition fails, leave production on `parallel-7632312` and retain the validation report for diagnosis.

- [ ] **Step 4: Deploy only after the gate passes**

Back up Compose and config, switch the image tag to the new candidate, keep `parallel_download_workers: 2`, and recreate only `telegram_media_downloader_us`.

- [ ] **Step 5: Observe production for at least one complete large file**

Measure progress from SQLite manifest byte deltas over 20 seconds. Verify Web HTTP 200, restart count 0, no authorization/CDN/hash errors, successful final SSD readback, and movement to HDD.

- [ ] **Step 6: Report deployment evidence**

Do not commit NAS runtime data. Report the final image tag, test count, sample message, matching SHA-256, measured throughput, and rollback backup path in the task completion response.
