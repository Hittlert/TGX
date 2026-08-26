# Parallel Download Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, two-worker single-file downloader and prove its output against both an existing HDD baseline and Telegram's remote SHA-256 ranges before production enablement.

**Architecture:** A transport-neutral parallel engine owns range planning, positional writes, restart manifests, bounded retries, and integrity verification. A Kurigram adapter retains Kurigram's existing media-session and CDN behavior while exposing contiguous byte ranges and `upload.getFileHashes`; the current download path selects the engine only when an explicitly disabled-by-default feature flag is enabled. A separate validation CLI reads production records and HDD files without modifying either, writes candidates under SSD temp storage, and emits a JSON decision report.

**Tech Stack:** Python 3, asyncio, Kurigram 2.2.24, SQLite, SHA-256, Docker Compose, unittest.

## Global Constraints

- Keep the production container running throughout development and local tests.
- Stop production only immediately before the isolated Telegram integration run, because both clients use the same session.
- Restart the current non-parallel production image after integration validation whether validation passes or fails.
- Modify server files only under `/volume2/docker/telegram_media_downloader_us`.
- Keep the existing five-file consumer concurrency unchanged.
- Start with exactly two range workers and a fixed 1 MiB Kurigram range size.
- Keep SSD as staging and HDD as the archive destination.
- Never rename, overwrite, move, or delete the existing HDD baseline files.
- Never update production download records during validation.
- File size alone is never sufficient for a successful candidate result.
- Keep parallel mode disabled by default and do not enable it in production during this implementation.

## gotd Lessons Applied

The implementation and tests must encode the following gotd downloader history, not merely cite it:

- `5fcfdba59` reduced gotd's default part size after transport errors. Kurigram fixes its public generator at 1 MiB, so retries are bounded at the exact failed offset and parallel mode can fall back to the existing sequential path; concurrency stays at two.
- `842a57675` added remote hash verification. Every candidate must have gap-free `upload.getFileHashes` coverage and every range must match before commit.
- `cbb03f8d2` retried FloodWait in both data and hash requests. The adapter honors the server-provided delay for both operations.
- `48c3ab371` changed gotd's default worker count to one. This feature is disabled by default and uses an explicit two-worker canary only.
- `89394ccaa` retried Telegram timeouts; `a8e66dc59` later fixed wrapped transport timeouts. Retry classification walks `__cause__` and `__context__`, but never retries cancellation.
- `d3e29878c` treats a short part as EOF. A short final part is accepted only at the declared file end; a short part before that point is a hard incomplete-range failure.
- `abadfe275` restored CDN redirect support with token refresh, late redirects, unaligned hash windows, and multi-thread tests. The adapter delegates CDN transfer and decryption to Kurigram rather than recreating that state machine, then independently verifies the completed file with master-DC hashes.
- gotd temporarily removed CDN support in 2021 and restored the full state machine in 2026. No custom CDN crypto, IV manipulation, token cache, or reupload protocol is introduced here.
- Repeated final hash batches must not create an infinite loop. Hash collection has a monotonic-progress guard and a bounded request count.

---

### Task 1: Range Model And Durable Manifest

**Files:**
- Create: `module/parallel_downloader.py`
- Test: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Produces: `MediaIdentity`, `ChunkSpec`, `RemoteHash`, `DownloadManifest`, `plan_chunks(file_size, chunk_size)`, and `split_missing_runs(chunks, completed_offsets, workers)`.
- `MediaIdentity.stable_key() -> str` serializes chat ID, message ID, media ID, DC ID, file unique ID, and declared size deterministically.
- `DownloadManifest.prepare(identity, file_size, chunk_size) -> None` creates or validates a SQLite manifest beside the SSD part file.
- `DownloadManifest.completed_chunks() -> Dict[int, Tuple[int, str]]` returns offset to `(length, sha256_hex)`.
- `DownloadManifest.mark_complete(chunk, digest, attempts) -> None` commits one verified local chunk atomically.

- [ ] **Step 1: Write failing model and partition tests**

```python
class ParallelDownloadPlanningTest(unittest.TestCase):
    def test_plan_chunks_keeps_exact_final_length(self):
        chunks = plan_chunks(10, 4)
        self.assertEqual(
            [ChunkSpec(0, 4), ChunkSpec(4, 4), ChunkSpec(8, 2)], chunks
        )

    def test_split_missing_runs_never_duplicates_offsets(self):
        chunks = plan_chunks(20, 4)
        runs = split_missing_runs(chunks, {4, 12}, workers=2)
        flattened = [chunk.offset for run in runs for chunk in run]
        self.assertEqual([0, 8, 16], sorted(flattened))
        self.assertEqual(len(flattened), len(set(flattened)))

    def test_plan_supports_files_larger_than_two_gibibytes(self):
        size = 2 * 1024**3 + 17
        chunks = plan_chunks(size, 1024 * 1024)
        self.assertEqual(size, sum(chunk.length for chunk in chunks))
        self.assertEqual(17, chunks[-1].length)
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python -m unittest tests.module.test_parallel_downloader.ParallelDownloadPlanningTest -v`

Expected: import failure for `module.parallel_downloader`.

- [ ] **Step 3: Implement immutable range types and deterministic partitioning**

```python
CHUNK_SIZE = 1024 * 1024

@dataclass(frozen=True)
class MediaIdentity:
    chat_id: str
    message_id: int
    media_id: int
    dc_id: int
    file_unique_id: str
    file_size: int

    def stable_key(self) -> str:
        payload = dataclasses.asdict(self)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

@dataclass(frozen=True)
class ChunkSpec:
    offset: int
    length: int

@dataclass(frozen=True)
class RemoteHash:
    offset: int
    limit: int
    digest: bytes

def plan_chunks(file_size: int, chunk_size: int = CHUNK_SIZE) -> List[ChunkSpec]:
    if file_size <= 0 or chunk_size <= 0:
        raise ValueError("file_size and chunk_size must be positive")
    return [
        ChunkSpec(offset, min(chunk_size, file_size - offset))
        for offset in range(0, file_size, chunk_size)
    ]
```

`split_missing_runs` filters completed offsets, groups adjacent chunks, and splits large groups near equal chunk counts until at most `workers` runnable segments exist. It never mutates or duplicates a `ChunkSpec`.

- [ ] **Step 4: Run planning tests and verify GREEN**

Run: `python -m unittest tests.module.test_parallel_downloader.ParallelDownloadPlanningTest -v`

Expected: three tests pass.

- [ ] **Step 5: Write failing manifest recovery tests**

```python
class DownloadManifestTest(unittest.TestCase):
    def test_manifest_rejects_changed_media_identity(self):
        manifest.prepare(first_identity, 8, 4)
        with self.assertRaises(MediaIdentityChanged):
            manifest.prepare(second_identity, 8, 4)

    def test_recovery_drops_chunk_whose_local_digest_changed(self):
        manifest.prepare(identity, 8, 4)
        manifest.mark_complete(ChunkSpec(0, 4), sha256(b"abcd").hexdigest(), 1)
        part_path.write_bytes(b"xbcd" + b"\0" * 4)
        valid = manifest.revalidate_completed(part_path)
        self.assertEqual({}, valid)
        self.assertEqual({}, manifest.completed_chunks())
```

- [ ] **Step 6: Run manifest tests and verify RED**

Run: `python -m unittest tests.module.test_parallel_downloader.DownloadManifestTest -v`

Expected: failure because manifest persistence is absent.

- [ ] **Step 7: Implement the SQLite manifest**

Create `download_meta(identity_key TEXT, file_size INTEGER, chunk_size INTEGER)` and `completed_chunks(offset INTEGER PRIMARY KEY, length INTEGER, sha256 TEXT, attempts INTEGER)` tables. Use `PRAGMA journal_mode=WAL`, `busy_timeout=30000`, an `RLock`, and one transaction per metadata or completed-chunk update. `revalidate_completed` reads each recorded byte range from the part file, removes missing/short/mismatched rows, and returns only valid rows.

- [ ] **Step 8: Run the complete Task 1 tests and commit**

Run: `python -m unittest tests.module.test_parallel_downloader -v`

Expected: planning and manifest tests pass.

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "feat: add durable parallel range manifest"
```

---

### Task 2: Retry Policy, Remote Hashes, And Kurigram Adapter

**Files:**
- Modify: `module/parallel_downloader.py`
- Modify: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Produces: `is_retryable_timeout(error) -> bool`, `retry_telegram(operation, max_attempts, sleep)`, `build_file_location(file_id)`, and `KurigramRangeSource`.
- `KurigramRangeSource.iter_range(start_offset, expected_length) -> AsyncIterator[bytes]` delegates one contiguous range to `Client.get_file`.
- `KurigramRangeSource.get_hashes(offset) -> List[RemoteHash]` invokes `upload.GetFileHashes` on the media DC.
- `collect_remote_hashes(source, file_size) -> List[RemoteHash]` deduplicates ranges and requires monotonic progress.

- [ ] **Step 1: Write failing retry tests based on gotd fixes**

```python
class RetryPolicyTest(unittest.IsolatedAsyncioTestCase):
    async def test_retries_timeout_wrapped_in_exception_cause(self):
        calls = 0
        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                inner = TimeoutError("proxy timed out")
                raise RuntimeError("media session failed") from inner
            return b"ok"
        result = await retry_telegram(operation, max_attempts=3, sleep=fake_sleep)
        self.assertEqual(b"ok", result)
        self.assertEqual(2, calls)

    async def test_does_not_retry_cancellation(self):
        with self.assertRaises(asyncio.CancelledError):
            await retry_telegram(cancelled_operation, 3, fake_sleep)

    async def test_flood_wait_uses_server_delay(self):
        result = await retry_telegram(flood_once_operation, 3, fake_sleep)
        self.assertEqual("ok", result)
        self.assertEqual([17], slept_seconds)
```

- [ ] **Step 2: Verify retry tests fail**

Run: `python -m unittest tests.module.test_parallel_downloader.RetryPolicyTest -v`

Expected: missing retry helpers.

- [ ] **Step 3: Implement bounded exception-chain retry**

Walk `error`, `error.__cause__`, and `error.__context__` with a visited-ID set. Retry `TimeoutError`, `asyncio.TimeoutError`, `ConnectionError`, and timeout-marked `OSError`; honor Kurigram `FloodWait.value`; raise the final exception after three attempts; use capped exponential sleeps of 1 and 2 seconds for transport failures; re-raise `CancelledError` immediately.

- [ ] **Step 4: Verify retry tests pass**

Run: `python -m unittest tests.module.test_parallel_downloader.RetryPolicyTest -v`

Expected: all retry policy tests pass.

- [ ] **Step 5: Write failing hash collection and adapter tests**

```python
class RemoteHashCollectionTest(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_final_batch_stops_without_looping(self):
        source = FakeHashSource([
            [remote_hash(0, 4, b"abcd")],
            [remote_hash(0, 4, b"abcd")],
        ])
        hashes = await collect_remote_hashes(source, 4)
        self.assertEqual([(0, 4)], [(item.offset, item.limit) for item in hashes])
        self.assertEqual(1, source.calls)

    async def test_hash_fetch_retries_wrapped_timeout(self):
        source = TimeoutThenHashSource()
        hashes = await collect_remote_hashes(source, 4)
        self.assertEqual(2, source.calls)
        self.assertEqual(1, len(hashes))

    async def test_adapter_requests_one_contiguous_kurigram_range(self):
        chunks = [chunk async for chunk in source.iter_range(2 * MIB, 2 * MIB)]
        client.get_file.assert_called_once_with(
            decoded_file_id, file_size, limit=2, offset=2
        )
        self.assertEqual([b"a" * MIB, b"b" * MIB], chunks)
```

- [ ] **Step 6: Verify adapter tests fail**

Run: `python -m unittest tests.module.test_parallel_downloader.RemoteHashCollectionTest tests.module.test_parallel_downloader.KurigramRangeSourceTest -v`

Expected: missing source and hash collector.

- [ ] **Step 7: Implement the Kurigram adapter and monotonic hash collector**

Decode `file_id` through `pyrogram.file_id.FileId.decode`, build the same `InputPhotoFileLocation`, `InputDocumentFileLocation`, or peer-photo location Kurigram uses, and obtain `client.get_session(dc_id, is_media=True)`. Range starts must be 1 MiB aligned; limits are `ceil(expected_length / 1 MiB)`. Validate every yielded chunk: only the declared final chunk may be short. Hash collection invokes `raw.functions.upload.GetFileHashes(location=..., offset=offset)` through `retry_telegram`, sorts and deduplicates `(offset, limit, digest)`, rejects non-positive or out-of-bounds starts, advances to the greatest newly covered end, and fails as `RemoteHashUnavailable` when coverage cannot advance toward `file_size`.

- [ ] **Step 8: Run Task 2 tests and commit**

Run: `python -m unittest tests.module.test_parallel_downloader -v`

Expected: all tests pass.

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "feat: add resilient Kurigram range transport"
```

---

### Task 3: Parallel Writer And End-To-End Integrity Gate

**Files:**
- Modify: `module/parallel_downloader.py`
- Modify: `tests/module/test_parallel_downloader.py`

**Interfaces:**
- Produces: `IntegrityReport`, `ParallelDownloadResult`, `verify_file_hashes(path, file_size, hashes)`, and `ParallelDownloader.download(identity, part_path, progress=None) -> ParallelDownloadResult`.
- Success means exact range coverage, exact file size, successful remote hash verification, `fsync`, and whole-file SHA-256 calculation.

- [ ] **Step 1: Write failing positional-write and coverage tests**

```python
class ParallelWriterTest(unittest.IsolatedAsyncioTestCase):
    async def test_out_of_order_workers_produce_exact_file(self):
        source = DelayedMemorySource(b"abcdefghijkl", completion_order=[8, 0, 4])
        result = await downloader(source, chunk_size=4, workers=2).download(
            identity_for(12), part_path
        )
        self.assertEqual(b"abcdefghijkl", part_path.read_bytes())
        self.assertEqual(sha256(b"abcdefghijkl").hexdigest(), result.sha256)

    async def test_short_non_final_chunk_fails(self):
        source = ShortChunkSource(offset=4)
        with self.assertRaises(IncompleteRange):
            await downloader(source, chunk_size=4, workers=2).download(
                identity_for(12), part_path
            )

    async def test_short_positional_write_is_completed(self):
        writer = ShortPwriteWriter(max_bytes=2)
        await write_all_at(writer, 4, b"abcdef")
        self.assertEqual([(4, b"ab"), (6, b"cd"), (8, b"ef")], writer.calls)
```

- [ ] **Step 2: Verify parallel writer tests fail**

Run: `python -m unittest tests.module.test_parallel_downloader.ParallelWriterTest -v`

Expected: missing downloader and exact positional writer.

- [ ] **Step 3: Implement two-worker range execution and positional writes**

Preallocate the SSD part to the declared size. Revalidate the manifest, split missing chunks into at most two contiguous runs, and consume each source generator into exact `ChunkSpec` boundaries. Use `os.pwrite` through `asyncio.to_thread`, loop until all bytes are written, hash each completed local chunk before recording it, and cancel sibling tasks on fatal failure. Never share a mutable file seek position. Preserve the part and manifest on failure.

- [ ] **Step 4: Verify writer tests pass**

Run: `python -m unittest tests.module.test_parallel_downloader.ParallelWriterTest -v`

Expected: out-of-order, short-read, and short-write tests pass.

- [ ] **Step 5: Write failing remote integrity tests**

```python
class IntegrityGateTest(unittest.IsolatedAsyncioTestCase):
    async def test_unaligned_hash_windows_cover_and_verify_file(self):
        path.write_bytes(b"abcdefghijkl")
        hashes = [hash_range(0, 3), hash_range(3, 5), hash_range(8, 8)]
        report = await verify_file_hashes(path, 12, hashes)
        self.assertTrue(report.verified)
        self.assertEqual(12, report.covered_bytes)

    async def test_corrupted_byte_is_hard_failure(self):
        path.write_bytes(b"abcxefgh")
        with self.assertRaises(HashMismatch):
            await verify_file_hashes(path, 8, original_hashes)

    async def test_gap_in_remote_hashes_is_unverified(self):
        with self.assertRaises(RemoteHashUnavailable):
            await verify_file_hashes(path, 8, [hash_range(0, 4)])

    async def test_final_hash_limit_may_extend_past_declared_eof(self):
        path.write_bytes(b"abcdef")
        report = await verify_file_hashes(path, 6, [remote_hash(0, 8, b"abcdef")])
        self.assertTrue(report.verified)
```

- [ ] **Step 6: Verify integrity tests fail**

Run: `python -m unittest tests.module.test_parallel_downloader.IntegrityGateTest -v`

Expected: missing integrity verifier.

- [ ] **Step 7: Implement gap-free Telegram hash verification and commit gate**

Sort hash ranges by offset, merge coverage only when the next range begins at or before current coverage, clamp only the final read to declared EOF, read each range independently with positional reads, compare SHA-256 using `hmac.compare_digest`, and raise on mismatch or incomplete coverage. After all chunks are present, collect remote hashes, verify them, check `stat().st_size`, call `fsync`, calculate whole SHA-256 in 8 MiB blocks, and return the result. Do not rename to an archive path inside this engine.

- [ ] **Step 8: Write and pass restart recovery test**

```python
async def test_restart_downloads_only_unfinished_chunks(self):
    first = ParallelDownloader(source, workers=2, abort_after_chunks=2)
    with self.assertRaises(InjectedAbort):
        await first.download(identity_for(16), part_path)
    completed_before = manifest.completed_chunks()
    second = ParallelDownloader(source, workers=2)
    result = await second.download(identity_for(16), part_path)
    self.assertEqual(completed_before.keys(), source.not_requested_on_second_run)
    self.assertEqual(expected_sha256, result.sha256)
```

Run: `python -m unittest tests.module.test_parallel_downloader -v`

Expected: every Task 1-3 test passes, including process recovery.

```bash
git add module/parallel_downloader.py tests/module/test_parallel_downloader.py
git commit -m "feat: verify parallel downloads before commit"
```

---

### Task 4: Disabled-By-Default Production Integration

**Files:**
- Modify: `module/app.py:417-465`
- Modify: `module/app.py:785-885`
- Modify: `media_downloader.py:512-655`
- Modify: `tests/module/test_app.py`
- Modify: `tests/test_media_downloader.py`

**Interfaces:**
- Adds config fields `parallel_download_enabled: bool = False`, `parallel_download_workers: int = 2`, and `parallel_download_min_size: int = 268435456`.
- Produces: `_should_use_parallel_download(media_size) -> bool` and `_download_to_temp(...) -> str`.
- Existing `download_media` remains responsible for moving the verified SSD temp file to HDD and recording success.

- [ ] **Step 1: Write failing config tests**

```python
def test_parallel_download_defaults_are_conservative(self):
    app = Application("", "")
    self.assertFalse(app.parallel_download_enabled)
    self.assertEqual(2, app.parallel_download_workers)
    self.assertEqual(256 * 1024 * 1024, app.parallel_download_min_size)

def test_assign_config_reads_parallel_canary_values(self):
    app.assign_config(valid_config_with(
        parallel_download_enabled=True,
        parallel_download_workers=2,
        parallel_download_min_size=1024,
    ))
    self.assertTrue(app.parallel_download_enabled)
```

- [ ] **Step 2: Verify config tests fail, implement config parsing, verify GREEN**

Run: `python -m unittest tests.module.test_app.ApplicationTestCase.test_parallel_download_defaults_are_conservative tests.module.test_app.ApplicationTestCase.test_assign_config_reads_parallel_canary_values -v`

Expected before implementation: missing attributes. Expected after implementation: both pass. Reject worker counts outside `1..4`; default invalid values back to two. Do not write these keys into the NAS production config.

- [ ] **Step 3: Write failing routing tests**

```python
class DownloadRoutingTest(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_flag_uses_existing_kurigram_download(self):
        app.parallel_download_enabled = False
        await _download_to_temp(client, message, media, temp_path, progress_args)
        client.download_media.assert_awaited_once()
        parallel_factory.assert_not_called()

    async def test_enabled_large_file_uses_parallel_downloader(self):
        app.parallel_download_enabled = True
        app.parallel_download_min_size = 1024
        await _download_to_temp(client, message, media_with_size(2048), temp_path, args)
        parallel_instance.download.assert_awaited_once()
        client.download_media.assert_not_called()

    async def test_parallel_failure_falls_back_to_existing_path(self):
        parallel_instance.download.side_effect = RemoteHashUnavailable("no hashes")
        result = await _download_to_temp(client, message, media, temp_path, args)
        self.assertEqual(sequential_path, result)
        client.download_media.assert_awaited_once()
```

- [ ] **Step 4: Verify routing tests fail**

Run: `python -m unittest tests.test_media_downloader.DownloadRoutingTest -v`

Expected: `_download_to_temp` does not exist.

- [ ] **Step 5: Implement routing without changing consumer concurrency**

Build `MediaIdentity` from the fetched message/media, use `<temp_file_name>.parallel.part`, and pass the existing progress callback through an aggregate-byte adapter. Parallel success returns the verified SSD path to the existing `_check_download_finish` and `_move_to_download_path` flow. Candidate failure is logged and falls back once to `client.download_media`; cancellation and process-exit exceptions are not swallowed. Leave `max_download_task` and worker startup unchanged.

- [ ] **Step 6: Run integration unit tests and commit**

Run: `python -m unittest tests.test_media_downloader tests.module.test_app -v`

Expected: all existing and new tests pass.

```bash
git add module/app.py media_downloader.py tests/module/test_app.py tests/test_media_downloader.py
git commit -m "feat: add opt-in parallel download route"
```

---

### Task 5: Read-Only Validation Selection And Decision Report

**Files:**
- Create: `module/parallel_validation.py`
- Create: `tests/module/test_parallel_validation.py`

**Interfaces:**
- Produces: `ValidationSample`, `SampleResult`, `select_samples(connection, path_exists)`, `decide_sample(...)`, and `build_run_report(results)`.
- Sample buckets are `<10 MiB x2`, `10-200 MiB x2`, `200 MiB-1 GiB x1`, and `>1 GiB x1`.
- Report eligibility requires six valid samples, all candidate remote checks passing, and zero unexplained baseline/candidate mismatch.

- [ ] **Step 1: Write failing read-only selection tests**

```python
def test_selects_exact_bucket_counts_from_success_rows(self):
    seed_success_rows(db, sizes=[1*MIB, 2*MIB, 20*MIB, 30*MIB, 300*MIB, 2*GIB])
    samples, gaps = select_samples(read_only_connection(db), os.path.exists)
    self.assertEqual([2, 2, 1, 1], bucket_counts(samples))
    self.assertEqual([], gaps)

def test_reports_missing_bucket_without_substitution(self):
    seed_success_rows(db, sizes=[1*MIB, 2*MIB, 20*MIB, 30*MIB, 300*MIB])
    samples, gaps = select_samples(read_only_connection(db), os.path.exists)
    self.assertEqual([">1GiB"], gaps)

def test_selection_never_writes_production_database(self):
    before = db.read_bytes()
    select_samples(sqlite3.connect(f"file:{db}?mode=ro", uri=True), exists)
    self.assertEqual(before, db.read_bytes())
```

- [ ] **Step 2: Verify selection tests fail, implement, and verify GREEN**

Run: `python -m unittest tests.module.test_parallel_validation.ValidationSelectionTest -v`

Expected before implementation: missing module. Expected after implementation: exact counts and read-only behavior pass.

- [ ] **Step 3: Write failing three-way decision tests**

```python
def test_matching_verified_files_pass(self):
    self.assertEqual("pass", decide_sample(same_sha=True, baseline_ok=True, candidate_ok=True).status)

def test_candidate_failure_blocks_parallel_mode(self):
    decision = decide_sample(same_sha=False, baseline_ok=True, candidate_ok=False)
    self.assertEqual("fail", decision.status)
    self.assertTrue(decision.blocks_parallel)

def test_both_remote_fail_is_invalid_not_pass(self):
    decision = decide_sample(same_sha=True, baseline_ok=False, candidate_ok=False)
    self.assertEqual("invalid", decision.status)

def test_report_requires_six_of_six(self):
    report = build_run_report([passing_sample()] * 5)
    self.assertFalse(report["eligible"])
```

- [ ] **Step 4: Verify decision tests fail, implement the matrix, and verify GREEN**

Run: `python -m unittest tests.module.test_parallel_validation.ValidationDecisionTest -v`

Expected before implementation: missing decisions. Expected after implementation: every matrix branch passes.

- [ ] **Step 5: Implement deterministic JSON report serialization**

Include run ID, start/end timestamps, app commit, Kurigram version, worker count, sample identity, DC, baseline/candidate paths, sizes, whole SHA-256 values, remote coverage, mismatches, elapsed seconds, throughput, retry count, decision, reason, bucket gaps, and top-level eligibility. Sort keys and write atomically below the validation SSD directory only.

- [ ] **Step 6: Run validation module tests and commit**

Run: `python -m unittest tests.module.test_parallel_validation -v`

Expected: selection, matrix, and serialization tests pass.

```bash
git add module/parallel_validation.py tests/module/test_parallel_validation.py
git commit -m "feat: add parallel integrity validation report"
```

---

### Task 6: Isolated Validation CLI And Crash Injection

**Files:**
- Create: `tools/validate_parallel_downloads.py`
- Create: `tests/tools/__init__.py`
- Create: `tests/tools/test_validate_parallel_downloads.py`
- Modify: `Dockerfile.local`

**Interfaces:**
- CLI arguments: `--config`, `--records`, `--downloads-root`, `--output-dir`, `--workers 2`, `--resume-run`, `--abort-after-chunks`, and `--report`.
- `--dry-select` performs read-only sample discovery without connecting Telegram.
- The CLI never imports or calls production record mutation methods.

- [ ] **Step 1: Write failing CLI dry-selection test**

```python
def test_dry_select_prints_six_samples_without_starting_client(self):
    result = run_cli("--dry-select", "--records", str(db), "--output-dir", str(tmp))
    self.assertEqual(0, result.returncode)
    self.assertEqual(6, len(json.loads(result.stdout)["samples"]))
    client_factory.assert_not_called()
```

- [ ] **Step 2: Verify CLI test fails**

Run: `python -m unittest tests.tools.test_validate_parallel_downloads.ValidationCliTest.test_dry_select_prints_six_samples_without_starting_client -v`

Expected: CLI module missing.

- [ ] **Step 3: Implement dry selection and Telegram validation flow**

Load YAML only for API/session/proxy settings. Open SQLite through `file:<path>?mode=ro`. For each selected row, fetch the message, locate the media object, verify its immutable identity, hash and remotely verify the HDD baseline, download and verify the SSD candidate, and append one report result. Candidate paths are `<output-dir>/<run-id>/<chat-id>-<message-id>.candidate`; manifests remain beside candidates. Continue after a sample failure, but set top-level eligibility false.

- [ ] **Step 4: Add deterministic crash injection and resume**

`--abort-after-chunks N` raises `InjectedAbort` only after N manifest commits and exits with code 75 while retaining the part and manifest. `--resume-run RUN_ID` uses the same directory and must reject a different media identity. The resumed report includes `recovered_chunks` and `downloaded_chunks`.

- [ ] **Step 5: Run CLI tests and commit**

Run: `python -m unittest tests.tools.test_validate_parallel_downloads -v`

Expected: dry selection, failed-sample continuation, exit 75, and resume tests pass.

```bash
git add tools/validate_parallel_downloads.py tests/tools Dockerfile.local
git commit -m "feat: add isolated parallel validation CLI"
```

---

### Task 7: Local Regression And Docker Build

**Files:**
- Modify only files required by failures introduced by Tasks 1-6.

**Interfaces:**
- Produces a local Docker image with parallel mode still disabled by default and the validation CLI available.

- [ ] **Step 1: Run the complete test suite**

Run: `python -m unittest discover -s tests -v`

Expected: all tests pass. Record pre-existing failures separately; do not alter unrelated behavior to hide them.

- [ ] **Step 2: Compile every changed Python module**

Run: `python -m compileall module media_downloader.py tools tests`

Expected: no syntax errors.

- [ ] **Step 3: Build the candidate image without starting it**

Run: `docker build -f Dockerfile.local -t telegram-media-downloader:parallel-candidate .`

Expected: image builds successfully. Do not run a Telegram client locally or on the NAS while production is active.

- [ ] **Step 4: Inspect defaults in the built image**

Run: `docker run --rm --entrypoint python telegram-media-downloader:parallel-candidate -c "from module.app import Application; a=Application('', ''); assert not a.parallel_download_enabled; assert a.parallel_download_workers == 2"`

Expected: exit code 0.

- [ ] **Step 5: Review diff and commit only implementation files**

Run: `git diff --check && git status --short`

Expected: no whitespace errors; unrelated pre-existing modifications remain untouched.

---

### Task 8: NAS Read-Only Preflight, Six-Sample Validation, And Restoration

**Files:**
- Copy candidate source only under `/volume2/docker/telegram_media_downloader_us/app-src`.
- Write candidate output only under `/volume2/docker/telegram_media_downloader_us/temp/parallel_validation`.
- Write reports only under `/volume2/docker/telegram_media_downloader_us/state/parallel_validation`.
- Do not modify production `config.yaml`, production SQLite, HDD baselines, or any path outside the allowed server directory.

**Interfaces:**
- Produces one six-sample JSON report and one explicit crash/restart result.
- Leaves the current non-parallel production service running at the end.

- [ ] **Step 1: Snapshot current production state before any stop**

Run over SSH: record `docker compose ps`, current image ID, container restart count, current Git/source checksum, proxy reachability, active download count, and SQLite WAL state. Store the snapshot under `state/parallel_validation/<run-id>-preflight.txt`.

Expected: current production is healthy enough to restore; no source or config changes yet.

- [ ] **Step 2: Copy and build candidate while production remains running**

Copy only changed source files into `app-src`, then build a separately tagged image such as `telegram_media_downloader_us:parallel-candidate-<commit>`. Do not recreate or restart the service.

Expected: production container ID and restart count remain unchanged.

- [ ] **Step 3: Run read-only dry selection while production remains running**

Invoke the candidate image with `--dry-select`, mounting config, records, and downloads read-only and validation output read-write.

Expected: exactly two `<10 MiB`, two `10-200 MiB`, one `200 MiB-1 GiB`, and one `>1 GiB` sample, or an explicit bucket gap. Do not stop production if six valid candidates cannot be selected.

- [ ] **Step 4: Stop production immediately before Telegram validation**

Run: `docker compose stop telegram_media_downloader`

Expected: production exits cleanly and no second process holds the session.

- [ ] **Step 5: Run crash injection on the sample larger than 200 MiB**

Start the candidate with the production session mounted read-write, config/state/downloads read-only, and validation temp/report directories read-write. Use `--abort-after-chunks` and confirm exit code 75, then rerun with `--resume-run` and confirm previously verified chunks were not requested again.

Expected: resumed candidate matches its HDD baseline whole SHA-256 and all Telegram hash ranges.

- [ ] **Step 6: Run all six validation samples**

Expected: report contains six valid samples, baseline and candidate SHA-256 values, both remote verification results, no unexplained mismatch, measured throughput, and retry counts. Eligibility is true only for six of six candidate passes.

- [ ] **Step 7: Restore current production in a finally-style operation**

Recreate/start the service using the original non-parallel image and unchanged production config whether Task 8 Step 5 or Step 6 passed, failed, or was interrupted.

Expected: web UI responds on `http://192.168.79.37:5875`, consumer scans resume, restart count is stable, and `parallel_download_enabled` remains absent/false.

- [ ] **Step 8: Report evidence without enabling the feature**

Summarize each sample's bucket, IDs, size, baseline SHA-256, candidate SHA-256, Telegram verification, throughput, retries, and decision. Include crash recovery evidence and restored production container status. Do not change the production feature flag; rollout is a separate user decision.
