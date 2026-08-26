"""Tests for resumable single-file parallel downloads."""

import asyncio
import collections
import contextlib
import hashlib
import os
import sqlite3
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from pyrogram import raw
from pyrogram.errors import FloodWait, Unauthorized
from pyrogram.file_id import FileId, FileType

from module.file_media_session_pool import (
    CoordinatorConfig,
    FilePoolConfig,
    MediaTransferCoordinator,
)
from module.media_session_pool import ReusableMediaSession
from module.parallel_downloader import (
    CHUNK_SIZE,
    ChunkSpec,
    DownloadManifest,
    HashMismatch,
    IncompleteRange,
    InjectedAbort,
    KurigramRangeSource,
    MediaIdentity,
    MediaIdentityChanged,
    ParallelDownloader,
    RemoteHash,
    RemoteHashUnavailable,
    _SessionBoundClient,
    collect_remote_hashes,
    plan_chunks,
    plan_missing_stripes,
    retry_telegram,
    split_missing_runs,
    verify_file_hashes,
    write_all_at,
)


class ParallelDownloadPlanningTest(unittest.TestCase):
    """Range plans must cover the declared file exactly once."""

    def test_plan_chunks_keeps_exact_final_length(self):
        chunks = plan_chunks(10, 4)

        self.assertEqual(
            [ChunkSpec(0, 4), ChunkSpec(4, 4), ChunkSpec(8, 2)],
            chunks,
        )

    def test_split_missing_runs_never_duplicates_offsets(self):
        chunks = plan_chunks(20, 4)

        runs = split_missing_runs(chunks, {4, 12}, workers=2)

        offsets = [chunk.offset for run in runs for chunk in run]
        self.assertEqual([0, 8, 16], sorted(offsets))
        self.assertEqual(len(offsets), len(set(offsets)))
        self.assertLessEqual(len(runs), 2)

    def test_plan_supports_files_larger_than_two_gibibytes(self):
        size = 2 * 1024**3 + 17

        chunks = plan_chunks(size, 1024 * 1024)

        self.assertEqual(size, sum(chunk.length for chunk in chunks))
        self.assertEqual(17, chunks[-1].length)

    def test_plan_rejects_non_positive_sizes(self):
        for file_size, chunk_size in ((0, 4), (4, 0), (-1, 4), (4, -1)):
            with self.subTest(file_size=file_size, chunk_size=chunk_size):
                with self.assertRaises(ValueError):
                    plan_chunks(file_size, chunk_size)


class StripePlanningTest(unittest.TestCase):
    """Logical stripes group exact 1 MiB chunks without crossing gaps."""

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

        self.assertEqual(
            [5 * CHUNK_SIZE, 123],
            [sum(chunk.length for chunk in stripe) for stripe in stripes],
        )

    def test_rejects_non_mib_stripe_sizes(self):
        chunks = plan_chunks(CHUNK_SIZE)

        for stripe_size in (0, CHUNK_SIZE - 1, CHUNK_SIZE + 1):
            with self.subTest(stripe_size=stripe_size):
                with self.assertRaises(ValueError):
                    plan_missing_stripes(chunks, set(), stripe_size)


class PooledConstructorTest(unittest.TestCase):
    def test_pooled_mode_requires_exactly_five_mib_stripes(self):
        for stripe_size in (CHUNK_SIZE, 10 * CHUNK_SIZE):
            with self.subTest(stripe_size=stripe_size):
                with self.assertRaisesRegex(ValueError, "exactly 5 MiB"):
                    ParallelDownloader(
                        object(),
                        pool=object(),
                        stripe_size=stripe_size,
                    )

        downloader = ParallelDownloader(
            object(),
            pool=object(),
            stripe_size=5 * CHUNK_SIZE,
        )
        self.assertEqual(5 * CHUNK_SIZE, downloader.stripe_size)

    def test_global_pool_and_per_file_coordinator_are_mutually_exclusive(self):
        with self.assertRaisesRegex(ValueError, "pool and coordinator"):
            ParallelDownloader(
                object(),
                pool=object(),
                coordinator=object(),
            )

    def test_per_file_mode_requires_one_mib_manifest_chunks(self):
        with self.assertRaisesRegex(ValueError, "per-file.*1 MiB"):
            ParallelDownloader(
                object(),
                coordinator=object(),
                chunk_size=4,
            )


class DownloadManifestTest(unittest.TestCase):
    """Persisted chunk state is trusted only after checking SSD bytes."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.part_path = self.root / "sample.part"
        self.manifest = DownloadManifest(self.root / "sample.sqlite3")
        self.identity = MediaIdentity(
            chat_id="-100123",
            message_id=42,
            media_id=9001,
            dc_id=5,
            file_unique_id="stable-file",
            file_size=8,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_manifest_rejects_changed_media_identity(self):
        changed = MediaIdentity(
            chat_id=self.identity.chat_id,
            message_id=self.identity.message_id,
            media_id=9002,
            dc_id=self.identity.dc_id,
            file_unique_id=self.identity.file_unique_id,
            file_size=self.identity.file_size,
        )
        self.manifest.prepare(self.identity, file_size=8, chunk_size=4)

        with self.assertRaises(MediaIdentityChanged):
            self.manifest.prepare(changed, file_size=8, chunk_size=4)

    def test_recovery_keeps_chunk_with_matching_local_digest(self):
        self.part_path.write_bytes(b"abcdefgh")
        self.manifest.prepare(self.identity, file_size=8, chunk_size=4)
        digest = hashlib.sha256(b"abcd").hexdigest()
        self.manifest.mark_complete(ChunkSpec(0, 4), digest, attempts=1)

        valid = self.manifest.revalidate_completed(self.part_path)

        self.assertEqual({0: (4, digest)}, valid)
        self.assertEqual({0: (4, digest)}, self.manifest.completed_chunks())

    def test_recovery_drops_chunk_whose_local_digest_changed(self):
        self.part_path.write_bytes(b"abcdefgh")
        self.manifest.prepare(self.identity, file_size=8, chunk_size=4)
        digest = hashlib.sha256(b"abcd").hexdigest()
        self.manifest.mark_complete(ChunkSpec(0, 4), digest, attempts=1)
        self.part_path.write_bytes(b"xbcdefgh")

        valid = self.manifest.revalidate_completed(self.part_path)

        self.assertEqual({}, valid)
        self.assertEqual({}, self.manifest.completed_chunks())

    def test_recovery_drops_short_chunk(self):
        self.part_path.write_bytes(b"ab")
        self.manifest.prepare(self.identity, file_size=8, chunk_size=4)
        digest = hashlib.sha256(b"abcd").hexdigest()
        self.manifest.mark_complete(ChunkSpec(0, 4), digest, attempts=2)

        self.assertEqual({}, self.manifest.revalidate_completed(self.part_path))

    def test_every_manifest_connection_is_closed_including_chunk_commits(self):
        connections = []
        original_connect = DownloadManifest._connect

        def tracked_connect(manifest):
            connection = original_connect(manifest)
            connections.append(connection)
            return connection

        manifest_path = self.root / "tracked.sqlite3"
        self.part_path.write_bytes(b"xbcdefgh")
        digest = hashlib.sha256(b"abcd").hexdigest()
        with mock.patch.object(DownloadManifest, "_connect", tracked_connect):
            manifest = DownloadManifest(manifest_path)
            manifest.prepare(self.identity, file_size=8, chunk_size=4)
            manifest.mark_complete(ChunkSpec(0, 4), digest, attempts=1)
            manifest.completed_chunks()
            manifest.revalidate_completed(self.part_path)

        self.assertGreaterEqual(len(connections), 6)
        for connection in connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")


class RetryPolicyTest(unittest.IsolatedAsyncioTestCase):
    """Transient Telegram failures retry without hiding cancellation."""

    async def test_retries_timeout_wrapped_in_exception_cause(self):
        calls = 0
        sleeps = []

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                try:
                    raise TimeoutError("proxy timed out")
                except TimeoutError as error:
                    raise RuntimeError("media session failed") from error
            return b"ok"

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        result = await retry_telegram(
            operation,
            max_attempts=3,
            sleep=fake_sleep,
        )

        self.assertEqual(b"ok", result)
        self.assertEqual(2, calls)
        self.assertEqual([1], sleeps)

    async def test_does_not_retry_cancellation(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await retry_telegram(operation, max_attempts=3)

        self.assertEqual(1, calls)

    async def test_flood_wait_uses_server_delay(self):
        calls = 0
        sleeps = []

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise FloodWait(value=17)
            return "ok"

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        result = await retry_telegram(operation, max_attempts=3, sleep=fake_sleep)

        self.assertEqual("ok", result)
        self.assertEqual(2, calls)
        self.assertEqual([17], sleeps)

    async def test_timeout_retry_count_is_bounded(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise TimeoutError("still unavailable")

        async def fake_sleep(_seconds):
            return None

        with self.assertRaises(TimeoutError):
            await retry_telegram(operation, max_attempts=3, sleep=fake_sleep)

        self.assertEqual(3, calls)


class FakeHashSource:
    """Return prearranged remote hash batches."""

    def __init__(self, batches):
        self.batches = list(batches)
        self.calls = 0

    async def get_hashes(self, _offset):
        index = min(self.calls, len(self.batches) - 1)
        self.calls += 1
        return self.batches[index]


def remote_hash(offset, data, limit=None):
    return RemoteHash(
        offset=offset,
        limit=len(data) if limit is None else limit,
        digest=hashlib.sha256(data).digest(),
    )


class RemoteHashCollectionTest(unittest.IsolatedAsyncioTestCase):
    """Master-DC hash enumeration must cover the file and always progress."""

    async def test_complete_first_batch_does_not_request_repeated_final_batch(self):
        item = remote_hash(0, b"abcd")
        source = FakeHashSource([[item], [item]])

        hashes = await collect_remote_hashes(source, file_size=4)

        self.assertEqual([(0, 4)], [(item.offset, item.limit) for item in hashes])
        self.assertEqual(1, source.calls)

    async def test_repeated_incomplete_batch_fails_without_looping(self):
        item = remote_hash(0, b"abcd")
        source = FakeHashSource([[item], [item]])

        with self.assertRaises(RemoteHashUnavailable):
            await collect_remote_hashes(source, file_size=8)

        self.assertEqual(2, source.calls)

    async def test_hash_fetch_retries_wrapped_timeout(self):
        item = remote_hash(0, b"abcd")

        class TimeoutThenHashSource:
            def __init__(self):
                self.calls = 0

            async def get_hashes(self, _offset):
                self.calls += 1
                if self.calls == 1:
                    try:
                        raise TimeoutError("proxy")
                    except TimeoutError as error:
                        raise RuntimeError("get hashes") from error
                return [item]

        source = TimeoutThenHashSource()

        hashes = await collect_remote_hashes(
            source,
            file_size=4,
            sleep=lambda _seconds: asyncio.sleep(0),
        )

        self.assertEqual([item], hashes)
        self.assertEqual(2, source.calls)


class FakeKurigramClient:
    """Small client double that preserves async-generator behavior."""

    def __init__(self, chunks=None, hashes=None):
        self.chunks = list(chunks or [])
        self.hashes = list(hashes or [])
        self.sessions_lock = asyncio.Lock()
        self.get_file_calls = []
        self.get_session_calls = []
        self.invocations = []

    async def get_file(self, file_id, file_size, limit=0, offset=0):
        self.get_file_calls.append((file_id, file_size, limit, offset))
        for chunk in self.chunks:
            yield chunk

    async def get_session(self, dc_id, is_media=False):
        self.get_session_calls.append((dc_id, is_media))
        return self

    async def invoke(self, request, sleep_threshold=None):
        self.invocations.append((request, sleep_threshold))
        return self.hashes


class BlockingCloseKurigramClient(FakeKurigramClient):
    def __init__(self, lifecycle):
        super().__init__()
        self.lifecycle = lifecycle
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()

    async def get_file(self, file_id, file_size, limit=0, offset=0):
        self.get_file_calls.append((file_id, file_size, limit, offset))
        try:
            yield b"x" * CHUNK_SIZE
        finally:
            self.close_started.set()
            await self.allow_close.wait()
            self.lifecycle.append("stream_closed")


class SessionBoundClientTest(unittest.IsolatedAsyncioTestCase):
    """A range adapter substitutes only its leased media session."""

    class RoutingClient:
        def __init__(self):
            self.calls = []
            self.delegated_session = object()

        async def get_session(
            self,
            dc_id=None,
            is_media=False,
            is_cdn=False,
            **kwargs,
        ):
            self.calls.append((dc_id, is_media, is_cdn, kwargs))
            return self.delegated_session

    async def test_uses_leased_session_for_media_request(self):
        client = self.RoutingClient()
        leased_session = object()
        bound = _SessionBoundClient(client, leased_session)

        result = await bound.get_session(4, is_media=True)

        self.assertIs(leased_session, result)
        self.assertEqual([], client.calls)

    async def test_delegates_cdn_session_request(self):
        client = self.RoutingClient()
        bound = _SessionBoundClient(client, object())

        result = await bound.get_session(
            4,
            is_cdn=True,
            temporary=True,
        )

        self.assertIs(client.delegated_session, result)
        self.assertEqual(
            [(4, False, True, {"temporary": True})],
            client.calls,
        )

    async def test_uses_worker_owned_reusable_cdn_session(self):
        class ReusableWorkerSession:
            def __init__(self):
                self.calls = []
                self.cdn_session = object()

            async def get_cdn_session(self, dc_id=None, **kwargs):
                self.calls.append((dc_id, kwargs))
                return self.cdn_session

        client = self.RoutingClient()
        worker_session = ReusableWorkerSession()
        bound = _SessionBoundClient(client, worker_session)

        result = await bound.get_session(
            5,
            is_cdn=True,
            temporary=True,
        )

        self.assertIs(worker_session.cdn_session, result)
        self.assertEqual(
            [(5, {"is_media": False, "is_cdn": True, "temporary": True})],
            worker_session.calls,
        )
        self.assertEqual([], client.calls)

    def test_delegates_regular_attributes(self):
        client = self.RoutingClient()
        client.loop = "event-loop"

        bound = _SessionBoundClient(client, object())

        self.assertEqual("event-loop", bound.loop)


class FakeMediaSession:
    """Stoppable identity used by the temporary media-session tests."""

    def __init__(self, number):
        self.number = number
        self.stop_calls = 0

    async def stop(self):
        self.stop_calls += 1


class PoolKurigramClient(FakeKurigramClient):
    """Client double that exposes overlapping temporary-session creation."""

    def __init__(self):
        super().__init__()
        self.temporary_sessions = []
        self.range_session_numbers = []
        self.creation_active = 0
        self.max_creation_active = 0
        self.fail_temporary_session_number = None
        self.fail_offsets = set()
        self.temporary_session_kwargs = []

    async def get_session(
        self,
        dc_id=None,
        is_media=False,
        is_cdn=False,
        temporary=False,
        **_kwargs,
    ):
        self.get_session_calls.append((dc_id, is_media, is_cdn, temporary))
        if not (is_media and temporary):
            return self

        self.temporary_session_kwargs.append(_kwargs)

        self.creation_active += 1
        self.max_creation_active = max(
            self.max_creation_active,
            self.creation_active,
        )
        number = len(self.temporary_sessions) + 1
        try:
            await asyncio.sleep(0.01)
            if number == self.fail_temporary_session_number:
                raise RuntimeError("temporary media session failed")
            session = FakeMediaSession(number)
            self.temporary_sessions.append(session)
            return session
        finally:
            self.creation_active -= 1

    async def get_file(self, file_id, file_size, limit=0, offset=0):
        session = await self.get_session(file_id.dc_id, is_media=True)
        self.range_session_numbers.append(session.number)
        self.get_file_calls.append((file_id, file_size, limit, offset))
        await asyncio.sleep(0.01)
        if offset in self.fail_offsets:
            self.fail_offsets.remove(offset)
            raise RuntimeError("range failed")
        yield bytes([offset + 1]) * CHUNK_SIZE


class KurigramSessionPoolTest(unittest.IsolatedAsyncioTestCase):
    """Prepared range workers use distinct, safely managed media sessions."""

    def setUp(self):
        decoded_file_id = FileId(
            file_type=FileType.DOCUMENT,
            dc_id=4,
            media_id=123456,
            access_hash=987654,
            file_reference=b"reference",
        )
        self.encoded_file_id = decoded_file_id.encode()

    def new_source(self, client, file_size=2 * CHUNK_SIZE):
        return KurigramRangeSource(
            client,
            self.encoded_file_id,
            file_size,
        )

    async def consume(self, source, offset):
        return [chunk async for chunk in source.iter_range(offset, CHUNK_SIZE)]

    async def test_prepare_creates_sessions_sequentially(self):
        client = PoolKurigramClient()
        source = self.new_source(client)

        await source.prepare(2)

        self.assertEqual(2, len(client.temporary_sessions))
        self.assertEqual(1, client.max_creation_active)

    async def test_prepare_reuses_existing_dc_authorization(self):
        client = PoolKurigramClient()
        source = self.new_source(client)

        await source.prepare(2)

        self.assertTrue(
            all(
                kwargs["export_authorization"] is False
                for kwargs in client.temporary_session_kwargs
            )
        )

    async def test_concurrent_sources_share_creation_lock(self):
        client = PoolKurigramClient()
        first = self.new_source(client)
        second = self.new_source(client)

        await asyncio.gather(first.prepare(2), second.prepare(2))

        self.assertEqual(4, len(client.temporary_sessions))
        self.assertEqual(1, client.max_creation_active)

    async def test_concurrent_ranges_use_distinct_sessions(self):
        client = PoolKurigramClient()
        source = self.new_source(client)
        await source.prepare(2)

        first, second = await asyncio.gather(
            self.consume(source, 0),
            self.consume(source, CHUNK_SIZE),
        )

        self.assertEqual(CHUNK_SIZE, len(first[0]))
        self.assertEqual(CHUNK_SIZE, len(second[0]))
        self.assertEqual(2, len(set(client.range_session_numbers)))

    async def test_range_error_returns_session_to_pool(self):
        client = PoolKurigramClient()
        client.fail_offsets.add(0)
        source = self.new_source(client)
        await source.prepare(2)

        with self.assertRaisesRegex(RuntimeError, "range failed"):
            await self.consume(source, 0)

        self.assertEqual(2, source.available_session_count)
        result = await self.consume(source, CHUNK_SIZE)
        self.assertEqual(CHUNK_SIZE, len(result[0]))

    async def test_prepare_failure_closes_created_sessions(self):
        client = PoolKurigramClient()
        client.fail_temporary_session_number = 2
        source = self.new_source(client)

        with self.assertRaisesRegex(
            RuntimeError,
            "temporary media session failed",
        ):
            await source.prepare(2)

        self.assertEqual(1, len(client.temporary_sessions))
        self.assertEqual(1, client.temporary_sessions[0].stop_calls)

    async def test_close_stops_all_temporary_sessions_once(self):
        client = PoolKurigramClient()
        source = self.new_source(client)
        await source.prepare(2)

        await source.close()
        await source.close()

        self.assertTrue(
            all(session.stop_calls == 1 for session in client.temporary_sessions)
        )


class KurigramRangeSourceTest(unittest.IsolatedAsyncioTestCase):
    """The adapter delegates contiguous ranges and raw hashes to Kurigram."""

    def setUp(self):
        self.decoded_file_id = FileId(
            file_type=FileType.DOCUMENT,
            dc_id=4,
            media_id=123456,
            access_hash=987654,
            file_reference=b"reference",
        )
        self.encoded_file_id = self.decoded_file_id.encode()

    async def test_requests_one_contiguous_kurigram_range(self):
        client = FakeKurigramClient(chunks=[b"a" * CHUNK_SIZE, b"b" * CHUNK_SIZE])
        source = KurigramRangeSource(client, self.encoded_file_id, 4 * CHUNK_SIZE)

        chunks = [
            chunk
            async for chunk in source.iter_range(
                2 * CHUNK_SIZE,
                2 * CHUNK_SIZE,
            )
        ]

        self.assertEqual([b"a" * CHUNK_SIZE, b"b" * CHUNK_SIZE], chunks)
        self.assertEqual(1, len(client.get_file_calls))
        decoded, file_size, limit, offset = client.get_file_calls[0]
        self.assertEqual(self.decoded_file_id.media_id, decoded.media_id)
        self.assertEqual(4 * CHUNK_SIZE, file_size)
        self.assertEqual((2, 2), (limit, offset))

    async def test_requests_range_on_supplied_media_session(self):
        class SessionRecordingClient(FakeKurigramClient):
            def __init__(self):
                super().__init__(chunks=[b"a" * CHUNK_SIZE])
                self.range_sessions = []

            async def get_file(self, file_id, file_size, limit=0, offset=0):
                session = await self.get_session(file_id.dc_id, is_media=True)
                self.range_sessions.append(session)
                async for chunk in FakeKurigramClient.get_file(
                    self,
                    file_id,
                    file_size,
                    limit=limit,
                    offset=offset,
                ):
                    yield chunk

        client = SessionRecordingClient()
        source = KurigramRangeSource(client, self.encoded_file_id, CHUNK_SIZE)
        leased_session = object()

        chunks = [
            chunk
            async for chunk in source.iter_range_on_session(
                leased_session,
                0,
                CHUNK_SIZE,
            )
        ]

        self.assertEqual([b"a" * CHUNK_SIZE], chunks)
        self.assertEqual([leased_session], client.range_sessions)
        self.assertEqual([], client.get_session_calls)

    async def test_reuses_cdn_session_for_consecutive_ranges_on_same_worker(self):
        class CdnRedirectClient(FakeKurigramClient):
            def __init__(self):
                super().__init__()
                self.cdn_sessions = []

            async def get_session(
                self,
                dc_id=None,
                is_media=False,
                is_cdn=False,
                temporary=False,
                **_kwargs,
            ):
                if not is_cdn:
                    raise AssertionError("media session must stay worker-bound")
                self.assert_temporary = temporary
                session = FakeMediaSession(len(self.cdn_sessions))
                self.cdn_sessions.append(session)
                return session

            async def get_file(self, file_id, file_size, limit=0, offset=0):
                cdn_session = await self.get_session(
                    file_id.dc_id,
                    is_cdn=True,
                    temporary=True,
                )
                try:
                    yield bytes([offset + 1]) * CHUNK_SIZE
                finally:
                    await cdn_session.stop()

        client = CdnRedirectClient()
        source = KurigramRangeSource(
            client,
            self.encoded_file_id,
            2 * CHUNK_SIZE,
        )
        media_session = FakeMediaSession("media")
        worker_session = ReusableMediaSession(client, media_session)

        first = [
            chunk
            async for chunk in source.iter_range_on_session(
                worker_session,
                0,
                CHUNK_SIZE,
            )
        ]
        second = [
            chunk
            async for chunk in source.iter_range_on_session(
                worker_session,
                CHUNK_SIZE,
                CHUNK_SIZE,
            )
        ]

        self.assertEqual(CHUNK_SIZE, len(first[0]))
        self.assertEqual(CHUNK_SIZE, len(second[0]))
        self.assertEqual(1, len(client.cdn_sessions))
        self.assertEqual(0, client.cdn_sessions[0].stop_calls)

        await worker_session.stop()
        await worker_session.stop()

        self.assertEqual(1, client.cdn_sessions[0].stop_calls)
        self.assertEqual(1, media_session.stop_calls)

    async def test_short_chunk_before_requested_range_end_fails(self):
        client = FakeKurigramClient(chunks=[b"a" * (CHUNK_SIZE // 2)])
        source = KurigramRangeSource(client, self.encoded_file_id, 4 * CHUNK_SIZE)

        with self.assertRaises(IncompleteRange):
            _ = [chunk async for chunk in source.iter_range(0, 2 * CHUNK_SIZE)]

    async def test_closing_validated_range_closes_underlying_stream_first(self):
        lifecycle = []
        client = BlockingCloseKurigramClient(lifecycle)
        source = KurigramRangeSource(client, self.encoded_file_id, CHUNK_SIZE)
        iterator = source.iter_range_on_session(object(), 0, CHUNK_SIZE)

        self.assertEqual(b"x" * CHUNK_SIZE, await anext(iterator))
        close_task = asyncio.create_task(iterator.aclose())
        try:
            await asyncio.wait_for(client.close_started.wait(), 1)
            self.assertFalse(close_task.done())
            self.assertEqual([], lifecycle)
        finally:
            client.allow_close.set()
            await asyncio.gather(close_task, return_exceptions=True)

        self.assertEqual(["stream_closed"], lifecycle)

    async def test_maps_upload_get_file_hashes_response(self):
        digest = hashlib.sha256(b"abcd").digest()
        client = FakeKurigramClient(
            hashes=[raw.types.FileHash(offset=0, limit=4, hash=digest)]
        )
        source = KurigramRangeSource(client, self.encoded_file_id, 4)

        hashes = await source.get_hashes(0)

        self.assertEqual([RemoteHash(0, 4, digest)], hashes)
        self.assertEqual([(4, True)], client.get_session_calls)
        request, sleep_threshold = client.invocations[0]
        self.assertIsInstance(request, raw.functions.upload.GetFileHashes)
        self.assertEqual(0, request.offset)
        self.assertEqual(30, sleep_threshold)

    async def test_concurrent_ranges_warm_one_media_session_before_download(self):
        class ColdMediaSessionClient(FakeKurigramClient):
            def __init__(self):
                super().__init__()
                self.session_ready = False
                self.session_creation_active = False
                self.session_creation_count = 0

            async def get_session(self, dc_id, is_media=False):
                self.get_session_calls.append((dc_id, is_media))
                if self.session_ready:
                    return self
                if self.session_creation_active:
                    raise RuntimeError("concurrent media session creation")
                self.session_creation_active = True
                self.session_creation_count += 1
                await asyncio.sleep(0.01)
                self.session_ready = True
                self.session_creation_active = False
                return self

            async def get_file(self, file_id, file_size, limit=0, offset=0):
                await self.get_session(file_id.dc_id, is_media=True)
                self.get_file_calls.append((file_id, file_size, limit, offset))
                yield bytes([offset + 1]) * CHUNK_SIZE

        client = ColdMediaSessionClient()
        source = KurigramRangeSource(
            client,
            self.encoded_file_id,
            2 * CHUNK_SIZE,
        )

        async def consume(offset):
            return [chunk async for chunk in source.iter_range(offset, CHUNK_SIZE)]

        first, second = await asyncio.gather(
            consume(0),
            consume(CHUNK_SIZE),
        )

        self.assertEqual(CHUNK_SIZE, len(first[0]))
        self.assertEqual(CHUNK_SIZE, len(second[0]))
        self.assertEqual(1, client.session_creation_count)


def identity_for(file_size):
    return MediaIdentity(
        chat_id="-100555",
        message_id=77,
        media_id=111,
        dc_id=2,
        file_unique_id="parallel-test",
        file_size=file_size,
    )


class MemoryRangeSource:
    """Serve deterministic bytes and Telegram-style hashes from memory."""

    def __init__(self, data, chunk_size, delays=None):
        self.data = data
        self.chunk_size = chunk_size
        self.delays = delays or {}
        self.range_calls = []

    async def iter_range(self, start_offset, expected_length):
        self.range_calls.append((start_offset, expected_length))
        await asyncio.sleep(self.delays.get(start_offset, 0))
        end = start_offset + expected_length
        for offset in range(start_offset, end, self.chunk_size):
            yield self.data[offset : min(offset + self.chunk_size, end)]

    async def get_hashes(self, offset):
        if offset >= len(self.data):
            return []
        return [remote_hash(offset, self.data[offset:])]


class FakeLease:
    """Observable pool lease with transport and fatal release marking."""

    def __init__(self, pool, session):
        self.pool = pool
        self.session = session
        self.failure = ""
        self.released = False

    def mark_transport_failure(self):
        if self.failure != "fatal":
            self.failure = "transport"

    def mark_unhealthy(self):
        self.failure = "fatal"

    async def release(self):
        if self.released:
            return
        self.released = True
        self.pool.released.append((self.session, self.failure))
        if self.failure:
            self.pool.unhealthy_sessions.append(self.session)
        self.pool.active_leases -= 1
        self.pool.lifecycle.append("lease_released")

    async def __aenter__(self):
        return self

    async def __aexit__(self, _type, _value, _traceback):
        await self.release()


class FakeLeasePool:
    """Rotate fake sessions while recording the complete pool contract."""

    def __init__(self, sessions):
        self.sessions = collections.deque(sessions)
        self.transfer_ids = []
        self.closed_transfers = []
        self.unhealthy_sessions = []
        self.released = []
        self.active_leases = 0
        self.committed = 0
        self.committed_observations = []
        self.retries = 0
        self.attempts = 0
        self.pauses = []
        self.manifest_path = None
        self.lifecycle = []

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
        if self.manifest_path is not None:
            records = DownloadManifest(self.manifest_path).completed_chunks()
            durable_bytes = sum(length for length, _digest in records.values())
            self.committed_observations.append(
                (self.committed + byte_count, durable_bytes)
            )
        self.committed += byte_count

    def record_retry(self):
        self.retries += 1

    def record_stripe_attempt(self):
        self.attempts += 1

    def pause_dc(self, dc_id, seconds):
        self.pauses.append((dc_id, seconds))


class SessionAwareMemorySource(MemoryRangeSource):
    """Serve ranges while recording the leased session for each attempt."""

    def __init__(self, data):
        super().__init__(data, CHUNK_SIZE)
        self.sessions_seen = []
        self.session_ranges = []

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        async for chunk in MemoryRangeSource.iter_range(
            self,
            start_offset,
            expected_length,
        ):
            yield chunk


class PartialFailingSessionMemorySource(SessionAwareMemorySource):
    """Commit one chunk on a bad lease, then expose it before retry I/O."""

    def __init__(self, data, bad_session, manifest_path):
        super().__init__(data)
        self.bad_session = bad_session
        self.manifest_path = manifest_path
        self.failed = False
        self.durable_offsets_before_retry = set()

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        if session == self.bad_session and not self.failed:
            self.failed = True
            yield self.data[start_offset : start_offset + CHUNK_SIZE]
            raise asyncio.TimeoutError()

        self.durable_offsets_before_retry = set(
            DownloadManifest(self.manifest_path).completed_chunks()
        )
        async for chunk in MemoryRangeSource.iter_range(
            self,
            start_offset,
            expected_length,
        ):
            yield chunk


class TwoChunkTimeoutSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data):
        super().__init__(data)
        self.failed = False

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        if not self.failed:
            self.failed = True
            yield self.data[start_offset : start_offset + CHUNK_SIZE]
            yield self.data[start_offset + CHUNK_SIZE : start_offset + 2 * CHUNK_SIZE]
            raise asyncio.TimeoutError()
        async for chunk in MemoryRangeSource.iter_range(
            self,
            start_offset,
            expected_length,
        ):
            yield chunk


class BlockingSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data):
        super().__init__(data)
        self.started = asyncio.Event()

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        self.started.set()
        await asyncio.Event().wait()
        if False:
            yield b""


class FloodWaitSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data):
        super().__init__(data)
        self.failed = False

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        if not self.failed:
            self.failed = True
            raise FloodWait(value=7)
        async for chunk in MemoryRangeSource.iter_range(
            self,
            start_offset,
            expected_length,
        ):
            yield chunk


class ImmediateFloodWaitSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data):
        super().__init__(data)
        self.failed = False

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        if not self.failed:
            self.failed = True
            raise FloodWait(value=0)
        async for chunk in MemoryRangeSource.iter_range(
            self,
            start_offset,
            expected_length,
        ):
            yield chunk


class ErrorSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data, error):
        super().__init__(data)
        self.error = error

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        raise self.error
        if False:
            yield b""


class FinalChunkThenErrorSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data, error):
        super().__init__(data)
        self.error = error

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        yield self.data[start_offset : start_offset + expected_length]
        raise self.error


class BlockingCloseSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data, lifecycle):
        super().__init__(data)
        self.lifecycle = lifecycle
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()

    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        try:
            yield self.data[start_offset : start_offset + expected_length]
        finally:
            self.close_started.set()
            await self.allow_close.wait()
            self.lifecycle.append("stream_closed")


class FailingCloseSessionMemorySource(SessionAwareMemorySource):
    async def iter_range_on_session(self, session, start_offset, expected_length):
        self.sessions_seen.append(session)
        self.session_ranges.append((session, start_offset, expected_length))
        try:
            yield self.data[start_offset : start_offset + expected_length]
        finally:
            raise RuntimeError("iterator close failed")


class LifecycleSessionMemorySource(SessionAwareMemorySource):
    def __init__(self, data):
        super().__init__(data)
        self.lifecycle = []

    async def prepare(self, worker_count):
        self.lifecycle.append(("prepare", worker_count))

    async def close(self):
        self.lifecycle.append(("close",))


class BlockingAcquirePool(FakeLeasePool):
    def __init__(self, sessions):
        super().__init__(sessions)
        self.acquire_started = asyncio.Event()
        self.acquire_allowed = asyncio.Event()

    async def acquire(self, dc_id, transfer_id):
        self.acquire_started.set()
        await self.acquire_allowed.wait()
        return await super().acquire(dc_id, transfer_id)


class FakeMediaSession:
    def __init__(self, number):
        self.number = number
        self.stop_calls = 0

    async def stop(self):
        self.stop_calls += 1


class FakeMediaSessionFactory:
    def __init__(self):
        self.sessions = []

    async def __call__(self, _dc_id):
        session = FakeMediaSession(len(self.sessions))
        self.sessions.append(session)
        return session


async def no_sleep(_seconds):
    return None


class PooledParallelDownloaderTest(unittest.IsolatedAsyncioTestCase):
    """Logical stripes execute on fresh, fairly registered pool leases."""

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.part_path = Path(self.temp_dir.name) / "pooled.part"

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    async def test_each_stripe_uses_a_fair_pool_lease(self):
        size = 6 * CHUNK_SIZE + 123
        data = (bytes(range(251)) * ((size + 250) // 251))[:size]
        source = SessionAwareMemorySource(data)
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
        self.assertEqual({"session-a", "session-b"}, set(source.sessions_seen))
        self.assertEqual(["file-a"], pool.transfer_ids)
        self.assertEqual(["file-a"], pool.closed_transfers)
        self.assertEqual(2, pool.attempts)
        self.assertEqual(len(data), pool.committed)
        self.assertEqual(data, self.part_path.read_bytes())

    async def test_retry_keeps_durable_chunks_and_uses_a_fresh_lease(self):
        data = b"x" * (5 * CHUNK_SIZE)
        manifest_path = f"{self.part_path}.manifest.sqlite3"
        source = PartialFailingSessionMemorySource(
            data,
            bad_session="bad",
            manifest_path=manifest_path,
        )
        pool = FakeLeasePool(["bad", "good"])
        pool.manifest_path = manifest_path
        downloader = ParallelDownloader(
            source,
            pool=pool,
            stripe_size=5 * CHUNK_SIZE,
            transfer_id="retry-file",
            sleep=no_sleep,
        )

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertEqual(
            [
                ("bad", 0, 5 * CHUNK_SIZE),
                ("good", CHUNK_SIZE, 4 * CHUNK_SIZE),
            ],
            source.session_ranges,
        )
        self.assertIn(0, source.durable_offsets_before_retry)
        self.assertEqual(("bad", "transport"), pool.released[0])
        self.assertEqual(2, pool.attempts)
        self.assertEqual(1, pool.retries)
        self.assertEqual(5 * CHUNK_SIZE, pool.committed)
        self.assertTrue(
            all(
                recorded == durable for recorded, durable in pool.committed_observations
            )
        )

    async def test_lease_waiting_is_not_a_stripe_attempt(self):
        data = b"x" * CHUNK_SIZE
        source = SessionAwareMemorySource(data)
        pool = BlockingAcquirePool(["session"])
        task = asyncio.create_task(
            ParallelDownloader(
                source,
                pool=pool,
                transfer_id="waiting-file",
            ).download(identity_for(len(data)), self.part_path)
        )
        await pool.acquire_started.wait()

        self.assertEqual(0, pool.attempts)
        self.assertEqual([], source.sessions_seen)

        pool.acquire_allowed.set()
        result = await task
        self.assertTrue(result.integrity.verified)
        self.assertEqual(1, pool.attempts)

    async def test_flood_wait_pauses_dc_and_retries_on_a_fresh_lease(self):
        data = b"x" * CHUNK_SIZE
        source = FloodWaitSessionMemorySource(data)
        pool = FakeLeasePool(["first", "second"])
        downloader = ParallelDownloader(
            source,
            pool=pool,
            transfer_id="flood-file",
            sleep=no_sleep,
        )

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertEqual(["first", "second"], source.sessions_seen)
        self.assertEqual([(2, 7)], pool.pauses)
        self.assertEqual([("first", ""), ("second", "")], pool.released)
        self.assertEqual(2, pool.attempts)
        self.assertEqual(1, pool.retries)

    async def test_cancellation_during_retry_sleep_records_no_retry(self):
        sleep_started = asyncio.Event()

        async def blocking_sleep(_seconds):
            sleep_started.set()
            await asyncio.Event().wait()

        source = ErrorSessionMemorySource(
            b"x" * CHUNK_SIZE,
            asyncio.TimeoutError(),
        )
        pool = FakeLeasePool(["session"])
        task = asyncio.create_task(
            ParallelDownloader(
                source,
                pool=pool,
                transfer_id="cancelled-backoff",
                sleep=blocking_sleep,
            ).download(identity_for(CHUNK_SIZE), self.part_path)
        )
        await asyncio.wait_for(sleep_started.wait(), 1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(1, pool.attempts)
        self.assertEqual(0, pool.retries)

    async def test_post_final_timeout_is_suppressed_without_retry(self):
        data = b"x" * CHUNK_SIZE
        source = FinalChunkThenErrorSessionMemorySource(
            data,
            asyncio.TimeoutError(),
        )
        pool = FakeLeasePool(["session"])

        result = await ParallelDownloader(
            source,
            pool=pool,
            transfer_id="complete-before-error",
            sleep=no_sleep,
        ).download(identity_for(len(data)), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertEqual(0, result.retries)
        self.assertEqual(1, pool.attempts)
        self.assertEqual(0, pool.retries)
        self.assertEqual([("session", "transport")], pool.released)

    async def test_post_final_flood_wait_pauses_dc_without_retry(self):
        data = b"x" * CHUNK_SIZE
        source = FinalChunkThenErrorSessionMemorySource(
            data,
            FloodWait(value=11),
        )
        pool = FakeLeasePool(["session"])

        result = await ParallelDownloader(
            source,
            pool=pool,
            transfer_id="complete-before-flood-wait",
            sleep=no_sleep,
        ).download(identity_for(len(data)), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertEqual([(2, 11)], pool.pauses)
        self.assertEqual(0, result.retries)
        self.assertEqual(1, pool.attempts)
        self.assertEqual(0, pool.retries)
        self.assertEqual([("session", "")], pool.released)

    async def test_post_final_unrelated_runtime_error_propagates(self):
        data = b"x" * CHUNK_SIZE
        source = FinalChunkThenErrorSessionMemorySource(
            data,
            RuntimeError("post-final callback failed"),
        )
        pool = FakeLeasePool(["session"])

        with self.assertRaisesRegex(RuntimeError, "post-final callback failed"):
            await ParallelDownloader(
                source,
                pool=pool,
                transfer_id="complete-before-runtime-error",
                sleep=no_sleep,
            ).download(identity_for(len(data)), self.part_path)

        self.assertEqual(1, pool.attempts)
        self.assertEqual(0, pool.retries)
        self.assertEqual([("session", "")], pool.released)

    async def test_authorization_loss_marks_lease_fatal(self):
        source = ErrorSessionMemorySource(b"x" * CHUNK_SIZE, Unauthorized())
        pool = FakeLeasePool(["unauthorized"])
        downloader = ParallelDownloader(
            source,
            pool=pool,
            transfer_id="auth-file",
        )

        with self.assertRaises(Unauthorized):
            await downloader.download(identity_for(CHUNK_SIZE), self.part_path)

        self.assertEqual([("unauthorized", "fatal")], pool.released)
        self.assertEqual(1, pool.attempts)
        self.assertEqual(0, pool.retries)

    async def test_fatal_error_dominates_timeout_in_exception_chain(self):
        error = RuntimeError("wrapped authorization loss")
        error.__cause__ = Unauthorized()
        error.__context__ = TimeoutError("earlier transport timeout")
        source = ErrorSessionMemorySource(b"x" * CHUNK_SIZE, error)
        pool = FakeLeasePool(["unsafe-session"])
        downloader = ParallelDownloader(
            source,
            pool=pool,
            transfer_id="wrapped-auth-file",
            sleep=no_sleep,
        )

        with self.assertRaisesRegex(RuntimeError, "wrapped authorization loss"):
            await downloader.download(identity_for(CHUNK_SIZE), self.part_path)

        self.assertEqual([("unsafe-session", "fatal")], pool.released)
        self.assertEqual(1, pool.attempts)
        self.assertEqual(0, pool.retries)

    async def test_incomplete_session_range_marks_lease_fatal(self):
        source = ErrorSessionMemorySource(
            b"x" * CHUNK_SIZE,
            IncompleteRange("short session response"),
        )
        pool = FakeLeasePool(["protocol-error"])
        downloader = ParallelDownloader(
            source,
            pool=pool,
            transfer_id="protocol-file",
        )

        with self.assertRaises(IncompleteRange):
            await downloader.download(identity_for(CHUNK_SIZE), self.part_path)

        self.assertEqual([("protocol-error", "fatal")], pool.released)
        self.assertEqual(1, pool.attempts)
        self.assertEqual(0, pool.retries)

    async def test_pooled_mode_closes_source_without_preparing_it(self):
        data = b"x" * CHUNK_SIZE
        source = LifecycleSessionMemorySource(data)
        pool = FakeLeasePool(["session"])

        result = await ParallelDownloader(
            source,
            pool=pool,
            transfer_id="lifecycle-file",
        ).download(identity_for(len(data)), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertEqual([("close",)], source.lifecycle)

    async def test_cancellation_unregisters_transfer_and_releases_leases(self):
        source = BlockingSessionMemorySource(b"x" * (6 * CHUNK_SIZE))
        pool = FakeLeasePool(["session"])
        task = asyncio.create_task(
            ParallelDownloader(
                source,
                pool=pool,
                transfer_id="cancel-file",
            ).download(identity_for(6 * CHUNK_SIZE), self.part_path)
        )
        await source.started.wait()

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(["cancel-file"], pool.closed_transfers)
        self.assertEqual(0, pool.active_leases)

    async def test_repeated_cancellation_closes_iterator_before_lease_release(self):
        lifecycle = []
        source = BlockingCloseSessionMemorySource(
            b"x" * CHUNK_SIZE,
            lifecycle,
        )
        pool = FakeLeasePool(["session"])
        pool.lifecycle = lifecycle
        progress_started = asyncio.Event()

        async def blocking_progress(_completed, _total):
            progress_started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(
            ParallelDownloader(
                source,
                pool=pool,
                transfer_id="cancel-during-progress",
            ).download(
                identity_for(CHUNK_SIZE),
                self.part_path,
                progress=blocking_progress,
            )
        )
        await asyncio.wait_for(progress_started.wait(), 1)

        task.cancel()
        try:
            await asyncio.wait_for(source.close_started.wait(), 1)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            self.assertEqual([], pool.released)
            self.assertEqual([], lifecycle)
        finally:
            source.allow_close.set()
            result = await asyncio.gather(task, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(["stream_closed", "lease_released"], lifecycle)


class PerFileParallelDownloaderTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.part_path = Path(self.temp_dir.name) / "per-file.part"
        self.factory = FakeMediaSessionFactory()
        self.coordinator = MediaTransferCoordinator(
            self.factory,
            CoordinatorConfig(max_sessions=4),
        )

    async def asyncTearDown(self):
        await self.coordinator.close()
        self.temp_dir.cleanup()

    async def test_worker_keeps_session_across_stripes_and_preserves_sha256(self):
        size = 21 * CHUNK_SIZE + 123
        data = (bytes(range(251)) * ((size + 250) // 251))[:size]
        source = SessionAwareMemorySource(data)
        downloader = ParallelDownloader(
            source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            stripe_size=5 * CHUNK_SIZE,
            transfer_id="file-a",
        )

        result = await downloader.download(identity_for(len(data)), self.part_path)

        calls_by_session = collections.Counter(source.sessions_seen)
        self.assertGreaterEqual(max(calls_by_session.values()), 2)
        self.assertEqual(4, len(self.factory.sessions))
        self.assertTrue(result.integrity.verified)
        self.assertEqual(hashlib.sha256(data).hexdigest(), result.sha256)
        self.assertEqual(data, self.part_path.read_bytes())

    async def test_timeout_resumes_only_uncommitted_tail_on_same_session(self):
        data = b"x" * (5 * CHUNK_SIZE)
        source = TwoChunkTimeoutSessionMemorySource(data)
        downloader = ParallelDownloader(
            source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            stripe_size=5 * CHUNK_SIZE,
            transfer_id="retry-file",
        )

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertEqual(
            [
                (self.factory.sessions[0], 0, 5 * CHUNK_SIZE),
                (self.factory.sessions[0], 2 * CHUNK_SIZE, 3 * CHUNK_SIZE),
            ],
            source.session_ranges,
        )
        self.assertEqual(1, result.retries)
        self.assertEqual(data, self.part_path.read_bytes())

    async def test_post_final_timeout_is_complete_without_retry(self):
        data = b"x" * CHUNK_SIZE
        source = FinalChunkThenErrorSessionMemorySource(
            data,
            asyncio.TimeoutError(),
        )
        downloader = ParallelDownloader(
            source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            transfer_id="post-final-timeout",
        )

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertEqual(0, result.retries)
        self.assertEqual(data, self.part_path.read_bytes())

    async def test_flood_wait_retries_same_stripe_on_same_session(self):
        data = b"x" * CHUNK_SIZE
        source = ImmediateFloodWaitSessionMemorySource(data)
        downloader = ParallelDownloader(
            source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            transfer_id="flood-file",
        )

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertEqual(1, result.retries)
        self.assertEqual(
            [self.factory.sessions[0], self.factory.sessions[0]],
            source.sessions_seen,
        )
        self.assertEqual(data, self.part_path.read_bytes())

    async def test_authorization_loss_fails_file_and_closes_owned_session(self):
        source = ErrorSessionMemorySource(b"x" * CHUNK_SIZE, Unauthorized())
        downloader = ParallelDownloader(
            source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            transfer_id="auth-file",
        )

        with self.assertRaises(Unauthorized):
            await downloader.download(identity_for(CHUNK_SIZE), self.part_path)

        self.assertEqual([1], [session.stop_calls for session in self.factory.sessions])
        self.assertEqual(0, self.coordinator.snapshot().used)

    async def test_injected_abort_resumes_from_durable_manifest_chunks(self):
        data = b"x" * (5 * CHUNK_SIZE)
        first_source = SessionAwareMemorySource(data)
        first = ParallelDownloader(
            first_source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            transfer_id="abort-file",
            abort_after_chunks=2,
        )

        with self.assertRaises(InjectedAbort) as raised:
            await first.download(identity_for(len(data)), self.part_path)

        self.assertTrue(raised.exception.durability.verified)
        completed = DownloadManifest(
            f"{self.part_path}.manifest.sqlite3"
        ).completed_chunks()
        self.assertEqual({0, CHUNK_SIZE}, set(completed))

        second_source = SessionAwareMemorySource(data)
        result = await ParallelDownloader(
            second_source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            transfer_id="resume-file",
        ).download(identity_for(len(data)), self.part_path)

        self.assertEqual(
            [(self.factory.sessions[-1], 2 * CHUNK_SIZE, 3 * CHUNK_SIZE)],
            second_source.session_ranges,
        )
        self.assertEqual(2, result.recovered_chunks)
        self.assertEqual(data, self.part_path.read_bytes())

    async def test_per_file_mode_closes_source_without_preparing_it(self):
        source = LifecycleSessionMemorySource(b"x" * CHUNK_SIZE)
        downloader = ParallelDownloader(
            source,
            coordinator=self.coordinator,
            file_pool_config=FilePoolConfig(max_sessions=4),
            transfer_id="lifecycle-file",
        )

        result = await downloader.download(identity_for(CHUNK_SIZE), self.part_path)

        self.assertTrue(result.integrity.verified)
        self.assertEqual([("close",)], source.lifecycle)

    async def test_cancellation_closes_stream_before_owned_session(self):
        lifecycle = []
        source = BlockingCloseSessionMemorySource(b"x" * CHUNK_SIZE, lifecycle)
        progress_started = asyncio.Event()

        async def blocking_progress(_completed, _total):
            progress_started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(
            ParallelDownloader(
                source,
                coordinator=self.coordinator,
                file_pool_config=FilePoolConfig(max_sessions=4),
                transfer_id="cancel-file",
            ).download(
                identity_for(CHUNK_SIZE),
                self.part_path,
                progress=blocking_progress,
            )
        )
        await asyncio.wait_for(progress_started.wait(), 1)

        task.cancel()
        try:
            await asyncio.wait_for(source.close_started.wait(), 1)
            self.assertEqual(0, self.factory.sessions[0].stop_calls)
        finally:
            source.allow_close.set()

        result = await asyncio.gather(task, return_exceptions=True)
        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(["stream_closed"], lifecycle)
        self.assertEqual(1, self.factory.sessions[0].stop_calls)

    async def test_iterator_close_error_does_not_hide_cancellation(self):
        source = FailingCloseSessionMemorySource(b"x" * CHUNK_SIZE)
        progress_started = asyncio.Event()

        async def blocking_progress(_completed, _total):
            progress_started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(
            ParallelDownloader(
                source,
                coordinator=self.coordinator,
                file_pool_config=FilePoolConfig(max_sessions=4),
                transfer_id="cancel-close-error",
            ).download(
                identity_for(CHUNK_SIZE),
                self.part_path,
                progress=blocking_progress,
            )
        )
        await asyncio.wait_for(progress_started.wait(), 1)

        task.cancel()
        result = await asyncio.gather(task, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(1, self.factory.sessions[0].stop_calls)


class ParallelWriterTest(unittest.IsolatedAsyncioTestCase):
    """Workers may finish out of order but never share a seek cursor."""

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.part_path = self.root / "candidate.part"

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    async def test_out_of_order_workers_produce_exact_file(self):
        data = b"abcdefghijkl"
        source = MemoryRangeSource(data, chunk_size=4, delays={0: 0.02, 8: 0})
        downloader = ParallelDownloader(source, workers=2, chunk_size=4)

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertEqual(data, self.part_path.read_bytes())
        self.assertEqual(hashlib.sha256(data).hexdigest(), result.sha256)
        self.assertEqual(2, result.workers)

    async def test_default_path_does_not_trust_stale_master_hashes(self):
        class StaleHashSource(MemoryRangeSource):
            def __init__(self, data, chunk_size):
                super().__init__(data, chunk_size)
                self.hash_calls = 0

            async def get_hashes(self, offset):
                self.hash_calls += 1
                return [remote_hash(offset, b"stale Telegram bytes")]

        data = b"abcdefghijkl"
        source = StaleHashSource(data, chunk_size=4)
        downloader = ParallelDownloader(source, workers=2, chunk_size=4)

        result = await downloader.download(identity_for(len(data)), self.part_path)

        self.assertEqual(data, self.part_path.read_bytes())
        self.assertEqual(0, source.hash_calls)
        self.assertEqual("mtproto_manifest_sha256", result.integrity.method)

    async def test_expected_target_inode_rejects_symlink_before_truncate(self):
        data = b"abcdefghijkl"
        self.part_path.write_bytes(b"reserved")
        reserved = self.part_path.stat()
        original = self.root / "reserved-candidate"
        self.part_path.rename(original)
        victim = self.root / "victim.bin"
        victim.write_bytes(b"must-not-change")
        self.part_path.symlink_to(victim)
        downloader = ParallelDownloader(
            MemoryRangeSource(data, chunk_size=4),
            workers=1,
            chunk_size=4,
        )

        with self.assertRaisesRegex(OSError, "symlink|symbolic|loop"):
            await downloader.download(
                identity_for(len(data)),
                self.part_path,
                expected_target_identity=(reserved.st_dev, reserved.st_ino),
            )

        self.assertEqual(b"must-not-change", victim.read_bytes())

    async def test_expected_target_inode_rejects_swap_before_truncate(self):
        data = b"abcdefghijkl"
        self.part_path.write_bytes(b"reserved")
        reserved = self.part_path.stat()
        self.part_path.rename(self.root / "old-candidate")
        replacement = b"replacement-must-not-be-truncated"
        self.part_path.write_bytes(replacement)
        downloader = ParallelDownloader(
            MemoryRangeSource(data, chunk_size=4),
            workers=1,
            chunk_size=4,
        )

        with self.assertRaisesRegex(ValueError, "identity"):
            await downloader.download(
                identity_for(len(data)),
                self.part_path,
                expected_target_identity=(reserved.st_dev, reserved.st_ino),
            )

        self.assertEqual(replacement, self.part_path.read_bytes())

    async def test_expected_target_inode_rejects_hardlink_before_truncate(self):
        data = b"abcdefghijkl"
        payload = b"reserved-must-not-be-truncated"
        self.part_path.write_bytes(payload)
        reserved = self.part_path.stat()
        os.link(self.part_path, self.root / "candidate-hardlink")
        downloader = ParallelDownloader(
            MemoryRangeSource(data, chunk_size=4),
            workers=1,
            chunk_size=4,
        )

        with self.assertRaisesRegex(ValueError, "hardlink"):
            await downloader.download(
                identity_for(len(data)),
                self.part_path,
                expected_target_identity=(reserved.st_dev, reserved.st_ino),
            )

        self.assertEqual(payload, self.part_path.read_bytes())

    async def test_expected_manifest_inode_rejects_symlink_before_sqlite_write(self):
        data = b"abcdefghijkl"
        self.part_path.write_bytes(b"")
        candidate = self.part_path.stat()
        manifest_path = Path(f"{self.part_path}.manifest.sqlite3")
        manifest_path.write_bytes(b"")
        manifest = manifest_path.stat()
        manifest_path.rename(self.root / "old-manifest")
        victim = self.root / "manifest-victim.sqlite3"
        victim.write_bytes(b"must-not-change")
        manifest_path.symlink_to(victim)
        downloader = ParallelDownloader(
            MemoryRangeSource(data, chunk_size=4),
            workers=1,
            chunk_size=4,
        )

        with self.assertRaisesRegex(OSError, "symlink|symbolic|loop"):
            await downloader.download(
                identity_for(len(data)),
                self.part_path,
                expected_target_identity=(candidate.st_dev, candidate.st_ino),
                expected_manifest_identity=(manifest.st_dev, manifest.st_ino),
            )

        self.assertEqual(b"must-not-change", victim.read_bytes())

    async def test_expected_candidate_and_manifest_inodes_preserve_normal_download(
        self,
    ):
        data = b"abcdefghijkl"
        self.part_path.write_bytes(b"")
        candidate = self.part_path.stat()
        manifest_path = Path(f"{self.part_path}.manifest.sqlite3")
        manifest_path.write_bytes(b"")
        manifest = manifest_path.stat()
        downloader = ParallelDownloader(
            MemoryRangeSource(data, chunk_size=4),
            workers=2,
            chunk_size=4,
        )

        result = await downloader.download(
            identity_for(len(data)),
            self.part_path,
            expected_target_identity=(candidate.st_dev, candidate.st_ino),
            expected_manifest_identity=(manifest.st_dev, manifest.st_ino),
        )

        self.assertEqual(data, self.part_path.read_bytes())
        self.assertEqual(hashlib.sha256(data).hexdigest(), result.sha256)

    async def test_final_manifest_readback_detects_late_ssd_corruption(self):
        data = b"abcdefghijkl"

        class LateTamperSource(MemoryRangeSource):
            async def iter_range(self, start_offset, expected_length):
                async for chunk in super().iter_range(
                    start_offset,
                    expected_length,
                ):
                    yield chunk
                with self.part_path.open("r+b") as part_file:
                    part_file.seek(0)
                    part_file.write(b"x")

        source = LateTamperSource(data, chunk_size=4)
        source.part_path = self.part_path
        downloader = ParallelDownloader(source, workers=1, chunk_size=4)

        with self.assertRaisesRegex(HashMismatch, "SSD chunk mismatch"):
            await downloader.download(identity_for(len(data)), self.part_path)

    async def test_cancellation_waits_for_final_integrity_and_closes_worker_fd_once(
        self,
    ):
        data = b"abcdefghijkl"
        verification_started = threading.Event()
        verification_release = threading.Event()
        verifier_fd = {}
        verifier_close_count = 0
        real_close = os.close

        def blocking_verifier(fd, _file_size, _records):
            verifier_fd["value"] = fd
            verification_started.set()
            verification_release.wait(timeout=5)
            return hashlib.sha256(data).hexdigest()

        def tracking_close(fd):
            nonlocal verifier_close_count
            if fd == verifier_fd.get("value"):
                verifier_close_count += 1
            return real_close(fd)

        downloader = ParallelDownloader(
            MemoryRangeSource(data, chunk_size=4),
            workers=1,
            chunk_size=4,
        )
        with mock.patch(
            "module.parallel_downloader._verify_manifest_and_hash_sync",
            side_effect=blocking_verifier,
        ):
            with mock.patch(
                "module.parallel_downloader.os.close",
                side_effect=tracking_close,
            ):
                task = asyncio.create_task(
                    downloader.download(identity_for(len(data)), self.part_path)
                )
                await asyncio.wait_for(
                    asyncio.to_thread(verification_started.wait),
                    timeout=5,
                )
                task.cancel()
                try:
                    await asyncio.sleep(0)
                    self.assertFalse(task.done())
                    os.fstat(verifier_fd["value"])
                finally:
                    verification_release.set()

                with self.assertRaises(asyncio.CancelledError):
                    await task

        self.assertEqual(1, verifier_close_count)
        with self.assertRaises(OSError):
            os.fstat(verifier_fd["value"])

    async def test_strict_validation_still_rejects_stale_master_hashes(self):
        class StaleHashSource(MemoryRangeSource):
            async def get_hashes(self, offset):
                return [remote_hash(offset, b"stale Telegram bytes")]

        data = b"abcdefghijkl"
        source = StaleHashSource(data, chunk_size=4)
        downloader = ParallelDownloader(
            source,
            workers=2,
            chunk_size=4,
            verify_remote_hashes=True,
        )

        with self.assertRaises(HashMismatch):
            await downloader.download(identity_for(len(data)), self.part_path)

    async def test_short_non_final_chunk_fails(self):
        class ShortSource(MemoryRangeSource):
            async def iter_range(self, start_offset, expected_length):
                if start_offset == 0:
                    yield b"ab"
                    return
                async for chunk in super().iter_range(start_offset, expected_length):
                    yield chunk

        source = ShortSource(b"abcdefghijkl", chunk_size=4)
        downloader = ParallelDownloader(source, workers=2, chunk_size=4)

        with self.assertRaises(IncompleteRange):
            await downloader.download(identity_for(12), self.part_path)

    async def test_short_positional_write_is_completed(self):
        calls = []

        def short_pwrite(_fd, data, offset):
            written = bytes(data[:2])
            calls.append((offset, written))
            return len(written)

        await write_all_at(123, 4, b"abcdef", pwrite=short_pwrite)

        self.assertEqual(
            [(4, b"ab"), (6, b"cd"), (8, b"ef")],
            calls,
        )

    async def test_zero_length_positional_write_fails(self):
        with self.assertRaises(OSError):
            await write_all_at(123, 0, b"abc", pwrite=lambda *_args: 0)


class LifecycleRangeSource(MemoryRangeSource):
    """Range source double with observable prepare and close behavior."""

    def __init__(self, data, chunk_size):
        super().__init__(data, chunk_size)
        self.lifecycle = []
        self.range_error = None
        self.close_error = None
        self.block_ranges = False
        self.started = asyncio.Event()

    async def prepare(self, worker_count):
        self.lifecycle.append(("prepare", worker_count))

    async def close(self):
        self.lifecycle.append(("close",))
        if self.close_error is not None:
            raise self.close_error

    async def iter_range(self, start_offset, expected_length):
        self.started.set()
        if self.block_ranges:
            await asyncio.Event().wait()
        if self.range_error is not None:
            raise self.range_error
        async for chunk in super().iter_range(start_offset, expected_length):
            yield chunk


class ParallelSourceLifecycleTest(unittest.IsolatedAsyncioTestCase):
    """ParallelDownloader owns optional source setup and cleanup."""

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.part_path = Path(self.temp_dir.name) / "candidate.part"
        self.data = b"abcdefghijkl"

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    def new_download(self, source):
        return ParallelDownloader(source, workers=2, chunk_size=4)

    async def test_prepares_and_closes_source_on_success(self):
        source = LifecycleRangeSource(self.data, chunk_size=4)

        result = await self.new_download(source).download(
            identity_for(len(self.data)),
            self.part_path,
        )

        self.assertTrue(result.integrity.verified)
        self.assertEqual(
            [("prepare", 2), ("close",)],
            source.lifecycle,
        )

    async def test_closes_source_after_range_failure(self):
        source = LifecycleRangeSource(self.data, chunk_size=4)
        source.range_error = RuntimeError("download failed")

        with self.assertRaisesRegex(RuntimeError, "download failed"):
            await self.new_download(source).download(
                identity_for(len(self.data)),
                self.part_path,
            )

        self.assertEqual(("close",), source.lifecycle[-1])

    async def test_closes_source_after_cancellation(self):
        source = LifecycleRangeSource(self.data, chunk_size=4)
        source.block_ranges = True
        task = asyncio.create_task(
            self.new_download(source).download(
                identity_for(len(self.data)),
                self.part_path,
            )
        )
        await source.started.wait()

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(("close",), source.lifecycle[-1])

    async def test_cleanup_error_after_success_is_a_failure(self):
        source = LifecycleRangeSource(self.data, chunk_size=4)
        source.close_error = RuntimeError("close failed")

        with self.assertRaisesRegex(RuntimeError, "close failed"):
            await self.new_download(source).download(
                identity_for(len(self.data)),
                self.part_path,
            )

    async def test_cleanup_error_does_not_hide_download_error(self):
        source = LifecycleRangeSource(self.data, chunk_size=4)
        source.range_error = RuntimeError("download failed")
        source.close_error = RuntimeError("close failed")

        with self.assertRaisesRegex(RuntimeError, "download failed"):
            await self.new_download(source).download(
                identity_for(len(self.data)),
                self.part_path,
            )

        self.assertEqual(("close",), source.lifecycle[-1])


class IntegrityGateTest(unittest.IsolatedAsyncioTestCase):
    """Candidate success requires gap-free Telegram SHA-256 verification."""

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "candidate.bin"

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    async def test_unaligned_hash_windows_cover_and_verify_file(self):
        data = b"abcdefghijkl"
        self.path.write_bytes(data)
        hashes = [
            remote_hash(0, data[0:3]),
            remote_hash(3, data[3:8]),
            remote_hash(8, data[8:12]),
        ]

        report = await verify_file_hashes(self.path, len(data), hashes)

        self.assertTrue(report.verified)
        self.assertEqual(12, report.covered_bytes)
        self.assertEqual(3, report.range_count)

    async def test_corrupted_byte_is_hard_failure(self):
        original = b"abcdefgh"
        self.path.write_bytes(b"abcxefgh")

        with self.assertRaises(HashMismatch):
            await verify_file_hashes(
                self.path,
                len(original),
                [remote_hash(0, original)],
            )

    async def test_gap_in_remote_hashes_is_unverified(self):
        data = b"abcdefgh"
        self.path.write_bytes(data)

        with self.assertRaises(RemoteHashUnavailable):
            await verify_file_hashes(
                self.path,
                len(data),
                [remote_hash(0, data[:4]), remote_hash(5, data[5:])],
            )

    async def test_final_hash_limit_may_extend_past_declared_eof(self):
        data = b"abcdef"
        self.path.write_bytes(data)

        report = await verify_file_hashes(
            self.path,
            len(data),
            [remote_hash(0, data, limit=8)],
        )

        self.assertTrue(report.verified)
        self.assertEqual(6, report.covered_bytes)

    async def test_size_match_without_hashes_is_not_success(self):
        data = b"abcdefgh"
        self.path.write_bytes(data)

        with self.assertRaises(RemoteHashUnavailable):
            await verify_file_hashes(self.path, len(data), [])

    async def test_malformed_remote_digest_is_unavailable_not_local_corruption(self):
        data = b"abcdefgh"
        self.path.write_bytes(data)
        malformed = RemoteHash(offset=0, limit=len(data), digest=b"too-short")

        with self.assertRaises(RemoteHashUnavailable):
            await verify_file_hashes(self.path, len(data), [malformed])


class ParallelRecoveryTest(unittest.IsolatedAsyncioTestCase):
    """A new process reuses only chunks whose SSD digests still match."""

    async def test_abort_durability_waits_for_parallel_ranges_to_drain(self):
        class DrainingAbortSource:
            def __init__(self, data):
                self.data = data
                self.active_ranges = 0
                self.all_started = asyncio.Event()
                self.blocked = asyncio.Event()

            async def iter_range(self, start_offset, expected_length):
                self.active_ranges += 1
                if self.active_ranges == 2:
                    self.all_started.set()
                try:
                    await self.all_started.wait()
                    if start_offset == 0:
                        yield self.data[start_offset : start_offset + expected_length]
                    else:
                        await self.blocked.wait()
                finally:
                    self.active_ranges -= 1

        data = b"abcdefgh"
        source = DrainingAbortSource(data)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            part_path = root / "drained-abort.part"
            manifest_path = Path(f"{part_path}.manifest.sqlite3")
            part_path.touch(mode=0o600)
            manifest_path.touch(mode=0o600)
            candidate_identity = (
                part_path.stat().st_dev,
                part_path.stat().st_ino,
            )
            manifest_identity = (
                manifest_path.stat().st_dev,
                manifest_path.stat().st_ino,
            )
            candidate_sync_active_ranges = []
            real_fsync = os.fsync

            def recording_fsync(fd):
                metadata = os.fstat(fd)
                if (metadata.st_dev, metadata.st_ino) == candidate_identity:
                    candidate_sync_active_ranges.append(source.active_ranges)
                return real_fsync(fd)

            downloader = ParallelDownloader(
                source,
                workers=2,
                chunk_size=4,
                abort_after_chunks=1,
            )
            with mock.patch(
                "module.parallel_downloader.os.fsync",
                side_effect=recording_fsync,
            ):
                with self.assertRaises(InjectedAbort) as caught:
                    await downloader.download(
                        identity_for(len(data)),
                        part_path,
                        expected_target_identity=candidate_identity,
                        expected_manifest_identity=manifest_identity,
                    )

        self.assertTrue(caught.exception.durability.verified)
        self.assertEqual([0], candidate_sync_active_ranges)
        self.assertEqual(0, source.active_ranges)

    async def test_injected_abort_follows_candidate_manifest_and_directory_sync(self):
        data = b"abcdefghijklmnop"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            part_path = root / "durable-abort.part"
            manifest_path = Path(f"{part_path}.manifest.sqlite3")
            part_path.touch(mode=0o600)
            manifest_path.touch(mode=0o600)
            candidate_identity = (
                part_path.stat().st_dev,
                part_path.stat().st_ino,
            )
            manifest_identity = (
                manifest_path.stat().st_dev,
                manifest_path.stat().st_ino,
            )
            directory_identity = (root.stat().st_dev, root.stat().st_ino)
            sync_events = []
            real_fsync = os.fsync

            def recording_fsync(fd):
                metadata = os.fstat(fd)
                identity = (metadata.st_dev, metadata.st_ino)
                if identity == candidate_identity:
                    sync_events.append("candidate")
                elif identity == manifest_identity:
                    sync_events.append("manifest")
                elif stat.S_ISDIR(metadata.st_mode) and identity == directory_identity:
                    sync_events.append("directory")
                else:
                    sync_events.append("manifest-sidecar")
                return real_fsync(fd)

            downloader = ParallelDownloader(
                MemoryRangeSource(data, chunk_size=4),
                workers=1,
                chunk_size=4,
                abort_after_chunks=1,
            )
            with mock.patch(
                "module.parallel_downloader.os.fsync",
                side_effect=recording_fsync,
            ):
                with self.assertRaises(InjectedAbort) as caught:
                    await downloader.download(
                        identity_for(len(data)),
                        part_path,
                        expected_target_identity=candidate_identity,
                        expected_manifest_identity=manifest_identity,
                    )

            durability = caught.exception.durability
            self.assertTrue(durability.verified)
            self.assertTrue(durability.candidate_synced)
            self.assertTrue(durability.manifest_checkpointed)
            self.assertTrue(durability.manifest_synced)
            self.assertTrue(durability.directory_synced)
            self.assertLess(
                sync_events.index("candidate"),
                sync_events.index("manifest"),
            )
            self.assertLess(
                sync_events.index("manifest"),
                sync_events.index("directory"),
            )
            self.assertEqual("directory", sync_events[-1])

    async def test_directory_sync_failure_never_transitions_to_injected_abort(self):
        data = b"abcdefgh"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            part_path = root / "failed-directory-sync.part"
            manifest_path = Path(f"{part_path}.manifest.sqlite3")
            part_path.touch(mode=0o600)
            manifest_path.touch(mode=0o600)
            candidate_identity = (
                part_path.stat().st_dev,
                part_path.stat().st_ino,
            )
            manifest_identity = (
                manifest_path.stat().st_dev,
                manifest_path.stat().st_ino,
            )
            directory_identity = (root.stat().st_dev, root.stat().st_ino)
            real_fsync = os.fsync

            def fail_directory_fsync(fd):
                metadata = os.fstat(fd)
                if (
                    stat.S_ISDIR(metadata.st_mode)
                    and (metadata.st_dev, metadata.st_ino) == directory_identity
                ):
                    raise OSError("run directory fsync failed")
                return real_fsync(fd)

            downloader = ParallelDownloader(
                MemoryRangeSource(data, chunk_size=4),
                workers=1,
                chunk_size=4,
                abort_after_chunks=1,
            )
            with mock.patch(
                "module.parallel_downloader.os.fsync",
                side_effect=fail_directory_fsync,
            ):
                with self.assertRaisesRegex(OSError, "directory fsync failed"):
                    await downloader.download(
                        identity_for(len(data)),
                        part_path,
                        expected_target_identity=candidate_identity,
                        expected_manifest_identity=manifest_identity,
                    )

    async def test_restart_requests_only_unfinished_chunks(self):
        data = b"abcdefghijklmnop"
        with tempfile.TemporaryDirectory() as temp_dir:
            part_path = Path(temp_dir) / "recovery.part"
            first_source = MemoryRangeSource(data, chunk_size=4)
            first = ParallelDownloader(
                first_source,
                workers=2,
                chunk_size=4,
                abort_after_chunks=2,
            )

            with self.assertRaises(InjectedAbort):
                await first.download(identity_for(len(data)), part_path)

            manifest = DownloadManifest(f"{part_path}.manifest.sqlite3")
            completed_before = manifest.completed_chunks()
            self.assertGreaterEqual(len(completed_before), 2)

            second_source = MemoryRangeSource(data, chunk_size=4)
            second = ParallelDownloader(second_source, workers=2, chunk_size=4)
            result = await second.download(identity_for(len(data)), part_path)

            for completed_offset in completed_before:
                self.assertFalse(
                    any(
                        start <= completed_offset < start + length
                        for start, length in second_source.range_calls
                    ),
                    f"completed offset {completed_offset} was requested again",
                )
            self.assertEqual(data, part_path.read_bytes())
            self.assertEqual(len(completed_before), result.recovered_chunks)
            self.assertEqual(
                len(data) // 4,
                result.recovered_chunks + result.downloaded_chunks,
            )
            self.assertEqual(hashlib.sha256(data).hexdigest(), result.sha256)


if __name__ == "__main__":
    unittest.main()
