"""Tests for resumable single-file parallel downloads."""

import asyncio
import hashlib
import tempfile
import unittest
from pathlib import Path

from pyrogram import raw
from pyrogram.errors import FloodWait
from pyrogram.file_id import FileId, FileType

from module.parallel_downloader import (
    CHUNK_SIZE,
    ChunkSpec,
    DownloadManifest,
    HashMismatch,
    InjectedAbort,
    IncompleteRange,
    KurigramRangeSource,
    MediaIdentity,
    MediaIdentityChanged,
    ParallelDownloader,
    RemoteHash,
    RemoteHashUnavailable,
    collect_remote_hashes,
    plan_chunks,
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

    async def test_short_chunk_before_requested_range_end_fails(self):
        client = FakeKurigramClient(chunks=[b"a" * (CHUNK_SIZE // 2)])
        source = KurigramRangeSource(client, self.encoded_file_id, 4 * CHUNK_SIZE)

        with self.assertRaises(IncompleteRange):
            _ = [
                chunk
                async for chunk in source.iter_range(0, 2 * CHUNK_SIZE)
            ]

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
            return [
                chunk
                async for chunk in source.iter_range(offset, CHUNK_SIZE)
            ]

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
