"""Resumable, integrity-checked parallel downloads for one Telegram file."""

import asyncio
import errno
import hashlib
import hmac
import inspect
import json
import os
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from pyrogram import raw, utils
from pyrogram.errors import FloodWait
from pyrogram.file_id import PHOTO_TYPES, FileId, FileType


CHUNK_SIZE = 1024 * 1024
T = TypeVar("T")


class MediaIdentityChanged(RuntimeError):
    """A partial file belongs to a different Telegram media identity."""


class IncompleteRange(RuntimeError):
    """Telegram returned fewer or more bytes than the declared range."""


class RemoteHashUnavailable(RuntimeError):
    """Telegram hashes do not make complete monotonic file coverage."""


class HashMismatch(RuntimeError):
    """A local byte range differs from Telegram's SHA-256 reference."""


class InjectedAbort(RuntimeError):
    """Intentional validation interruption after durable chunk commits."""


@dataclass(frozen=True)
class MediaIdentity:
    """Stable fields that prevent mixing bytes from different media."""

    chat_id: str
    message_id: int
    media_id: int
    dc_id: int
    file_unique_id: str
    file_size: int

    def stable_key(self) -> str:
        """Return deterministic serialized identity metadata."""
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class ChunkSpec:
    """One exact byte range in a declared file."""

    offset: int
    length: int


@dataclass(frozen=True)
class RemoteHash:
    """One SHA-256 range returned by ``upload.getFileHashes``."""

    offset: int
    limit: int
    digest: bytes


@dataclass(frozen=True)
class IntegrityReport:
    """Result of validating one complete local file against Telegram."""

    verified: bool
    covered_bytes: int
    range_count: int
    mismatch_count: int


@dataclass(frozen=True)
class ParallelDownloadResult:
    """Evidence produced by one completed candidate download."""

    path: str
    sha256: str
    file_size: int
    elapsed_seconds: float
    retries: int
    workers: int
    integrity: IntegrityReport
    recovered_chunks: int
    downloaded_chunks: int


def plan_chunks(file_size: int, chunk_size: int = CHUNK_SIZE) -> List[ChunkSpec]:
    """Partition a file into non-overlapping chunks with exact EOF coverage."""
    if file_size <= 0 or chunk_size <= 0:
        raise ValueError("file_size and chunk_size must be positive")

    return [
        ChunkSpec(offset, min(chunk_size, file_size - offset))
        for offset in range(0, file_size, chunk_size)
    ]


def split_missing_runs(
    chunks: Collection[ChunkSpec],
    completed_offsets: Collection[int],
    workers: int,
) -> List[List[ChunkSpec]]:
    """Assign every missing chunk to at most ``workers`` ordered queues."""
    if workers <= 0:
        raise ValueError("workers must be positive")

    completed = set(completed_offsets)
    missing = sorted(
        (chunk for chunk in chunks if chunk.offset not in completed),
        key=lambda chunk: chunk.offset,
    )
    if not missing:
        return []

    queue_count = min(workers, len(missing))
    base_size, remainder = divmod(len(missing), queue_count)
    queues: List[List[ChunkSpec]] = []
    cursor = 0
    for index in range(queue_count):
        queue_size = base_size + (1 if index < remainder else 0)
        queues.append(missing[cursor : cursor + queue_size])
        cursor += queue_size

    return queues


class DownloadManifest:
    """SQLite-backed completion state for one SSD partial file."""

    def __init__(self, db_path: Union[str, os.PathLike]):
        self.db_path = os.fspath(db_path)
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        parent = os.path.dirname(os.path.abspath(self.db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _ensure_schema(self):
        with self._lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS download_meta (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    identity_key TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    chunk_size INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS completed_chunks (
                    offset INTEGER PRIMARY KEY,
                    length INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    attempts INTEGER NOT NULL
                )
                """
            )

    def prepare(
        self,
        identity: MediaIdentity,
        file_size: int,
        chunk_size: int,
    ) -> None:
        """Create metadata once, or reject an incompatible partial file."""
        if file_size <= 0 or chunk_size <= 0:
            raise ValueError("file_size and chunk_size must be positive")
        if identity.file_size != file_size:
            raise MediaIdentityChanged("declared size differs from media identity")

        expected = (identity.stable_key(), file_size, chunk_size)
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT identity_key, file_size, chunk_size
                FROM download_meta
                WHERE singleton = 1
                """
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO download_meta (
                        singleton, identity_key, file_size, chunk_size
                    ) VALUES (1, ?, ?, ?)
                    """,
                    expected,
                )
                return

            current = (
                str(row["identity_key"]),
                int(row["file_size"]),
                int(row["chunk_size"]),
            )
            if current != expected:
                raise MediaIdentityChanged(
                    "partial download manifest belongs to different media"
                )

    def completed_chunks(self) -> Dict[int, Tuple[int, str]]:
        """Return recorded chunk lengths and local SHA-256 values by offset."""
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT offset, length, sha256
                FROM completed_chunks
                ORDER BY offset
                """
            ).fetchall()
        return {
            int(row["offset"]): (int(row["length"]), str(row["sha256"]))
            for row in rows
        }

    def mark_complete(
        self,
        chunk: ChunkSpec,
        digest: str,
        attempts: int,
    ) -> None:
        """Atomically record one locally verified chunk."""
        if chunk.offset < 0 or chunk.length <= 0 or attempts <= 0:
            raise ValueError("invalid completed chunk metadata")
        if len(digest) != 64:
            raise ValueError("digest must be a SHA-256 hex string")

        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO completed_chunks (offset, length, sha256, attempts)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(offset) DO UPDATE SET
                    length = excluded.length,
                    sha256 = excluded.sha256,
                    attempts = excluded.attempts
                """,
                (chunk.offset, chunk.length, digest, attempts),
            )

    def revalidate_completed(
        self,
        part_path: Union[str, os.PathLike],
    ) -> Dict[int, Tuple[int, str]]:
        """Discard manifest rows that no longer match bytes on SSD."""
        records = self.completed_chunks()
        invalid_offsets = []
        path = Path(part_path)
        if not path.is_file():
            invalid_offsets = list(records)
        else:
            with path.open("rb") as part_file:
                for offset, (length, expected_digest) in records.items():
                    part_file.seek(offset)
                    data = part_file.read(length)
                    if (
                        len(data) != length
                        or hashlib.sha256(data).hexdigest() != expected_digest
                    ):
                        invalid_offsets.append(offset)

        if invalid_offsets:
            with self._lock, self._connect() as connection:
                connection.executemany(
                    "DELETE FROM completed_chunks WHERE offset = ?",
                    ((offset,) for offset in invalid_offsets),
                )

        return self.completed_chunks()


def _exception_chain(error: BaseException):
    """Yield a wrapped exception chain once, including implicit contexts."""
    pending = [error]
    visited = set()
    while pending:
        current = pending.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))
        yield current
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)


def _find_flood_wait(error: BaseException) -> Optional[FloodWait]:
    for current in _exception_chain(error):
        if isinstance(current, FloodWait):
            return current
    return None


def is_retryable_timeout(error: BaseException) -> bool:
    """Recognize direct and wrapped transport timeouts from unstable proxies."""
    retryable_errno = {
        errno.ECONNABORTED,
        errno.ECONNRESET,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.ENETUNREACH,
        errno.EPIPE,
        errno.ETIMEDOUT,
    }
    for current in _exception_chain(error):
        if isinstance(current, (TimeoutError, ConnectionError)):
            return True
        if isinstance(current, OSError) and current.errno in retryable_errno:
            return True
    return False


async def retry_telegram(
    operation: Callable[[], Awaitable[T]],
    max_attempts: int = 3,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run one Telegram operation with bounded timeout and FloodWait retries."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")

    for attempt in range(1, max_attempts + 1):
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if attempt >= max_attempts:
                raise

            flood_wait = _find_flood_wait(error)
            if flood_wait is not None:
                await sleep(max(float(flood_wait.value), 0))
                continue

            if is_retryable_timeout(error):
                await sleep(min(2 ** (attempt - 1), 8))
                continue
            raise

    raise RuntimeError("unreachable retry state")


def build_file_location(file_id: FileId):
    """Build the same raw file location used by Kurigram's ``get_file``."""
    if file_id.file_type == FileType.CHAT_PHOTO:
        if file_id.chat_id > 0:
            peer = raw.types.InputPeerUser(
                user_id=file_id.chat_id,
                access_hash=file_id.chat_access_hash,
            )
        elif file_id.chat_access_hash == 0:
            peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
        else:
            peer = raw.types.InputPeerChannel(
                channel_id=utils.get_channel_id(file_id.chat_id),
                access_hash=file_id.chat_access_hash,
            )
        return raw.types.InputPeerPhotoFileLocation(
            peer=peer,
            photo_id=file_id.media_id,
            big=file_id.thumbnail_source.name.endswith("BIG"),
        )

    if file_id.file_type in PHOTO_TYPES:
        return raw.types.InputPhotoFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    return raw.types.InputDocumentFileLocation(
        id=file_id.media_id,
        access_hash=file_id.access_hash,
        file_reference=file_id.file_reference,
        thumb_size=file_id.thumbnail_size,
    )


class KurigramRangeSource:
    """Expose Kurigram media sessions as exact contiguous byte ranges."""

    def __init__(self, client, encoded_file_id: str, file_size: int):
        if file_size <= 0:
            raise ValueError("file_size must be positive")
        self.client = client
        self.file_id = FileId.decode(encoded_file_id)
        self.file_size = file_size
        self.location = build_file_location(self.file_id)
        self._media_session = None
        self._session_lock = getattr(client, "sessions_lock", asyncio.Lock())

    async def _get_media_session(self):
        if self._media_session is not None:
            return self._media_session
        async with self._session_lock:
            if self._media_session is None:
                self._media_session = await self.client.get_session(
                    self.file_id.dc_id,
                    is_media=True,
                )
        return self._media_session

    async def iter_range(
        self,
        start_offset: int,
        expected_length: int,
    ) -> AsyncIterator[bytes]:
        """Yield one exact range through Kurigram's built-in CDN handling."""
        if start_offset < 0 or start_offset % CHUNK_SIZE != 0:
            raise ValueError("range start must be a non-negative 1 MiB boundary")
        if expected_length <= 0 or start_offset + expected_length > self.file_size:
            raise ValueError("range exceeds declared file size")
        if (
            expected_length % CHUNK_SIZE != 0
            and start_offset + expected_length != self.file_size
        ):
            raise ValueError("only the declared final range may be short")

        chunk_limit = (expected_length + CHUNK_SIZE - 1) // CHUNK_SIZE
        remaining = expected_length
        await self._get_media_session()
        async for chunk in self.client.get_file(
            self.file_id,
            self.file_size,
            limit=chunk_limit,
            offset=start_offset // CHUNK_SIZE,
        ):
            expected_chunk_length = min(CHUNK_SIZE, remaining)
            if len(chunk) != expected_chunk_length:
                raise IncompleteRange(
                    f"range at {start_offset} expected {expected_chunk_length} "
                    f"bytes, got {len(chunk)}"
                )
            remaining -= len(chunk)
            yield chunk

        if remaining:
            raise IncompleteRange(
                f"range at {start_offset} ended with {remaining} bytes missing"
            )

    async def get_hashes(self, offset: int) -> List[RemoteHash]:
        """Fetch a master-DC hash batch for this immutable file location."""
        session = await self._get_media_session()
        result = await session.invoke(
            raw.functions.upload.GetFileHashes(
                location=self.location,
                offset=offset,
            ),
            sleep_threshold=30,
        )
        return [
            RemoteHash(
                offset=int(item.offset),
                limit=int(item.limit),
                digest=bytes(item.hash),
            )
            for item in result
        ]


def _covered_prefix(hashes: Collection[RemoteHash], file_size: int) -> int:
    covered = 0
    for item in sorted(hashes, key=lambda value: (value.offset, value.limit)):
        if item.offset > covered:
            break
        covered = max(covered, min(file_size, item.offset + item.limit))
        if covered >= file_size:
            break
    return covered


async def collect_remote_hashes(
    source,
    file_size: int,
    *,
    max_requests: int = 4096,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> List[RemoteHash]:
    """Collect unique master-DC hashes until they cover declared EOF."""
    if file_size <= 0 or max_requests <= 0:
        raise ValueError("file_size and max_requests must be positive")

    hashes: Dict[Tuple[int, int, bytes], RemoteHash] = {}
    covered = 0
    for _request_number in range(max_requests):
        cursor = covered

        async def fetch_batch():
            return await source.get_hashes(cursor)

        batch = await retry_telegram(fetch_batch, max_attempts=3, sleep=sleep)
        for item in batch:
            if (
                item.offset < 0
                or item.offset >= file_size
                or item.limit <= 0
                or len(item.digest) != hashlib.sha256().digest_size
            ):
                raise RemoteHashUnavailable(
                    f"invalid remote hash range at offset {item.offset}"
                )
            hashes[(item.offset, item.limit, item.digest)] = item

        next_covered = _covered_prefix(hashes.values(), file_size)
        if next_covered >= file_size:
            return sorted(hashes.values(), key=lambda item: (item.offset, item.limit))
        if next_covered <= covered:
            raise RemoteHashUnavailable(
                f"remote hash coverage made no progress at offset {covered}"
            )
        covered = next_covered

    raise RemoteHashUnavailable(
        f"remote hash request limit reached with {covered}/{file_size} bytes covered"
    )


async def write_all_at(
    fd: int,
    offset: int,
    data: bytes,
    *,
    pwrite: Callable = os.pwrite,
) -> None:
    """Complete a positional write even when the OS reports a short write."""
    view = memoryview(data)
    written = 0
    while written < len(view):
        count = await asyncio.to_thread(
            pwrite,
            fd,
            view[written:],
            offset + written,
        )
        if count <= 0:
            raise OSError("positional write made no progress")
        written += count


def _hash_file(path: Union[str, os.PathLike]) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source_file:
        while True:
            block = source_file.read(8 * 1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _verify_file_hashes_sync(
    path: Union[str, os.PathLike],
    file_size: int,
    hashes: Collection[RemoteHash],
) -> IntegrityReport:
    if file_size <= 0:
        raise ValueError("file_size must be positive")
    if os.path.getsize(path) != file_size:
        raise IncompleteRange("local file size differs from declared Telegram size")

    ordered = sorted(hashes, key=lambda item: (item.offset, item.limit))
    if not ordered:
        raise RemoteHashUnavailable("Telegram returned no file hashes")

    covered = 0
    with open(path, "rb") as local_file:
        for item in ordered:
            if (
                item.offset < 0
                or item.offset >= file_size
                or item.limit <= 0
                or len(item.digest) != hashlib.sha256().digest_size
            ):
                raise RemoteHashUnavailable(
                    f"invalid remote hash range at offset {item.offset}"
                )
            if item.offset > covered:
                raise RemoteHashUnavailable(
                    f"remote hash gap from {covered} to {item.offset}"
                )

            actual_length = min(item.limit, file_size - item.offset)
            local_file.seek(item.offset)
            data = local_file.read(actual_length)
            if len(data) != actual_length:
                raise IncompleteRange(
                    f"local hash range at {item.offset} is short"
                )
            actual_digest = hashlib.sha256(data).digest()
            if not hmac.compare_digest(actual_digest, item.digest):
                raise HashMismatch(
                    f"Telegram hash mismatch at offset {item.offset}"
                )
            covered = max(covered, item.offset + actual_length)

    if covered < file_size:
        raise RemoteHashUnavailable(
            f"remote hashes cover {covered}/{file_size} bytes"
        )
    return IntegrityReport(
        verified=True,
        covered_bytes=covered,
        range_count=len(ordered),
        mismatch_count=0,
    )


async def verify_file_hashes(
    path: Union[str, os.PathLike],
    file_size: int,
    hashes: Collection[RemoteHash],
) -> IntegrityReport:
    """Verify arbitrary, potentially unaligned remote hash windows."""
    return await asyncio.to_thread(
        _verify_file_hashes_sync,
        path,
        file_size,
        hashes,
    )


def _contiguous_groups(chunks: Collection[ChunkSpec]) -> List[List[ChunkSpec]]:
    groups: List[List[ChunkSpec]] = []
    for chunk in sorted(chunks, key=lambda item: item.offset):
        if not groups or groups[-1][-1].offset + groups[-1][-1].length != chunk.offset:
            groups.append([chunk])
        else:
            groups[-1].append(chunk)
    return groups


class ParallelDownloader:
    """Download one file through bounded, non-overlapping range workers."""

    def __init__(
        self,
        source,
        *,
        workers: int = 2,
        chunk_size: int = CHUNK_SIZE,
        max_attempts: int = 3,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        abort_after_chunks: Optional[int] = None,
    ):
        if workers <= 0 or workers > 4:
            raise ValueError("workers must be between 1 and 4")
        if chunk_size <= 0 or max_attempts <= 0:
            raise ValueError("chunk_size and max_attempts must be positive")
        if abort_after_chunks is not None and abort_after_chunks <= 0:
            raise ValueError("abort_after_chunks must be positive")
        self.source = source
        self.workers = workers
        self.chunk_size = chunk_size
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.abort_after_chunks = abort_after_chunks
        self._downloaded_chunks = 0
        self._recovered_chunks = 0
        self._completed_bytes = 0
        self._retries = 0

    async def _notify_progress(self, progress, file_size: int):
        if progress is None:
            return
        result = progress(self._completed_bytes, file_size)
        if inspect.isawaitable(result):
            await result

    async def _download_group(
        self,
        fd: int,
        manifest: DownloadManifest,
        chunks: List[ChunkSpec],
        file_size: int,
        progress,
    ):
        chunk_index = 0
        attempts = 0
        while chunk_index < len(chunks):
            attempts += 1
            pending = chunks[chunk_index:]
            start_offset = pending[0].offset
            expected_length = sum(chunk.length for chunk in pending)
            try:
                async for data in self.source.iter_range(
                    start_offset,
                    expected_length,
                ):
                    if chunk_index >= len(chunks):
                        raise IncompleteRange(
                            f"range at {start_offset} returned extra bytes"
                        )
                    chunk = chunks[chunk_index]
                    if len(data) != chunk.length:
                        raise IncompleteRange(
                            f"chunk at {chunk.offset} expected {chunk.length} "
                            f"bytes, got {len(data)}"
                        )
                    await write_all_at(fd, chunk.offset, data)
                    manifest.mark_complete(
                        chunk,
                        hashlib.sha256(data).hexdigest(),
                        attempts=attempts,
                    )
                    chunk_index += 1
                    self._downloaded_chunks += 1
                    self._completed_bytes += chunk.length
                    await self._notify_progress(progress, file_size)
                    if (
                        self.abort_after_chunks is not None
                        and self._downloaded_chunks >= self.abort_after_chunks
                    ):
                        raise InjectedAbort(
                            f"aborted after {self._downloaded_chunks} chunks"
                        )

                if chunk_index < len(chunks):
                    raise IncompleteRange(
                        f"range at {start_offset} ended before all chunks arrived"
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if isinstance(error, (InjectedAbort, IncompleteRange, HashMismatch)):
                    raise
                if attempts >= self.max_attempts:
                    raise
                flood_wait = _find_flood_wait(error)
                if flood_wait is not None:
                    self._retries += 1
                    await self.sleep(max(float(flood_wait.value), 0))
                    continue
                if is_retryable_timeout(error):
                    self._retries += 1
                    await self.sleep(min(2 ** (attempts - 1), 8))
                    continue
                raise

    async def _download_queue(
        self,
        fd: int,
        manifest: DownloadManifest,
        chunks: List[ChunkSpec],
        file_size: int,
        progress,
    ):
        for group in _contiguous_groups(chunks):
            await self._download_group(
                fd,
                manifest,
                group,
                file_size,
                progress,
            )

    async def download(
        self,
        identity: MediaIdentity,
        part_path: Union[str, os.PathLike],
        progress=None,
    ) -> ParallelDownloadResult:
        """Download and verify one candidate while retaining restart metadata."""
        started = time.monotonic()
        path = Path(part_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        manifest = DownloadManifest(f"{path}.manifest.sqlite3")
        manifest.prepare(identity, identity.file_size, self.chunk_size)

        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.ftruncate(fd, identity.file_size)
            completed = manifest.revalidate_completed(path)
            chunks = plan_chunks(identity.file_size, self.chunk_size)
            self._recovered_chunks = len(completed)
            self._completed_bytes = sum(
                length for length, _digest in completed.values()
            )
            queues = split_missing_runs(chunks, completed.keys(), self.workers)
            tasks = [
                asyncio.create_task(
                    self._download_queue(
                        fd,
                        manifest,
                        queue,
                        identity.file_size,
                        progress,
                    )
                )
                for queue in queues
            ]
            try:
                if tasks:
                    await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

            final_records = manifest.completed_chunks()
            expected = {chunk.offset: chunk.length for chunk in chunks}
            actual = {
                offset: length
                for offset, (length, _digest) in final_records.items()
            }
            if actual != expected:
                raise IncompleteRange("manifest does not cover the declared file")
            os.fsync(fd)
        finally:
            os.close(fd)

        remote_hashes = await collect_remote_hashes(
            self.source,
            identity.file_size,
            sleep=self.sleep,
        )
        integrity = await verify_file_hashes(
            path,
            identity.file_size,
            remote_hashes,
        )
        whole_digest = await asyncio.to_thread(_hash_file, path)
        return ParallelDownloadResult(
            path=str(path),
            sha256=whole_digest,
            file_size=identity.file_size,
            elapsed_seconds=time.monotonic() - started,
            retries=self._retries,
            workers=self.workers,
            integrity=integrity,
            recovered_chunks=self._recovered_chunks,
            downloaded_chunks=self._downloaded_chunks,
        )
