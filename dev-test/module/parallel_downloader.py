"""Resumable, integrity-checked parallel downloads for one Telegram file."""

import asyncio
import errno
import hashlib
import hmac
import inspect
import json
import os
import sqlite3
import stat
import threading
import time
from contextlib import closing
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

from loguru import logger
from pyrogram import raw, utils
from pyrogram.errors import (
    BadMsgNotification,
    FloodWait,
    SecurityError,
    Unauthorized,
)
from pyrogram.file_id import PHOTO_TYPES, FileId, FileType


CHUNK_SIZE = 1024 * 1024
T = TypeVar("T")
FileIdentity = Tuple[int, int]


class MediaIdentityChanged(RuntimeError):
    """A partial file belongs to a different Telegram media identity."""


class IncompleteRange(RuntimeError):
    """Telegram returned fewer or more bytes than the declared range."""


class RemoteHashUnavailable(RuntimeError):
    """Telegram hashes do not make complete monotonic file coverage."""


class HashMismatch(RuntimeError):
    """A local byte range differs from Telegram's SHA-256 reference."""


@dataclass(frozen=True)
class AbortDurability:
    """Completed durability barriers attached to an intentional abort."""

    candidate_synced: bool = False
    manifest_checkpointed: bool = False
    manifest_synced: bool = False
    directory_synced: bool = False
    manifest_sidecars_synced: Tuple[str, ...] = ()

    @property
    def verified(self) -> bool:
        return bool(
            self.candidate_synced
            and self.manifest_checkpointed
            and self.manifest_synced
            and self.directory_synced
        )


class InjectedAbort(RuntimeError):
    """Intentional validation interruption after durable chunk commits."""

    def __init__(
        self,
        message: str,
        *,
        durability: Optional[AbortDurability] = None,
    ):
        super().__init__(message)
        self.durability = durability or AbortDurability()


class _InjectedAbortRequested(RuntimeError):
    """Internal request that becomes public only after all writers drain."""


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
    """Result of validating one complete local file."""

    verified: bool
    covered_bytes: int
    range_count: int
    mismatch_count: int
    method: str


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


def plan_missing_stripes(
    chunks: Collection[ChunkSpec],
    completed_offsets: Collection[int],
    stripe_size: int,
) -> List[List[ChunkSpec]]:
    """Group contiguous missing 1 MiB chunks into bounded logical stripes."""
    if stripe_size < CHUNK_SIZE or stripe_size % CHUNK_SIZE:
        raise ValueError("stripe_size must be a positive 1 MiB multiple")

    completed = set(completed_offsets)
    stripes: List[List[ChunkSpec]] = []
    current: List[ChunkSpec] = []
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
        if current and (
            not contiguous or current_bytes + chunk.length > stripe_size
        ):
            stripes.append(current)
            current = []
            current_bytes = 0
        current.append(chunk)
        current_bytes += chunk.length
        previous_end = chunk.offset + chunk.length

    if current:
        stripes.append(current)
    return stripes


def _open_verified_regular_file(
    path: Union[str, os.PathLike],
    flags: int,
    mode: int,
    expected_identity: FileIdentity,
    label: str,
) -> int:
    """Open and pin one expected unlinked regular inode."""
    open_flags = flags | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, open_flags, mode)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{label} is not a regular file")
        if metadata.st_nlink != 1:
            raise ValueError(f"{label} is a hardlink")
        if (metadata.st_dev, metadata.st_ino) != tuple(expected_identity):
            raise ValueError(f"{label} identity changed before use")

        path_metadata = os.lstat(path)
        if stat.S_ISLNK(path_metadata.st_mode):
            raise OSError(f"{label} is a symbolic link")
        if (
            path_metadata.st_dev,
            path_metadata.st_ino,
        ) != (metadata.st_dev, metadata.st_ino):
            raise ValueError(f"{label} identity changed before use")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _open_regular_file_for_sync(
    path: Union[str, os.PathLike],
    label: str,
) -> int:
    """Open one pathname without following links and pin its live inode."""
    path_value = os.fspath(path)
    fd = os.open(
        path_value,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{label} is not a regular file")
        if metadata.st_nlink != 1:
            raise ValueError(f"{label} is a hardlink")
        path_metadata = os.lstat(path_value)
        if stat.S_ISLNK(path_metadata.st_mode):
            raise OSError(f"{label} is a symbolic link")
        if (
            path_metadata.st_dev,
            path_metadata.st_ino,
        ) != (metadata.st_dev, metadata.st_ino):
            raise ValueError(f"{label} identity changed before sync")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _fsync_directory(path: Union[str, os.PathLike]) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        if not stat.S_ISDIR(os.fstat(fd).st_mode):
            raise ValueError(f"download run directory is not a directory: {path}")
        os.fsync(fd)
    finally:
        os.close(fd)


class DownloadManifest:
    """SQLite-backed completion state for one SSD partial file."""

    def __init__(
        self,
        db_path: Union[str, os.PathLike],
        expected_file_identity: Optional[FileIdentity] = None,
    ):
        self.db_path = os.fspath(db_path)
        self.expected_file_identity = expected_file_identity
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        parent = os.path.dirname(os.path.abspath(self.db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        guard_fd = None
        if self.expected_file_identity is not None:
            guard_fd = _open_verified_regular_file(
                self.db_path,
                os.O_RDWR,
                0o600,
                self.expected_file_identity,
                "download manifest",
            )
        try:
            connection = sqlite3.connect(self.db_path, timeout=30)
            if self.expected_file_identity is not None:
                verification_fd = _open_verified_regular_file(
                    self.db_path,
                    os.O_RDWR,
                    0o600,
                    self.expected_file_identity,
                    "download manifest",
                )
                os.close(verification_fd)
        except BaseException:
            if "connection" in locals():
                connection.close()
            raise
        finally:
            if guard_fd is not None:
                os.close(guard_fd)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _ensure_schema(self):
        with self._lock, closing(self._connect()) as connection, connection:
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
        with self._lock, closing(self._connect()) as connection, connection:
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
        with self._lock, closing(self._connect()) as connection, connection:
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

        with self._lock, closing(self._connect()) as connection, connection:
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

    def checkpoint_and_sync(self) -> Tuple[str, ...]:
        """Checkpoint WAL state and fsync every surviving manifest artifact."""
        with self._lock, closing(self._connect()) as connection:
            checkpoint = connection.execute("PRAGMA wal_checkpoint(FULL)").fetchone()
            if checkpoint is not None and int(checkpoint[0] or 0) != 0:
                raise sqlite3.OperationalError("download manifest checkpoint is busy")

        if self.expected_file_identity is None:
            manifest_fd = _open_regular_file_for_sync(
                self.db_path,
                "download manifest",
            )
        else:
            manifest_fd = _open_verified_regular_file(
                self.db_path,
                os.O_RDONLY,
                0o600,
                self.expected_file_identity,
                "download manifest",
            )
        try:
            os.fsync(manifest_fd)
        finally:
            os.close(manifest_fd)

        synced_sidecars = []
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar_path = f"{self.db_path}{suffix}"
            if not os.path.lexists(sidecar_path):
                continue
            try:
                sidecar_fd = _open_regular_file_for_sync(
                    sidecar_path,
                    f"download manifest{suffix}",
                )
            except FileNotFoundError:
                continue
            try:
                os.fsync(sidecar_fd)
            finally:
                os.close(sidecar_fd)
            synced_sidecars.append(suffix)
        return tuple(synced_sidecars)

    def revalidate_completed(
        self,
        part_path: Union[str, os.PathLike],
        *,
        file_fd: Optional[int] = None,
    ) -> Dict[int, Tuple[int, str]]:
        """Discard manifest rows that no longer match bytes on SSD."""
        records = self.completed_chunks()
        invalid_offsets = []
        path = Path(part_path)
        if file_fd is None and not path.is_file():
            invalid_offsets = list(records)
        else:
            part_file = (
                os.fdopen(os.dup(file_fd), "rb")
                if file_fd is not None
                else path.open("rb")
            )
            with part_file:
                for offset, (length, expected_digest) in records.items():
                    part_file.seek(offset)
                    data = part_file.read(length)
                    if (
                        len(data) != length
                        or hashlib.sha256(data).hexdigest() != expected_digest
                    ):
                        invalid_offsets.append(offset)

        if invalid_offsets:
            with self._lock, closing(self._connect()) as connection, connection:
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


def _is_fatal_session_error(error: BaseException) -> bool:
    """Recognize failures that make a leased media session unsafe to reuse."""
    fatal_errors = (
        BadMsgNotification,
        IncompleteRange,
        SecurityError,
        Unauthorized,
    )
    return any(
        isinstance(current, fatal_errors)
        for current in _exception_chain(error)
    )


async def _close_async_iterator(iterator) -> None:
    """Close an async iterator fully before re-propagating cancellation."""
    close = getattr(iterator, "aclose", None)
    if close is None:
        return

    close_task = asyncio.ensure_future(close())
    cancellation = None
    while not close_task.done():
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError as error:
            cancellation = cancellation or error
        except BaseException:
            break

    close_error = None
    try:
        close_task.result()
    except BaseException as error:
        close_error = error

    if cancellation is not None:
        if close_error is not None:
            raise cancellation from close_error
        raise cancellation
    if close_error is not None:
        raise close_error


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


class _SessionBoundClient:
    """Delegate a Kurigram client while pinning media calls to one session."""

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
        self._temporary_sessions = []
        self._session_queue = None

    @property
    def available_session_count(self) -> int:
        if self._session_queue is None:
            return 0
        return self._session_queue.qsize()

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

    async def prepare(self, worker_count: int) -> None:
        """Create one independent temporary media session per range worker."""
        if worker_count <= 0:
            raise ValueError("worker_count must be positive")
        if self._session_queue is not None:
            if worker_count != len(self._temporary_sessions):
                raise RuntimeError("range source already prepared")
            return

        await self._get_media_session()
        sessions = []
        try:
            async with self._session_lock:
                for _worker in range(worker_count):
                    session = await self.client.get_session(
                        self.file_id.dc_id,
                        is_media=True,
                        export_authorization=False,
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

    async def close(self) -> None:
        """Stop every source-owned temporary session exactly once."""
        sessions = self._temporary_sessions
        self._temporary_sessions = []
        self._session_queue = None
        if not sessions:
            return

        results = await asyncio.gather(
            *(session.stop() for session in sessions),
            return_exceptions=True,
        )
        errors = [
            result
            for result in results
            if isinstance(result, BaseException)
        ]
        if errors:
            raise errors[0]

    def _range_chunk_limit(
        self,
        start_offset: int,
        expected_length: int,
    ) -> int:
        if start_offset < 0 or start_offset % CHUNK_SIZE != 0:
            raise ValueError("range start must be a non-negative 1 MiB boundary")
        if expected_length <= 0 or start_offset + expected_length > self.file_size:
            raise ValueError("range exceeds declared file size")
        if (
            expected_length % CHUNK_SIZE != 0
            and start_offset + expected_length != self.file_size
        ):
            raise ValueError("only the declared final range may be short")
        return (expected_length + CHUNK_SIZE - 1) // CHUNK_SIZE

    async def _validated_stream(
        self,
        stream,
        start_offset: int,
        expected_length: int,
    ) -> AsyncIterator[bytes]:
        remaining = expected_length
        try:
            async for chunk in stream:
                expected_chunk_length = min(CHUNK_SIZE, remaining)
                if len(chunk) != expected_chunk_length:
                    raise IncompleteRange(
                        f"range at {start_offset} expected "
                        f"{expected_chunk_length} bytes, got {len(chunk)}"
                    )
                remaining -= len(chunk)
                yield chunk
        finally:
            await _close_async_iterator(stream)

        if remaining:
            raise IncompleteRange(
                f"range at {start_offset} ended with {remaining} bytes missing"
            )

    async def iter_range(
        self,
        start_offset: int,
        expected_length: int,
    ) -> AsyncIterator[bytes]:
        """Yield one exact range through Kurigram's built-in CDN handling."""
        chunk_limit = self._range_chunk_limit(start_offset, expected_length)
        queue = self._session_queue
        session = None
        try:
            if queue is None:
                await self._get_media_session()
                stream = self.client.get_file(
                    self.file_id,
                    self.file_size,
                    limit=chunk_limit,
                    offset=start_offset // CHUNK_SIZE,
                )
            else:
                session = await queue.get()
                bound_client = _SessionBoundClient(self.client, session)
                stream = self.client.get_file.__func__(
                    bound_client,
                    self.file_id,
                    self.file_size,
                    limit=chunk_limit,
                    offset=start_offset // CHUNK_SIZE,
                )

            validated_stream = self._validated_stream(
                stream,
                start_offset,
                expected_length,
            )
            try:
                async for chunk in validated_stream:
                    yield chunk
            finally:
                await _close_async_iterator(validated_stream)
        finally:
            if session is not None:
                queue.put_nowait(session)

    async def iter_range_on_session(
        self,
        session,
        start_offset: int,
        expected_length: int,
    ) -> AsyncIterator[bytes]:
        """Yield one exact range through a caller-owned media session."""
        chunk_limit = self._range_chunk_limit(start_offset, expected_length)
        bound_client = _SessionBoundClient(self.client, session)
        stream = self.client.get_file.__func__(
            bound_client,
            self.file_id,
            self.file_size,
            limit=chunk_limit,
            offset=start_offset // CHUNK_SIZE,
        )
        validated_stream = self._validated_stream(
            stream,
            start_offset,
            expected_length,
        )
        try:
            async for chunk in validated_stream:
                yield chunk
        finally:
            await _close_async_iterator(validated_stream)

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


def _verify_manifest_and_hash_sync(
    path: Union[str, os.PathLike, int],
    file_size: int,
    records: Dict[int, Tuple[int, str]],
) -> str:
    """Read back every SSD chunk and return the complete file SHA-256."""
    local_size = os.fstat(path).st_size if isinstance(path, int) else os.path.getsize(path)
    if local_size != file_size:
        raise IncompleteRange("local file size differs from declared Telegram size")

    whole_digest = hashlib.sha256()
    covered = 0
    local_file = (
        os.fdopen(os.dup(path), "rb")
        if isinstance(path, int)
        else open(path, "rb")
    )
    with local_file:
        local_file.seek(0)
        for offset, (length, expected_digest) in sorted(records.items()):
            if offset != covered:
                raise IncompleteRange(
                    f"manifest gap from {covered} to {offset}"
                )
            data = local_file.read(length)
            if len(data) != length:
                raise IncompleteRange(f"SSD chunk at {offset} is short")
            actual_digest = hashlib.sha256(data).hexdigest()
            if not hmac.compare_digest(actual_digest, expected_digest):
                raise HashMismatch(f"SSD chunk mismatch at offset {offset}")
            whole_digest.update(data)
            covered += length

    if covered != file_size:
        raise IncompleteRange(
            f"manifest covers {covered}/{file_size} bytes"
        )
    return whole_digest.hexdigest()


def _verify_file_hashes_sync(
    path: Union[str, os.PathLike, int],
    file_size: int,
    hashes: Collection[RemoteHash],
) -> IntegrityReport:
    if file_size <= 0:
        raise ValueError("file_size must be positive")
    local_size = os.fstat(path).st_size if isinstance(path, int) else os.path.getsize(path)
    if local_size != file_size:
        raise IncompleteRange("local file size differs from declared Telegram size")

    ordered = sorted(hashes, key=lambda item: (item.offset, item.limit))
    if not ordered:
        raise RemoteHashUnavailable("Telegram returned no file hashes")

    covered = 0
    local_file = (
        os.fdopen(os.dup(path), "rb")
        if isinstance(path, int)
        else open(path, "rb")
    )
    with local_file:
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
        method="telegram_file_hashes",
    )


async def _verify_fd_in_thread(verifier, fd: int, *args):
    """Run integrity work on a worker-owned duplicate before closing `fd`."""
    worker_fd = os.dup(fd)

    def run_verifier():
        try:
            return verifier(worker_fd, *args)
        finally:
            os.close(worker_fd)

    try:
        verifier_future = asyncio.get_running_loop().run_in_executor(
            None,
            run_verifier,
        )
    except BaseException:
        os.close(worker_fd)
        raise

    cancellation = None
    while True:
        try:
            await asyncio.shield(verifier_future)
            break
        except asyncio.CancelledError as error:
            cancellation = cancellation or error
            if verifier_future.done():
                break
        except BaseException:
            break

    try:
        result = verifier_future.result()
    except BaseException as error:
        if cancellation is not None:
            raise cancellation from error
        raise
    if cancellation is not None:
        raise cancellation
    return result


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
        verify_remote_hashes: bool = False,
        pool=None,
        stripe_size: int = 5 * CHUNK_SIZE,
        transfer_id: Optional[str] = None,
    ):
        if workers <= 0 or workers > 4:
            raise ValueError("workers must be between 1 and 4")
        if chunk_size <= 0 or max_attempts <= 0:
            raise ValueError("chunk_size and max_attempts must be positive")
        if abort_after_chunks is not None and abort_after_chunks <= 0:
            raise ValueError("abort_after_chunks must be positive")
        if stripe_size < CHUNK_SIZE or stripe_size % CHUNK_SIZE:
            raise ValueError("stripe_size must be a positive 1 MiB multiple")
        if pool is not None and stripe_size != 5 * CHUNK_SIZE:
            raise ValueError("pooled downloads require exactly 5 MiB stripes")
        if pool is not None and chunk_size != CHUNK_SIZE:
            raise ValueError("pooled downloads require 1 MiB manifest chunks")
        self.source = source
        self.workers = workers
        self.chunk_size = chunk_size
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.abort_after_chunks = abort_after_chunks
        # Mature clients use upload.getFileHashes for CDN verification, not as
        # a universal master-DC gate. Keep the latter opt-in for diagnostics.
        self.verify_remote_hashes = verify_remote_hashes
        self.pool = pool
        self.stripe_size = stripe_size
        self.transfer_id = transfer_id
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

    def _raise_durable_abort(
        self,
        fd: int,
        manifest: DownloadManifest,
    ) -> None:
        os.fsync(fd)
        synced_sidecars = manifest.checkpoint_and_sync()
        _fsync_directory(Path(manifest.db_path).parent)
        durability = AbortDurability(
            candidate_synced=True,
            manifest_checkpointed=True,
            manifest_synced=True,
            directory_synced=True,
            manifest_sidecars_synced=synced_sidecars,
        )
        raise InjectedAbort(
            f"aborted after {self._downloaded_chunks} chunks",
            durability=durability,
        )

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
            iterator = self.source.iter_range(
                start_offset,
                expected_length,
            )
            range_error = None
            try:
                async for data in iterator:
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
                        raise _InjectedAbortRequested(
                            "parallel download abort requested"
                        )

                if chunk_index < len(chunks):
                    raise IncompleteRange(
                        f"range at {start_offset} ended before all chunks arrived"
                    )
            except BaseException as error:
                range_error = error
                if isinstance(error, asyncio.CancelledError):
                    raise
                if isinstance(
                    error,
                    (_InjectedAbortRequested, IncompleteRange, HashMismatch),
                ):
                    raise
                if not isinstance(error, Exception):
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
            finally:
                try:
                    await _close_async_iterator(iterator)
                except BaseException:
                    if range_error is None:
                        raise

    async def _download_pooled_group(
        self,
        fd: int,
        manifest: DownloadManifest,
        chunks: List[ChunkSpec],
        file_size: int,
        progress,
        *,
        dc_id: int,
        transfer_id: str,
    ):
        chunk_index = 0
        attempts = 0
        while chunk_index < len(chunks):
            pending = chunks[chunk_index:]
            start_offset = pending[0].offset
            expected_length = sum(chunk.length for chunk in pending)
            lease = await self.pool.acquire(dc_id, transfer_id)
            try:
                async with lease:
                    if attempts:
                        self._retries += 1
                        self.pool.record_retry()
                    attempts += 1
                    self.pool.record_stripe_attempt()
                    iterator = self.source.iter_range_on_session(
                        lease.session,
                        start_offset,
                        expected_length,
                    )
                    try:
                        try:
                            async for data in iterator:
                                if chunk_index >= len(chunks):
                                    raise IncompleteRange(
                                        f"range at {start_offset} returned extra bytes"
                                    )
                                chunk = chunks[chunk_index]
                                if len(data) != chunk.length:
                                    raise IncompleteRange(
                                        f"chunk at {chunk.offset} expected "
                                        f"{chunk.length} bytes, got {len(data)}"
                                    )
                                await write_all_at(fd, chunk.offset, data)
                                manifest.mark_complete(
                                    chunk,
                                    hashlib.sha256(data).hexdigest(),
                                    attempts=attempts,
                                )
                                self.pool.record_committed(chunk.length)
                                chunk_index += 1
                                self._downloaded_chunks += 1
                                self._completed_bytes += chunk.length
                                await self._notify_progress(progress, file_size)
                                if (
                                    self.abort_after_chunks is not None
                                    and self._downloaded_chunks
                                    >= self.abort_after_chunks
                                ):
                                    raise _InjectedAbortRequested(
                                        "parallel download abort requested"
                                    )

                            if chunk_index < len(chunks):
                                raise IncompleteRange(
                                    f"range at {start_offset} ended before all "
                                    "chunks arrived"
                                )
                        finally:
                            await _close_async_iterator(iterator)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        if _is_fatal_session_error(error):
                            lease.mark_unhealthy()
                        elif (
                            _find_flood_wait(error) is None
                            and is_retryable_timeout(error)
                        ):
                            lease.mark_transport_failure()
                        raise
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if (
                    isinstance(error, (_InjectedAbortRequested, HashMismatch))
                    or _is_fatal_session_error(error)
                ):
                    raise

                flood_wait = _find_flood_wait(error)
                if flood_wait is not None:
                    wait_seconds = max(float(flood_wait.value), 0)
                    self.pool.pause_dc(dc_id, wait_seconds)
                    if chunk_index >= len(chunks):
                        return
                elif chunk_index >= len(chunks):
                    if is_retryable_timeout(error):
                        return
                    raise

                if attempts >= self.max_attempts:
                    raise
                if flood_wait is not None:
                    await self.sleep(wait_seconds)
                    continue
                if is_retryable_timeout(error):
                    await self.sleep(min(2 ** (attempts - 1), 8))
                    continue
                raise

    @staticmethod
    async def _gather_or_cancel(tasks):
        try:
            if tasks:
                await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
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
        *,
        expected_target_identity: Optional[FileIdentity] = None,
        expected_manifest_identity: Optional[FileIdentity] = None,
    ) -> ParallelDownloadResult:
        """Own optional range-source setup and cleanup for one download."""
        if self.pool is not None:
            return await self._download_prepared(
                identity,
                part_path,
                progress,
                expected_target_identity=expected_target_identity,
                expected_manifest_identity=expected_manifest_identity,
            )

        prepare = getattr(self.source, "prepare", None)
        close = getattr(self.source, "close", None)
        primary_error = None

        if prepare is not None:
            await prepare(self.workers)
        try:
            return await self._download_prepared(
                identity,
                part_path,
                progress,
                expected_target_identity=expected_target_identity,
                expected_manifest_identity=expected_manifest_identity,
            )
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
                    logger.exception(
                        "Failed to close parallel download range source"
                    )

    async def _download_prepared(
        self,
        identity: MediaIdentity,
        part_path: Union[str, os.PathLike],
        progress=None,
        *,
        expected_target_identity: Optional[FileIdentity] = None,
        expected_manifest_identity: Optional[FileIdentity] = None,
    ) -> ParallelDownloadResult:
        """Download and verify one candidate while retaining restart metadata."""
        started = time.monotonic()
        path = Path(part_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if expected_target_identity is None:
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        else:
            fd = _open_verified_regular_file(
                path,
                os.O_RDWR,
                0o600,
                expected_target_identity,
                "download candidate",
            )
        try:
            manifest = DownloadManifest(
                f"{path}.manifest.sqlite3",
                expected_file_identity=expected_manifest_identity,
            )
            manifest.prepare(identity, identity.file_size, self.chunk_size)
            os.ftruncate(fd, identity.file_size)
            completed = manifest.revalidate_completed(path, file_fd=fd)
            chunks = plan_chunks(identity.file_size, self.chunk_size)
            self._recovered_chunks = len(completed)
            self._completed_bytes = sum(
                length for length, _digest in completed.values()
            )
            try:
                if self.pool is None:
                    queues = split_missing_runs(
                        chunks,
                        completed.keys(),
                        self.workers,
                    )
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
                else:
                    transfer_id = self.transfer_id or identity.stable_key()
                    stripes = plan_missing_stripes(
                        chunks,
                        completed.keys(),
                        self.stripe_size,
                    )
                    async with self.pool.transfer(identity.dc_id, transfer_id):
                        tasks = [
                            asyncio.create_task(
                                self._download_pooled_group(
                                    fd,
                                    manifest,
                                    stripe,
                                    identity.file_size,
                                    progress,
                                    dc_id=identity.dc_id,
                                    transfer_id=transfer_id,
                                )
                            )
                            for stripe in stripes
                        ]
                        await self._gather_or_cancel(tasks)
            except _InjectedAbortRequested:
                self._raise_durable_abort(fd, manifest)

            final_records = manifest.completed_chunks()
            expected = {chunk.offset: chunk.length for chunk in chunks}
            actual = {
                offset: length
                for offset, (length, _digest) in final_records.items()
            }
            if actual != expected:
                raise IncompleteRange("manifest does not cover the declared file")
            os.fsync(fd)
            whole_digest = await _verify_fd_in_thread(
                _verify_manifest_and_hash_sync,
                fd,
                identity.file_size,
                final_records,
            )
            if self.verify_remote_hashes:
                remote_hashes = await collect_remote_hashes(
                    self.source,
                    identity.file_size,
                    sleep=self.sleep,
                )
                integrity = await _verify_fd_in_thread(
                    _verify_file_hashes_sync,
                    fd,
                    identity.file_size,
                    remote_hashes,
                )
            else:
                integrity = IntegrityReport(
                    verified=True,
                    covered_bytes=identity.file_size,
                    range_count=len(final_records),
                    mismatch_count=0,
                    method="mtproto_manifest_sha256",
                )
        finally:
            os.close(fd)
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
