"""Benchmark one immutable production record through the global media pool."""

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

from module.media_session_pool import (
    GlobalMediaSessionPool,
    KurigramMediaSessionFactory,
    MediaSessionPoolConfig,
)
from module.parallel_downloader import (
    CHUNK_SIZE,
    DownloadManifest,
    InjectedAbort,
    KurigramRangeSource,
    MediaIdentity,
    ParallelDownloader,
)
from module.pyrogram_extension import HookClient, set_max_concurrent_transmissions


MEDIA_ATTRIBUTES = (
    "audio",
    "document",
    "photo",
    "video",
    "voice",
    "video_note",
    "animation",
    "sticker",
)

MOUNT_ISOLATION_REQUIREMENT = (
    "production downloads, config, records, and sessions must be mounted "
    "read-only, stdin must be closed, and benchmark output must be a "
    "physically separate writable mount; an external timeout is also required"
)

REPORT_SCHEMA_VERSION = 2
RESUME_REPORT_PATTERN = re.compile(r"resume-report-(\d{4})\.json\Z")


@dataclass(frozen=True)
class BenchmarkSample:
    """One exact successful download record used as the immutable baseline."""

    chat_id: str
    message_id: int
    save_path: str
    file_name: str
    media_type: str
    file_size: int
    file_unique_id: str = ""
    media_id: Optional[int] = None
    dc_id: Optional[int] = None


@dataclass(frozen=True)
class BenchmarkOutputPaths:
    """Unique paths reserved below the explicit benchmark output root."""

    run_dir: Path
    candidate_path: Path
    manifest_path: Path
    report_path: Optional[Path]
    prior_report_path: Optional[Path] = None


@dataclass(frozen=True)
class ArtifactIdentity:
    """Pinned device and inode for one private benchmark artifact."""

    device: int
    inode: int

    def as_tuple(self):
        return (self.device, self.inode)


@dataclass(frozen=True)
class DownloadArtifactIdentities:
    """Pinned identities passed through the downloader candidate boundary."""

    candidate: ArtifactIdentity
    manifest: ArtifactIdentity


@dataclass(frozen=True)
class ResumeContext:
    """Validated durable state and exact parent evidence for one resume."""

    artifacts: DownloadArtifactIdentities
    prior_report_path: Path
    prior_report_identity: ArtifactIdentity
    prior_report: dict
    media_identity: dict
    recovered_chunks: int
    recovered_bytes: int
    total_chunks: int
    provenance_verified: bool = True


class _RecoveryGateState(Enum):
    PARENT_SELECTED = "parent_selected"
    PARENT_VERIFIED = "parent_verified"
    REPORT_RESERVED = "report_reserved"
    PERSISTED = "persisted"


class _RecoveryGate:
    """Own parent validation, one report inode, and its final persistence."""

    def __init__(self, paths: BenchmarkOutputPaths, *, resume: bool):
        self.paths = paths
        self.resume = bool(resume)
        self.state = _RecoveryGateState.PARENT_SELECTED
        self.context = None
        self.report_identity = None

    def verify_parent(self, sample: BenchmarkSample) -> ResumeContext:
        if not self.resume or self.state is not _RecoveryGateState.PARENT_SELECTED:
            raise RuntimeError("recovery parent cannot be verified in this state")
        self.context = _validate_resume_provenance(self.paths, sample)
        self.state = _RecoveryGateState.PARENT_VERIFIED
        return self.context

    def reserve_report(self) -> BenchmarkOutputPaths:
        if self.resume:
            if self.state is not _RecoveryGateState.PARENT_VERIFIED:
                raise RuntimeError("resume report requires a verified parent")
            report_path, identity = _reserve_next_resume_report(
                self.paths.run_dir
            )
            self.paths = replace(self.paths, report_path=report_path)
        else:
            if self.state is not _RecoveryGateState.PARENT_SELECTED:
                raise RuntimeError("fresh report cannot be reserved in this state")
            if self.paths.report_path is None:
                raise RuntimeError("fresh report path is missing")
            identity = _reserve_report_path(self.paths.report_path)
        self.report_identity = identity
        self.state = _RecoveryGateState.REPORT_RESERVED
        return self.paths

    def persist(self, report: dict) -> ArtifactIdentity:
        if (
            self.state is not _RecoveryGateState.REPORT_RESERVED
            or self.report_identity is None
            or self.paths.report_path is None
        ):
            raise RuntimeError("recovery report is not owned by this invocation")
        final_identity = _write_reserved_report_atomic(
            report,
            self.paths.report_path,
            self.report_identity,
        )
        self.report_identity = final_identity
        self.state = _RecoveryGateState.PERSISTED
        return final_identity


class _NonInteractiveHookClient(HookClient):
    """Fail closed if a future lifecycle path attempts authorization."""

    async def authorize(self):
        raise PermissionError("benchmark authorization is non-interactive")

    async def authorize_qr(self, *args, **kwargs):
        del args, kwargs
        raise PermissionError("benchmark authorization is non-interactive")


class _PoolEvidence:
    """Collect committed bytes and high-water pool snapshots during a run."""

    def __init__(self, pool):
        self.pool = pool
        self.committed_bytes = 0
        self.peak_live = 0
        self.peak_by_dc = {}
        self.final_snapshot = {}

    def sample(self) -> dict:
        snapshot = _snapshot_dict(self.pool.snapshot())
        self.final_snapshot = snapshot
        self.peak_live = max(self.peak_live, int(snapshot.get("live", 0) or 0))
        for dc_id, raw_counts in (snapshot.get("by_dc") or {}).items():
            counts = self.peak_by_dc.setdefault(str(dc_id), {})
            for name, raw_value in raw_counts.items():
                value = int(raw_value or 0)
                counts[name] = max(int(counts.get(name, 0)), value)
        return snapshot

    def progress(self, current: int, _total: int) -> None:
        self.committed_bytes = max(self.committed_bytes, int(current))
        self.sample()

    def report_snapshot(self) -> dict:
        self.sample()
        return {
            "peak_live": self.peak_live,
            "peak_by_dc": self.peak_by_dc,
            "final": self.final_snapshot,
        }


class _LeaseLifecycleState(Enum):
    ACQUIRED = "acquired"
    REGISTERED = "registered"
    ENTERED = "entered"
    QUARANTINED = "quarantined"
    RELEASED = "released"


class _FaultGateState(Enum):
    ARMED = "armed"
    QUARANTINED = "quarantined"
    SESSION_STOPPED = "session_stopped"
    REPLACEMENT_COMPLETED = "replacement_completed"


@dataclass(eq=False)
class _SessionAudit:
    session: object
    identifier: str
    quarantined: bool = False
    leases: set = field(default_factory=set)


@dataclass(frozen=True)
class _RangeAttempt:
    audit: _SessionAudit
    stripe: dict
    attempt: int
    replacement_candidate: bool


class _InjectedLease:
    """Own one underlying lease through an explicit shielded lifecycle."""

    def __init__(self, injector, lease):
        self._injector = injector
        self._lease = lease
        self._audit = None
        self._state = _LeaseLifecycleState.ACQUIRED
        self._underlying_entered = False
        self._cleanup_task = None

    @property
    def state(self) -> _LeaseLifecycleState:
        return self._state

    @property
    def session(self):
        return self._lease.session

    def mark_unhealthy(self) -> None:
        self._lease.mark_unhealthy()

    def mark_transport_failure(self) -> None:
        self._lease.mark_transport_failure()

    async def _cleanup_body(self, exit_args):
        unregister_error = None
        release_error = None
        result = None
        try:
            await self._injector._unregister(self)
        except BaseException as error:
            unregister_error = error
        try:
            try:
                if self._underlying_entered:
                    result = await self._lease.__aexit__(*exit_args)
                else:
                    release = getattr(self._lease, "release", None)
                    if release is not None:
                        await release()
                    else:
                        result = await self._lease.__aexit__(None, None, None)
            except BaseException as error:
                release_error = error
        finally:
            async with self._injector._lock:
                self._state = _LeaseLifecycleState.RELEASED
        if unregister_error is not None:
            if release_error is not None:
                self._injector._lease_cleanup_errors.append(
                    _error_text(release_error)
                )
                raise unregister_error from release_error
            raise unregister_error
        if release_error is not None:
            raise release_error
        return result

    async def _finish_cleanup(self, primary_error=None, exit_args=None):
        if exit_args is None:
            exit_args = (None, None, None)
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(
                self._cleanup_body(exit_args)
            )
        cleanup_task = self._cleanup_task
        cancellation = None
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
            except BaseException:
                break
        cleanup_error = None
        result = None
        try:
            result = cleanup_task.result()
        except BaseException as error:
            cleanup_error = error
            self._injector._lease_cleanup_errors.append(_error_text(error))
        if primary_error is not None:
            return result
        if cancellation is not None:
            if cleanup_error is not None:
                raise cancellation from cleanup_error
            raise cancellation
        if cleanup_error is not None:
            raise cleanup_error
        return result

    async def __aenter__(self):
        try:
            if self._state is _LeaseLifecycleState.ACQUIRED:
                if not await self._injector._register(self):
                    self.mark_unhealthy()
                    raise ConnectionResetError(
                        "quarantined benchmark session was rejected"
                    )
            if self._state is not _LeaseLifecycleState.REGISTERED:
                raise RuntimeError("benchmark lease is not registered")
            await self._lease.__aenter__()
            self._underlying_entered = True
            if not await self._injector._enter(self):
                self.mark_unhealthy()
                raise ConnectionResetError(
                    "quarantined benchmark session was rejected"
                )
            return self
        except BaseException as error:
            await self._finish_cleanup(error)
            raise

    async def __aexit__(self, error_type, error, traceback):
        return await self._finish_cleanup(
            error,
            (error_type, error, traceback),
        )

    async def release(self) -> None:
        await self._finish_cleanup()

    async def reject(self) -> None:
        try:
            self.mark_unhealthy()
        except BaseException as error:
            self._injector._lease_cleanup_errors.append(_error_text(error))
            await self._finish_cleanup(error)
            raise
        await self._finish_cleanup()


class _FaultInjectingPool:
    def __init__(self, pool, injector):
        self._pool = pool
        self._injector = injector

    async def acquire(self, dc_id: int, transfer_id: str):
        while True:
            lease = await self._pool.acquire(dc_id, transfer_id)
            wrapped = self._injector.wrap_lease(lease)
            try:
                accepted = await self._injector._register(wrapped)
            except BaseException as error:
                await wrapped._finish_cleanup(error)
                raise
            if accepted:
                return wrapped
            await wrapped.reject()

    def __getattr__(self, name):
        return getattr(self._pool, name)


class _LeasedConnectionFailureInjector:
    """Own lease quarantine and exact replacement-completion evidence."""

    def __init__(self, source):
        self._source = source
        self._lock = asyncio.Lock()
        self._session_audits = []
        self._next_session_id = 1
        self._range_attempts = {}
        self._failed_audit = None
        self._failed_session_id = ""
        self._failed_stripe = {}
        self._failed_attempt = 0
        self._failed_resume_offset = None
        self._state = _FaultGateState.ARMED
        self._triggered = False
        self._terminated_connections = 0
        self._replacement_session_observed = False
        self._replacement_session_id = ""
        self._replacement_stripe = {}
        self._replacement_attempt = 0
        self._correlated_replacements = 0
        self._stop_error = ""
        self._iterator_cleanup_errors = []
        self._lease_cleanup_errors = []

    def __getattr__(self, name):
        return getattr(self._source, name)

    def wrap_pool(self, pool):
        return _FaultInjectingPool(pool, self)

    def wrap_lease(self, lease):
        return _InjectedLease(self, lease)

    def _session_audit_locked(self, session) -> _SessionAudit:
        for audit in self._session_audits:
            if audit.session is session:
                return audit
        audit = _SessionAudit(
            session=session,
            identifier=f"session-{self._next_session_id:04d}",
        )
        self._next_session_id += 1
        self._session_audits.append(audit)
        return audit

    def _session_identifier_locked(self, session) -> str:
        return self._session_audit_locked(session).identifier

    async def _register(self, lease: _InjectedLease) -> bool:
        async with self._lock:
            if lease.state is _LeaseLifecycleState.RELEASED:
                raise RuntimeError("released benchmark lease cannot register")
            audit = self._session_audit_locked(lease.session)
            lease._audit = audit
            if audit.quarantined:
                lease._state = _LeaseLifecycleState.QUARANTINED
                return False
            audit.leases.add(lease)
            lease._state = _LeaseLifecycleState.REGISTERED
            return True

    async def _unregister(self, lease: _InjectedLease) -> None:
        async with self._lock:
            if lease._audit is not None:
                lease._audit.leases.discard(lease)

    async def _enter(self, lease: _InjectedLease) -> bool:
        async with self._lock:
            audit = lease._audit or self._session_audit_locked(lease.session)
            lease._audit = audit
            if audit.quarantined:
                lease._state = _LeaseLifecycleState.QUARANTINED
                return False
            if lease not in audit.leases:
                raise RuntimeError("benchmark lease lost registration ownership")
            lease._state = _LeaseLifecycleState.ENTERED
            return True

    async def _lease_can_enter(self, lease: _InjectedLease) -> bool:
        async with self._lock:
            audit = lease._audit or self._session_audit_locked(lease.session)
            return not audit.quarantined

    async def _begin_range(
        self,
        session,
        start_offset: int,
        expected_length: int,
    ) -> _RangeAttempt:
        stripe = {
            "start_offset": int(start_offset),
            "expected_length": int(expected_length),
            "end_offset": int(start_offset) + int(expected_length),
        }
        async with self._lock:
            end_offset = stripe["end_offset"]
            attempt = self._range_attempts.get(end_offset, 0) + 1
            self._range_attempts[end_offset] = attempt
            audit = self._session_audit_locked(session)
            replacement_candidate = bool(
                self._triggered
                and not self._replacement_session_observed
                and audit is not self._failed_audit
                and end_offset == self._failed_stripe.get("end_offset")
                and stripe["start_offset"] == self._failed_resume_offset
                and attempt > self._failed_attempt
            )
        return _RangeAttempt(
            audit=audit,
            stripe=stripe,
            attempt=attempt,
            replacement_candidate=replacement_candidate,
        )

    async def _complete_range(
        self,
        range_attempt: _RangeAttempt,
        completed_bytes: int,
    ) -> None:
        if (
            not range_attempt.replacement_candidate
            or completed_bytes != range_attempt.stripe["expected_length"]
        ):
            return
        async with self._lock:
            if self._replacement_session_observed:
                return
            if range_attempt.audit is self._failed_audit:
                return
            self._replacement_session_observed = True
            self._replacement_session_id = range_attempt.audit.identifier
            self._replacement_stripe = dict(range_attempt.stripe)
            self._replacement_attempt = range_attempt.attempt
            self._correlated_replacements = 1
            self._state = _FaultGateState.REPLACEMENT_COMPLETED

    async def _quarantine(
        self,
        session,
        stripe: dict,
        attempt: int,
        resume_offset: int,
    ) -> bool:
        async with self._lock:
            if self._triggered:
                return False
            audit = self._session_audit_locked(session)
            self._triggered = True
            self._failed_audit = audit
            self._failed_session_id = audit.identifier
            self._failed_stripe = dict(stripe)
            self._failed_attempt = int(attempt)
            self._failed_resume_offset = int(resume_offset)
            self._state = _FaultGateState.QUARANTINED
            audit.quarantined = True
            leases = tuple(audit.leases)
            for lease in leases:
                lease._state = _LeaseLifecycleState.QUARANTINED
        mark_error = None
        for lease in leases:
            try:
                lease.mark_unhealthy()
            except BaseException as error:
                self._lease_cleanup_errors.append(_error_text(error))
                mark_error = mark_error or error
        if mark_error is not None:
            raise mark_error
        return True

    async def _has_active_lease(self, session) -> bool:
        async with self._lock:
            return bool(self._session_audit_locked(session).leases)

    async def _record_stopped_session(self) -> None:
        async with self._lock:
            self._terminated_connections += 1
            if self._state is not _FaultGateState.REPLACEMENT_COMPLETED:
                self._state = _FaultGateState.SESSION_STOPPED

    async def _close_iterator(self, iterator, primary_error) -> None:
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
            self._iterator_cleanup_errors.append(_error_text(error))
        if primary_error is not None:
            return
        if cancellation is not None:
            if close_error is not None:
                raise cancellation from close_error
            raise cancellation
        if close_error is not None:
            raise close_error

    async def iter_range_on_session(
        self,
        session,
        start_offset: int,
        expected_length: int,
    ):
        range_attempt = await self._begin_range(
            session,
            start_offset,
            expected_length,
        )
        iterator = self._source.iter_range_on_session(
            session,
            start_offset,
            expected_length,
        )
        primary_error = None
        completed = False
        completed_bytes = 0
        try:
            async for chunk in iterator:
                completed_bytes += len(chunk)
                yield chunk
                if self._triggered:
                    continue
                if not await self._has_active_lease(session):
                    raise RuntimeError(
                        "leased connection failure injector has no active lease"
                    )
                quarantined = await self._quarantine(
                    session,
                    range_attempt.stripe,
                    range_attempt.attempt,
                    int(start_offset) + len(chunk),
                )
                if not quarantined:
                    continue
                try:
                    await session.stop()
                except Exception as error:
                    self._stop_error = _error_text(error)
                    raise ConnectionResetError(
                        "injected leased connection stop failed"
                    ) from error
                await self._record_stopped_session()
                raise ConnectionResetError("injected leased connection failure")
            completed = True
        except BaseException as error:
            primary_error = error
            raise
        finally:
            await self._close_iterator(iterator, primary_error)
            if completed and primary_error is None:
                await self._complete_range(range_attempt, completed_bytes)

    def evidence(self) -> dict:
        return {
            "requested": True,
            "state": self._state.value,
            "triggered": self._triggered,
            "terminated_connections": self._terminated_connections,
            "replacement_session_observed": self._replacement_session_observed,
            "failed_session_id": self._failed_session_id,
            "failed_stripe": dict(self._failed_stripe),
            "failed_attempt": self._failed_attempt,
            "replacement_session_id": self._replacement_session_id,
            "replacement_stripe": dict(self._replacement_stripe),
            "replacement_attempt": self._replacement_attempt,
            "correlated_replacements": self._correlated_replacements,
            "stop_error": self._stop_error,
            "iterator_cleanup_errors": list(self._iterator_cleanup_errors),
            "lease_cleanup_errors": list(self._lease_cleanup_errors),
        }


def _session_target(value: str) -> int:
    target = int(value)
    if not 1 <= target <= 48:
        raise argparse.ArgumentTypeError("session target must be between 1 and 48")
    return target


def _positive_timeout(value: str) -> float:
    timeout = float(value)
    if timeout <= 0:
        raise argparse.ArgumentTypeError("start timeout must be positive")
    return timeout


def _positive_count(value: str) -> int:
    count = int(value)
    if count <= 0:
        raise argparse.ArgumentTypeError("chunk count must be positive")
    return count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark one exact record without mutating production state."
    )
    parser.add_argument("--config", default="/app/config.yaml")
    parser.add_argument("--records", default="/app/state/download_records.sqlite3")
    parser.add_argument("--downloads-root", default="/app/downloads")
    parser.add_argument("--sessions", default="")
    parser.add_argument("--chat-id", required=True)
    parser.add_argument("--message-id", required=True, type=int)
    parser.add_argument("--output", required=True)
    parser.add_argument("--session-target", required=True, type=_session_target)
    parser.add_argument("--pipeline-depth", required=True, type=int, choices=(1, 2))
    parser.add_argument("--start-timeout", type=_positive_timeout)
    parser.add_argument(
        "--resume-candidate",
        default="",
        help="resume the exact candidate.part and adjacent manifest in output",
    )
    parser.add_argument(
        "--resume-report",
        default="",
        help="exact private abort report that authorizes this resume attempt",
    )
    parser.add_argument("--abort-after-chunks", type=_positive_count)
    parser.add_argument(
        "--inject-leased-connection-failure",
        action="store_true",
        help="stop one active leased session after its first committed chunk",
    )
    return parser


def _open_records_read_only(path: Path) -> sqlite3.Connection:
    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    return sqlite3.connect(uri, uri=True)


def _select_successful_record(
    connection: sqlite3.Connection,
    chat_id: str,
    message_id: int,
) -> BenchmarkSample:
    available_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(download_records)").fetchall()
    }
    optional_columns = [
        name
        for name in ("file_unique_id", "media_id", "dc_id")
        if name in available_columns
    ]
    selected_columns = ", ".join(
        [
            "chat_id",
            "message_id",
            "save_path",
            "file_name",
            "media_type",
            "file_size",
        ]
        + optional_columns
    )
    cursor = connection.execute(
        f"""
        SELECT {selected_columns}
        FROM download_records
        WHERE chat_id = ?
          AND message_id = ?
          AND status = 'success'
        """,
        (str(chat_id), int(message_id)),
    )
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError(
            "expected exactly one successful download record for "
            f"chat {chat_id} message {message_id}; found {len(rows)}"
        )
    row = dict(zip(columns, rows[0]))
    save_path = str(row["save_path"] or "")
    file_size = int(row["file_size"] or 0)
    if not save_path or file_size <= 0:
        raise ValueError("successful download record has no usable path or size")
    return BenchmarkSample(
        chat_id=str(row["chat_id"]),
        message_id=int(row["message_id"]),
        save_path=save_path,
        file_name=str(row["file_name"] or ""),
        media_type=str(row["media_type"] or ""),
        file_size=file_size,
        file_unique_id=str(row.get("file_unique_id") or ""),
        media_id=(
            int(row["media_id"])
            if row.get("media_id") is not None
            else None
        ),
        dc_id=(int(row["dc_id"]) if row.get("dc_id") is not None else None),
    )


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _paths_overlap(first: Path, second: Path) -> bool:
    return _is_within(first, second) or _is_within(second, first)


def _validate_protected_output(
    output_root: Path,
    protected_paths: Mapping[str, Path],
) -> Path:
    """Reject every parent/child overlap before creating benchmark output."""
    output = Path(os.path.abspath(os.fspath(output_root)))
    if output.is_symlink():
        raise ValueError("benchmark output must not be a protected-path symlink")
    output_resolved = output.resolve(strict=False)
    for name, raw_path in protected_paths.items():
        protected = Path(os.path.abspath(os.fspath(raw_path)))
        protected_resolved = protected.resolve(strict=False)
        if _paths_overlap(output, protected) or _paths_overlap(
            output_resolved,
            protected_resolved,
        ):
            raise ValueError(
                f"benchmark output overlaps protected {name} path: {protected}"
            )
    return output_resolved


def _mount_details(path: Path) -> dict:
    caller_path = Path(path)
    caller_metadata = caller_path.lstat()
    if stat.S_ISLNK(caller_metadata.st_mode):
        raise ValueError(f"mount path must not be a symlink: {caller_path}")
    resolved = caller_path.resolve(strict=True)
    metadata = resolved.stat()
    filesystem = os.statvfs(resolved)
    read_only = bool(filesystem.f_flag & getattr(os, "ST_RDONLY", 1))
    return {
        "path": str(resolved),
        "device": int(metadata.st_dev),
        "read_only": read_only,
        "writable": bool(os.access(resolved, os.W_OK) and not read_only),
    }


def _validate_mount_isolation(
    output_root: Path,
    protected_paths: Mapping[str, Path],
) -> dict:
    """Require the Task 9 kernel isolation before reserving output."""
    output = _mount_details(output_root)
    if not Path(output["path"]).is_dir():
        raise ValueError("benchmark output mount must already exist as a directory")
    protected = {
        name: _mount_details(path) for name, path in protected_paths.items()
    }
    protected_read_only = all(item["read_only"] for item in protected.values())
    separate_output_device = all(
        item["device"] != output["device"] for item in protected.values()
    )
    evidence = {
        "verified": bool(
            output["writable"] and protected_read_only and separate_output_device
        ),
        "separate_output_device": separate_output_device,
        "protected_read_only": protected_read_only,
        "output": output,
        "protected": protected,
        "requirement": MOUNT_ISOLATION_REQUIREMENT,
    }
    if not output["writable"]:
        raise ValueError("benchmark output mount must be writable")
    if not protected_read_only:
        raise ValueError("all protected production mounts must be read-only")
    if not separate_output_device:
        raise ValueError(
            "benchmark output must use a physically separate filesystem device"
        )
    return evidence


def _safe_component(value: str) -> str:
    safe = "".join(
        character
        if character in "-_.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        else "_"
        for character in value
    )
    return safe or "sample"


def _reserve_output_paths(
    output_root: Path,
    downloads_root: Path,
    chat_id: str,
    message_id: int,
    session_target: int,
    pipeline_depth: int,
) -> BenchmarkOutputPaths:
    output = Path(output_root).resolve()
    downloads = Path(downloads_root).resolve()
    if _is_within(output, downloads):
        raise ValueError("benchmark output must be outside downloads root")
    output.mkdir(parents=True, exist_ok=True)
    prefix = (
        f"{_safe_component(str(chat_id))}-{int(message_id)}-"
        f"s{int(session_target)}-p{int(pipeline_depth)}-"
    )
    run_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=output)).resolve()
    os.chmod(run_dir, 0o700)
    if not _is_within(run_dir, output):
        raise ValueError("reserved benchmark path escaped output root")
    return BenchmarkOutputPaths(
        run_dir=run_dir,
        candidate_path=run_dir / "candidate.part",
        manifest_path=run_dir / "candidate.part.manifest.sqlite3",
        report_path=run_dir / "report.json",
    )


def _resume_output_paths(
    output_root: Path,
    downloads_root: Path,
    candidate_path: Path,
    prior_report_path: Path,
) -> BenchmarkOutputPaths:
    """Select one existing private candidate without resolving artifact links."""
    output = Path(output_root).resolve(strict=True)
    downloads = Path(downloads_root).resolve(strict=True)
    if _is_within(output, downloads):
        raise ValueError("benchmark output must be outside downloads root")

    requested = Path(os.path.abspath(os.fspath(candidate_path)))
    if requested.name != "candidate.part":
        raise ValueError("resume candidate must be named candidate.part")
    raw_run_dir = requested.parent
    run_metadata = raw_run_dir.lstat()
    if stat.S_ISLNK(run_metadata.st_mode) or not stat.S_ISDIR(run_metadata.st_mode):
        raise ValueError("resume run directory must be a real directory")
    run_dir = raw_run_dir.resolve(strict=True)
    if run_dir == output or not _is_within(run_dir, output):
        raise ValueError("resume candidate must be beneath benchmark output")
    if stat.S_IMODE(run_metadata.st_mode) != 0o700:
        raise ValueError("resume run directory must remain private mode 0700")

    candidate = run_dir / "candidate.part"
    if requested != candidate:
        raise ValueError("resume candidate path changed during resolution")

    requested_report = Path(os.path.abspath(os.fspath(prior_report_path)))
    if requested_report.parent != raw_run_dir:
        raise ValueError("resume report must be adjacent to the candidate")
    try:
        report_metadata = requested_report.lstat()
    except FileNotFoundError as error:
        raise ValueError("resume report is missing") from error
    if stat.S_ISLNK(report_metadata.st_mode):
        raise ValueError("resume report must not be a symlink")
    if (
        requested_report.name != "report.json"
        and RESUME_REPORT_PATTERN.fullmatch(requested_report.name) is None
    ):
        raise ValueError("resume report name is not an explicit sequence")
    return BenchmarkOutputPaths(
        run_dir=run_dir,
        candidate_path=candidate,
        manifest_path=run_dir / "candidate.part.manifest.sqlite3",
        report_path=None,
        prior_report_path=requested_report,
    )


def _open_artifact_no_follow(path: Path, flags: int, mode: int = 0o600) -> int:
    return os.open(path, flags | getattr(os, "O_NOFOLLOW", 0), mode)


def _artifact_identity_from_fd(
    fd: int,
    label: str,
    *,
    require_private: bool = True,
) -> ArtifactIdentity:
    metadata = os.fstat(fd)
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"benchmark {label} is not a regular file")
    if metadata.st_nlink != 1:
        raise ValueError(f"benchmark {label} is a hardlink")
    if require_private and stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ValueError(f"benchmark {label} must remain private mode 0600")
    return ArtifactIdentity(int(metadata.st_dev), int(metadata.st_ino))


def _reserve_artifact(path: Path, label: str) -> ArtifactIdentity:
    try:
        fd = _open_artifact_no_follow(
            path,
            os.O_RDWR | os.O_CREAT | os.O_EXCL,
        )
    except FileExistsError as error:
        raise ValueError(f"benchmark {label} path already exists") from error
    try:
        identity = _artifact_identity_from_fd(fd, label)
        os.fsync(fd)
        return identity
    finally:
        os.close(fd)


def _reserve_report_path(path: Path) -> ArtifactIdentity:
    return _reserve_artifact(Path(path), "report")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(path, flags)
    try:
        if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
            raise ValueError(f"benchmark path is not a directory: {path}")
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _reserve_next_resume_report(
    run_dir: Path,
) -> tuple[Path, ArtifactIdentity]:
    """Atomically own the lowest unused sequence, ignoring parent numbering."""
    root = Path(run_dir)
    for sequence in range(1, 10000):
        report_path = root / f"resume-report-{sequence:04d}.json"
        try:
            fd = _open_artifact_no_follow(
                report_path,
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
            )
        except FileExistsError:
            continue
        try:
            identity = _artifact_identity_from_fd(fd, "report")
            os.fsync(fd)
        finally:
            os.close(fd)
        _fsync_directory(root)
        return report_path, identity
    raise ValueError("resume report sequence is exhausted")


def _verify_artifact(
    path: Path,
    expected: ArtifactIdentity,
    label: str,
) -> int:
    fd = _open_artifact_no_follow(path, os.O_RDONLY)
    try:
        actual = _artifact_identity_from_fd(
            fd,
            label,
            require_private=False,
        )
        if actual != expected:
            raise ValueError(f"benchmark {label} identity changed")
        if stat.S_IMODE(os.fstat(fd).st_mode) != 0o600:
            raise ValueError(f"benchmark {label} must remain private mode 0600")
        path_metadata = path.lstat()
        if stat.S_ISLNK(path_metadata.st_mode):
            raise ValueError(f"benchmark {label} is a symlink")
        if (path_metadata.st_dev, path_metadata.st_ino) != actual.as_tuple():
            raise ValueError(f"benchmark {label} identity changed")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _write_reserved_report_atomic(
    report: dict,
    report_path: Path,
    expected: ArtifactIdentity,
) -> ArtifactIdentity:
    """Durably replace only the report inode reserved by this invocation."""
    path = Path(report_path)
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    temp_fd = None
    temp_name = None
    try:
        temp_fd, temp_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        os.fchmod(temp_fd, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(temp_fd, view)
            view = view[written:]
        os.fsync(temp_fd)
        os.close(temp_fd)
        temp_fd = None

        reserved_fd = _verify_artifact(path, expected, "report")
        os.close(reserved_fd)
        os.replace(temp_name, path)
        temp_name = None
        final_fd = _open_artifact_no_follow(path, os.O_RDONLY)
        try:
            final_identity = _artifact_identity_from_fd(final_fd, "report")
        finally:
            os.close(final_fd)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return final_identity
    finally:
        if temp_fd is not None:
            os.close(temp_fd)
        if temp_name is not None:
            Path(temp_name).unlink(missing_ok=True)


def _load_private_json_report(path: Path) -> tuple[dict, ArtifactIdentity]:
    report_path = Path(path)
    try:
        fd = _open_artifact_no_follow(report_path, os.O_RDONLY)
    except FileNotFoundError as error:
        raise ValueError("resume report is missing") from error
    try:
        identity = _artifact_identity_from_fd(fd, "resume report")
        metadata = os.fstat(fd)
        if metadata.st_size <= 0 or metadata.st_size > 4 * 1024 * 1024:
            raise ValueError("resume report has an invalid size")
        path_metadata = report_path.lstat()
        if (path_metadata.st_dev, path_metadata.st_ino) != identity.as_tuple():
            raise ValueError("resume report identity changed")
        payload = bytearray()
        while True:
            block = os.read(fd, 64 * 1024)
            if not block:
                break
            payload.extend(block)
    finally:
        os.close(fd)
    try:
        report = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("resume report is not valid JSON") from error
    if not isinstance(report, dict):
        raise ValueError("resume report must contain a JSON object")
    return report, identity


def _reserve_download_artifacts(
    paths: BenchmarkOutputPaths,
) -> DownloadArtifactIdentities:
    candidate = _reserve_artifact(paths.candidate_path, "candidate")
    try:
        manifest = _reserve_artifact(paths.manifest_path, "manifest")
    except BaseException:
        paths.candidate_path.unlink(missing_ok=True)
        raise
    return DownloadArtifactIdentities(candidate=candidate, manifest=manifest)


def _load_artifact_identity(path: Path, label: str) -> ArtifactIdentity:
    try:
        fd = _open_artifact_no_follow(path, os.O_RDWR)
    except FileNotFoundError as error:
        raise ValueError(f"benchmark {label} is missing") from error
    try:
        identity = _artifact_identity_from_fd(fd, label)
        path_metadata = path.lstat()
        if stat.S_ISLNK(path_metadata.st_mode):
            raise ValueError(f"benchmark {label} is a symlink")
        if (path_metadata.st_dev, path_metadata.st_ino) != identity.as_tuple():
            raise ValueError(f"benchmark {label} identity changed")
        return identity
    finally:
        os.close(fd)


def _load_download_artifacts(
    paths: BenchmarkOutputPaths,
) -> DownloadArtifactIdentities:
    """Re-pin an existing same-path candidate and manifest for resume."""
    candidate = _load_artifact_identity(paths.candidate_path, "candidate")
    manifest = _load_artifact_identity(paths.manifest_path, "manifest")
    _verify_manifest_artifacts(paths.manifest_path, manifest)
    return DownloadArtifactIdentities(candidate=candidate, manifest=manifest)


def _artifact_identity_from_report(value, label: str) -> ArtifactIdentity:
    if not isinstance(value, dict):
        raise ValueError(f"resume report {label} identity is missing")
    try:
        identity = ArtifactIdentity(
            device=int(value["device"]),
            inode=int(value["inode"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"resume report {label} identity is invalid") from error
    if identity.device <= 0 or identity.inode <= 0:
        raise ValueError(f"resume report {label} identity is invalid")
    return identity


def _media_identity_payload(identity: MediaIdentity) -> dict:
    return {**asdict(identity), "stable_key": identity.stable_key()}


def _total_chunks(file_size: int) -> int:
    return (int(file_size) + CHUNK_SIZE - 1) // CHUNK_SIZE


def _inspect_durable_partial(
    paths: BenchmarkOutputPaths,
    artifacts: DownloadArtifactIdentities,
    media_identity: Mapping,
) -> dict:
    """Read and hash committed manifest rows through pinned private files."""
    candidate_fd = _verify_artifact(
        paths.candidate_path,
        artifacts.candidate,
        "candidate",
    )
    manifest_fd = _verify_artifact(
        paths.manifest_path,
        artifacts.manifest,
        "manifest",
    )
    try:
        file_size = int(media_identity.get("file_size", 0) or 0)
        stable_key = str(media_identity.get("stable_key", "") or "")
        if file_size <= 0 or not stable_key:
            raise ValueError("resume report media identity is invalid")
        if os.fstat(candidate_fd).st_size != file_size:
            raise ValueError("resume candidate size differs from media identity")
        uri = f"{paths.manifest_path.resolve(strict=True).as_uri()}?mode=ro"
        try:
            with closing(sqlite3.connect(uri, uri=True)) as connection:
                meta = connection.execute(
                    """
                    SELECT identity_key, file_size, chunk_size
                    FROM download_meta
                    WHERE singleton = 1
                    """
                ).fetchone()
                rows = connection.execute(
                    """
                    SELECT offset, length, sha256
                    FROM completed_chunks
                    ORDER BY offset
                    """
                ).fetchall()
        except sqlite3.Error as error:
            raise ValueError("resume manifest is not a durable download manifest") from error
        if meta is None or (
            str(meta[0]),
            int(meta[1]),
            int(meta[2]),
        ) != (stable_key, file_size, CHUNK_SIZE):
            raise ValueError("resume manifest media identity differs from report")

        expected_lengths = {
            offset: min(CHUNK_SIZE, file_size - offset)
            for offset in range(0, file_size, CHUNK_SIZE)
        }
        durable_bytes = 0
        for raw_offset, raw_length, raw_digest in rows:
            offset = int(raw_offset)
            length = int(raw_length)
            digest = str(raw_digest)
            if expected_lengths.get(offset) != length or len(digest) != 64:
                raise ValueError("resume manifest has invalid committed chunks")
            data = os.pread(candidate_fd, length, offset)
            if len(data) != length or hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("resume candidate differs from durable manifest")
            durable_bytes += length

        verified_manifest_fd = _verify_artifact(
            paths.manifest_path,
            artifacts.manifest,
            "manifest",
        )
        os.close(verified_manifest_fd)
        return {
            "durable_chunks": len(rows),
            "durable_bytes": durable_bytes,
            "total_chunks": len(expected_lengths),
        }
    finally:
        os.close(manifest_fd)
        os.close(candidate_fd)


def _validate_resume_provenance(
    paths: BenchmarkOutputPaths,
    sample: BenchmarkSample,
) -> ResumeContext:
    """Validate the parent report before opening either resume artifact."""
    if paths.prior_report_path is None:
        raise ValueError("resume requires an explicit prior abort report")
    report, report_identity = _load_private_json_report(paths.prior_report_path)

    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        raise ValueError("resume report schema is unsupported")
    status = report.get("status")
    if status not in ("aborted", "failed", "interrupted"):
        raise ValueError("resume report does not prove an incomplete attempt")
    if report.get("incomplete") is not True:
        raise ValueError("resume report does not prove an incomplete attempt")
    if report.get("eligible") is not False:
        raise ValueError("resume report abort eligibility is invalid")
    if report.get("run_mode") not in ("fresh", "resume"):
        raise ValueError("resume report mode is invalid")

    expected_sample = asdict(sample)
    identity_matches = (
        str(report.get("chat_id")) == sample.chat_id
        and int(report.get("message_id", 0) or 0) == sample.message_id
        and report.get("sample_identity") == expected_sample
    )
    if not identity_matches:
        raise ValueError("resume report sample identity differs from selection")

    expected_paths = {
        "run_dir": paths.run_dir,
        "candidate_path": paths.candidate_path,
        "manifest_path": paths.manifest_path,
        "report_path": paths.prior_report_path,
    }
    for name, expected in expected_paths.items():
        if report.get(name) != str(expected):
            raise ValueError(f"resume report {name} identity differs")

    media_identity = report.get("media_identity")
    if not isinstance(media_identity, dict):
        raise ValueError("resume report media identity is missing")
    try:
        media_matches = (
            str(media_identity["chat_id"]) == sample.chat_id
            and int(media_identity["message_id"]) == sample.message_id
            and int(media_identity["file_size"]) == sample.file_size
            and int(media_identity["media_id"]) > 0
            and int(media_identity["dc_id"]) > 0
            and bool(str(media_identity["file_unique_id"]))
            and bool(str(media_identity["stable_key"]))
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("resume report media identity is invalid") from error
    if sample.file_unique_id and (
        str(media_identity["file_unique_id"]) != sample.file_unique_id
    ):
        media_matches = False
    if sample.media_id is not None and int(media_identity["media_id"]) != sample.media_id:
        media_matches = False
    if sample.dc_id is not None and int(media_identity["dc_id"]) != sample.dc_id:
        media_matches = False
    if not media_matches:
        raise ValueError("resume report media identity differs from selection")

    raw_artifacts = report.get("artifact_identities")
    if not isinstance(raw_artifacts, dict):
        raise ValueError("resume report artifact identities are missing")
    reported_artifacts = DownloadArtifactIdentities(
        candidate=_artifact_identity_from_report(
            raw_artifacts.get("candidate"),
            "candidate",
        ),
        manifest=_artifact_identity_from_report(
            raw_artifacts.get("manifest"),
            "manifest",
        ),
    )

    recovery = report.get("recovery")
    if not isinstance(recovery, dict):
        raise ValueError("resume report recovery evidence is missing")
    abort_durability = _durability_evidence(
        recovery.get("abort_durability")
    )
    partial_durability = _durability_evidence(
        recovery.get("partial_durability")
    )
    if status == "aborted" and not abort_durability["verified"]:
        raise ValueError("resume report abort durability is unverified")
    if status != "aborted" and not partial_durability["verified"]:
        raise ValueError("resume report partial durability is unverified")
    if status != "aborted" and not (
        report.get("errors") or report.get("interruptions")
    ):
        raise ValueError("resume failed report has no failure evidence")
    try:
        recovered_chunks = int(recovery["durable_chunks"])
        recovered_bytes = int(recovery["durable_bytes"])
        total_chunks = int(recovery["total_chunks"])
        prior_recovered = int(recovery["recovered_chunks"])
        prior_downloaded = int(recovery["downloaded_chunks"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("resume report recovery evidence is invalid") from error
    if (
        recovered_chunks <= 0
        or recovered_chunks >= total_chunks
        or recovered_bytes <= 0
        or total_chunks != _total_chunks(sample.file_size)
        or prior_recovered + prior_downloaded != recovered_chunks
    ):
        raise ValueError("resume report lacks incomplete durable recovery")

    artifacts = _load_download_artifacts(paths)
    if artifacts != reported_artifacts:
        raise ValueError("resume artifact identity differs from abort report")
    durable = _inspect_durable_partial(paths, artifacts, media_identity)
    if (
        durable["durable_chunks"] != recovered_chunks
        or durable["durable_bytes"] != recovered_bytes
        or durable["total_chunks"] != total_chunks
    ):
        raise ValueError("resume report differs from incomplete durable state")

    return ResumeContext(
        artifacts=artifacts,
        prior_report_path=paths.prior_report_path,
        prior_report_identity=report_identity,
        prior_report=report,
        media_identity=dict(media_identity),
        recovered_chunks=recovered_chunks,
        recovered_bytes=recovered_bytes,
        total_chunks=total_chunks,
    )


def _sha256_fd(fd: int) -> str:
    digest = hashlib.sha256()
    os.lseek(fd, 0, os.SEEK_SET)
    while True:
        block = os.read(fd, 8 * 1024 * 1024)
        if not block:
            return digest.hexdigest()
        digest.update(block)


def _inspect_candidate(
    path: Path,
    expected: ArtifactIdentity,
) -> tuple[int, str]:
    fd = _verify_artifact(path, expected, "candidate")
    try:
        return os.fstat(fd).st_size, _sha256_fd(fd)
    finally:
        os.close(fd)


def _verify_manifest_artifacts(
    path: Path,
    expected: ArtifactIdentity,
) -> None:
    fd = _verify_artifact(path, expected, "manifest")
    os.close(fd)
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if not sidecar.exists() and not sidecar.is_symlink():
            continue
        sidecar_fd = _open_artifact_no_follow(sidecar, os.O_RDONLY)
        try:
            _artifact_identity_from_fd(
                sidecar_fd,
                f"manifest{suffix}",
                require_private=False,
            )
        finally:
            os.close(sidecar_fd)


def _load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}
    if not config.get("api_id") or not config.get("api_hash"):
        raise ValueError("config must contain api_id and api_hash")
    return config


def _copy_session_workspace(source: Path, run_dir: Path) -> Path:
    """Copy session inputs into the unique run directory without source writes."""
    session_source = Path(source).resolve()
    run_root = Path(run_dir).resolve()
    if not session_source.is_dir():
        raise ValueError(f"session directory is unavailable: {session_source}")
    if _is_within(run_root, session_source):
        raise ValueError("session source must not contain benchmark output")

    workspace = run_root / "sessions"
    workspace.mkdir()
    try:
        for source_path in sorted(session_source.rglob("*")):
            if source_path.is_symlink():
                raise ValueError(
                    f"session workspace contains a symlink: {source_path}"
                )
            relative_path = source_path.relative_to(session_source)
            destination = workspace / relative_path
            if source_path.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
            elif source_path.is_file():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source_path, destination)
            else:
                raise ValueError(
                    "session workspace contains a non-regular entry: "
                    f"{source_path}"
                )
        required_session = workspace / "media_downloader.session"
        if not required_session.is_file():
            raise ValueError("session source has no media_downloader.session")
    except BaseException:
        shutil.rmtree(workspace, ignore_errors=True)
        raise
    return workspace


def _sha256_file(path: Path) -> str:
    fd = _open_artifact_no_follow(Path(path), os.O_RDONLY)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"not a regular file: {path}")
        return _sha256_fd(fd)
    finally:
        os.close(fd)


def _extract_media(message):
    media_type = getattr(message, "media", None)
    media_name = getattr(media_type, "value", "")
    if media_name and getattr(message, media_name, None) is not None:
        return media_name, getattr(message, media_name)
    for attribute in MEDIA_ATTRIBUTES:
        media = getattr(message, attribute, None)
        if media is not None:
            return attribute, media
    raise ValueError("message has no downloadable media object")


def _normalize_identity(value) -> str:
    text = str(value or "").strip()
    try:
        return str(int(text))
    except (TypeError, ValueError):
        return text.removeprefix("@").casefold()


def _normalize_media_type(value) -> str:
    raw_value = getattr(value, "value", value)
    return str(raw_value or "").strip().casefold().rsplit(".", 1)[-1]


def _validate_telegram_message(sample: BenchmarkSample, response):
    if isinstance(response, (list, tuple)):
        if len(response) != 1:
            raise ValueError(
                "expected exactly one Telegram message; "
                f"received {len(response)}"
            )
        message = response[0]
    else:
        message = response
    if not message or getattr(message, "empty", False):
        raise ValueError("Telegram message is unavailable")
    if int(getattr(message, "id", 0) or 0) != sample.message_id:
        raise ValueError("Telegram message ID differs from selected record")

    chat = getattr(message, "chat", None)
    actual_chat_ids = {
        _normalize_identity(value)
        for value in (
            getattr(chat, "id", None),
            getattr(chat, "username", None),
        )
        if value not in (None, "")
    }
    if _normalize_identity(sample.chat_id) not in actual_chat_ids:
        raise ValueError("Telegram chat ID differs from selected record")

    media_type, media = _extract_media(message)
    expected_type = _normalize_media_type(sample.media_type)
    actual_type = _normalize_media_type(media_type)
    if expected_type != actual_type:
        raise ValueError(
            f"Telegram media type is {actual_type}, expected {expected_type}"
        )
    media_size = int(getattr(media, "file_size", 0) or 0)
    if media_size != sample.file_size:
        raise ValueError(
            f"Telegram size is {media_size}, expected {sample.file_size}"
        )
    if not str(getattr(media, "file_id", "") or ""):
        raise ValueError("Telegram media has no stable file_id")
    unique_id = str(getattr(media, "file_unique_id", "") or "")
    if not unique_id:
        raise ValueError("Telegram media has no stable file_unique_id")
    if sample.file_unique_id and unique_id != sample.file_unique_id:
        raise ValueError("Telegram file_unique_id differs from selected record")
    return media


def _build_download_identity(
    sample: BenchmarkSample,
    client,
    media,
    *,
    source_factory: Callable,
):
    media_size = int(getattr(media, "file_size", 0) or 0)
    source = source_factory(client, media.file_id, media_size)
    media_id = int(getattr(source.file_id, "media_id", 0) or 0)
    dc_id = int(getattr(source.file_id, "dc_id", 0) or 0)
    if media_id <= 0:
        raise ValueError("Telegram media_id is unavailable")
    if dc_id <= 0:
        raise ValueError("Telegram dc_id is unavailable")
    if sample.media_id is not None and media_id != sample.media_id:
        raise ValueError("Telegram media_id differs from selected record")
    if sample.dc_id is not None and dc_id != sample.dc_id:
        raise ValueError("Telegram dc_id differs from selected record")
    identity = MediaIdentity(
        chat_id=sample.chat_id,
        message_id=sample.message_id,
        media_id=media_id,
        dc_id=dc_id,
        file_unique_id=str(media.file_unique_id),
        file_size=media_size,
    )
    return source, identity


def _chat_id_value(chat_id: str):
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


async def _connect_client_noninteractive(client, timeout: float) -> None:
    """Connect an existing authorized session without invoking authorization."""

    async def connect_authorized():
        authorized = await client.connect()
        if not authorized:
            raise PermissionError("copied Telegram session is not authorized")

    try:
        await asyncio.wait_for(connect_authorized(), timeout=float(timeout))
    except asyncio.TimeoutError as error:
        raise TimeoutError(
            f"Telegram client startup timed out after {float(timeout):g} seconds"
        ) from error


def _snapshot_dict(snapshot) -> dict:
    if isinstance(snapshot, dict):
        return {
            key: (
                {nested_key: dict(counts) for nested_key, counts in value.items()}
                if key == "by_dc"
                else value
            )
            for key, value in snapshot.items()
        }
    if is_dataclass(snapshot):
        return asdict(snapshot)
    raise TypeError("pool snapshot must be a mapping or dataclass")


def build_report(
    *,
    baseline_sha256: str,
    candidate_sha256: str,
    file_size: int,
    elapsed_seconds: float,
    snapshot: dict,
    retries: int,
    candidate_file_size: int,
    integrity_verified: bool,
    committed_bytes: int,
    recovered_bytes: int = 0,
    errors: Sequence[str] = (),
    total_elapsed_seconds: Optional[float] = None,
) -> dict:
    """Build the deterministic integrity and goodput decision payload."""
    candidate_size = int(candidate_file_size)
    total_committed = max(int(committed_bytes), 0)
    recovered = max(int(recovered_bytes), 0)
    committed = max(total_committed - recovered, 0)
    transfer_elapsed = max(float(elapsed_seconds), 0.0)
    total_elapsed = max(
        float(
            transfer_elapsed
            if total_elapsed_seconds is None
            else total_elapsed_seconds
        ),
        0.0,
    )
    same_sha256 = bool(
        baseline_sha256
        and candidate_sha256
        and baseline_sha256 == candidate_sha256
    )
    exact_size = bool(file_size > 0 and candidate_size == file_size)
    error_list = [str(error) for error in errors if str(error)]
    eligible = bool(
        exact_size and same_sha256 and integrity_verified and not error_list
    )
    pool_peak = int(snapshot.get("peak_live", snapshot.get("live", 0)) or 0)
    dc_counts = snapshot.get("peak_by_dc", snapshot.get("by_dc", {})) or {}
    final_snapshot = snapshot.get("final", snapshot)
    return {
        "file_size": int(file_size),
        "candidate_file_size": int(candidate_size),
        "exact_size": exact_size,
        "baseline_sha256": baseline_sha256,
        "candidate_sha256": candidate_sha256,
        "same_sha256": same_sha256,
        "integrity_verified": bool(integrity_verified),
        "eligible": eligible,
        "elapsed_seconds": transfer_elapsed,
        "transfer_elapsed_seconds": transfer_elapsed,
        "total_elapsed_seconds": total_elapsed,
        "committed_bytes": int(committed),
        "recovered_bytes": recovered,
        "total_committed_bytes": total_committed,
        "goodput_bytes_per_second": (
            committed / transfer_elapsed if transfer_elapsed > 0 else 0.0
        ),
        "retries": int(retries),
        "pool_peak": pool_peak,
        "dc_counts": dc_counts,
        "pool_snapshot": final_snapshot,
        "errors": error_list,
    }


def _default_fault_evidence(requested: bool = False) -> dict:
    return {
        "requested": bool(requested),
        "state": _FaultGateState.ARMED.value,
        "triggered": False,
        "terminated_connections": 0,
        "failed_session_id": "",
        "failed_stripe": {},
        "failed_attempt": 0,
        "replacement_session_id": "",
        "replacement_stripe": {},
        "replacement_attempt": 0,
        "correlated_replacements": 0,
        "replacement_session_observed": False,
        "stop_error": "",
        "iterator_cleanup_errors": [],
        "lease_cleanup_errors": [],
    }


def _default_durability_evidence() -> dict:
    return {
        "verified": False,
        "candidate_synced": False,
        "manifest_checkpointed": False,
        "manifest_synced": False,
        "directory_synced": False,
        "manifest_sidecars_synced": [],
    }


def _durability_evidence(value=None) -> dict:
    evidence = _default_durability_evidence()
    if value is None:
        return evidence
    raw = asdict(value) if is_dataclass(value) else dict(value)
    for name in (
        "candidate_synced",
        "manifest_checkpointed",
        "manifest_synced",
        "directory_synced",
    ):
        evidence[name] = bool(raw.get(name, False))
    evidence["manifest_sidecars_synced"] = list(
        raw.get("manifest_sidecars_synced", ()) or ()
    )
    evidence["verified"] = all(
        evidence[name]
        for name in (
            "candidate_synced",
            "manifest_checkpointed",
            "manifest_synced",
            "directory_synced",
        )
    )
    return evidence


def _sync_partial_artifacts(
    paths: BenchmarkOutputPaths,
    artifacts: DownloadArtifactIdentities,
) -> dict:
    """Make an interrupted partial durable through the production manifest API."""
    candidate_fd = _verify_artifact(
        paths.candidate_path,
        artifacts.candidate,
        "candidate",
    )
    try:
        os.fsync(candidate_fd)
    finally:
        os.close(candidate_fd)
    manifest = DownloadManifest(
        paths.manifest_path,
        expected_file_identity=artifacts.manifest.as_tuple(),
    )
    sidecars = manifest.checkpoint_and_sync()
    _fsync_directory(paths.run_dir)
    return _durability_evidence(
        {
            "candidate_synced": True,
            "manifest_checkpointed": True,
            "manifest_synced": True,
            "directory_synced": True,
            "manifest_sidecars_synced": sidecars,
        }
    )


def _default_recovery_evidence(args=None, run_mode: Optional[str] = None) -> dict:
    mode = run_mode or (
        "resume" if bool(getattr(args, "resume_candidate", "")) else "fresh"
    )
    return {
        "mode": mode,
        "abort_after_chunks": getattr(args, "abort_after_chunks", None),
        "recovered_chunks": 0,
        "recovered_bytes": 0,
        "downloaded_chunks": 0,
        "current_run_committed_bytes": 0,
        "durable_chunks": 0,
        "durable_bytes": 0,
        "total_chunks": 0,
        "whole_file_fallback": False,
        "provenance_verified": False,
        "parent_report": {},
        "abort_durability": _default_durability_evidence(),
        "partial_durability": _default_durability_evidence(),
    }


def _apply_report_envelope(
    report: dict,
    args=None,
    *,
    run_mode: Optional[str] = None,
) -> dict:
    report["schema_version"] = REPORT_SCHEMA_VERSION
    report.setdefault("errors", [])
    recovery = _default_recovery_evidence(args, run_mode)
    recovery.update(report.get("recovery") or {})
    report["recovery"] = recovery
    requested = bool(getattr(args, "inject_leased_connection_failure", False))
    fault = _default_fault_evidence(requested)
    fault.update(report.get("fault_injection") or {})
    report["fault_injection"] = fault
    report["run_mode"] = recovery["mode"]
    report["abort_after_chunks"] = recovery["abort_after_chunks"]
    report["recovered_chunks"] = recovery["recovered_chunks"]
    report["downloaded_chunks"] = recovery["downloaded_chunks"]
    report["whole_file_fallback"] = recovery["whole_file_fallback"]
    report.setdefault("status", "completed" if report.get("eligible") else "failed")
    report.setdefault("incomplete", report["status"] == "aborted")
    return report


def _enforce_resume_eligibility(report: dict, context: ResumeContext) -> None:
    recovery = report["recovery"]
    recovered = int(recovery.get("recovered_chunks", 0) or 0)
    downloaded = int(recovery.get("downloaded_chunks", 0) or 0)
    durable = int(recovery.get("durable_chunks", 0) or 0)
    total = int(recovery.get("total_chunks", 0) or 0)
    failures = []
    if not bool(getattr(context, "provenance_verified", False)):
        failures.append("resume provenance was not verified")
    if recovered <= 0:
        failures.append("resume recovered no durable chunks")
    if recovered != int(context.recovered_chunks):
        failures.append("resume recovery differs from the parent abort state")
    if downloaded <= 0:
        failures.append("resume downloaded no remaining chunks")
    if total != int(context.total_chunks) or durable != total:
        failures.append("resume did not complete the exact durable manifest")
    if durable != recovered + downloaded:
        failures.append("resume chunk accounting does not reconcile")
    if downloaded >= total:
        failures.append("resume performed a complete redownload")
    if recovery.get("whole_file_fallback"):
        failures.append("resume used whole-file fallback")
    if failures:
        report["errors"].extend(
            failure for failure in failures if failure not in report["errors"]
        )
        report["eligible"] = False


def _error_text(error: BaseException) -> str:
    message = str(error)
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


async def _shutdown_client(client) -> None:
    if client is None:
        return
    if bool(getattr(client, "is_initialized", False)):
        await client.stop()
    elif bool(getattr(client, "is_connected", False)):
        await client.disconnect()


async def _close_pool_then_client(
    pool,
    client,
    session_workspace: Optional[Path],
) -> list:
    """Finish ordered cleanup even when the caller is being cancelled."""

    failures = []

    async def finish_cleanup():
        if pool is not None:
            try:
                await pool.close()
            except BaseException as error:
                failures.append(("pool close", error))
        if client is not None:
            try:
                await _shutdown_client(client)
            except BaseException as error:
                failures.append(("client stop/disconnect", error))
        if session_workspace is not None:
            try:
                shutil.rmtree(session_workspace)
            except FileNotFoundError:
                pass
            except BaseException as error:
                failures.append(("session copy cleanup", error))

    cleanup_task = asyncio.create_task(finish_cleanup())
    while True:
        try:
            await asyncio.shield(cleanup_task)
            break
        except asyncio.CancelledError as error:
            failures.append(("cleanup wait", error))
            if cleanup_task.done():
                break
        except BaseException as error:
            failures.append(("cleanup task", error))
            break
    try:
        cleanup_task.result()
    except BaseException as error:
        if not failures or failures[-1][1] is not error:
            failures.append(("cleanup task", error))
    return failures


def _interruption_evidence(phase: str, error: BaseException) -> dict:
    return {
        "phase": phase,
        "class": type(error).__name__,
        "message": str(error),
    }


def _record_interruption(
    report: dict,
    phase: str,
    error: BaseException,
) -> None:
    evidence = _interruption_evidence(phase, error)
    report.setdefault("interruptions", []).append(evidence)
    report.setdefault("errors", []).append(f"{phase}: {_error_text(error)}")
    report.setdefault("interruption", dict(evidence))


async def _run_benchmark_async(
    args,
    sample: BenchmarkSample,
    paths: BenchmarkOutputPaths,
    *,
    client_factory: Callable = _NonInteractiveHookClient,
    pool_factory: Callable = GlobalMediaSessionPool,
    source_factory: Callable = KurigramRangeSource,
    downloader_factory: Callable = ParallelDownloader,
    resume_context: Optional[ResumeContext] = None,
) -> dict:
    wall_started = time.monotonic()
    artifacts = None
    baseline_path = Path(sample.save_path).resolve()
    baseline_stat = None
    baseline_sha256 = ""
    session_workspace = None
    client = None
    pool = None
    evidence = None
    downloader = None
    transfer_started = None
    transfer_elapsed = 0.0
    result = None
    report_snapshot = {}
    primary_error = None
    secondary_failures = []
    cleanup_failures = []
    fault_injector = None
    media_identity_payload = {}
    durable_state = {}
    abort_durability = _default_durability_evidence()
    partial_durability = _default_durability_evidence()
    try:
        if getattr(args, "resume_candidate", ""):
            if resume_context is None or not resume_context.provenance_verified:
                raise ValueError("resume provenance was not validated")
            artifacts = resume_context.artifacts
        else:
            artifacts = _reserve_download_artifacts(paths)
        config_path = Path(args.config)
        config = _load_config(config_path)
        sessions = (
            Path(args.sessions)
            if args.sessions
            else config_path.parent / "sessions"
        )
        session_workspace = _copy_session_workspace(sessions, paths.run_dir)

        try:
            baseline_stat = baseline_path.lstat()
        except FileNotFoundError as error:
            raise ValueError(f"baseline is missing: {baseline_path}") from error
        if not stat.S_ISREG(baseline_stat.st_mode):
            raise ValueError(f"baseline is not a regular file: {baseline_path}")
        if baseline_stat.st_size != sample.file_size:
            raise ValueError(
                f"baseline size is {baseline_stat.st_size}, expected {sample.file_size}"
            )
        baseline_sha256 = await asyncio.to_thread(_sha256_file, baseline_path)

        start_timeout = float(
            getattr(args, "start_timeout", None)
            or config.get("start_timeout", 60)
        )
        client = client_factory(
            "media_downloader",
            api_id=config["api_id"],
            api_hash=config["api_hash"],
            proxy=config.get("proxy") or {},
            workdir=str(session_workspace),
            start_timeout=start_timeout,
            no_updates=True,
        )
        await _connect_client_noninteractive(client, start_timeout)
        set_max_concurrent_transmissions(
            client,
            max(args.session_target * args.pipeline_depth, 2),
        )
        pool_config = MediaSessionPoolConfig(
            soft_sessions=args.session_target,
            max_sessions=args.session_target,
            pipeline_depth=args.pipeline_depth,
            adaptive=False,
        )
        pool = pool_factory(KurigramMediaSessionFactory(client), pool_config)
        pool.start()
        evidence = _PoolEvidence(pool)
        evidence.sample()

        message = await client.get_messages(
            chat_id=_chat_id_value(sample.chat_id),
            message_ids=sample.message_id,
        )
        media = _validate_telegram_message(sample, message)
        source, identity = _build_download_identity(
            sample,
            client,
            media,
            source_factory=source_factory,
        )
        media_identity_payload = _media_identity_payload(identity)
        if (
            resume_context is not None
            and media_identity_payload != resume_context.media_identity
        ):
            raise ValueError(
                "live Telegram media identity differs from abort report"
            )
        downloader_pool = pool
        if getattr(args, "inject_leased_connection_failure", False):
            fault_injector = _LeasedConnectionFailureInjector(source)
            source = fault_injector
            downloader_pool = fault_injector.wrap_pool(pool)
        downloader = downloader_factory(
            source,
            workers=1,
            pool=downloader_pool,
            abort_after_chunks=getattr(args, "abort_after_chunks", None),
            transfer_id=(
                f"benchmark:{sample.chat_id}:{sample.message_id}:"
                f"{args.session_target}:{args.pipeline_depth}"
            ),
        )
        transfer_started = time.monotonic()
        try:
            result = await downloader.download(
                identity,
                paths.candidate_path,
                progress=evidence.progress,
                expected_target_identity=artifacts.candidate.as_tuple(),
                expected_manifest_identity=artifacts.manifest.as_tuple(),
            )
        finally:
            transfer_elapsed = max(time.monotonic() - transfer_started, 0.0)
    except BaseException as error:
        primary_error = error
    finally:
        if evidence is not None:
            try:
                report_snapshot = evidence.report_snapshot()
            except BaseException as error:
                if primary_error is None:
                    primary_error = error
                else:
                    secondary_failures.append(("pool evidence", error))
        try:
            cleanup_failures = await _close_pool_then_client(
                pool,
                client,
                session_workspace,
            )
        except BaseException as error:
            cleanup_failures.append(("cleanup task", error))

    errors = []
    if primary_error is not None:
        errors.append(_error_text(primary_error))
    for phase, error in (*secondary_failures, *cleanup_failures):
        errors.append(f"{phase}: {_error_text(error)}")

    if isinstance(primary_error, InjectedAbort):
        abort_durability = _durability_evidence(
            getattr(primary_error, "durability", None)
        )
        if abort_durability["verified"]:
            partial_durability = dict(abort_durability)
    if (
        primary_error is not None
        and artifacts is not None
        and media_identity_payload
        and not partial_durability["verified"]
    ):
        try:
            partial_durability = _sync_partial_artifacts(paths, artifacts)
        except BaseException as error:
            errors.append(f"partial durability: {_error_text(error)}")

    candidate_size = 0
    candidate_sha256 = ""
    if artifacts is not None:
        try:
            candidate_size, candidate_sha256 = _inspect_candidate(
                paths.candidate_path,
                artifacts.candidate,
            )
        except BaseException as error:
            errors.append(f"candidate evidence: {_error_text(error)}")
        try:
            _verify_manifest_artifacts(paths.manifest_path, artifacts.manifest)
        except BaseException as error:
            errors.append(f"manifest evidence: {_error_text(error)}")
        if primary_error is not None and media_identity_payload:
            try:
                durable_state = _inspect_durable_partial(
                    paths,
                    artifacts,
                    media_identity_payload,
                )
            except BaseException as error:
                errors.append(f"durable abort evidence: {_error_text(error)}")

    if baseline_stat is not None:
        try:
            final_baseline_stat = baseline_path.lstat()
            immutable_fields = (
                "st_dev",
                "st_ino",
                "st_mode",
                "st_size",
                "st_mtime_ns",
            )
            if any(
                getattr(final_baseline_stat, field)
                != getattr(baseline_stat, field)
                for field in immutable_fields
            ):
                errors.append("baseline metadata changed during benchmark")
        except BaseException as error:
            errors.append(f"baseline final metadata: {_error_text(error)}")

    downloader_sha256 = str(getattr(result, "sha256", "") or "")
    if result is not None and downloader_sha256 != candidate_sha256:
        errors.append("downloader and independent candidate SHA-256 differ")
    retries = int(
        getattr(result, "retries", getattr(downloader, "_retries", 0)) or 0
    )
    final_snapshot = report_snapshot.get("final") or {}
    retries = max(retries, int(final_snapshot.get("retries", 0) or 0))
    integrity = getattr(result, "integrity", None)
    recovered_chunks = int(
        getattr(
            result,
            "recovered_chunks",
            getattr(downloader, "_recovered_chunks", 0),
        )
        or 0
    )
    downloaded_chunks = int(
        getattr(
            result,
            "downloaded_chunks",
            getattr(downloader, "_downloaded_chunks", 0),
        )
        or 0
    )
    total_chunks = (
        int(resume_context.total_chunks)
        if resume_context is not None
        else _total_chunks(sample.file_size)
    )
    recovered_bytes = (
        int(resume_context.recovered_bytes)
        if resume_context is not None
        else 0
    )
    total_committed_bytes = int(evidence.committed_bytes if evidence else 0)
    durable_chunks = int(
        durable_state.get(
            "durable_chunks",
            recovered_chunks + downloaded_chunks,
        )
        or 0
    )
    durable_bytes = int(
        durable_state.get("durable_bytes", total_committed_bytes) or 0
    )
    whole_file_fallback = bool(int(final_snapshot.get("fallbacks", 0) or 0))
    report = build_report(
        baseline_sha256=baseline_sha256,
        candidate_sha256=candidate_sha256,
        file_size=sample.file_size,
        candidate_file_size=candidate_size,
        elapsed_seconds=transfer_elapsed,
        total_elapsed_seconds=time.monotonic() - wall_started,
        snapshot=report_snapshot,
        retries=retries,
        integrity_verified=bool(integrity and integrity.verified),
        committed_bytes=total_committed_bytes,
        recovered_bytes=recovered_bytes,
        errors=errors,
    )
    report["integrity"] = asdict(integrity) if integrity is not None else {}
    report["downloader_sha256"] = downloader_sha256
    report["sample_identity"] = asdict(sample)
    report["media_identity"] = dict(media_identity_payload)
    report["artifact_identities"] = (
        {
            "candidate": asdict(artifacts.candidate),
            "manifest": asdict(artifacts.manifest),
        }
        if artifacts is not None
        else {}
    )
    mode = "resume" if resume_context is not None else "fresh"
    parent_report = {}
    if resume_context is not None:
        parent_report = {
            "path": str(resume_context.prior_report_path),
            "identity": asdict(resume_context.prior_report_identity),
        }
    report["recovery"] = {
        "mode": mode,
        "abort_after_chunks": getattr(args, "abort_after_chunks", None),
        "recovered_chunks": recovered_chunks,
        "recovered_bytes": recovered_bytes,
        "downloaded_chunks": downloaded_chunks,
        "current_run_committed_bytes": report["committed_bytes"],
        "durable_chunks": durable_chunks,
        "durable_bytes": durable_bytes,
        "total_chunks": total_chunks,
        "whole_file_fallback": whole_file_fallback,
        "provenance_verified": bool(
            resume_context is not None and resume_context.provenance_verified
        ),
        "parent_report": parent_report,
        "abort_durability": dict(abort_durability),
        "partial_durability": dict(partial_durability),
    }
    if whole_file_fallback:
        report["errors"].append("whole-file fallback was recorded")
        report["eligible"] = False

    fault_requested = bool(
        getattr(args, "inject_leased_connection_failure", False)
    )
    report["fault_injection"] = (
        fault_injector.evidence()
        if fault_injector is not None
        else _default_fault_evidence(fault_requested)
    )
    if fault_requested:
        fault = report["fault_injection"]
        if not fault["triggered"]:
            report["errors"].append(
                "requested leased connection failure did not trigger"
            )
        elif fault["terminated_connections"] != 1:
            report["errors"].append(
                "leased connection failure did not terminate exactly one session"
            )
        elif (
            not fault["replacement_session_observed"]
            or fault["correlated_replacements"] != 1
            or not fault["failed_session_id"]
            or not fault["replacement_session_id"]
            or fault["failed_session_id"] == fault["replacement_session_id"]
        ):
            report["errors"].append(
                "leased connection failure did not use the exact stripe replacement"
            )
        if fault["iterator_cleanup_errors"]:
            report["errors"].append(
                "injected iterator cleanup recorded a BaseException"
            )
        if fault["lease_cleanup_errors"]:
            report["errors"].append(
                "injected lease cleanup recorded a BaseException"
            )
        if report["errors"]:
            report["eligible"] = False

    _apply_report_envelope(report, args, run_mode=mode)
    durable_abort = bool(
        isinstance(primary_error, InjectedAbort)
        and abort_durability["verified"]
        and durable_state
        and 0 < durable_chunks < total_chunks
        and downloaded_chunks > 0
        and recovered_chunks + downloaded_chunks == durable_chunks
        and durable_bytes > 0
    )
    if isinstance(primary_error, InjectedAbort) and not durable_abort:
        if not abort_durability["verified"]:
            report["errors"].append(
                "injected abort did not complete the real durability transition"
            )
        else:
            report["errors"].append(
                "injected abort did not leave incomplete durable recovery"
            )
        report["eligible"] = False
    if resume_context is not None and not isinstance(primary_error, InjectedAbort):
        _enforce_resume_eligibility(report, resume_context)
    if durable_abort:
        report["status"] = "aborted"
        report["incomplete"] = True
        report["eligible"] = False
    elif report["eligible"]:
        report["status"] = "completed"
        report["incomplete"] = False
    else:
        report["status"] = "failed"
        report["incomplete"] = bool(
            primary_error is not None
            and partial_durability["verified"]
            and durable_state
            and 0 < durable_chunks < total_chunks
            and recovered_chunks + downloaded_chunks == durable_chunks
            and durable_bytes > 0
        )

    interruption = None
    lifecycle_failures = []
    if primary_error is not None:
        lifecycle_failures.append(("benchmark", primary_error))
    lifecycle_failures.extend(secondary_failures)
    lifecycle_failures.extend(cleanup_failures)
    for phase, error in lifecycle_failures:
        if not isinstance(error, Exception):
            report.setdefault("interruptions", []).append(
                _interruption_evidence(phase, error)
            )
            if interruption is None:
                interruption = error
    if interruption is not None:
        report["interruption"] = next(
            item
            for item in report["interruptions"]
            if item["class"] == type(interruption).__name__
            and item["message"] == str(interruption)
        )
        interruption._benchmark_report = report
        raise interruption
    return report


def _failure_report(file_size: int, error: BaseException, args=None) -> dict:
    report = build_report(
        baseline_sha256="",
        candidate_sha256="",
        file_size=max(int(file_size), 0),
        candidate_file_size=0,
        elapsed_seconds=0.0,
        snapshot={},
        retries=0,
        integrity_verified=False,
        committed_bytes=0,
        errors=[_error_text(error)],
    )
    _apply_report_envelope(report, args)
    report["status"] = "failed"
    report["incomplete"] = False
    return report


def _validate_abort_threshold(
    sample: BenchmarkSample,
    abort_after_chunks: Optional[int],
    resume_context: Optional[ResumeContext],
) -> None:
    if abort_after_chunks is None:
        return
    total = _total_chunks(sample.file_size)
    recovered = (
        int(resume_context.recovered_chunks)
        if resume_context is not None
        else 0
    )
    remaining = total - recovered
    if int(abort_after_chunks) >= remaining:
        raise ValueError(
            "abort threshold would trigger at or after the final chunk"
        )


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    emit: Callable[[str], None] = print,
) -> int:
    args = build_parser().parse_args(argv)
    config_path = Path(args.config)
    sessions_path = (
        Path(args.sessions) if args.sessions else config_path.parent / "sessions"
    )
    protected_paths = {
        "downloads": Path(args.downloads_root),
        "sessions": sessions_path,
        "records": Path(args.records),
        "config": config_path,
    }
    mount_isolation = {
        "verified": False,
        "requirement": MOUNT_ISOLATION_REQUIREMENT,
    }
    try:
        output_root = _validate_protected_output(
            Path(args.output),
            protected_paths,
        )
        mount_isolation = _validate_mount_isolation(
            output_root,
            protected_paths,
        )
    except Exception as error:
        preflight = _failure_report(0, error, args)
        preflight["mount_isolation"] = mount_isolation
        emit(json.dumps(preflight, indent=2, sort_keys=True))
        return 1

    try:
        if bool(args.resume_candidate) != bool(args.resume_report):
            raise ValueError(
                "resume candidate and exact resume report must be provided together"
            )
        if args.resume_candidate:
            paths = _resume_output_paths(
                output_root,
                Path(args.downloads_root),
                Path(args.resume_candidate),
                Path(args.resume_report),
            )
        else:
            paths = _reserve_output_paths(
                output_root,
                Path(args.downloads_root),
                args.chat_id,
                args.message_id,
                args.session_target,
                args.pipeline_depth,
            )
    except Exception as error:
        preflight = _failure_report(0, error, args)
        preflight["mount_isolation"] = mount_isolation
        emit(json.dumps(preflight, indent=2, sort_keys=True))
        return 1

    sample = None
    interruption = None
    async_state = {}
    resume_context = None
    recovery_gate = _RecoveryGate(
        paths,
        resume=bool(args.resume_candidate),
    )
    try:
        if not args.resume_candidate:
            paths = recovery_gate.reserve_report()
        with closing(_open_records_read_only(Path(args.records))) as connection:
            sample = _select_successful_record(
                connection,
                args.chat_id,
                args.message_id,
            )
        baseline_path = Path(sample.save_path).resolve()
        if not _is_within(baseline_path, Path(args.downloads_root)):
            raise ValueError("baseline is outside the read-only downloads root")
        if args.resume_candidate:
            resume_context = recovery_gate.verify_parent(sample)
        _validate_abort_threshold(
            sample,
            args.abort_after_chunks,
            resume_context,
        )
        if args.resume_candidate:
            paths = recovery_gate.reserve_report()

        async def run_with_report_capture():
            try:
                return await _run_benchmark_async(
                    args,
                    sample,
                    paths,
                    resume_context=resume_context,
                )
            except BaseException as error:
                async_state["error"] = error
                async_state["report"] = getattr(error, "_benchmark_report", None)
                raise

        report = asyncio.run(run_with_report_capture())
    except BaseException as error:
        inner_error = async_state.get("error")
        report = getattr(error, "_benchmark_report", None)
        if report is None:
            report = async_state.get("report")
        if report is None:
            report = _failure_report(
                sample.file_size if sample else 0,
                error,
                args,
            )
        elif error is not inner_error and not isinstance(error, Exception):
            _record_interruption(report, "asyncio.run", error)
        if not isinstance(error, Exception):
            interruption = error

    _apply_report_envelope(report, args)
    report.update(
        {
            "chat_id": args.chat_id,
            "message_id": args.message_id,
            "session_target": args.session_target,
            "pipeline_depth": args.pipeline_depth,
            "run_dir": str(paths.run_dir),
            "candidate_path": str(paths.candidate_path),
            "manifest_path": str(paths.manifest_path),
            "report_path": str(paths.report_path),
            "mount_isolation": mount_isolation,
        }
    )
    if sample is not None:
        report.setdefault("sample_identity", asdict(sample))
    if not mount_isolation.get("verified", False):
        report["eligible"] = False
        report["errors"].append("required mount isolation was not verified")
        report["status"] = "failed"
    if recovery_gate.report_identity is None:
        emit(json.dumps(report, indent=2, sort_keys=True))
        if interruption is not None:
            raise interruption
        return 1
    recovery_gate.persist(report)
    emit(json.dumps(report, indent=2, sort_keys=True))
    if interruption is not None:
        raise interruption
    return 0 if report["eligible"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
