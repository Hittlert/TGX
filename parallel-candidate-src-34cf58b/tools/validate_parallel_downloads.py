"""Validate parallel Telegram downloads against HDD archives and remote hashes."""

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Sequence

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyrogram
import yaml

from module.parallel_downloader import (
    HashMismatch,
    IncompleteRange,
    InjectedAbort,
    KurigramRangeSource,
    MediaIdentity,
    ParallelDownloader,
    RemoteHashUnavailable,
    collect_remote_hashes,
    verify_file_hashes,
)
from module.parallel_validation import (
    SampleResult,
    build_run_report,
    decide_sample,
    select_samples,
    write_report_atomic,
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate two-worker Telegram range downloads without DB writes."
    )
    parser.add_argument("--config", default="/app/config.yaml")
    parser.add_argument("--records", default="/app/state/download_records.sqlite3")
    parser.add_argument("--downloads-root", default="/app/downloads")
    parser.add_argument("--output-dir", default="/app/temp/parallel_validation")
    parser.add_argument("--sessions", default="")
    parser.add_argument("--report", default="")
    parser.add_argument("--workers", type=int, default=2, choices=range(1, 5))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--resume-run", default="")
    parser.add_argument("--only-message-id", type=int)
    parser.add_argument("--abort-after-chunks", type=int)
    parser.add_argument("--dry-select", action="store_true")
    return parser


def _open_records_read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)


def _load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}
    if not config.get("api_id") or not config.get("api_hash"):
        raise ValueError("config must contain api_id and api_hash")
    return config


def _new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_run_id(value: str) -> str:
    if not value or any(character not in "-_.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" for character in value):
        raise ValueError("run id may contain only letters, digits, dot, dash, underscore")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        while True:
            block = source_file.read(8 * 1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


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


def _chat_id_value(chat_id: str):
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((path.resolve(), root.resolve())) == str(root.resolve())
    except ValueError:
        return False


async def _validate_sample(
    client,
    sample,
    run_dir: Path,
    downloads_root: Path,
    workers: int,
    abort_after_chunks: Optional[int],
) -> SampleResult:
    started = time.monotonic()
    baseline_path = Path(sample.save_path)
    if not _path_is_within(baseline_path, downloads_root):
        raise ValueError(f"baseline is outside read-only downloads root: {baseline_path}")

    message = await client.get_messages(
        chat_id=_chat_id_value(sample.chat_id),
        message_ids=sample.message_id,
    )
    if isinstance(message, list):
        message = message[0] if message else None
    if not message or getattr(message, "empty", False):
        raise ValueError("Telegram message is unavailable")

    _media_type, media = _extract_media(message)
    media_size = int(getattr(media, "file_size", 0) or 0)
    if media_size != sample.file_size:
        raise ValueError(
            f"declared size changed from {sample.file_size} to {media_size}"
        )

    source = KurigramRangeSource(client, media.file_id, media_size)
    identity = MediaIdentity(
        chat_id=str(sample.chat_id),
        message_id=int(sample.message_id),
        media_id=int(source.file_id.media_id or 0),
        dc_id=int(source.file_id.dc_id),
        file_unique_id=str(getattr(media, "file_unique_id", "") or ""),
        file_size=media_size,
    )
    safe_chat_id = str(sample.chat_id).replace("/", "_")
    candidate_path = run_dir / f"{safe_chat_id}-{sample.message_id}.candidate"

    baseline_sha256 = await asyncio.to_thread(_sha256_file, baseline_path)
    baseline_verified = False
    baseline_error = ""
    remote_hashes = []
    try:
        remote_hashes = await collect_remote_hashes(source, media_size)
        await verify_file_hashes(
            baseline_path,
            media_size,
            remote_hashes,
        )
        baseline_verified = True
    except (HashMismatch, IncompleteRange, RemoteHashUnavailable) as error:
        baseline_error = str(error)

    candidate_sha256 = ""
    candidate_verified = False
    candidate_error = ""
    covered_bytes = 0
    range_count = len(remote_hashes)
    retries = 0
    elapsed_download = 0.0
    try:
        downloader = ParallelDownloader(
            source,
            workers=workers,
            abort_after_chunks=abort_after_chunks,
        )
        result = await downloader.download(identity, candidate_path)
        candidate_sha256 = result.sha256
        candidate_verified = result.integrity.verified
        covered_bytes = result.integrity.covered_bytes
        range_count = result.integrity.range_count
        retries = result.retries
        elapsed_download = result.elapsed_seconds
    except InjectedAbort:
        raise
    except Exception as error:
        candidate_error = str(error)
        if candidate_path.is_file() and candidate_path.stat().st_size == media_size:
            candidate_sha256 = await asyncio.to_thread(_sha256_file, candidate_path)

    same_sha = bool(candidate_sha256 and baseline_sha256 == candidate_sha256)
    decision = decide_sample(
        same_sha=same_sha,
        baseline_verified=baseline_verified,
        candidate_verified=candidate_verified,
    )
    reasons = [decision.reason]
    if baseline_error:
        reasons.append(f"baseline: {baseline_error}")
    if candidate_error:
        reasons.append(f"candidate: {candidate_error}")
    elapsed = time.monotonic() - started
    throughput = (
        media_size / elapsed_download
        if elapsed_download > 0 and candidate_verified
        else 0.0
    )
    return SampleResult(
        bucket=sample.bucket,
        chat_id=sample.chat_id,
        message_id=sample.message_id,
        file_size=media_size,
        baseline_path=str(baseline_path),
        candidate_path=str(candidate_path),
        baseline_sha256=baseline_sha256,
        candidate_sha256=candidate_sha256,
        baseline_verified=baseline_verified,
        candidate_verified=candidate_verified,
        telegram_covered_bytes=covered_bytes,
        telegram_range_count=range_count,
        elapsed_seconds=elapsed,
        throughput_bytes_per_second=throughput,
        retries=retries,
        workers=workers,
        decision=decision.status,
        reason="; ".join(reasons),
        unexplained_mismatch=decision.unexplained_mismatch,
    )


def _failed_sample(sample, run_dir: Path, workers: int, error: Exception) -> SampleResult:
    safe_chat_id = str(sample.chat_id).replace("/", "_")
    return SampleResult(
        bucket=sample.bucket,
        chat_id=sample.chat_id,
        message_id=sample.message_id,
        file_size=sample.file_size,
        baseline_path=sample.save_path,
        candidate_path=str(run_dir / f"{safe_chat_id}-{sample.message_id}.candidate"),
        baseline_sha256="",
        candidate_sha256="",
        baseline_verified=False,
        candidate_verified=False,
        telegram_covered_bytes=0,
        telegram_range_count=0,
        elapsed_seconds=0.0,
        throughput_bytes_per_second=0.0,
        retries=0,
        workers=workers,
        decision="invalid",
        reason=str(error),
        unexplained_mismatch=False,
    )


async def _run_validation_async(
    args,
    samples,
    bucket_gaps,
    run_id: str,
    client_factory: Callable,
) -> dict:
    config_path = Path(args.config)
    config = _load_config(config_path)
    sessions = Path(args.sessions) if args.sessions else config_path.parent / "sessions"
    run_dir = Path(args.output_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    client = client_factory(
        "media_downloader",
        api_id=config["api_id"],
        api_hash=config["api_hash"],
        proxy=config.get("proxy") or {},
        workdir=str(sessions),
        start_timeout=int(config.get("start_timeout", 60)),
        no_updates=True,
    )
    started_at = datetime.now(timezone.utc).isoformat()
    results = []
    await client.start()
    try:
        set_max_concurrent_transmissions(client, max(args.workers * 2, 2))
        for sample in samples:
            try:
                result = await _validate_sample(
                    client,
                    sample,
                    run_dir,
                    Path(args.downloads_root),
                    args.workers,
                    args.abort_after_chunks,
                )
            except InjectedAbort:
                raise
            except Exception as error:
                result = _failed_sample(sample, run_dir, args.workers, error)
            results.append(result)
    finally:
        await client.stop()

    report = build_run_report(
        results,
        bucket_gaps,
        run_id=run_id,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc).isoformat(),
        app_commit=os.environ.get("APP_GIT_COMMIT", ""),
        kurigram_version=getattr(pyrogram, "__version__", "unknown"),
        workers=args.workers,
    )
    report_path = (
        Path(args.report)
        if args.report
        else Path(args.output_dir) / f"{run_id}-report.json"
    )
    write_report_atomic(report, report_path)
    report["report_path"] = str(report_path)
    return report


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    client_factory: Callable = HookClient,
    emit: Callable[[str], None] = print,
) -> int:
    args = build_parser().parse_args(argv)
    if args.abort_after_chunks is not None and args.abort_after_chunks <= 0:
        raise ValueError("abort-after-chunks must be positive")

    records_path = Path(args.records)
    with _open_records_read_only(records_path) as connection:
        samples, bucket_gaps = select_samples(connection)

    selection_payload = {
        "samples": [asdict(sample) for sample in samples],
        "bucket_gaps": bucket_gaps,
    }
    if args.dry_select:
        emit(json.dumps(selection_payload, indent=2, sort_keys=True))
        return 0 if not bucket_gaps and len(samples) == 6 else 2

    if bucket_gaps or len(samples) != 6:
        emit(json.dumps(selection_payload, indent=2, sort_keys=True))
        return 2

    if args.only_message_id is not None:
        samples = [
            sample for sample in samples if sample.message_id == args.only_message_id
        ]
        if not samples:
            raise ValueError("only-message-id is not in the selected six samples")

    run_id = _safe_run_id(args.resume_run or args.run_id or _new_run_id())
    try:
        report = asyncio.run(
            _run_validation_async(
                args,
                samples,
                bucket_gaps,
                run_id,
                client_factory,
            )
        )
    except InjectedAbort as error:
        emit(json.dumps({"run_id": run_id, "status": "aborted", "error": str(error)}))
        return 75

    emit(json.dumps(report, indent=2, sort_keys=True))
    if args.only_message_id is not None:
        return 0 if report["samples"][0]["decision"] == "pass" else 1
    return 0 if report["eligible"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
