"""Read-only sample selection and evidence reports for parallel downloads."""

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union


MIB = 1024 * 1024
GIB = 1024 * 1024 * 1024

BUCKET_QUOTAS = (
    ("lt10MiB", 2),
    ("10to200MiB", 2),
    ("200MiBto1GiB", 1),
    ("gt1GiB", 1),
)


@dataclass(frozen=True)
class ValidationSample:
    """One successful production record with an existing HDD archive."""

    bucket: str
    chat_id: str
    message_id: int
    save_path: str
    file_name: str
    media_type: str
    file_size: int


@dataclass(frozen=True)
class SampleDecision:
    """Result of comparing baseline, candidate, and Telegram evidence."""

    status: str
    reason: str
    blocks_parallel: bool
    unexplained_mismatch: bool = False


@dataclass(frozen=True)
class SampleResult:
    """Serializable evidence for one completed validation sample."""

    bucket: str
    chat_id: str
    message_id: int
    file_size: int
    baseline_path: str
    candidate_path: str
    baseline_sha256: str
    candidate_sha256: str
    baseline_verified: bool
    candidate_verified: bool
    telegram_covered_bytes: int
    telegram_range_count: int
    elapsed_seconds: float
    throughput_bytes_per_second: float
    retries: int
    workers: int
    decision: str
    reason: str
    unexplained_mismatch: bool


def _bucket_for_size(file_size: int) -> Optional[str]:
    if 0 < file_size < 10 * MIB:
        return "lt10MiB"
    if 10 * MIB <= file_size <= 200 * MIB:
        return "10to200MiB"
    if 200 * MIB < file_size <= GIB:
        return "200MiBto1GiB"
    if file_size > GIB:
        return "gt1GiB"
    return None


def select_samples(
    connection,
    path_exists: Callable[[str], bool] = os.path.isfile,
) -> Tuple[List[ValidationSample], List[str]]:
    """Select exact validation quotas without mutating production SQLite."""
    cursor = connection.execute(
        """
        SELECT
            chat_id,
            message_id,
            save_path,
            file_name,
            media_type,
            file_size
        FROM download_records
        WHERE status = 'success'
          AND save_path IS NOT NULL
          AND file_size > 0
        ORDER BY file_size ASC, message_id ASC
        """
    )
    columns = [description[0] for description in cursor.description]
    selected: Dict[str, List[ValidationSample]] = {
        bucket: [] for bucket, _quota in BUCKET_QUOTAS
    }
    quotas = dict(BUCKET_QUOTAS)

    for raw_row in cursor.fetchall():
        row = dict(zip(columns, raw_row))
        save_path = str(row["save_path"] or "")
        file_size = int(row["file_size"] or 0)
        bucket = _bucket_for_size(file_size)
        if (
            bucket is None
            or len(selected[bucket]) >= quotas[bucket]
            or not save_path
            or not path_exists(save_path)
        ):
            continue

        selected[bucket].append(
            ValidationSample(
                bucket=bucket,
                chat_id=str(row["chat_id"]),
                message_id=int(row["message_id"]),
                save_path=save_path,
                file_name=str(row["file_name"] or ""),
                media_type=str(row["media_type"] or ""),
                file_size=file_size,
            )
        )

    samples = [
        sample
        for bucket, _quota in BUCKET_QUOTAS
        for sample in selected[bucket]
    ]
    gaps = [
        bucket
        for bucket, quota in BUCKET_QUOTAS
        if len(selected[bucket]) < quota
    ]
    return samples, gaps


def decide_sample(
    *,
    same_sha: bool,
    baseline_verified: bool,
    candidate_verified: bool,
) -> SampleDecision:
    """Apply the explicit three-way integrity decision matrix."""
    if baseline_verified and candidate_verified:
        if same_sha:
            return SampleDecision(
                status="pass",
                reason="candidate and baseline match Telegram",
                blocks_parallel=False,
            )
        return SampleDecision(
            status="fail",
            reason="both files verify but whole-file SHA-256 values differ",
            blocks_parallel=True,
            unexplained_mismatch=True,
        )

    if baseline_verified and not candidate_verified:
        return SampleDecision(
            status="fail",
            reason="candidate failed Telegram verification",
            blocks_parallel=True,
        )

    if candidate_verified and not baseline_verified:
        if same_sha:
            return SampleDecision(
                status="invalid",
                reason="identical files produced conflicting Telegram verification",
                blocks_parallel=True,
                unexplained_mismatch=True,
            )
        return SampleDecision(
            status="pass",
            reason="candidate verifies; existing baseline archive is suspect",
            blocks_parallel=False,
        )

    return SampleDecision(
        status="invalid",
        reason="neither file has valid Telegram hash evidence",
        blocks_parallel=True,
        unexplained_mismatch=not same_sha,
    )


def build_run_report(
    results: Sequence[SampleResult],
    bucket_gaps: Sequence[str],
    *,
    run_id: str = "",
    started_at: str = "",
    finished_at: str = "",
    app_commit: str = "",
    kurigram_version: str = "2.2.24",
    workers: int = 2,
) -> dict:
    """Build the eligibility decision and all per-sample evidence."""
    now = datetime.now(timezone.utc).isoformat()
    valid_results = [result for result in results if result.decision != "invalid"]
    eligible = bool(
        len(results) == 6
        and len(valid_results) == 6
        and not bucket_gaps
        and all(result.decision == "pass" for result in results)
        and all(result.candidate_verified for result in results)
        and not any(result.unexplained_mismatch for result in results)
    )
    return {
        "run_id": run_id,
        "started_at": started_at or now,
        "finished_at": finished_at or now,
        "app_commit": app_commit,
        "kurigram_version": kurigram_version,
        "workers": workers,
        "sample_count": len(results),
        "valid_sample_count": len(valid_results),
        "bucket_gaps": list(bucket_gaps),
        "eligible": eligible,
        "samples": [asdict(result) for result in results],
    }


def write_report_atomic(
    report: dict,
    report_path: Union[str, os.PathLike],
) -> None:
    """Replace a JSON report only after its complete content is durable."""
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(payload)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()
