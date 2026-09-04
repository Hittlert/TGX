#!/usr/bin/env python3
"""Analyze sealed TGX evaluation facts without modifying raw evidence."""

import argparse
import hashlib
import json
import re
from pathlib import Path


RAW_ARTIFACTS = (
    "protocol.json",
    "run_spec.json",
    "artifact.json",
    "environment.json",
    "manifest.jsonl",
    "events.jsonl",
    "metrics.jsonl",
    "task_results.jsonl",
    "file_inventory.jsonl",
    "hashes.jsonl",
    "errors.jsonl",
    "process.log",
)

RUN_SPEC_REQUIRED_FIELDS = (
    "protocol_version",
    "protocol_sha256",
    "run_id",
    "engine",
    "artifact_ref",
    "source_repository",
    "expected_source_commit",
    "expected_binary_sha256",
    "profile_id",
    "manifest_path",
    "manifest_sha256",
    "baseline_cohort_id",
    "sample_seed",
    "net_concurrency",
    "file_concurrency",
    "dc_pool_size",
    "duration_seconds",
    "warmup_seconds",
    "scratch_root",
    "session_source_dir",
    "runner_image",
    "docker_command",
    "api_base",
    "environment",
)

def compute_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    with open(path, "r", encoding="utf-8") as stream:
        return json.load(stream)


def read_jsonl(path):
    records = []
    with open(path, "r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid JSONL at {path}:{line_number}: {error}") from error
    return records


def seal_raw_directory(raw_dir):
    raw_path = Path(raw_dir)
    missing = [name for name in RAW_ARTIFACTS if not (raw_path / name).is_file()]
    if missing:
        raise ValueError(f"cannot seal raw directory; missing: {', '.join(missing)}")

    checksum_path = raw_path / "checksums.sha256"
    files = sorted(
        path for path in raw_path.iterdir() if path.is_file() and path != checksum_path
    )
    with checksum_path.open("x", encoding="ascii", newline="\n") as stream:
        for path in files:
            stream.write(f"{compute_sha256(path)}  {path.name}\n")
    return checksum_path


def verify_raw_directory(raw_dir):
    raw_path = Path(raw_dir)
    problems = []
    for name in RAW_ARTIFACTS:
        if not (raw_path / name).is_file():
            problems.append(f"missing raw artifact: {name}")

    checksum_path = raw_path / "checksums.sha256"
    if not checksum_path.is_file():
        problems.append("missing raw artifact: checksums.sha256")
        return problems

    expected = {}
    with checksum_path.open("r", encoding="ascii") as stream:
        for line_number, line in enumerate(stream, 1):
            parts = line.rstrip("\n").split("  ", 1)
            if len(parts) != 2 or len(parts[0]) != 64:
                problems.append(f"invalid checksum line: {line_number}")
                continue
            expected[parts[1]] = parts[0]

    present = {
        path.name
        for path in raw_path.iterdir()
        if path.is_file() and path.name != "checksums.sha256"
    }
    for name in sorted(present | set(expected)):
        path = raw_path / name
        if name not in expected:
            problems.append(f"checksum missing for: {name}")
        elif name not in present:
            problems.append(f"checksummed file is missing: {name}")
        elif compute_sha256(path) != expected[name]:
            problems.append(f"checksum mismatch: {name}")
    return problems


def _artifact_identity_problems(artifact):
    problems = []
    commit = str(artifact.get("source_commit") or "").strip().lower()
    version = str(artifact.get("version") or "").strip().lower()
    binary_sha = str(artifact.get("binary_sha256") or "")
    image_digest = str(artifact.get("image_digest") or "").strip().lower()
    source_repository = str(artifact.get("source_repository") or "").strip()
    build_time = str(artifact.get("build_time") or "").strip().lower()
    go_version = str(artifact.get("go_version") or "").strip().lower()

    if (
        commit in ("", "unknown")
        or len(commit) < 7
        or any(character not in "0123456789abcdef" for character in commit)
    ):
        problems.append("artifact source_commit is unknown")
    if version in ("", "dev", "unknown"):
        problems.append("artifact version is not release-identifiable")
    if artifact.get("source_dirty") is not False:
        problems.append("artifact source is dirty or unreported")
    if len(binary_sha) != 64 or any(
        character not in "0123456789abcdef" for character in binary_sha.lower()
    ):
        problems.append("artifact binary_sha256 is invalid")
    if image_digest in ("", "unknown") or image_digest.endswith(":latest"):
        problems.append("runner image is not immutable")
    if not source_repository:
        problems.append("artifact source_repository is missing")
    if build_time in ("", "unknown"):
        problems.append("artifact build_time is unknown")
    if go_version in ("", "unknown"):
        problems.append("artifact go_version is unknown")
    if not artifact.get("os") or not artifact.get("arch"):
        problems.append("artifact platform identity is missing")
    return problems


def _duplicates(values):
    seen = set()
    duplicates = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _longest_zero_seconds(metrics):
    longest = 0.0
    current = 0.0
    previous_elapsed = None
    for record in metrics:
        elapsed = record.get("monotonic_elapsed_sec")
        delta = 0.0
        if isinstance(elapsed, (int, float)) and previous_elapsed is not None:
            delta = max(0.0, elapsed - previous_elapsed)
        busy = (record.get("active_files") or 0) > 0 or (
            record.get("queued_jobs") or 0
        ) > 0
        stall = busy and (record.get("rolling_5s_bps") or 0) == 0
        if stall:
            current += delta
            longest = max(longest, current)
        else:
            current = 0.0
        if isinstance(elapsed, (int, float)):
            previous_elapsed = elapsed
    return longest


def evaluate_policy(run_root, policy_path, overwrite=False):
    run_path = Path(run_root)
    raw_path = run_path / "raw"
    policy = read_json(policy_path)
    policy_version = policy["policy_version"]
    analysis_path = run_path / "analysis" / policy_version
    if analysis_path.exists() and not overwrite:
        raise FileExistsError(f"analysis already exists: {analysis_path}")

    invalid_reasons = verify_raw_directory(raw_path)
    blocked_reasons = []
    failure_reasons = []
    manifest = []
    task_results = []
    hashes = []
    inventory = []
    metrics = []
    errors = []
    artifact = {}
    run_spec = {}
    non_golden_case_ids = set()

    if not invalid_reasons:
        try:
            artifact = read_json(raw_path / "artifact.json")
            run_spec = read_json(raw_path / "run_spec.json")
            manifest = read_jsonl(raw_path / "manifest.jsonl")
            task_results = read_jsonl(raw_path / "task_results.jsonl")
            hashes = read_jsonl(raw_path / "hashes.jsonl")
            inventory = read_jsonl(raw_path / "file_inventory.jsonl")
            metrics = read_jsonl(raw_path / "metrics.jsonl")
            errors = read_jsonl(raw_path / "errors.jsonl")
        except (OSError, ValueError, json.JSONDecodeError) as error:
            invalid_reasons.append(str(error))
        else:
            invalid_reasons.extend(_artifact_identity_problems(artifact))

            for field in RUN_SPEC_REQUIRED_FIELDS:
                if field not in run_spec or run_spec[field] is None:
                    invalid_reasons.append(f"RunSpec is missing required field: {field}")

            protocol = read_json(raw_path / "protocol.json")
            if run_spec.get("protocol_version") != protocol.get("protocol_version"):
                invalid_reasons.append("RunSpec protocol version does not match raw protocol")
            if policy.get("protocol_version") != protocol.get("protocol_version"):
                invalid_reasons.append(
                    "analysis policy protocol version does not match raw protocol"
                )
            if run_spec.get("protocol_sha256") != protocol.get("protocol_sha256"):
                invalid_reasons.append("RunSpec protocol hash does not match raw protocol")
            if run_spec.get("expected_binary_sha256") != artifact.get("binary_sha256"):
                invalid_reasons.append("artifact binary does not match RunSpec expectation")
            if run_spec.get("expected_source_commit") != artifact.get("source_commit"):
                invalid_reasons.append("artifact commit does not match RunSpec expectation")

            manifest_sha = compute_sha256(raw_path / "manifest.jsonl")
            if run_spec.get("manifest_sha256") != manifest_sha:
                invalid_reasons.append("RunSpec manifest_sha256 does not match raw manifest")
            if not run_spec.get("baseline_cohort_id"):
                invalid_reasons.append("RunSpec baseline_cohort_id is missing")

            manifest_ids = [record.get("case_id") for record in manifest]
            task_ids = [record.get("case_id") for record in task_results]
            hash_ids = [record.get("case_id") for record in hashes]
            if _duplicates(manifest_ids):
                invalid_reasons.append("manifest contains duplicate case_id values")
            if _duplicates(task_ids):
                invalid_reasons.append("task results contain duplicate case_id values")
            if set(task_ids) != set(manifest_ids):
                invalid_reasons.append("task results do not cover the manifest exactly")
            if set(hash_ids) != set(manifest_ids):
                invalid_reasons.append("hash results do not cover the manifest exactly")

            non_golden_case_ids = {
                record.get("case_id")
                for record in manifest
                if record.get("baseline_trust") != "golden"
            }
            untrusted = [
                record.get("case_id")
                for record in manifest
                if record.get("baseline_trust") not in ("golden", "local_disk")
            ]
            if untrusted:
                blocked_reasons.append(
                    f"{len(untrusted)} manifest cases lack a usable baseline reference"
                )

            if not metrics:
                blocked_reasons.append("metrics.jsonl contains no samples")
            metric_policy = policy.get("required_metrics", {})
            if isinstance(metric_policy, dict):
                required_metrics = metric_policy.get("common", []) + metric_policy.get(
                    run_spec.get("engine"), []
                )
            else:
                required_metrics = metric_policy
            for index, record in enumerate(metrics):
                collection_errors = record.get("collection_errors") or []
                blocking_errors = [
                    error
                    for error in collection_errors
                    if error.get("source") in required_metrics
                    or str(error.get("source", "")).startswith("/api/")
                ]
                if blocking_errors:
                    blocked_reasons.append(
                        f"metrics sample {index} has required collection_errors"
                    )
                for field in required_metrics:
                    if field not in record or record[field] is None:
                        blocked_reasons.append(
                            f"metrics sample {index} is missing required field {field}"
                        )

            environment = read_json(raw_path / "environment.json")
            for field in policy.get("required_environment_fields", []):
                if not environment.get(field):
                    invalid_reasons.append(f"environment field is missing: {field}")

            effective = environment.get("effective_daemon_config")
            if isinstance(effective, dict):
                if (
                    effective.get("file_concurrency") is not None
                    and effective.get("file_concurrency") != run_spec.get("file_concurrency")
                ):
                    invalid_reasons.append(
                        f"effective file concurrency {effective.get('file_concurrency')} "
                        f"does not match RunSpec {run_spec.get('file_concurrency')}"
                    )
                if (
                    effective.get("dc_pool_size") is not None
                    and effective.get("dc_pool_size") != run_spec.get("dc_pool_size")
                ):
                    invalid_reasons.append(
                        f"effective dc pool size {effective.get('dc_pool_size')} "
                        f"does not match RunSpec {run_spec.get('dc_pool_size')}"
                    )

            process_log = (raw_path / "process.log").read_text(
                encoding="utf-8", errors="replace"
            )
            marker = "=== Container State ==="
            if marker not in process_log:
                blocked_reasons.append("container exit/OOM/restart state was not collected")
            else:
                state = process_log.split(marker, 1)[1]
                restart_match = re.search(r"restart_count=(\d+)", state)
                if restart_match is None:
                    blocked_reasons.append("container restart count is unavailable")
                elif int(restart_match.group(1)) > 0:
                    failure_reasons.append(
                        f"container restart count is {restart_match.group(1)}"
                    )
                if re.search(r'"OOMKilled"\s*:\s*true', state):
                    failure_reasons.append("container was OOM-killed")

                state_json_str = state.split("restart_count=")[0].strip()
                parsed_state = {}
                if state_json_str:
                    try:
                        parsed_state = json.loads(state_json_str)
                    except json.JSONDecodeError:
                        pass

                exit_code = parsed_state.get("ExitCode")
                if exit_code is None:
                    exit_match = re.search(r'"ExitCode"\s*:\s*(-?\d+)', state)
                    if exit_match:
                        exit_code = int(exit_match.group(1))
                if exit_code is not None and exit_code != 0:
                    failure_reasons.append(
                        f"container exited with non-zero exit code {exit_code}"
                    )

                running = parsed_state.get("Running")
                if running is None:
                    run_match = re.search(
                        r'"Running"\s*:\s*(true|false)', state, re.IGNORECASE
                    )
                    if run_match:
                        running = run_match.group(1).lower() == "true"
                if running is False:
                    failure_reasons.append("container stopped unexpectedly")

            required_error_fields = policy.get("diagnosability", {}).get(
                "required_error_fields", []
            )
            for index, record in enumerate(errors):
                missing = [
                    field
                    for field in required_error_fields
                    if field not in record or record[field] in (None, "")
                ]
                if missing:
                    failure_reasons.append(
                        f"error record {index} lacks trace fields: {', '.join(missing)}"
                    )

    total_cases = len(manifest)
    manifest_by_case = {record.get("case_id"): record for record in manifest}
    task_by_case = {record.get("case_id"): record for record in task_results}
    hash_by_case = {record.get("case_id"): record for record in hashes}
    verified_completed = {
        case_id
        for case_id, record in task_by_case.items()
        if record.get("terminal_state") == "COMPLETED"
        and hash_by_case.get(case_id, {}).get("size_match") is True
        and hash_by_case.get(case_id, {}).get("sha_match") is True
    }
    completed_cases = len(verified_completed)
    failed_cases = sum(
        1 for record in task_results if record.get("terminal_state") == "FAILED"
    )
    timed_out_cases = sum(
        1 for record in task_results if record.get("terminal_state") == "TIMED_OUT"
    )
    canceled_cases = sum(
        1 for record in task_results if record.get("terminal_state") == "CANCELED"
    )
    match_fraction = completed_cases / total_cases if total_cases else 0.0

    exempt_buckets = set(
        policy.get("correctness", {}).get("completion_exempt_size_buckets", [])
    )
    required_case_ids = {
        case_id
        for case_id, record in manifest_by_case.items()
        if record.get("size_bucket") not in exempt_buckets
    }
    required_completed = len(required_case_ids & verified_completed)
    required_match_fraction = (
        required_completed / len(required_case_ids) if required_case_ids else 1.0
    )
    expected_fraction = policy.get("correctness", {}).get(
        "required_completed_file_match_fraction", 1.0
    )
    if required_match_fraction < expected_fraction:
        failure_reasons.append(
            "required completed match fraction "
            f"{required_match_fraction:.4f} is below {expected_fraction:.4f}"
        )
    mismatched_hashes = [
        record.get("case_id")
        for record in hashes
        if (
            record.get("case_id") in required_case_ids
            or task_by_case.get(record.get("case_id"), {}).get("terminal_state")
            == "COMPLETED"
        )
        and (record.get("size_match") is not True or record.get("sha_match") is not True)
    ]
    if mismatched_hashes:
        failure_reasons.append(
            f"size/SHA verification failed for {len(mismatched_hashes)} cases"
        )

    golden_attested_case_ids = []
    if run_spec.get("engine") == "tdl":
        golden_attested_case_ids = sorted(verified_completed)
    elif run_spec.get("engine") == "tgx" and non_golden_case_ids:
        paired_attested = set(run_spec.get("paired_tdl_attested_case_ids") or [])
        completed_claims = {
            case_id
            for case_id, record in task_by_case.items()
            if record.get("terminal_state") == "COMPLETED"
        }
        needs_attestation = non_golden_case_ids & (
            required_case_ids | completed_claims
        )
        missing_attestation = needs_attestation - paired_attested
        if missing_attestation:
            blocked_reasons.append(
                f"{len(missing_attestation)} cases lack matching TDL attestation"
            )

    orphan_count = sum(
        1
        for record in inventory
        if record.get("classification") != "expected_target"
    )
    if orphan_count and not policy.get("correctness", {}).get(
        "allow_unowned_residue", False
    ):
        failure_reasons.append(f"unowned residue count is {orphan_count}")
    actionable_errors = [
        record
        for record in errors
        if not (
            record.get("error_code") == "TIMED_OUT"
            and manifest_by_case.get(record.get("case_id"), {}).get("size_bucket")
            in exempt_buckets
        )
    ]
    if actionable_errors:
        failure_reasons.append(f"actionable error record count is {len(actionable_errors)}")

    measurement_metrics = [
        record for record in metrics if record.get("phase", "measurement") == "measurement"
    ]
    speeds = [
        record.get("rolling_5s_bps")
        for record in measurement_metrics
        if isinstance(record.get("rolling_5s_bps"), (int, float))
    ]
    active_speeds = [speed for speed in speeds if speed > 0]
    average_total_mbps = (
        sum(speeds) / len(speeds) * 8 / (1024 * 1024) if speeds else 0.0
    )
    average_active_mbps = (
        sum(active_speeds) / len(active_speeds) * 8 / (1024 * 1024)
        if active_speeds
        else 0.0
    )
    busy_metrics = [
        record
        for record in measurement_metrics
        if (record.get("active_files") or 0) > 0
        or (record.get("queued_jobs") or 0) > 0
    ]
    zero_speed_samples = sum(
        1
        for record in busy_metrics
        if (record.get("rolling_5s_bps") or 0) == 0
    )
    zero_speed_fraction = (
        zero_speed_samples / len(busy_metrics) if busy_metrics else 0.0
    )
    zero_speed_with_active_rpc_samples = sum(
        1
        for record in busy_metrics
        if (record.get("rolling_5s_bps") or 0) == 0 and (record.get("active_rpc") or 0) > 0
    )
    stability = policy.get("stability", {})
    longest_zero_seconds = _longest_zero_seconds(measurement_metrics)
    if run_spec.get("engine") == "tgx":
        if longest_zero_seconds > stability.get(
            "maximum_unexplained_zero_throughput_seconds", 10
        ):
            failure_reasons.append("unexplained zero-throughput run exceeded policy")
        if zero_speed_fraction >= stability.get("maximum_zero_throughput_fraction", 1.0):
            failure_reasons.append(
                f"zero-throughput fraction {zero_speed_fraction:.4f} exceeds policy"
            )

    performance = policy.get("performance", {})
    paired_tdl_mbps = None
    throughput_ratio = None
    if run_spec.get("engine") == "tdl" and average_active_mbps <= 0:
        failure_reasons.append("TDL baseline has no active throughput")
    if run_spec.get("engine") == "tgx":
        paired_tdl_mbps = run_spec.get("paired_tdl_average_active_mbps")
        paired_summary_sha = run_spec.get("paired_tdl_summary_sha256")
        paired_artifact_sha = run_spec.get("paired_tdl_artifact_sha256")
        if not isinstance(paired_tdl_mbps, (int, float)) or paired_tdl_mbps <= 0:
            blocked_reasons.append("matching TDL active throughput is unavailable")
        elif not paired_summary_sha or not paired_artifact_sha:
            blocked_reasons.append("matching TDL identity is incomplete")
        else:
            throughput_ratio = average_active_mbps / paired_tdl_mbps
            small_gate = performance.get(
                "small_absolute_mbps_when_control_at_least_mbps", {}
            )
            if (
                run_spec.get("profile_id") == "P-S"
                and paired_tdl_mbps >= small_gate.get("control_minimum_mbps", 250)
            ):
                required_mbps = small_gate.get("required_tgx_mbps", 200)
                if average_active_mbps < required_mbps:
                    failure_reasons.append(
                        f"small-file active throughput {average_active_mbps:.2f} Mbps "
                        f"is below {required_mbps:.2f} Mbps"
                    )
            else:
                required_ratio = performance.get(
                    "minimum_tgx_to_paired_tdl_payload_ratio", 0.75
                )
                if throughput_ratio < required_ratio:
                    failure_reasons.append(
                        f"TGX/TDL active throughput ratio {throughput_ratio:.4f} "
                        f"is below {required_ratio:.4f}"
                    )
            if run_spec.get("profile_id") == "P-L":
                required_large_mbps = performance.get(
                    "large_minimum_active_mbps", 150
                )
                if average_active_mbps < required_large_mbps:
                    failure_reasons.append(
                        f"large-file active throughput {average_active_mbps:.2f} Mbps "
                        f"is below {required_large_mbps:.2f} Mbps"
                    )

    invalid_reasons = sorted(set(invalid_reasons))
    blocked_reasons = sorted(set(blocked_reasons))
    failure_reasons = sorted(set(failure_reasons))
    if invalid_reasons:
        verdict = "INVALID"
    elif blocked_reasons:
        verdict = policy.get("missing_required_measurement", "BLOCKED")
    elif failure_reasons:
        verdict = "NO-GO"
    else:
        verdict = "GO"

    summary = {
        "policy_version": policy_version,
        "protocol_version": policy.get("protocol_version"),
        "run_id": run_spec.get("run_id") if not invalid_reasons else None,
        "engine": run_spec.get("engine") if not invalid_reasons else None,
        "profile_id": run_spec.get("profile_id") if not invalid_reasons else None,
        "baseline_cohort_id": (
            run_spec.get("baseline_cohort_id") if not invalid_reasons else None
        ),
        "artifact_sha256": (
            artifact.get("binary_sha256") if not invalid_reasons else None
        ),
        "net_concurrency": (
            run_spec.get("net_concurrency") if not invalid_reasons else None
        ),
        "file_concurrency": (
            run_spec.get("file_concurrency") if not invalid_reasons else None
        ),
        "dc_pool_size": run_spec.get("dc_pool_size") if not invalid_reasons else None,
        "verdict": verdict,
        "total_cases": total_cases,
        "completed_cases": completed_cases,
        "required_cases": len(required_case_ids),
        "required_completed_cases": required_completed,
        "failed_cases": failed_cases,
        "timed_out_cases": timed_out_cases,
        "canceled_cases": canceled_cases,
        "match_fraction": round(match_fraction, 4),
        "required_match_fraction": round(required_match_fraction, 4),
        "orphan_residue_count": orphan_count,
        "average_total_mbps": round(average_total_mbps, 2),
        "average_active_mbps": round(average_active_mbps, 2),
        "paired_tdl_average_active_mbps": paired_tdl_mbps,
        "tgx_to_tdl_active_throughput_ratio": (
            round(throughput_ratio, 4) if throughput_ratio is not None else None
        ),
        "golden_attested_case_ids": golden_attested_case_ids,
        "maximum_zero_speed_seconds": round(longest_zero_seconds, 3),
        "zero_speed_fraction": round(zero_speed_fraction, 4),
        "zero_speed_with_active_rpc_samples": zero_speed_with_active_rpc_samples,
        "error_count": len(errors),
        "invalid_reasons": invalid_reasons,
        "blocked_reasons": blocked_reasons,
        "failure_reasons": failure_reasons,
    }

    analysis_path.mkdir(parents=True, exist_ok=True)
    with (analysis_path / "summary.json").open("w", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2, sort_keys=True)
        stream.write("\n")
    with (analysis_path / "verdict.json").open("w", encoding="utf-8") as stream:
        json.dump(
            {"policy_version": policy_version, "verdict": verdict},
            stream,
            indent=2,
            sort_keys=True,
        )
        stream.write("\n")

    with (analysis_path / "report.md").open("w", encoding="utf-8") as stream:
        stream.write(f"# TGX Evaluation Analysis: {policy_version}\n\n")
        stream.write(f"- Verdict: `{verdict}`\n")
        stream.write(f"- Completed: {completed_cases}/{total_cases}\n")
        stream.write(f"- SHA/file match fraction: {match_fraction:.1%}\n")
        stream.write(
            f"- Required non-large match: {required_completed}/"
            f"{len(required_case_ids)} ({required_match_fraction:.1%})\n"
        )
        stream.write(f"- Orphan residue: {orphan_count}\n")
        stream.write(f"- Average active throughput: {average_active_mbps:.2f} Mbps\n")
        for heading, reasons in (
            ("Invalid evidence", invalid_reasons),
            ("Blocked measurements", blocked_reasons),
            ("Failed gates", failure_reasons),
        ):
            if reasons:
                stream.write(f"\n## {heading}\n\n")
                for reason in reasons:
                    stream.write(f"- {reason}\n")

    return summary


def find_project_root():
    p = Path(__file__).resolve()
    for parent in [p.parent, p.parent.parent, p.parent.parent.parent]:
        if (parent / "docs/evaluation/analysis-policy/baseline-v1.json").exists():
            return parent
    return p.parent.parent


def main():
    project_root = find_project_root()
    default_policy = project_root / "docs/evaluation/analysis-policy/baseline-v1.json"
    parser = argparse.ArgumentParser(description="Analyze sealed TGX raw evidence")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--policy", default=str(default_policy))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    summary = evaluate_policy(args.run_root, args.policy, overwrite=args.force)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
