#!/usr/bin/env python3
"""Analyze sealed TGX evaluation facts without modifying raw evidence."""

import argparse
import hashlib
import json
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

    if commit in ("", "unknown") or len(commit) < 7:
        problems.append("artifact source_commit is unknown")
    if version in ("", "dev", "unknown"):
        problems.append("artifact version is not release-identifiable")
    if artifact.get("source_dirty") is not False:
        problems.append("artifact source is dirty or unreported")
    if len(binary_sha) != 64:
        problems.append("artifact binary_sha256 is invalid")
    if not image_digest or image_digest.endswith(":latest"):
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


def _longest_zero_run(metrics):
    longest = 0
    current = 0
    for record in metrics:
        if record.get("rolling_5s_bps") == 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
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

            protocol = read_json(raw_path / "protocol.json")
            if run_spec.get("protocol_version") != protocol.get("protocol_version"):
                invalid_reasons.append("RunSpec protocol version does not match raw protocol")
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

            non_golden = [
                record.get("case_id")
                for record in manifest
                if record.get("baseline_trust") != "golden"
            ]
            if non_golden:
                blocked_reasons.append(
                    f"{len(non_golden)} manifest cases lack golden baseline trust"
                )

            if not metrics:
                blocked_reasons.append("metrics.jsonl contains no samples")
            required_metrics = policy.get("required_metrics", [])
            for index, record in enumerate(metrics):
                collection_errors = record.get("collection_errors") or []
                if collection_errors:
                    blocked_reasons.append(
                        f"metrics sample {index} has collection_errors"
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

            required_error_fields = policy.get("diagnosability", {}).get(
                "required_error_fields", []
            )
            for index, record in enumerate(errors):
                missing = [field for field in required_error_fields if not record.get(field)]
                if missing:
                    failure_reasons.append(
                        f"error record {index} lacks trace fields: {', '.join(missing)}"
                    )

    total_cases = len(manifest)
    completed_cases = sum(
        1 for record in task_results if record.get("terminal_state") == "COMPLETED"
    )
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

    expected_fraction = policy.get("correctness", {}).get(
        "required_completed_file_match_fraction", 1.0
    )
    if match_fraction < expected_fraction:
        failure_reasons.append(
            f"completed match fraction {match_fraction:.4f} is below {expected_fraction:.4f}"
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
    if errors:
        failure_reasons.append(f"error record count is {len(errors)}")

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
        "failed_cases": failed_cases,
        "timed_out_cases": timed_out_cases,
        "canceled_cases": canceled_cases,
        "match_fraction": round(match_fraction, 4),
        "orphan_residue_count": orphan_count,
        "average_total_mbps": round(average_total_mbps, 2),
        "average_active_mbps": round(average_active_mbps, 2),
        "maximum_zero_speed_samples": _longest_zero_run(measurement_metrics),
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


def main():
    project_root = Path(__file__).resolve().parents[2]
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
