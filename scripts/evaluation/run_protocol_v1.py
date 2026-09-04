#!/usr/bin/env python3
"""Canonical entry point for TGX Evaluation Protocol v1."""

import argparse
import json
import time
from pathlib import Path

from analyze import evaluate_policy, verify_raw_directory
from harness import PROTOCOL_SHA256, PROTOCOL_VERSION, execute_run
from manifest_generator import (
    DEFAULT_CASE_COUNTS,
    cohort_path,
    compute_sha256,
    generate_profile_manifest,
)


PROFILES = tuple(DEFAULT_CASE_COUNTS)
RUNTIME_CONFIG = (
    "session_source_dir",
    "runner_image",
    "docker_command",
    "api_base",
    "environment",
    "artifacts",
)


def load_config(path, mode):
    with open(path, "r", encoding="utf-8") as stream:
        config = json.load(stream)
    required = ["eval_dir"]
    if mode == "manifests":
        required.extend(("source_db", "baseline_root"))
    elif mode in ("baseline", "candidate"):
        required.extend(RUNTIME_CONFIG)
    missing = [key for key in required if not config.get(key)]
    if missing:
        raise ValueError(f"evaluation config is missing: {', '.join(missing)}")
    engines = {"baseline": ("tdl",), "candidate": ("tgx",)}.get(mode, ())
    for engine in engines:
        artifact = config["artifacts"].get(engine) or {}
        missing_artifact = [
            key
            for key in (
                "binary",
                "source_repository",
                "expected_commit",
                "expected_sha256",
            )
            if not artifact.get(key)
        ]
        if missing_artifact:
            raise ValueError(
                f"evaluation config artifacts.{engine} is missing: "
                + ", ".join(missing_artifact)
            )
        expected_sha = artifact["expected_sha256"]
        if len(expected_sha) != 64 or any(c not in "0123456789abcdef" for c in expected_sha):
            raise ValueError(f"artifacts.{engine}.expected_sha256 is not SHA-256")
        expected_commit = artifact["expected_commit"].lower()
        if len(expected_commit) < 7 or any(
            character not in "0123456789abcdef" for character in expected_commit
        ):
            raise ValueError(f"artifacts.{engine}.expected_commit is not a Git commit")
    if mode in ("baseline", "candidate") and not isinstance(
        config["docker_command"], list
    ):
        raise ValueError("evaluation config docker_command must be a JSON array")
    return config


def validate_environment(config, policy_path):
    with open(policy_path, "r", encoding="utf-8") as stream:
        policy = json.load(stream)
    environment = config["environment"]
    missing = [
        field
        for field in policy.get("required_environment_fields", [])
        if not environment.get(field)
    ]
    if missing:
        raise ValueError(f"evaluation environment is missing: {', '.join(missing)}")


def find_project_root():
    p = Path(__file__).resolve()
    for parent in [p.parent, p.parent.parent, p.parent.parent.parent]:
        if (parent / "docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md").exists():
            return parent
    return p.parent.parent


def validate_protocol_hash():
    protocol_path = find_project_root() / "docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md"
    actual = compute_sha256(protocol_path)
    if actual != PROTOCOL_SHA256:
        raise ValueError(
            f"protocol hash drift: harness={PROTOCOL_SHA256}, document={actual}"
        )


def validate_run_spec_shape(run_spec):
    schema_path = find_project_root() / "docs/evaluation/run-spec.schema.json"
    with schema_path.open("r", encoding="utf-8") as stream:
        schema = json.load(stream)
    missing = [key for key in schema["required"] if key not in run_spec]
    unknown = sorted(set(run_spec) - set(schema["properties"]))
    if missing or unknown:
        raise ValueError(
            f"RunSpec schema mismatch: missing={missing}, unknown={unknown}"
        )


def load_cohort(manifest_path):
    metadata_path = cohort_path(manifest_path)
    if not manifest_path.is_file() or not metadata_path.is_file():
        raise FileNotFoundError(f"fixed cohort is missing for {manifest_path.name}")
    with metadata_path.open("r", encoding="utf-8") as stream:
        metadata = json.load(stream)
    actual_sha = compute_sha256(manifest_path)
    if metadata.get("manifest_sha256") != actual_sha:
        raise ValueError(f"fixed cohort checksum mismatch: {manifest_path}")
    return metadata


def ensure_manifests(config, profiles, seed, force):
    manifest_dir = Path(config["eval_dir"]) / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    cohorts = {}
    for profile in profiles:
        manifest_path = manifest_dir / f"{profile}.jsonl"
        if manifest_path.exists() and not force:
            cohorts[profile] = load_cohort(manifest_path)
            continue
        cohorts[profile] = generate_profile_manifest(
            db_path=config["source_db"],
            baseline_storage_root=config["baseline_root"],
            profile_id=profile,
            seed=seed,
            output_file=manifest_path,
            overwrite=force,
        )
    return cohorts


def build_run_spec(
    config,
    engine,
    profile,
    cohort,
    run_id,
    duration,
    warmup,
    repetition,
    net_concurrency,
    file_concurrency,
    dc_pool_size,
):
    eval_root = Path(config["eval_dir"])
    artifact = config["artifacts"][engine]
    return {
        "protocol_version": PROTOCOL_VERSION,
        "protocol_sha256": PROTOCOL_SHA256,
        "run_id": run_id,
        "engine": engine,
        "artifact_ref": artifact["binary"],
        "source_repository": artifact["source_repository"],
        "expected_source_commit": artifact["expected_commit"],
        "expected_binary_sha256": artifact["expected_sha256"],
        "profile_id": profile,
        "manifest_path": cohort["manifest_path"],
        "manifest_sha256": cohort["manifest_sha256"],
        "baseline_cohort_id": cohort["baseline_cohort_id"],
        "sample_seed": cohort["sample_seed"],
        "net_concurrency": net_concurrency,
        "file_concurrency": file_concurrency,
        "dc_pool_size": dc_pool_size,
        "duration_seconds": duration,
        "warmup_seconds": warmup,
        "repetition": repetition,
        "scratch_root": str(eval_root.parent / "scratch_runs" / run_id),
        "session_source_dir": config["session_source_dir"],
        "runner_image": config["runner_image"],
        "docker_command": config["docker_command"],
        "network_container": config.get("network_container"),
        "api_base": config["api_base"],
        "namespace": config.get("namespace", "evaluation"),
        "environment": config["environment"],
        "drain_timeout_seconds": config.get("drain_timeout_seconds", 60),
    }


def find_matching_baseline(eval_dir, spec, include_artifact, policy_version):
    baseline_root = Path(eval_dir) / "baselines" / "tdl"
    if not baseline_root.is_dir():
        return None
    identity_fields = [
        "profile_id",
        "manifest_sha256",
        "net_concurrency",
        "file_concurrency",
        "dc_pool_size",
        "duration_seconds",
        "warmup_seconds",
    ]
    if include_artifact:
        identity_fields.extend(("expected_binary_sha256", "expected_source_commit"))
    for path in baseline_root.glob("*/raw/run_spec.json"):
        run_root = path.parents[1]
        if verify_raw_directory(path.parent):
            continue
        summary_path = run_root / "analysis" / policy_version / "summary.json"
        if not summary_path.is_file():
            continue
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if summary.get("verdict") != "GO":
                continue
        except (OSError, json.JSONDecodeError):
            continue
        try:
            with path.open("r", encoding="utf-8") as stream:
                existing = json.load(stream)
        except (OSError, json.JSONDecodeError):
            continue
        existing_cohort_id = existing.get("baseline_cohort_id")
        if not existing_cohort_id:
            raw_manifest = path.parent / "manifest.jsonl"
            if raw_manifest.is_file():
                existing_cohort_id = (
                    f"{spec['profile_id'].lower()}-"
                    f"{compute_sha256(raw_manifest)[:16]}"
                )
        if existing_cohort_id != spec.get("baseline_cohort_id"):
            continue
        if existing.get("manifest_sha256") != spec.get("manifest_sha256"):
            continue
        if all(existing.get(field) == spec.get(field) for field in identity_fields):
            return {
                "run_root": str(run_root),
                "summary": summary,
                "summary_path": str(summary_path),
            }
    return None


def run_engine(config, args, engine):
    eval_dir = Path(config["eval_dir"])
    policy_path = Path(args.policy)
    with policy_path.open("r", encoding="utf-8") as stream:
        policy_version = json.load(stream)["policy_version"]
    binary = Path(config["artifacts"][engine]["binary"])
    if not binary.is_file():
        raise FileNotFoundError(f"{engine} artifact does not exist: {binary}")

    suite_id = args.suite_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    for repetition in range(1, args.repetitions + 1):
        for profile in args.profiles:
            cohort = load_cohort(eval_dir / "manifests" / f"{profile}.jsonl")
            levels = args.sweep if profile == "P-LMS" and args.sweep else [args.net_concurrency]
            for concurrency in levels:
                run_id = (
                    f"{suite_id}-{engine}-{profile.lower()}-"
                    f"c{concurrency}-r{repetition}"
                )
                spec = build_run_spec(
                    config=config,
                    engine=engine,
                    profile=profile,
                    cohort=cohort,
                    run_id=run_id,
                    duration=args.duration,
                    warmup=args.warmup,
                    repetition=repetition,
                    net_concurrency=concurrency,
                    file_concurrency=args.file_concurrency,
                    dc_pool_size=args.dc_pool_size,
                )
                matching_baseline = find_matching_baseline(
                    eval_dir,
                    spec,
                    include_artifact=engine == "tdl",
                    policy_version=policy_version,
                )
                if engine == "tdl" and not args.force and matching_baseline:
                    raise FileExistsError(
                        "matching TDL baseline already exists; reuse it or pass --force "
                        "to run an intentional new repetition"
                    )
                if engine == "tgx" and not matching_baseline:
                    raise FileNotFoundError(
                        "no matching sealed TDL baseline exists for this cohort and RunSpec"
                    )
                if engine == "tgx" and matching_baseline:
                    baseline_summary = matching_baseline["summary"]
                    spec.update(
                        {
                            "paired_tdl_run_id": baseline_summary["run_id"],
                            "paired_tdl_summary_sha256": compute_sha256(
                                matching_baseline["summary_path"]
                            ),
                            "paired_tdl_artifact_sha256": baseline_summary[
                                "artifact_sha256"
                            ],
                            "paired_tdl_average_active_mbps": baseline_summary[
                                "average_active_mbps"
                            ],
                            "paired_tdl_attested_case_ids": baseline_summary[
                                "golden_attested_case_ids"
                            ],
                        }
                    )
                validate_run_spec_shape(spec)
                run_root = execute_run(
                    spec,
                    str(binary),
                    eval_dir=str(eval_dir),
                    host_port=args.port,
                )
                evaluate_policy(run_root, policy_path)


def generate_report(config, policy_version, force):
    eval_dir = Path(config["eval_dir"])
    output = eval_dir / "analysis" / policy_version / "comparison-report.md"
    if output.exists() and not force:
        raise FileExistsError(f"comparison report already exists: {output}")

    rows = []
    for engine, root in (
        ("tdl", eval_dir / "baselines" / "tdl"),
        ("tgx", eval_dir / "runs" / "tgx"),
    ):
        if not root.is_dir():
            continue
        for summary_path in sorted(root.glob(f"*/analysis/{policy_version}/summary.json")):
            with summary_path.open("r", encoding="utf-8") as stream:
                summary = json.load(stream)
            rows.append((engine, summary_path.parents[2].name, summary))

    output.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if force else "x"
    with output.open(mode, encoding="utf-8", newline="\n") as stream:
        stream.write(f"# TGX Evaluation Comparison: {policy_version}\n\n")
        grouped = {}
        for engine, run_id, summary in rows:
            identity = (
                summary.get("profile_id") or "invalid",
                summary.get("baseline_cohort_id") or "unidentified",
            )
            grouped.setdefault(identity, []).append((engine, run_id, summary))
        for (profile, cohort), group_rows in sorted(grouped.items()):
            stream.write(f"## {profile}: `{cohort}`\n\n")
            stream.write(
                "| Engine | Run | Artifact | Net/File/DC | Cases | Match | "
                "Active Mbps | Verdict |\n"
            )
            stream.write("|---|---|---|---|---:|---:|---:|---|\n")
            for engine, run_id, summary in group_rows:
                stream.write(
                    f"| {engine} | `{run_id}` | "
                    f"`{str(summary.get('artifact_sha256') or '')[:12]}` | "
                    f"{summary.get('net_concurrency')}/"
                    f"{summary.get('file_concurrency')}/"
                    f"{summary.get('dc_pool_size')} | {summary['total_cases']} | "
                    f"{summary['match_fraction']:.1%} | "
                    f"{summary['average_active_mbps']:.2f} | "
                    f"**{summary['verdict']}** |\n"
                )
            stream.write("\n")
    return output


def parse_args():
    project_root = find_project_root()
    default_policy = project_root / "docs/evaluation/analysis-policy/baseline-v1.json"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        choices=("self-test", "manifests", "baseline", "candidate", "report"),
    )
    parser.add_argument("--config", help="Environment-specific evaluation JSON")
    parser.add_argument("--policy", default=str(default_policy))
    parser.add_argument("--profiles", nargs="+", choices=PROFILES, default=list(PROFILES))
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--suite-id")
    parser.add_argument("--duration", type=int, default=240)
    parser.add_argument("--warmup", type=int, default=15)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--net-concurrency", type=int, default=32)
    parser.add_argument("--file-concurrency", type=int, default=5)
    parser.add_argument("--dc-pool-size", type=int, default=32)
    parser.add_argument("--sweep", nargs="+", type=int)
    parser.add_argument("--port", type=int, default=5890)
    return parser.parse_args()


def main():
    args = parse_args()
    validate_protocol_hash()
    if args.mode == "self-test":
        from self_test import run_self_tests

        if not run_self_tests():
            raise SystemExit(1)
        return
    if not args.config:
        raise SystemExit("--config is required for this mode")

    config = load_config(args.config, args.mode)
    if args.mode in ("baseline", "candidate"):
        validate_environment(config, args.policy)
    if args.mode == "manifests":
        ensure_manifests(config, args.profiles, args.seed, args.force)
    elif args.mode == "baseline":
        run_engine(config, args, "tdl")
    elif args.mode == "candidate":
        run_engine(config, args, "tgx")
    elif args.mode == "report":
        with open(args.policy, "r", encoding="utf-8") as stream:
            policy_version = json.load(stream)["policy_version"]
        print(generate_report(config, policy_version, args.force))


if __name__ == "__main__":
    main()
