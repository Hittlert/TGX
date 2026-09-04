#!/usr/bin/env python3
"""Canonical entry point for TGX Evaluation Protocol v1."""

import argparse
import json
import time
from pathlib import Path

from analyze import evaluate_policy
from harness import PROTOCOL_SHA256, PROTOCOL_VERSION, execute_run
from manifest_generator import (
    DEFAULT_CASE_COUNTS,
    cohort_path,
    compute_sha256,
    generate_profile_manifest,
)
from self_test import run_self_tests


PROFILES = tuple(DEFAULT_CASE_COUNTS)
REQUIRED_CONFIG = (
    "eval_dir",
    "source_db",
    "baseline_root",
    "session_source_dir",
    "runner_image",
    "docker_command",
    "api_base",
    "environment",
    "artifacts",
)


def load_config(path):
    with open(path, "r", encoding="utf-8") as stream:
        config = json.load(stream)
    missing = [key for key in REQUIRED_CONFIG if not config.get(key)]
    if missing:
        raise ValueError(f"evaluation config is missing: {', '.join(missing)}")
    for engine in ("tdl", "tgx"):
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
    if not isinstance(config["docker_command"], list):
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


def validate_protocol_hash():
    protocol_path = Path(__file__).resolve().parents[2] / "docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md"
    actual = compute_sha256(protocol_path)
    if actual != PROTOCOL_SHA256:
        raise ValueError(
            f"protocol hash drift: harness={PROTOCOL_SHA256}, document={actual}"
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


def matching_baseline_exists(eval_dir, spec):
    baseline_root = Path(eval_dir) / "baselines" / "tdl"
    if not baseline_root.is_dir():
        return False
    identity_fields = (
        "baseline_cohort_id",
        "expected_binary_sha256",
        "expected_source_commit",
        "profile_id",
        "net_concurrency",
        "file_concurrency",
        "dc_pool_size",
        "duration_seconds",
        "warmup_seconds",
    )
    for path in baseline_root.glob("*/raw/run_spec.json"):
        try:
            with path.open("r", encoding="utf-8") as stream:
                existing = json.load(stream)
        except (OSError, json.JSONDecodeError):
            continue
        if all(existing.get(field) == spec.get(field) for field in identity_fields):
            return True
    return False


def run_engine(config, args, engine):
    eval_dir = Path(config["eval_dir"])
    policy_path = Path(args.policy)
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
                if engine == "tdl" and not args.force and matching_baseline_exists(
                    eval_dir, spec
                ):
                    raise FileExistsError(
                        "matching TDL baseline already exists; reuse it or pass --force "
                        "to run an intentional new repetition"
                    )
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
        stream.write("| Engine | Run | Cases | Match | Active Mbps | Verdict |\n")
        stream.write("|---|---|---:|---:|---:|---|\n")
        for engine, run_id, summary in rows:
            stream.write(
                f"| {engine} | `{run_id}` | {summary['total_cases']} | "
                f"{summary['match_fraction']:.1%} | "
                f"{summary['average_active_mbps']:.2f} | "
                f"**{summary['verdict']}** |\n"
            )
    return output


def parse_args():
    project_root = Path(__file__).resolve().parents[2]
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
        if not run_self_tests():
            raise SystemExit(1)
        return
    if not args.config:
        raise SystemExit("--config is required for this mode")

    config = load_config(args.config)
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
