#!/usr/bin/env python3
"""Create a deterministic, immutable TGX evaluation cohort."""

import argparse
import hashlib
import json
import os
import posixpath
import random
import re
import sqlite3
from pathlib import Path


SIZE_BUCKETS = {
    "micro": (1, 65_536),
    "small_low": (65_537, 262_144),
    "small_mid": (262_145, 921_600),
    "small_high": (921_601, 1_048_576),
    "medium_low": (1_048_577, 16_777_216),
    "medium_mid": (16_777_217, 33_554_432),
    "medium_high": (33_554_433, 134_217_728),
    "large_low": (268_435_456, 536_870_912),
    "large_mid": (536_870_913, 1_073_741_824),
    "large_high": (1_073_741_825, 4 * 1024 * 1024 * 1024),
}

PROFILE_TARGETS = {
    "P-S": [
        (1, 65_536, 0.20),
        (65_537, 262_144, 0.45),
        (262_145, 921_600, 0.80),
        (921_601, 1_048_576, 1.00),
    ],
    "P-SM": [
        (65_537, 1_048_576, 0.80),
        (1_048_577, 134_217_728, 1.00),
    ],
    "P-LMS": [
        (65_537, 1_048_576, 0.80),
        (1_048_577, 134_217_728, 0.95),
        (268_435_456, None, 1.00),
    ],
    "P-L": [
        (268_435_456, 536_870_912, 0.30),
        (536_870_913, 1_073_741_824, 0.70),
        (1_073_741_825, None, 1.00),
    ],
}

DEFAULT_CASE_COUNTS = {
    "P-S": 100,
    "P-SM": 50,
    "P-LMS": 50,
    "P-L": 20,
}


def compute_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_bucket(size_bytes):
    for name, (lower, upper) in SIZE_BUCKETS.items():
        if lower <= size_bytes <= upper:
            return name
    return "other"


def clean_relative_path(save_path):
    path = save_path.replace("\\", "/").strip("/")
    path = re.sub(r"^(app/downloads/|downloads/)+", "", path)
    path = posixpath.normpath(path)
    if path in ("", ".") or path == ".." or path.startswith("../"):
        raise ValueError(f"invalid relative path: {save_path!r}")
    return path


def cohort_path(manifest_path):
    path = Path(manifest_path)
    return path.with_suffix(path.suffix + ".cohort.json")


def _load_candidates(cursor, lower, upper):
    upper_clause = "AND d.file_size <= ?" if upper is not None else ""
    params = [lower]
    if upper is not None:
        params.append(upper)
    cursor.execute(
        f"""
        SELECT d.chat_id, d.message_id, d.file_name, d.save_path,
               d.media_type, d.file_size, d.created_at,
               COALESCE(c.chat_type, 'channel')
        FROM download_records AS d
        LEFT JOIN dialog_cache AS c ON d.chat_id = c.chat_id
        WHERE d.status = 'success'
          AND (c.chat_type IN ('channel', 'supergroup', 'group')
               OR (c.chat_type IS NULL AND d.chat_id LIKE '-%'))
          AND d.file_size >= ?
          {upper_clause}
          AND d.save_path IS NOT NULL
          AND d.save_path != ''
        ORDER BY d.chat_id, d.message_id
        """,
        params,
    )
    return cursor.fetchall()


def _resolve_baseline(root, save_path, clean_path, expected_size):
    root = root.resolve()
    raw_relative = str(save_path).replace("\\", "/").lstrip("/")
    candidates = [root / clean_path, root / raw_relative]
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            resolved.relative_to(root)
            if resolved.is_file() and resolved.stat().st_size == expected_size:
                return resolved
        except OSError:
            continue
        except ValueError:
            continue
    return None


def generate_profile_manifest(
    db_path,
    baseline_storage_root,
    profile_id,
    seed,
    output_file,
    total_cases=None,
    overwrite=False,
):
    if profile_id not in PROFILE_TARGETS:
        raise ValueError(f"unknown profile: {profile_id}")

    output_path = Path(output_file)
    metadata_path = cohort_path(output_path)
    if not overwrite and (output_path.exists() or metadata_path.exists()):
        raise FileExistsError(
            f"cohort already exists: {output_path}; use --force only to create a new cohort"
        )

    case_count = total_cases or DEFAULT_CASE_COUNTS[profile_id]
    max_per_group = max(1, int(case_count * 0.20))
    baseline_root = Path(baseline_storage_root)
    rng = random.Random(seed)
    selected = []
    selected_keys = set()
    group_counts = {}

    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        cursor = connection.cursor()
        for lower, upper, cumulative_fraction in PROFILE_TARGETS[profile_id]:
            target = round(case_count * cumulative_fraction)
            candidates = _load_candidates(cursor, lower, upper)
            rng.shuffle(candidates)

            for row in candidates:
                if len(selected) >= target:
                    break
                chat_id, message_id, name, save_path, media_type, size, created_at, peer_type = row
                key = (str(chat_id), int(message_id))
                if key in selected_keys:
                    continue
                group_id = str(chat_id)
                if group_counts.get(group_id, 0) >= max_per_group:
                    continue

                try:
                    clean_path = clean_relative_path(save_path)
                except ValueError:
                    continue
                baseline_path = _resolve_baseline(
                    baseline_root, save_path, clean_path, int(size)
                )
                if baseline_path is None:
                    continue

                selected_keys.add(key)
                group_counts[group_id] = group_counts.get(group_id, 0) + 1
                selected.append(
                    {
                        "chat_id": group_id,
                        "message_id": int(message_id),
                        "source_file_name": name or "",
                        "expected_rel_path": clean_path,
                        "media_type": media_type or "unknown",
                        "peer_type": peer_type,
                        "expected_size": int(size),
                        "message_date": int(created_at),
                        "size_bucket": classify_bucket(int(size)),
                        "baseline_sha256": compute_sha256(baseline_path),
                    }
                )

    if len(selected) != case_count:
        raise RuntimeError(
            f"profile {profile_id} requires {case_count} valid cases; selected {len(selected)}"
        )

    records = []
    for index, item in enumerate(selected, 1):
        records.append(
            {
                "case_id": f"{profile_id}-{index:04d}",
                "chat_id": item["chat_id"],
                "peer_type": item["peer_type"],
                "message_id": item["message_id"],
                "media_type": item["media_type"],
                "dc_id": 0,
                "message_date": item["message_date"],
                "size_bucket": item["size_bucket"],
                "expected_size": item["expected_size"],
                "source_file_name": item["source_file_name"],
                "expected_tdl_path": item["expected_rel_path"],
                "expected_tgx_path": item["expected_rel_path"],
                "baseline_sha256": item["baseline_sha256"],
                "baseline_trust": "local_disk",
                "sample_seed": seed,
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if overwrite else "x"
    with output_path.open(mode, encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )

    manifest_sha256 = compute_sha256(output_path)
    metadata = {
        "baseline_cohort_id": f"{profile_id.lower()}-{manifest_sha256[:16]}",
        "case_count": len(records),
        "manifest_path": str(output_path),
        "manifest_sha256": manifest_sha256,
        "profile_id": profile_id,
        "sample_seed": seed,
    }
    with metadata_path.open(mode, encoding="utf-8", newline="\n") as stream:
        json.dump(metadata, stream, indent=2, sort_keys=True)
        stream.write("\n")

    return metadata


def main():
    parser = argparse.ArgumentParser(
        description="Create an immutable TGX evaluation cohort"
    )
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--baseline-root", required=True)
    parser.add_argument("--profile", required=True, choices=sorted(PROFILE_TARGETS))
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--cases", type=int)
    parser.add_argument("--out", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    metadata = generate_profile_manifest(
        db_path=args.db_path,
        baseline_storage_root=args.baseline_root,
        profile_id=args.profile,
        seed=args.seed,
        output_file=args.out,
        total_cases=args.cases,
        overwrite=args.force,
    )
    print(json.dumps(metadata, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
