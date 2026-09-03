#!/usr/bin/env python3
"""
TGX Evaluation Protocol v1.0 - Manifest Generator
Implements exact sampling rules from docs/evaluation/profiles-v1.json and TGX_EVALUATION_PROTOCOL_V1.md.
"""

import os
import re
import sys
import json
import random
import sqlite3
import hashlib
import argparse
import posixpath

# Exact size buckets from profiles-v1.json
SIZE_BUCKETS = {
    "micro": (1, 65536),
    "small_low": (65537, 262144),
    "small_mid": (262145, 921600),
    "small_high": (921601, 1048576),
    "medium_low": (1048577, 16777216),
    "medium_mid": (16777217, 33554432),
    "medium_high": (33554433, 134217728),
    "large_low": (268435456, 536870912),
    "large_mid": (536870913, 1073741824),
    "large_high": (1073741825, 4 * 1024 * 1024 * 1024),
}

SENTINELS = {
    "below_1MiB": (950 * 1024, 1048575),
    "at_1MiB": (1048576, 1048576),
    "around_1MiB": (950 * 1024, 1150 * 1024),
    "around_16MiB": (15 * 1024 * 1024, 17 * 1024 * 1024),
    "around_32MiB": (31 * 1024 * 1024, 33 * 1024 * 1024),
}

def classify_bucket(size_bytes):
    for name, (low, high) in SIZE_BUCKETS.items():
        if low <= size_bytes <= high:
            return name
    return "other"

def compute_sha256(filepath):
    if not os.path.isfile(filepath):
        return None
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()

def clean_relative_path(save_path):
    p = save_path.replace("\\", "/").strip("/")
    p = re.sub(r"^(app/downloads/|downloads/)+", "", p)
    return posixpath.normpath(p)

def generate_profile_manifest(db_path, baseline_storage_root, profile_id, seed, output_file, total_cases=None):
    random.seed(seed)
    print(f"[*] Generating Manifest for Profile: {profile_id} (Seed: {seed})")
    print(f"[*] Reading source DB (mode=ro): {db_path}")

    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    cursor = conn.cursor()

    if total_cases is None:
        if profile_id == "P-S":
            total_cases = 100
        elif profile_id in ("P-SM", "P-LMS"):
            total_cases = 50
        elif profile_id == "P-L":
            total_cases = 20
        else:
            total_cases = 50

    max_per_group = max(2, int(total_cases * 0.20))
    group_counts = {}
    selected_items = []
    seen_keys = set()

    def fetch_bucket_sample(low, high, needed):
        if needed <= 0 or len(selected_items) >= total_cases:
            return
        q = f"""
            SELECT d.chat_id, d.message_id, d.file_name, d.save_path, d.media_type, d.file_size, d.created_at, COALESCE(c.chat_type, 'channel') as chat_type
            FROM download_records d
            LEFT JOIN dialog_cache c ON d.chat_id = c.chat_id
            WHERE d.status = 'success' 
              AND (c.chat_type IN ('channel', 'supergroup', 'group') OR (c.chat_type IS NULL AND d.chat_id LIKE '-%'))
              AND d.file_size >= {low} AND ({f'd.file_size <= {high}' if high else '1=1'})
              AND d.save_path IS NOT NULL AND d.save_path != ''
              AND (d.media_type IN ('document', 'video', 'audio', 'voice', 'animation', 'MessageMediaType.DOCUMENT', 'MessageMediaType.VIDEO', 'MessageMediaType.ANIMATION') OR d.media_type IS NULL OR d.media_type = '')
            ORDER BY RANDOM()
            LIMIT {needed * 20}
        """
        cursor.execute(q)
        candidates = cursor.fetchall()
        
        for r in candidates:
            if len(selected_items) >= total_cases:
                break
            chat_id, msg_id, file_name, save_path, media_type, file_size, created_at, chat_type = r
            key = (str(chat_id), int(msg_id))
            if key in seen_keys:
                continue

            gid = str(chat_id)
            if group_counts.get(gid, 0) >= max_per_group:
                continue

            clean_path = clean_relative_path(save_path)
            baseline_file = os.path.join(baseline_storage_root, clean_path)
            if not os.path.exists(baseline_file):
                alt = os.path.join(baseline_storage_root, save_path)
                if os.path.exists(alt):
                    baseline_file = alt
                else:
                    continue

            # Verify actual disk file matches recorded size exactly
            try:
                actual_size = os.path.getsize(baseline_file)
                if actual_size != int(file_size) or actual_size <= 0:
                    continue
            except OSError:
                continue

            seen_keys.add(key)
            group_counts[gid] = group_counts.get(gid, 0) + 1
            selected_items.append({
                "chat_id": str(chat_id),
                "message_id": int(msg_id),
                "file_name": file_name or "",
                "clean_path": clean_path,
                "baseline_path": baseline_file,
                "media_type": media_type or "unknown",
                "peer_type": chat_type,
                "file_size": int(file_size),
                "created_at": int(created_at),
                "size_bucket": classify_bucket(file_size),
            })
            if len(selected_items) >= needed:
                break

    if profile_id == "P-S":
        # Target: 100 cases, small files (< 1MiB)
        fetch_bucket_sample(1, 65536, int(total_cases * 0.20))
        fetch_bucket_sample(65537, 262144, int(total_cases * 0.45))
        fetch_bucket_sample(262145, 921600, int(total_cases * 0.80))
        fetch_bucket_sample(921601, 1048576, total_cases)

    elif profile_id == "P-SM":
        # Target: 50 cases (80% small, 20% medium)
        fetch_bucket_sample(65537, 1048576, int(total_cases * 0.80))
        fetch_bucket_sample(1048577, 134217728, total_cases)

    elif profile_id == "P-LMS":
        # Target: 50 cases (80% small, 15% medium, 5% large)
        fetch_bucket_sample(65537, 1048576, int(total_cases * 0.80))
        fetch_bucket_sample(1048577, 134217728, int(total_cases * 0.95))
        fetch_bucket_sample(268435456, None, total_cases)

    elif profile_id == "P-L":
        # Target: 20 cases (large files > 256MiB)
        fetch_bucket_sample(268435456, 536870912, int(total_cases * 0.30))
        fetch_bucket_sample(536870913, 1073741824, int(total_cases * 0.70))
        fetch_bucket_sample(1073741825, None, total_cases)

    # If still below total_cases due to bucket constraints, fill from valid sizes
    if len(selected_items) < total_cases:
        low = 1 if profile_id == "P-S" else (268435456 if profile_id == "P-L" else 65537)
        high = 1048576 if profile_id == "P-S" else None
        fetch_bucket_sample(low, high, total_cases)

    # Compute baseline SHA256
    print(f"[*] Calculating baseline SHA256 for {len(selected_items)} selected cases...")
    manifest_records = []
    bucket_counts = {}
    for idx, item in enumerate(selected_items, 1):
        b = item["size_bucket"]
        bucket_counts[b] = bucket_counts.get(b, 0) + 1
        sha = compute_sha256(item["baseline_path"])
        rec = {
            "case_id": f"{profile_id}-{idx:04d}",
            "chat_id": item["chat_id"],
            "peer_type": item.get("peer_type", "channel"),
            "message_id": item["message_id"],
            "media_type": item["media_type"],
            "dc_id": 0,
            "message_date": item["created_at"],
            "size_bucket": item["size_bucket"],
            "expected_size": item["file_size"],
            "source_file_name": item["file_name"],
            "expected_tdl_path": item["clean_path"],
            "expected_tgx_path": item["clean_path"],
            "baseline_sha256": sha or "",
            "baseline_trust": "golden" if sha else "none",
            "sample_seed": seed,
        }
        manifest_records.append(rec)

    total_expected_bytes = sum(r["expected_size"] for r in manifest_records)
    print(f"[*] Profile {profile_id} Manifest Generated: {len(manifest_records)} cases, Total bytes: {total_expected_bytes / (1024*1024):.2f} MiB")
    print(f"[*] Bucket Distribution: {bucket_counts}")
    print(f"[*] Group Diversity: {len(group_counts)} distinct groups, Max per group: {max(group_counts.values()) if group_counts else 0}")

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        for r in manifest_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[✓] Successfully wrote {len(manifest_records)} cases to {output_file}")
    return manifest_records

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TGX Evaluation Protocol v1 Manifest Generator")
    parser.add_argument("--db-path", default="/volume2/docker/telegram_media_downloader_us/state/download_records.sqlite3")
    parser.add_argument("--baseline-root", default="/home/hittler/SpecialMedias/06-碎片整理区/碎片媒体/TG2/")
    parser.add_argument("--profile", required=True, choices=["P-S", "P-SM", "P-LMS", "P-L"])
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--cases", type=int, default=None)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    generate_profile_manifest(args.db_path, args.baseline_root, args.profile, args.seed, args.out, args.cases)
