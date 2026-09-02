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

    # Query stratified subsets directly from SQLite
    selected_items = []
    
    def fetch_bucket_sample(low, high, needed):
        if needed <= 0:
            return []
        q = f"""
            SELECT chat_id, message_id, file_name, save_path, media_type, file_size, created_at
            FROM download_records
            WHERE status = 'success' AND file_size >= {low} AND ({f'file_size <= {high}' if high else '1=1'})
              AND save_path IS NOT NULL AND save_path != ''
            ORDER BY RANDOM()
            LIMIT {needed * 4}
        """
        cursor.execute(q)
        candidates = cursor.fetchall()
        verified = []
        for r in candidates:
            chat_id, msg_id, file_name, save_path, media_type, file_size, created_at = r
            clean_path = clean_relative_path(save_path)
            baseline_file = os.path.join(baseline_storage_root, clean_path)
            if not os.path.exists(baseline_file):
                alt = os.path.join(baseline_storage_root, save_path)
                if os.path.exists(alt):
                    baseline_file = alt
                else:
                    continue
            verified.append({
                "chat_id": str(chat_id),
                "message_id": int(msg_id),
                "file_name": file_name or "",
                "clean_path": clean_path,
                "baseline_path": baseline_file,
                "media_type": media_type or "unknown",
                "file_size": int(file_size),
                "created_at": int(created_at),
                "size_bucket": classify_bucket(file_size),
            })
            if len(verified) >= needed:
                break
        return verified

    if profile_id == "P-S":
        # micro: 20%, small_low: 25%, small_mid: 35%, small_high: 20%
        selected_items.extend(fetch_bucket_sample(1, 65536, int(total_cases * 0.20)))
        selected_items.extend(fetch_bucket_sample(65537, 262144, int(total_cases * 0.25)))
        selected_items.extend(fetch_bucket_sample(262145, 921600, int(total_cases * 0.35)))
        rem = total_cases - len(selected_items)
        selected_items.extend(fetch_bucket_sample(921601, 1048576, rem))

    elif profile_id == "P-SM":
        # small 80%, medium 20%
        n_small = int(total_cases * 0.80)
        n_med = total_cases - n_small
        selected_items.extend(fetch_bucket_sample(65537, 1048576, n_small))
        selected_items.extend(fetch_bucket_sample(1048577, 16777216, int(n_med * 0.50)))
        rem = total_cases - len(selected_items)
        selected_items.extend(fetch_bucket_sample(16777217, 134217728, rem))

    elif profile_id == "P-LMS":
        # small 80%, med 15%, large 5%
        n_small = int(total_cases * 0.80)
        n_med = int(total_cases * 0.15)
        n_large = total_cases - n_small - n_med
        selected_items.extend(fetch_bucket_sample(65537, 1048576, n_small))
        selected_items.extend(fetch_bucket_sample(1048577, 134217728, n_med))
        selected_items.extend(fetch_bucket_sample(268435456, None, n_large))

    elif profile_id == "P-L":
        n_low = int(total_cases * 0.30)
        n_mid = int(total_cases * 0.40)
        n_high = total_cases - n_low - n_mid
        selected_items.extend(fetch_bucket_sample(268435456, 536870912, n_low))
        selected_items.extend(fetch_bucket_sample(536870913, 1073741824, n_mid))
        selected_items.extend(fetch_bucket_sample(1073741825, None, n_high))

    # Diversity enforcement: max 20% from a single group
    group_counts = {}
    capped_selected = []
    max_per_group = max(2, int(len(selected_items) * 0.20))

    random.shuffle(selected_items)
    for item in selected_items:
        gid = item["chat_id"]
        if group_counts.get(gid, 0) < max_per_group:
            group_counts[gid] = group_counts.get(gid, 0) + 1
            capped_selected.append(item)

    # Compute SHA256 baseline hashes
    print(f"[*] Calculating baseline SHA256 for {len(capped_selected)} selected cases...")
    manifest_records = []
    for idx, item in enumerate(capped_selected, 1):
        sha = compute_sha256(item["baseline_path"])
        rec = {
            "case_id": f"{profile_id}-{idx:04d}",
            "chat_id": item["chat_id"],
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
