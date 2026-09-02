#!/usr/bin/env python3
"""
TGX Evaluation v1 - Stratified Manifest Generator
Generates reproducible test manifests from production download_records in read-only mode.
Complies with specifications in docs/reviews/2026-09-02-tgx-evaluation-v1.md.
"""

import os
import sys
import json
import random
import sqlite3
import hashlib
import argparse
from datetime import datetime

# Size buckets (bytes)
SIZE_BUCKETS = {
    "micro": (1, 64 * 1024),                   # 1B - 64KiB
    "small": (64 * 1024 + 1, 1024 * 1024),     # >64KiB - 1MiB
    "medium": (1024 * 1024 + 1, 128 * 1024 * 1024), # >1MiB - 128MiB
    "large": (256 * 1024 * 1024, 4 * 1024 * 1024 * 1024), # >=256MiB
    "boundary-1": (900 * 1024, 1150 * 1024),   # ~1MiB boundary
    "boundary-16": (15 * 1024 * 1024, 17 * 1024 * 1024), # ~16MiB boundary
    "boundary-32": (30 * 1024 * 1024, 34 * 1024 * 1024), # ~32MiB boundary
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

def generate_manifest(db_path, output_root, scenario, target_count, seed, output_file):
    random.seed(seed)
    print(f"[*] Opening production DB (read-only): {db_path}")
    
    # Read-only SQLite URI
    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    cursor = conn.cursor()

    # Query success records with size > 0
    query = """
        SELECT chat_id, message_id, file_name, save_path, media_type, file_size, created_at
        FROM download_records
        WHERE status = 'success' AND file_size > 0 AND save_path IS NOT NULL AND save_path != ''
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    print(f"[*] Total success records in DB: {len(rows)}")

    # Filter according to scenario
    candidates = []
    for r in rows:
        chat_id, msg_id, file_name, save_path, media_type, file_size, created_at = r
        bucket = classify_bucket(file_size)
        
        # Scenario matching
        match = False
        if scenario == "S1" and bucket in ("micro", "small"):
            match = True
        elif scenario == "S2" and bucket in ("large", "medium"):
            match = True
        elif scenario == "S3": # Mixed
            match = True
        elif scenario == "S4" and bucket in ("boundary-1", "boundary-16", "boundary-32"):
            match = True
        elif scenario == "all":
            match = True

        if match:
            candidates.append({
                "chat_id": chat_id,
                "message_id": msg_id,
                "file_name": file_name or "",
                "save_path": save_path,
                "media_type": media_type or "unknown",
                "file_size": file_size,
                "created_at": created_at,
                "size_bucket": bucket,
            })

    print(f"[*] Candidates matching scenario {scenario}: {len(candidates)}")
    if not candidates:
        print("[!] Error: No matching candidates found.")
        sys.exit(1)

    # Group stratification: max 20% per group, >= 5 groups if possible
    by_group = {}
    for c in candidates:
        by_group.setdefault(c["chat_id"], []).append(c)

    print(f"[*] Distinct chat groups available: {len(by_group)}")
    max_per_group = max(1, int(target_count * 0.20))
    selected = []

    # Stratified sampling
    shuffled_groups = list(by_group.keys())
    random.shuffle(shuffled_groups)

    for gid in shuffled_groups:
        items = by_group[gid]
        random.shuffle(items)
        pick_count = min(len(items), max_per_group, target_count - len(selected))
        selected.extend(items[:pick_count])
        if len(selected) >= target_count:
            break

    random.shuffle(selected)
    print(f"[*] Selected {len(selected)} samples. Verifying baseline files...")

    import re
    import posixpath

    manifest_entries = []
    for idx, item in enumerate(selected, 1):
        clean_rel_path = item["save_path"].replace("\\", "/").strip("/")
        clean_rel_path = re.sub(r"^(app/downloads/|downloads/)+", "", clean_rel_path)
        clean_rel_path = posixpath.normpath(clean_rel_path)

        baseline_path = os.path.join(output_root, clean_rel_path)
        if not os.path.exists(baseline_path):
            # Try raw save_path if not found
            alt_path = os.path.join(output_root, item["save_path"])
            if os.path.exists(alt_path):
                baseline_path = alt_path
        baseline_sha = compute_sha256(baseline_path)
        
        entry = {
            "case_id": f"{scenario}-{idx:04d}",
            "chat_id": item["chat_id"],
            "message_id": item["message_id"],
            "group_title": "",
            "media_type": item["media_type"],
            "dc_id": 0,
            "message_date": item["created_at"],
            "size_bucket": item["size_bucket"],
            "expected_size": item["file_size"],
            "source_file_name": item["file_name"],
            "expected_rel_path": clean_rel_path,
            "baseline_path": baseline_path,
            "baseline_sha256": baseline_sha or "",
            "baseline_trust": "local_disk" if baseline_sha else "none",
            "sample_seed": seed,
        }
        manifest_entries.append(entry)

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        for entry in manifest_entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"[✓] Successfully wrote {len(manifest_entries)} cases to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TGX Evaluation Manifest Generator")
    parser.add_argument("--db-path", default="/volume2/docker/telegram_media_downloader_us/state/download_records.sqlite3")
    parser.add_argument("--output-root", default="/home/hittler/SpecialMedias/06-碎片整理区/碎片媒体/TG2/")
    parser.add_argument("--scenario", default="S1", choices=["S1", "S2", "S3", "S4", "all"])
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--out", default="manifest.jsonl")
    args = parser.parse_args()

    generate_manifest(args.db_path, args.output_root, args.scenario, args.count, args.seed, args.out)
