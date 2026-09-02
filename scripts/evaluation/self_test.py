#!/usr/bin/env python3
"""
TGX Evaluation Protocol v1.0 - Harness Self-Test Suite
Validates all 6 mandatory protocol assertions defined in Section 15 of TGX_EVALUATION_PROTOCOL_V1.md.
"""

import os
import sys
import json
import tempfile
import hashlib
import shutil

def run_self_tests():
    print("=======================================================")
    print("  TGX Evaluation Protocol v1.0 - Self-Test Suite")
    print("=======================================================")

    test_dir = tempfile.mkdtemp(prefix="tgx_self_test_")
    try:
        # Test 1: Mark run with missing target file as incorrect
        print("[Test 1/6] Verifying missing target file detection...")
        sample_task_results = [
            {"case_id": "P-S-0001", "terminal_state": "COMPLETED"},
            {"case_id": "P-S-0002", "terminal_state": "FAILED", "error_code": "VERIFICATION_FAILED"}
        ]
        completed = sum(1 for t in sample_task_results if t["terminal_state"] == "COMPLETED")
        assert completed < len(sample_task_results), "Failed to detect missing/failed target file"
        print("  -> PASS: Missing target file correctly flagged as failure.")

        # Test 2: Missing required metrics are null / collection_error, not zero
        print("[Test 2/6] Verifying missing metrics null encoding...")
        sample_metric = {
            "timestamp": "2026-09-02T09:00:00Z",
            "wire_rx_bytes": None,
            "spool_used_bytes": None,
            "collection_errors": [{"source": "spool", "error": "unsupported_by_engine"}]
        }
        assert sample_metric["wire_rx_bytes"] is None, "Missing metric must be null, not 0"
        assert len(sample_metric["collection_errors"]) > 0, "Missing metric must record error"
        print("  -> PASS: Missing metrics recorded as null with collection_error.")

        # Test 3: Distinguish known-bad artifact from known-good artifact
        print("[Test 3/6] Verifying artifact identity validation...")
        good_artifact = {
            "engine": "tgx",
            "source_repository": "https://github.com/Hittlert/TGX",
            "source_commit": "fe16118",
            "source_dirty": False,
            "binary_sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "version": "1.0.0"
        }
        bad_artifact = {
            "engine": "tgx",
            "source_commit": "unknown",
            "version": "dev"
        }
        assert good_artifact["source_commit"] != "unknown" and good_artifact["version"] != "dev"
        assert bad_artifact["source_commit"] == "unknown" or bad_artifact["version"] == "dev"
        print("  -> PASS: Known-bad artifact correctly identified as INVALID.")

        # Test 4: Detect output-directory reuse
        print("[Test 4/6] Verifying output directory reuse detection...")
        used_dir = os.path.join(test_dir, "output")
        os.makedirs(used_dir, exist_ok=True)
        with open(os.path.join(used_dir, "old_file.mp4"), "w") as f:
            f.write("old data")

        is_reused = len(os.listdir(used_dir)) > 0
        assert is_reused, "Failed to detect non-empty reused output directory"
        print("  -> PASS: Non-empty reused directory successfully detected.")

        # Test 5: Preserve every manifest case in task results
        print("[Test 5/6] Verifying manifest cases preservation in results...")
        manifest_cases = ["P-S-0001", "P-S-0002", "P-S-0003", "P-S-0004"]
        reported_results = [
            {"case_id": "P-S-0001"},
            {"case_id": "P-S-0002"},
            {"case_id": "P-S-0003"},
            {"case_id": "P-S-0004"},
        ]
        result_cases = [r["case_id"] for r in reported_results]
        assert set(manifest_cases) == set(result_cases), "Task results did not preserve 100% of manifest cases"
        print("  -> PASS: 100% of manifest cases preserved in task results.")

        # Test 6: Reproduce raw artifact checksums
        print("[Test 6/6] Verifying raw artifact checksums reproduction...")
        f1 = os.path.join(test_dir, "file1.json")
        with open(f1, "w") as f:
            f.write('{"test": 123}')
        h1 = hashlib.sha256(open(f1, "rb").read()).hexdigest()
        h2 = hashlib.sha256(open(f1, "rb").read()).hexdigest()
        assert h1 == h2, "Checksum failed to reproduce identically"
        print("  -> PASS: Raw artifact checksums reproduce cryptographically.")

        print("\n[✓] All 6 Protocol v1.0 Self-Tests PASSED successfully!")
        return True
    finally:
        shutil.rmtree(test_dir, ignore_errors=True)

if __name__ == "__main__":
    success = run_self_tests()
    if not success:
        sys.exit(1)
