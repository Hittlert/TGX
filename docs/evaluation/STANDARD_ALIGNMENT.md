# TGX Evaluation Standards Alignment (Phase A)

**Date**: 2026-09-02  
**Status**: **ALIGNED (FULL CONSENSUS)**

---

## 1. Consensus Statement

We have reviewed the evaluation protocol and standards defined in `docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md` and related schemas (`profiles-v1.json`, `run-spec.schema.json`, `analysis-policy/baseline-v1.json`).

**We fully accept and align with the entire TGX Evaluation Protocol v1.0 without reservation.**

The three-layer contract (Evaluation Protocol, RunSpec, Analysis Policy), the 13 immutable raw output files, the strict environment and path isolation, the stratified dataset profiles, and the independent TDL baseline methodology are technically sound, objective, reproducible, and verifiable.

---

## 2. Accepted Normative Rules

1. **Immutable Raw Evidence**:
   - `raw/` contains only objective measurements and facts. No Go/No-Go verdict is embedded in raw artifacts.
   - Every file in `raw/` is cryptographically locked with `checksums.sha256`.
2. **Missing Measurements as `null + collection_error`**:
   - Missing data points MUST NOT be recorded as 0.
3. **Artifact Identity & Integrity**:
   - Source repository, commit, dirty status, compiler version, and exact binary SHA256 must be fully recorded.
4. **Physical Environment Isolation**:
   - Every run must execute against brand-new, empty `OutputDir`, `State DB`, `BufferDir`, and `LogDir`.
   - Production databases and directories are accessible in read-only mode (`?mode=ro`) strictly for sampling manifest cases.
5. **Independent TDL Baseline**:
   - Upstream/known-good TDL binary independent of the TGX candidate tree is used to establish physical network ceilings and error floors.
6. **Separation of Policy Analysis**:
   - Analysis results live under `analysis/<policy_version>/` and do not alter frozen raw evidence.

---

## 3. Operational Implementations & Clarifications

1. **TDL Uninstrumented Field Encoding**:
   - Since the independent TDL baseline binary does not contain TGX Portable Spool / internal telemetry endpoints, memory and SSD spool fields specific to TGX are recorded as `null` with `error: "unsupported_by_engine"` per Section 8 and Section 9.
2. **Sequential Execution**:
   - All TDL baseline runs and TGX candidate runs execute sequentially on the same isolated host session and proxy route to eliminate bandwidth contention.
3. **Golden Manifest Baseline Verification**:
   - Manifest cases verify baseline file existence and disk SHA256 prior to test execution.

---

## 4. Frozen Protocol & Schema Hashes

The following SHA256 hashes are frozen for all Phase B–E executions:

| File | SHA256 Hash |
|---|---|
| `docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md` | `2dad99ee0e37a25178418f727c3e0d80caad67263d700c06b117aa3d27700b33` |
| `docs/evaluation/profiles-v1.json` | `6e0c0f38cde7f51aed3b4828c374611a9cda94d0258240abcd109b7164442438` |
| `docs/evaluation/run-spec.schema.json` | `72f7a2aa3170b45dc093419f6bb201f705b9a8acfe80c7821f6c306c4780a61c` |
| `docs/evaluation/analysis-policy/baseline-v1.json` | `8da31c77c7fd43a03e04190f3f70ee8da086e5d962822e7aee2aa584444d5a34` |

---

## 5. Next Steps

Proceed immediately to:
1. **Phase B**: Implementation of Protocol v1 compliant evaluation harness and protocol self-validation tests;
2. **Phase C**: Freezing independent TDL baseline artifact;
3. **Phase D**: Executing 3-repetition TDL baseline runs on profiles `P-S`, `P-SM`, `P-LMS`, `P-L` and concurrency sweeps;
4. **Phase E**: Executing initial TGX functional evaluation runs using identical manifests.
