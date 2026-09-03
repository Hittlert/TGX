# TGX Evaluation Standards Alignment

**Revision Date**: 2026-09-03
**Status**: **PENDING GEMINI RECONFIRMATION - DO NOT EXECUTE**

## 1. Why Reconfirmation Is Required

The 2026-09-02 alignment is retired. Its execution regenerated TGX manifests after the TDL run, reused run IDs, ran Memory/128 MiB while declaring SSD/5 GiB, inferred task terminal states, and issued policy verdicts with required metrics missing.

Those runs remain historical raw evidence, but they do not form a valid Baseline Cohort. No long evaluation should run again until the harness enforces this revision.

## 2. Fixed Decisions

1. **One immutable Baseline Cohort**
   - Generate each profile manifest once.
   - Workload identity is `baseline_cohort_id + manifest_sha256`, not `sample_seed`.
   - Candidate runs may select a frozen profile but may not regenerate or edit membership.

2. **One complete TDL baseline campaign per cohort**
   - Establish Golden with two independent matching reference downloads.
   - Run the complete TDL baseline and concurrency sweep once, then seal it.
   - A TGX code change, failed TGX run, or elapsed time does not trigger another complete TDL campaign.

3. **Lightweight checks for each TGX candidate**
   - Resolve metadata for every frozen case before the run.
   - Run the small frozen sentinel set to record current network/source conditions.
   - Use the frozen TDL binary only to adjudicate disputed cases after a source/size/SHA conflict.
   - These checks never replace or mutate the complete TDL baseline.

4. **Stable measurement semantics**
   - `P-S` is a fixed 100-item burst measured from first eligible admission to last eligible durable terminal.
   - Duration-based profiles count throughput/stalls only while admitted nonterminal work exists.
   - Engine state, timeout/cancel disposition, resource metrics and container exit state come from their authoritative owners; the collector does not infer them from files.

5. **Fail-closed identity and configuration**
   - Artifact source/binary, cohort, manifest, effective daemon config and empty run paths are validated before submission.
   - Existing raw directories are never overwritten.
   - A daemon API disappearance is `ABORTED` unless expected shutdown evidence and container exit state explain it.

## 3. Full TDL Baseline Invalidation

A new complete TDL Baseline Cohort is created only when at least one of these changes:

- manifest membership or SHA;
- independent TDL artifact;
- account/session identity;
- host, proxy route or target-storage identity used by the baseline claim;
- baseline checksum/validity;
- explicit operator decision to create a new cohort.

Environmental drift detected by lightweight calibration may block a candidate performance conclusion. It does not silently rebuild the baseline.

## 4. Frozen Files For Reconfirmation

Gemini must review these exact hashes before changing the harness:

| File | SHA256 |
|---|---|
| `docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md` | `e3bc2eecc4f821dd279c3d145dd6af99352d8a002ba0c8450f66e549d6df40fb` |
| `docs/evaluation/profiles-v1.json` | `39b8244af3de0ae9ab09c7e5c469cc9f8f6334d046261416c6a6e2c9eb1de6be` |
| `docs/evaluation/run-spec.schema.json` | `9f9f785e7ff02ed088ccba5ecdfdd95dd3bfb6e39c1cfbfa5d54ff95eb9041a5` |
| `docs/evaluation/analysis-policy/baseline-v1.json` | `595074b75d0c5638f0722e662c306af0826c768ef01e1b0afd497657b2e9d23a` |

## 5. Gemini Response Contract

If Gemini agrees, it must change this status to `ALIGNED`, record the same four hashes, and implement the harness gates. It must not start a long evaluation until the user separately requests execution.

If Gemini disagrees, it must leave the status unaligned, append the exact contradiction and proposed replacement, and stop before implementation or execution.
