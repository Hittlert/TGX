# Gemini Task: Establish One Baseline Cohort, Then Reuse It For TGX Evaluations

## Goal

Review and implement the fixed Protocol v1 standard. Do not execute another long evaluation until the user explicitly requests it. When execution is requested, establish one valid immutable TDL Baseline Cohort, then reuse it for later TGX candidates. Do not fix TGX product bugs during evaluation work.

## Authoritative Files

Read in this order:

1. `docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md`
2. `docs/evaluation/profiles-v1.json`
3. `docs/evaluation/run-spec.schema.json`
4. `docs/evaluation/analysis-policy/baseline-v1.json`
5. `docs/reviews/2026-09-02-tgx-evaluation-v1.md` as non-normative background only

## Phase A: Standards Alignment

Audit the Protocol and schemas for contradictions, unmeasurable fields and ambiguity. Record the result in:

```text
docs/evaluation/STANDARD_ALIGNMENT.md
```

The alignment file MUST list:

- accepted rules;
- proposed corrections with exact rationale;
- unresolved decisions;
- final Protocol/schema SHA256 values used for execution.

Do not edit Protocol/schema files while implementing the harness. If you cannot agree with the current standard, append the exact objection to `STANDARD_ALIGNMENT.md` and stop before execution. Once aligned, freeze the listed hashes before any run.

## Phase B: Repair The Evaluation Harness

The existing `scripts/evaluation` implementation is a prototype and its historical GO results are invalid. Archive old runs as `legacy-invalid`; do not reuse their outputs.

Required harness behavior:

- collector writes immutable raw artifacts only;
- analyzer generates policy-versioned verdicts separately;
- missing files increment failure counts;
- missing metrics are null + collection_error;
- OutputDir/DB/Buffer/Log are new and empty per run;
- every manifest case appears in task results;
- artifact identity is complete;
- candidate source/binary matches RunSpec before submission;
- baseline cohort and manifest hashes match before submission;
- RunSpec configuration matches daemon effective configuration;
- task state comes from the engine, not final-file inference;
- timeout/cancel is distinct from engine failure;
- container exit/OOM/signal state is captured before teardown;
- collector joins before checksums;
- protocol self-tests drive the real collector/analyzer and pass before real runs.

Evaluation-only code and read-only observability instrumentation may be changed. Do not fix downloader, scheduling, Spool, commit or recovery behavior during this task.

## Phase C: Freeze Independent TDL Artifact

Use a known-good TDL binary independent of the TGX candidate source tree. Record source version and binary SHA256. The current TGX HEAD `tdl dl` command is not an independent baseline because it shares candidate `core/downloader`.

If an independent TDL artifact cannot be established, mark Phase C `BLOCKED` and stop. Do not substitute an unidentified `dev/unknown` binary.

## Phase D: One-Time TDL Baseline Cohort

Generate manifests for `P-S`, `P-SM`, `P-LMS`, `P-L` exactly once. A seed is not identity: seal every manifest SHA256 and assign one `baseline_cohort_id`. Establish Golden only after two independent reference downloads match.

Canonical runs:

```text
net_concurrency=32
file_concurrency=5
dc_pool_size=32
duration_seconds=240
warmup_seconds=15
repetitions=3
```

Also run `P-LMS` with concurrency `8,16,32,48`, keeping file concurrency 5.

Use the same host, isolated session copy, proxy route and target storage for all runs. Every repetition uses a new empty output root. Seal the TDL artifact, all raw roots, sentinels and manifests as one immutable cohort. This full suite is not rerun for later TGX code changes.

## Phase E: First TGX Functional Evaluation

Use byte-identical frozen cohort manifests. Before TGX, run all-case metadata preflight and the fixed lightweight network sentinels. Run `P-S`, `P-SM`, `P-LMS`, `P-L` once each with canonical `32/5/32` using a fully identified TGX artifact. `P-S` is a 100-item burst; do not dilute it over a 240-second idle tail.

This is a functional run, not a performance stability certification. Collect all fixed raw artifacts and apply `baseline-v1` only after raw checksums are frozen.

For every later TGX change, reuse the same `baseline_cohort_id + manifest_sha256 + tdl_baseline_ref`. Run only metadata preflight, lightweight calibration and TGX. Use the frozen TDL binary only for disputed-case adjudication. Never regenerate manifests and never rerun the full TDL suite unless a Protocol baseline-invalidating identity changes.

## Stop Conditions

Stop and mark `BLOCKED` or `INVALID` when:

- production isolation cannot be proven;
- artifact identity is missing;
- required metrics cannot be collected;
- session/account safety is threatened;
- output reuse is detected;
- baseline cohort or manifest hash differs;
- TGX artifact does not match expected source/binary;
- effective daemon configuration differs from RunSpec;
- daemon API disappears and exit state is not captured;
- the harness self-tests fail.

Do not emit GO from incomplete data.

## Final Deliverables

Provide paths to:

```text
docs/evaluation/STANDARD_ALIGNMENT.md
evaluation/baselines/tdl/<baseline_id>/
evaluation/cohorts/<baseline_cohort_id>/
evaluation/runs/tgx/<run_ids>/
evaluation/analysis/<policy_version>/comparison-report.md
```

The final response must state what was executed, what was blocked, Protocol hashes, artifact hashes and the raw artifact roots. Do not describe planned work as completed work.
