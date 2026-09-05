# Gemini Task: Align Protocol, Run TDL Baseline, Run First TGX Functional Evaluation

## Goal

Evaluate and align the fixed evaluation standard, then produce a valid TDL baseline and the first TGX functional raw dataset. Do not fix TGX product bugs during this task.

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

You may correct Protocol/schema files only for an executable contradiction. Record every correction in the alignment file. Once Phase A is complete, freeze their hashes before any run.

## Phase B: Repair The Evaluation Harness

The existing `scripts/evaluation` implementation is a prototype and its historical GO results are invalid. Archive old runs as `legacy-invalid`; do not reuse their outputs.

Required harness behavior:

- collector writes immutable raw artifacts only;
- analyzer generates policy-versioned verdicts separately;
- missing files increment failure counts;
- missing metrics are null + collection_error;
- OutputDir/DB/Log are new and empty per run;
- every manifest case appears in task results;
- artifact identity is complete;
- collector joins before checksums;
- protocol self-tests pass before real runs.

Evaluation-only code and read-only observability instrumentation may be changed. Do not fix downloader, scheduling, SSD admission, archive, commit or recovery behavior during this task.

## Phase C: Freeze Independent TDL Artifact

Use a known-good TDL binary independent of the TGX candidate source tree. Record source version and binary SHA256. The current TGX HEAD `tdl dl` command is not an independent baseline because an independent baseline must use an immutable, external TDL release binary rather than code compiled from the TGX candidate tree.

If an independent TDL artifact cannot be established, mark Phase C `BLOCKED` and stop. Do not substitute an unidentified `dev/unknown` binary.

## Phase D: TDL Baseline

Generate Golden manifests for `P-S`, `P-SM`, `P-LMS`, `P-L` using one frozen seed per profile.

Canonical runs:

```text
net_concurrency=32
file_concurrency=5
dc_pool_size=32
duration_seconds=240
warmup_seconds=15
repetitions=1
```

Also run `P-LMS` with concurrency `8,16,32,48`, keeping file concurrency 5.
Reuse these sealed TDL results for later TGX candidates while cohort, artifact,
environment and concurrency identity remain unchanged; do not rerun TDL for
each TGX commit.

Use the same host, isolated session copy, proxy route and target storage for all runs. Every repetition uses a new empty output root.

## Phase E: First TGX Functional Evaluation

Use byte-identical TDL manifests. Run `P-S`, `P-SM`, `P-LMS`, `P-L` once each with canonical `32/5/32`, 240 seconds, using a fully identified TGX artifact.

This is a functional run, not a performance stability certification. Collect all fixed raw artifacts and apply `baseline-v1` only after raw checksums are frozen.

## Stop Conditions

Stop and mark `BLOCKED` or `INVALID` when:

- production isolation cannot be proven;
- artifact identity is missing;
- required metrics cannot be collected;
- session/account safety is threatened;
- output reuse is detected;
- the harness self-tests fail.

Do not emit GO from incomplete data.

## Final Deliverables

Provide paths to:

```text
docs/evaluation/STANDARD_ALIGNMENT.md
evaluation/baselines/tdl/<baseline_id>/
evaluation/runs/tgx/<run_ids>/
evaluation/analysis/<policy_version>/comparison-report.md
```

The final response must state what was executed, what was blocked, Protocol hashes, artifact hashes and the raw artifact roots. Do not describe planned work as completed work.
