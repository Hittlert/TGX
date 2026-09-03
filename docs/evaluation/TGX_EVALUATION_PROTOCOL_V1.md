# TGX Evaluation Protocol v1.0

Status: Normative for the next valid baseline cohort. Execution is pending harness alignment; no prior run is certified against this revision.

## 1. Four-Layer Contract

TGX evaluation is split into four independent layers:

```text
Evaluation Protocol  fixed: collection semantics, event/metric names, raw artifact schema
Baseline Cohort      fixed once: manifests, Golden oracle, TDL artifact/results, sentinel set
RunSpec              variable: TGX artifact, selected profile, concurrency, duration, buffer configuration
Analysis Policy      variable: thresholds, interpretation, Go/No-Go decision
```

Raw artifacts MUST NOT contain a built-in Go verdict. A changed interpretation creates a new policy result; it never rewrites raw evidence.

The Baseline Cohort owns workload identity. A RunSpec selects one frozen cohort profile by `baseline_cohort_id + manifest_sha256`; it MUST NOT generate or mutate workload membership.

## 2. Normative Rules

- `MUST`: required for a valid run.
- `SHOULD`: expected unless the run records a justified deviation.
- Missing measurements MUST be encoded as `null` with `collection_error`; they MUST NOT be encoded as zero.
- Every run MUST identify the exact source and executable artifact.
- Every run MUST use a new empty OutputDir, State DB, BufferDir and LogDir.
- A run with missing required artifacts is `INVALID`, not `PASS` or `GO`.
- A collector records facts only. Analysis and verdict generation are separate steps.
- Reusing files from a prior run invalidates correctness and throughput results.
- Reusing an existing `run_id` or raw directory is forbidden. The harness MUST fail before execution instead of overwriting it.
- A full TDL baseline is created once per Baseline Cohort. A TGX source change alone MUST NOT trigger another full TDL run.
- Candidate execution MUST NOT invoke the manifest generator. A manifest hash mismatch is `INVALID` before any download starts.

## 3. Immutable Raw Output

Every engine run MUST create:

```text
raw/
  protocol.json
  run_spec.json
  artifact.json
  environment.json
  manifest.jsonl
  events.jsonl
  metrics.jsonl
  task_results.jsonl
  file_inventory.jsonl
  hashes.jsonl
  errors.jsonl
  process.log
  checksums.sha256
analysis/
  <policy_version>/
    summary.json
    verdict.json
    report.md
```

`checksums.sha256` MUST cover every file under `raw/`. After checksums are written, `raw/` is immutable.

Every `run_spec.json` MUST bind the raw evidence to `baseline_cohort_id` and `manifest_sha256`. A TGX candidate RunSpec additionally binds the frozen `tdl_baseline_ref` and current `calibration_ref`. These references are facts, not analyzer labels.

## 4. Artifact Identity

`artifact.json` MUST contain:

```text
engine
source_repository
source_commit
source_dirty
binary_sha256
image_digest
version
build_time
go_version
os
arch
```

`Version=dev`, `Commit=unknown`, a missing binary hash, or an unresolvable image identity makes the run `INVALID` for comparison.

## 5. Environment And Isolation

`environment.json` MUST contain non-secret identifiers for:

```text
host_id
account/session_id
proxy_route_id
network_interface
target_storage_id
buffer_storage_id
filesystem types
container identity
clock source
baseline_cohort_id
calibration_id
source_preflight summary
network calibration summary
```

The one-time TDL baseline and all comparable TGX runs MUST identify the account/session, proxy route, host and target storage. A lightweight calibration records current drift; it does not rewrite the frozen baseline.

Production DB, OutputDir and BufferDir MUST be read-only sample sources only. Evaluation MUST NOT update production state.

## 6. Manifest Contract

Each JSONL case MUST contain:

```text
case_id
chat_id
peer_type
message_id
media_type
dc_id
message_date
size_bucket
expected_size
source_file_name
expected_tdl_path
expected_tgx_path
baseline_sha256
baseline_trust
sample_seed
```

Only Golden cases are used for hard SHA correctness. A Golden case requires two independent reference downloads matching the stored baseline SHA.

Every Baseline Cohort has a unique `baseline_cohort_id`. Each profile manifest is generated once, sealed with SHA256, and retained with the two Golden reference results and frozen TDL raw roots. `sample_seed` is provenance only; it is not workload identity and cannot substitute for the manifest hash.

The selected manifest MUST be copied byte-for-byte into each candidate run's `raw/` directory. The copied file hash MUST equal RunSpec `manifest_sha256` and the frozen cohort hash. A candidate run MUST stop as `INVALID` on any mismatch.

Before every TGX run, source preflight MUST resolve every manifest item without downloading its payload and record current existence, access, peer type, media type, DC and expected size. A changed, deleted or inaccessible item is `SOURCE_CONFLICT`, not a TGX failure.

When a candidate produces a size/SHA/source conflict that preflight did not explain, run the frozen independent TDL artifact only for the disputed case. This targeted adjudication does not rebuild or replace the full TDL baseline.

## 7. Fixed Dataset Profiles

Profile definitions are normative in [profiles-v1.json](profiles-v1.json). Actual message membership is fixed by the Baseline Cohort. Concurrency and duration remain RunSpec parameters.

All profiles MUST:

- cover at least five groups when inventory permits;
- cap a single group at 20 percent by file count;
- cover at least two DCs when inventory permits;
- report unmet diversity as `coverage_gap`;
- include fixed sentinels around 1 MiB, 16 MiB and 32 MiB where the profile allows them;
- declare a fixed measurement mode;
- contain enough work for that mode, or record `workload_exhausted_at` so idle time after exhaustion is excluded from throughput and stall calculations.

`P-S` is a fixed 100-item burst. Its throughput window is first eligible item admission through the last eligible durable terminal event; a 240-second wall-clock window MUST NOT dilute a burst that completed earlier. Duration-based profiles measure only intervals where admitted nonterminal work exists.

The cohort also freezes a small calibration sentinel set covering the DCs represented by the manifests. Sentinels are not added to or removed from candidate workload results.

## 8. Fixed Events

When supported by the engine, `events.jsonl` uses stable event names:

```text
run.started
item.submitted
item.admitted
item.resolved
rpc.started
rpc.retry
rpc.completed
buffer.ready
sink.durable
item.committed
item.terminal
run.draining
run.finished
```

Each task event MUST carry `case_id`, `task_id`, `attempt_id` or generation, and `engine`. Stage-specific events add DC, offset, segment, physical attempt and error disposition where applicable.

TDL fields unavailable by design are `null` with `unsupported_by_engine`; they are not fabricated.

## 9. Fixed Metrics

Metrics are sampled every second. Each record contains monotonic elapsed time, wall-clock time, collection duration and a `collection_error` for every missing required source. A gap longer than two seconds is a coverage gap and MUST NOT be silently treated as a one-second sample.

Network:

```text
wire_rx_bytes
unique_payload_bytes
active_rpc
queued_jobs
connection_count
connection_failures
retry_count
flood_wait_count
flood_wait_seconds
per_dc_payload_bps
```

Memory:

```text
process_rss
heap_alloc
heap_inuse
heap_objects
gc_count
gc_pause_total
buffer_logical_bytes
buffer_physical_bytes
```

SSD/Spool:

```text
max_bytes
used_bytes
reserved_bytes
ready_bytes
writing_bytes
reclaimed_bytes
actual_directory_bytes
active_segments
writeback_bps
read_bps
write_bps
sync_count
sync_latency
backpressured
```

Target storage:

```text
target_write_bytes
target_read_bytes
target_durable_bytes
target_writer_concurrency
target_backlog_bytes
moving_file_count
fsync_count
fsync_latency
device_util
device_await
```

Collection failure for any required metric is recorded explicitly and handled by Analysis Policy.

Metric names describe measured quantities, not configured capacity. In particular, `active_rpc` MUST come from in-flight RPC ownership, `connection_count` from live connections, and `target_writer_concurrency` from active target writes. Pool size or active-file count cannot be substituted under those names.

## 10. Fixed Task And File Facts

`task_results.jsonl` MUST contain one record per manifest case, including cases that were never admitted:

```text
case_id
task_id
attempt_id
submitted
admitted
terminal_state
run_disposition
attempt_count
error_code
error_stage
error_op
error_cause
started_at
finished_at
network_unique_bytes
target_durable_bytes
```

`file_inventory.jsonl` and `hashes.jsonl` MUST record expected and actual path, size, SHA and residue classification. A missing target is an explicit file-oracle result; it is never omitted, but it does not overwrite the engine-owned terminal state.

`terminal_state` is engine-owned. The collector MUST NOT infer it from final-file existence. At timebox, a nonterminal task uses `run_disposition=TIMED_OUT` or `CANCELED`; it is not rewritten as engine `FAILED`. Unsupported engine facts are `null + collection_error`, never fabricated constants.

## 11. Run Completion

A duration-based run stops network admission at `duration_seconds`, then enters drain mode. It ends only when:

- admitted work is terminal or explicitly canceled by the RunSpec;
- target backlog is zero or drain timeout is reached;
- raw artifacts and logs are flushed.

The collector MUST join before checksums are generated.

Run status is one of:

```text
COMPLETE
TIMED_OUT
ABORTED
INVALID
```

This status is a fact, not a Go verdict.

Before deleting the container, the harness MUST capture container exit code, OOMKilled, restart count, signal, last health result, and daemon task snapshots. Loss of the daemon API during a run is a stability fact and makes the run `ABORTED` unless an expected stop event explains it.

## 12. Baseline Cohort And One-Time TDL Baseline

The TDL baseline MUST use a frozen, known-good TDL artifact that is independent of the TGX candidate source tree. Rebuilding `tdl dl` from the TGX candidate does not qualify because it shares the candidate `core/downloader`.

TDL baseline purpose:

1. verify source-message availability;
2. establish Telegram/network/direct-target throughput for the host and account;
3. establish concurrency scaling;
4. establish direct-download correctness and error floor.

TDL is not used to validate TGX Spool, Recovery or DB terminal semantics.

The complete TDL suite runs once while establishing a new Baseline Cohort. Its manifests and raw outputs are immutable. Later TGX changes reuse the frozen baseline reference and MUST NOT rerun the complete TDL suite.

Canonical TDL baseline RunSpec:

```text
net_concurrency = 32
file_concurrency = 5
dc_pool_size = 32
duration_seconds = 240
warmup_seconds = 15
repetitions = 3
```

`P-LMS` additionally runs a concurrency sweep at `8, 16, 32, 48`, keeping file concurrency at 5.

After the cohort is sealed, each TGX evaluation performs only:

```text
all-case source metadata preflight
-> lightweight fixed-sentinel network calibration
-> TGX candidate run
-> targeted TDL adjudication only for disputed cases
```

The lightweight calibration MUST record its own artifact, sentinel manifest hash, bytes, duration, throughput, DCs and errors under the candidate run's `environment.json`. It is not a replacement TDL baseline and does not mutate cohort artifacts.

A new full TDL Baseline Cohort is required only when at least one of these identities changes:

- any manifest membership or manifest SHA;
- the independent TDL artifact;
- account/session identity;
- host, proxy route or target-storage identity used for the baseline claim;
- a baseline raw artifact fails checksum or validity checks;
- the operator explicitly creates a new cohort.

TGX source changes, elapsed time, or a failed TGX run alone do not invalidate the frozen TDL baseline. If current calibration shows material environmental drift, the analysis policy may mark performance `BLOCKED`; it does not silently rerun TDL or replace the baseline.

## 13. First TGX Functional Run

The first TGX functional run uses the exact frozen cohort manifests and canonical `32/5/32` concurrency. It runs each profile once and focuses on raw correctness, lifecycle completeness, resource bounds and diagnosability. `P-S` uses its fixed burst window; duration-based profiles use the RunSpec duration.

One TGX functional run does not establish performance stability. Later repetitions rerun TGX against the same cohort; they do not rerun the full TDL baseline.

## 14. Analysis Policy

Interpretation rules live outside this Protocol. The initial policy is [analysis-policy/baseline-v1.json](analysis-policy/baseline-v1.json).

Changing a threshold creates a new policy version and new files under `analysis/`; it never changes this run's `raw/` artifacts.

## 15. Protocol Validation

Before evaluating a candidate, the harness MUST demonstrate that it can:

- mark a run with a missing target file as incorrect;
- mark missing required metrics as invalid or blocked, not zero;
- distinguish a known-bad artifact from a known-good artifact;
- detect output-directory reuse;
- preserve every manifest case in task results;
- reproduce raw artifact checksums;
- reject an existing run directory without modifying it;
- reject candidate artifact/source mismatch before submission;
- reject manifest/cohort hash mismatch before submission;
- reject RunSpec/effective daemon configuration mismatch;
- distinguish engine FAILED from TIMED_OUT/CANCELED work;
- capture container exit/OOM/signal state when the daemon API disappears;
- drive the real collector and analyzer with known-bad fixtures instead of asserting hand-written sample constants.

A harness that cannot pass these self-tests MUST NOT issue an analysis verdict.
