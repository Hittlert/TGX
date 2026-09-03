# TGX Evaluation Protocol v1.0

Status: Candidate for standards alignment. Once frozen for a run, this file and the referenced schemas are immutable for that run.

## 1. Three-Layer Contract

TGX evaluation is split into three independent layers:

```text
Evaluation Protocol  fixed: collection semantics, event/metric names, raw artifact schema
RunSpec              variable: manifest, concurrency, duration, engine, buffer configuration
Analysis Policy      variable: thresholds, interpretation, Go/No-Go decision
```

Raw artifacts MUST NOT contain a built-in Go verdict. A changed interpretation creates a new policy result; it never rewrites raw evidence.

## 2. Normative Rules

- `MUST`: required for a valid run.
- `SHOULD`: expected unless the run records a justified deviation.
- Missing measurements MUST be encoded as `null` with `collection_error`; they MUST NOT be encoded as zero.
- Every run MUST identify the exact source and executable artifact.
- Every run MUST use a new empty OutputDir, State DB, BufferDir and LogDir.
- A run with missing required artifacts is `INVALID`, not `PASS` or `GO`.
- A collector records facts only. Analysis and verdict generation are separate steps.
- Reusing files from a prior run invalidates correctness and throughput results.

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
```

TDL and TGX comparison runs MUST use the same account/session copy, proxy route, host and target storage. They run sequentially, never concurrently.

Production DB, OutputDir and BufferDir MUST be read-only sample sources only. Evaluation MUST NOT update production state.

## 6. Manifest Contract

Each JSONL case MUST contain:

```text
case_id
chat_id
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

The selected manifest MUST be copied into each run's `raw/` directory. TDL and TGX comparison runs use byte-identical manifests.

## 7. Fixed Dataset Profiles

Profile definitions are normative in [profiles-v1.json](profiles-v1.json). The actual messages, concurrency and duration remain RunSpec parameters.

All profiles MUST:

- cover at least five groups when inventory permits;
- cap a single group at 20 percent by file count;
- cover at least two DCs when inventory permits;
- report unmet diversity as `coverage_gap`;
- include fixed sentinels around 1 MiB, 16 MiB and 32 MiB where the profile allows them;
- contain at least 1.5 times the predicted bytes needed for the requested duration.

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

Metrics are sampled every second. Each record contains monotonic elapsed time and wall-clock time.

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

## 10. Fixed Task And File Facts

`task_results.jsonl` MUST contain one record per manifest case, including cases that were never admitted:

```text
case_id
submitted
admitted
terminal_state
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

`file_inventory.jsonl` and `hashes.jsonl` MUST record expected and actual path, size, SHA and residue classification. A missing target is an explicit failed task result; it is never omitted from failure counts.

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

## 12. TDL Baseline

The TDL baseline MUST use a frozen, known-good TDL artifact that is independent of the TGX candidate source tree. Rebuilding `tdl dl` from the TGX candidate does not qualify because it shares the candidate `core/downloader`.

TDL baseline purpose:

1. verify source-message availability;
2. establish Telegram/network/direct-target throughput for the host and account;
3. establish concurrency scaling;
4. establish direct-download correctness and error floor.

TDL is not used to validate TGX Spool, Recovery or DB terminal semantics.

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

TDL and TGX paired order:

```text
round 1: TDL -> TGX
round 2: TGX -> TDL
round 3: TDL -> TGX
```

Each paired member uses a separate empty output directory.

## 13. First TGX Functional Run

The first TGX functional run uses the exact TDL baseline manifests and canonical `32/5/32` concurrency. It runs each profile once for 240 seconds and focuses on raw correctness, lifecycle completeness, resource bounds and diagnosability.

One TGX functional run does not establish performance stability. Performance comparison requires the full paired repetitions.

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
- reproduce raw artifact checksums.

A harness that cannot pass these self-tests MUST NOT issue an analysis verdict.
