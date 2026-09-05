# Known Issues

## Active Issues

- Evaluation / Benchmark: An immutable Linux/NAS reference benchmark candidate establishing truthful data-plane measurements (wire RX, process RSS, separate target read/write physical counters) is in progress under #5 and #8.
- Physical I/O Metrics & Evaluation Gates: Real `WriteAt`/SHA-256 read/recovery instrumentation, two-tier storage separation (Direct SSD vs Archive HDD), partial byte failure retention, and immutable cohort/artifact identity gates are under active closure and owner verification (Refs #8, #9).

## Resolved in Current Candidate

- `app/daemon` DB-to-Registry failure-atomic handoff: `Registry.RetryTaskWithDecider` checks capacity and state before executing the durable conflict->pending transition, preventing queue-full or active task from clearing durable conflict (Issue #4 closed and verified).
- `app/daemon` monotonic network telemetry: Old attempt counters are archived to cumulative totals upon task retry/prune, preventing wire RX regression across logical retries (Refs #8).
- `cmd/version.go`: Resolved in `57c95b5` and `0ce78f7`. The binary version output exposes explicit three-state source dirty representation (`"true"`, `"false"`, `"unknown"`), and release/Docker pipelines propagate `DIRTY=false`.
- `Dockerfile` and `.github/workflows`: Resolved in `0ce78f7`. Go version is pinned to `1.25.0`, Alpine base image to `3.21.3`, and `DIRTY=false` is wired to release ldflags.
