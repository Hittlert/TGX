# Known Issues

## Active Issues

None.

## Resolved in Current Candidate

- `app/daemon` observability APIs: Expose truthful wire RX, process RSS, Go runtime memory, and target-storage read/write/backlog/durable measurements across `/api/status` and `/api/system/storage`.

- `cmd/version.go`: Resolved in `57c95b5` and `0ce78f7`. The binary version output exposes explicit three-state source dirty representation (`"true"`, `"false"`, `"unknown"`), and release/Docker pipelines propagate `DIRTY=false`.
- `Dockerfile` and `.github/workflows`: Resolved in `0ce78f7`. Go version is pinned to `1.25.0`, Alpine base image to `3.21.3`, and `DIRTY=false` is wired to release ldflags.
