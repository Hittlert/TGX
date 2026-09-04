# Known Issues

## Active Issues

- `app/daemon` observability APIs: required wire RX, process RSS, and target-storage read/write/backlog measurements are not exposed, so the governed analyzer correctly returns `BLOCKED`; add read-only metrics at the owning runtime/storage layers.

## Resolved in Current Candidate

- `cmd/version.go`: Resolved in `57c95b5` and `0ce78f7`. The binary version output exposes explicit three-state source dirty representation (`"true"`, `"false"`, `"unknown"`), and release/Docker pipelines propagate `DIRTY=false`.
- `Dockerfile` and `.github/workflows`: Resolved in `0ce78f7`. Go version is pinned to `1.25.0`, Alpine base image to `3.21.3`, and `DIRTY=false` is wired to release ldflags.
