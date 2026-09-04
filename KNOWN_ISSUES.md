# Known Issues

- `cmd/version.go`: the binary version output does not expose the Go build-info dirty flag, so the evaluation artifact preflight cannot prove `source_dirty=false`; add immutable build provenance before a valid candidate run.
- `app/daemon` observability APIs: required wire RX, process RSS, and target-storage read/write/backlog measurements are not exposed, so the governed analyzer correctly returns `BLOCKED`; add read-only metrics at the owning runtime/storage layers.
- `Dockerfile` and `.github/workflows`: Go/toolchain and base-image selectors still use floating `stable`/`latest` values, so release artifacts are not reproducible from source identity alone; pin exact versions or digests in the release path.
