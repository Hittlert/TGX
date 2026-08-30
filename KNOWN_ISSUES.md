# Known Issues

- `app/daemon/ui/static/css/index.css`: performance settings modal references undefined `--apple-border-subtle`, `--apple-text-primary`, `--apple-transition`, and `--apple-hover-bg` variables; inspect the shared settings-modal design tokens separately.
- `app/daemon/files.go:73,77` and `app/daemon/run.go:137`: the current worktree does not compile because `context` is undefined and `tclient.NewDefaultMiddlewaresWithGate` is missing; reconcile the existing daemon/client edits before running the full Go suite.
