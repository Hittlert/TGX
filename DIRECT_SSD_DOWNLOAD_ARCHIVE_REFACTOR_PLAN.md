# TGX Direct SSD Download + Async Archive Refactor Plan

Status: Proposed, implementation not started

Date: 2026-09-03 (Asia/Shanghai)

Architecture baseline: `83c5e49364bfbc5ff3ec1d24c355434cc6bf4177`

Current planning snapshot: `05b7185d32a8d6b2532aaa48ac6e0044eb5d957d`

## 1. Final decision

This is a data-plane replacement, not another storage-mode patch.

The final production path is:

```text
Telegram update
-> durable pending record
-> resolve media once
-> official gotd downloader
-> SSD sibling .part file
-> fsync + size/hash verification
-> atomic SSD rename
-> download success
-> durable archive job (optional)
-> one whole-file archive worker
-> HDD .moving file
-> fsync + size/hash verification
-> atomic HDD rename
-> archive success
-> delete SSD source
```

The following concepts do not exist in the final runtime:

- memory/SSD/none buffer modes;
- application-level segment files or segment manifests;
- segment aggregation, frontier scheduling or moved bitmaps;
- Spool, WriteBack, TargetSink or streaming TargetWriter;
- separate small/large download lanes;
- small-file whole-file memory buffering;
- download disk-writer pools;
- download state `moving`;
- synchronous waiting for HDD before download success;
- old and new data paths selected by feature flags.

MTProto/gotd will still split a Telegram file into protocol chunks. Those chunks are transient network work, not application storage objects. Removing application-level segment persistence must not remove gotd's normal parallel download capability.

## 2. Why the original three-line plan was insufficient

The direction was correct, but four contracts must be explicit to prevent another patch stack:

1. `83c5e49` already contains the complete Spool/WriteBack architecture. Returning to that commit does not produce direct SSD downloading; it only establishes the control-plane starting point from which the data plane must be deleted.
2. "Remove segments" must distinguish gotd protocol chunks from TGX's own persistent segment state. The former is required for throughput; the latter is being removed.
3. "Asynchronous archive" is not asynchronous if the task remains `moving`, holds a download slot, or becomes failed when HDD copy fails. Download and archive require separate state machines and terminal owners.
4. Direct SSD downloading still needs explicit global RPC capacity, SSD admission, crash recovery and error ownership. Otherwise old failures move to a new function instead of disappearing.

## 3. Scope

### 3.1 Included

- daemon and CLI download execution move to the official gotd downloader;
- files download and commit directly on the configured SSD download filesystem;
- optional whole-file archive to a second directory/filesystem;
- one durable archive queue and one archive worker;
- download/archive state separation and restart recovery;
- global in-flight RPC protection without a steady-state requests-per-second ceiling;
- stable lifecycle logs and metrics;
- one-time migration from the old Spool/WriteBack database and filesystem state;
- README, examples, UI/status API and evaluation protocol alignment;
- removal of obsolete code, configuration, tests and dependencies.

### 3.2 Explicitly excluded from this refactor

- FUSE, rclone VFS, aria2 or an MTProto-to-HTTP layer;
- resuming arbitrary incomplete `.part` ranges after restart;
- multiple HDD archive writers or archive seek optimization;
- application-level chunk cache or deduplication;
- retaining the old data path as a fallback;
- performance tuning based only on intuition before the fixed tests produce evidence.

An incomplete download is restarted as a file attempt. A completed SSD file is never redownloaded merely because archive failed.

## 4. Baseline handling

### 4.1 Code baseline

Create the implementation branch from `83c5e49`. Do not merge or cherry-pick the downloader changes from `05b7185`:

- `app/daemon/files.go`;
- `core/downloader/downloader.go`;
- `core/downloader/downloader_test.go`.

Those changes modify the defective data path that this refactor deletes.

### 4.2 Evaluation assets to retain

Retain the evaluation-only changes from `05b7185` because they are independent of the retired data path:

- `docs/evaluation/GEMINI_EVALUATION_TASK.md`;
- `docs/evaluation/STANDARD_ALIGNMENT.md`;
- `docs/evaluation/TGX_EVALUATION_PROTOCOL_V1.md`;
- `docs/evaluation/analysis-policy/baseline-v1.json`;
- `docs/evaluation/profiles-v1.json`;
- `docs/evaluation/run-spec.schema.json`;
- `scripts/evaluation/manifest_generator.py`;
- `scripts/evaluation/run_protocol_v1.py`.

Then revise their storage metric names once, before sealing the first valid baseline cohort. No full TDL baseline should be rerun merely because TGX source changes afterward.

## 5. Architecture ownership

Each invariant has exactly one owner.

| Concern | Unique owner | Durable authority | Must not also own |
|---|---|---|---|
| Media ingress | `UpdatesStream` + ingest DB transaction | `download_records` pending row | Network execution |
| Active-file admission | `DownloadManager` | Runtime permits, DB task identity | Archive concurrency |
| Telegram protocol chunks/retries/CDN | official gotd downloader | Telegram protocol | File terminal state |
| Global data RPC capacity/cooldown | `DataGate` | Runtime counters + server FloodWait | Per-file chunk planning |
| SSD space admission | `SSDAdmission` | Real free space + active reservations | HDD backlog ordering |
| SSD file commit | `SSDFile` | `.part`, commit intent, final SSD file | Archive success |
| Download terminal transition | `DownloadCompletion` | `download_records` | Archive terminal transition |
| Archive queue and transition | `ArchiveRepository` | `archive_jobs` | Download terminal transition |
| HDD copy | one `ArchiveWorker` | `.moving`, final archive file | Telegram retries |
| Restart reconciliation | `Reconciler` | DB state + verified files | Guessing from filenames alone |

`GlobalSlotPool`, the current downloader `FileConcurrency`, the FloodGate data semaphore and the current disk writers are overlapping capacity owners. The final design replaces them with one `DownloadManager` for active files and one `DataGate` for physical data RPCs.

## 6. Network execution design

### 6.1 Use the official downloader

Pin gotd to a tested release that includes the native parallel downloader, CDN flow and retry callback. The planning target is `github.com/gotd/td v0.161.0`; do not use an unpinned branch.

Use the official builder contract:

```go
gotdDownloader.Download(client, location).
    WithThreads(fileThreads).
    Parallel(ctx, ssdWriterAt)
```

TGX should not retain its own chunk scheduler, positive-short-read EOF rules, CDN decryptor, reorder buffer or retry loop beside gotd.

Construct the official downloader with CDN explicitly enabled and test that the production adapter implements the CDN provider contract. The current project's implicit/default CDN setting must not silently disable this path.

### 6.2 Concurrency has three independent meanings

```text
file_concurrency       = files allowed to execute concurrently
max_file_threads       = maximum gotd workers for one file
max_data_in_flight     = maximum physical data RPCs across all files/DCs
```

Recommended initial product defaults:

```text
file_concurrency   = 32
max_file_threads   = 8
max_data_in_flight = 40
archive_workers    = 1 (not configurable in the first version)
```

Per-file gotd workers are derived from logical work, not a small/large label:

```text
chunk_count  = ceil(expected_size / gotd_part_size)
file_threads = min(max_file_threads, max(1, chunk_count))
```

With gotd's 512 KiB default part size:

- a 300 KiB image requests one file worker;
- a 900 KiB image requests two file workers;
- a 1.75 MiB file has four logical payload chunks and requests at most four gotd workers;
- a 100 MiB video requests at most eight file workers;
- five large files can offer 40 workers, while 100 small files can keep up to 32 files active and continuously reuse the same global RPC permits.

There is no file-size threshold and no small/large lane.

gotd may perform a bounded number of physical EOF probes because its public parallel API is size-agnostic. Those probes are physical attempts only: TGX must not persist them as additional logical chunk jobs, archive objects or retry tasks. Metrics must distinguish payload chunks from EOF probes/retries.

### 6.3 The global gate limits simultaneous work, not requests per second

Every master-DC and CDN data RPC acquires one `DataGate` permit immediately before the physical request and releases it immediately after that request returns.

The steady-state data path must not contain a token bucket such as 100/160 requests per second. Forty simultaneous permits may be reused hundreds of times per second when latency and Telegram allow it.

Account protection is limited to facts owned by the boundary:

- hard maximum simultaneous data RPCs;
- Telegram-issued FloodWait applied to the affected account/DC;
- context cancellation;
- one retry owner;
- metrics for attempts, FloodWait and connection failures.

The official gotd downloader owns chunk/CDN retry. `DataGate` observes and coordinates server cooldown; it does not add another nested retry loop. Generic transport failures must not permanently reduce throughput through an unbounded adaptive state machine.

### 6.4 gotd adapter

Implement only the small adapter required by gotd's public `downloader.Client` and `downloader.CDNProvider` contracts:

- master methods delegate to the existing authenticated DC client;
- CDN creation delegates to `telegram.Client.CDN`;
- master and CDN physical calls pass through the same `DataGate`;
- retry callback records `task_id`, `attempt_id`, `dc`, operation and original error;
- no Telegram protocol logic is copied into TGX.

This adapter is an integration boundary, not a fork of gotd.

### 6.5 Failure containment

A file download error terminates only that file attempt. It must never be returned as the daemon's top-level `errgroup` error.

Only an actual subsystem failure, such as the HTTP listener failing to bind or the root Telegram session ending, may terminate `daemon.Run`. A failed `UploadGetFile`, SSD write, hash check or archive copy must not restart the service or cancel sibling files.

## 7. Direct SSD file contract

### 7.1 Paths

`--dir` is the SSD download root. A file is written to a sibling temporary path:

```text
<download-root>/<relative-final-path>.part
<download-root>/<relative-final-path>
```

The sibling layout guarantees the commit stays on the same filesystem, including when subdirectories beneath the root are separate mounts. The download commit API must be rename-only; it must not silently fall back to copying to another filesystem.

### 7.2 Commit transaction

The successful order is:

```text
create/truncate .part
-> gotd Parallel returns success
-> verify exact expected size
-> fsync and close .part
-> compute SHA-256
-> DB PrepareDownloadCommit(status=committing, size, SHA, relative path)
-> non-replacing atomic rename .part -> final
-> fsync final parent directory where supported
-> DB CompleteDownloadAndQueueArchive transaction
-> Registry success
-> release file and SSD reservations
```

`CompleteDownloadAndQueueArchive` performs two durable actions in one SQLite transaction:

1. mark `download_records.status=success` and store committed size/SHA/path;
2. insert the archive job when `archive_dir` is enabled.

The SSD final file is the download commit point. HDD state is not consulted.

### 7.3 Collision behavior

- Never overwrite an existing final SSD file.
- If the DB has commit intent for the same task/path/size/SHA and the existing file verifies, finish the interrupted commit idempotently.
- If identity differs or ownership is absent, report a typed collision, preserve both the existing final and the completed `.part`, and require operator resolution.
- File length alone is not sufficient to claim identity when a stored SHA is available.

### 7.4 SSD capacity

Before any Telegram data RPC begins, `SSDAdmission` must reserve the complete expected file size against:

```text
real filesystem free bytes
- configured minimum free bytes
- reservations held by other active files
```

The reservation is released exactly once on success, failure or cancellation. Completed files remain visible to real filesystem free-space accounting until archive removes them.

This reservation may be conservative while bytes are actively being written; it must be simple and safe. It must not become a second chunk buffer or range tracker. When SSD space is insufficient, the task stays pending with a visible `ssd_space` reason and holds no network permit.

The platform implementation must query real free space on Linux, macOS and Windows. Returning a fabricated value for unsupported platforms is forbidden.

## 8. Download and archive states

### 8.1 Download state

```text
pending
-> resolving
-> downloading
-> committing
-> success
```

Alternative terminal outcomes are `failed` and `unavailable`. A transient internal/network failure remains retryable; only a typed Telegram response proving that the media cannot be obtained may become `unavailable`.

There is no download state named `moving`. Archive delay or failure cannot change `success` back to another download state.

### 8.2 Archive state

No archive row exists when archive is disabled. When enabled:

```text
pending -> copying -> archived
                    -> conflict
```

A transient error changes `copying` back to `pending`, preserving `last_error`, `attempts` and a bounded `next_retry_at`. Retry delay may grow to at most 30 minutes; there is no seven-day freeze and no permanent abandonment caused only by attempt count.

`conflict` is reserved for a durable destination whose content differs from the SSD source. It is visible and requires an explicit resolution; retrying cannot overwrite it.

### 8.3 Durable archive queue

Use one table as both queue and state authority:

```sql
archive_jobs(
  chat_id,
  message_id,
  relative_path,
  expected_size,
  sha256,
  state,
  attempts,
  next_retry_at,
  last_error,
  created_at,
  updated_at,
  PRIMARY KEY(chat_id, message_id)
)
```

An in-memory wake channel may reduce polling latency, but it is never queue authority. At startup the worker reads due rows from SQLite, and any stale `copying` row is reconciled before new work starts.

## 9. Whole-file archive contract

### 9.1 Configuration

```text
--dir <absolute SSD download root>
--archive-dir <absolute archive root>  # optional; empty disables archive
--min-free-space <bytes>               # default 5 GiB, configurable
```

Remove:

```text
--temp-dir
--buffer-type
--buffer-dir
--buffer-size
```

The download and archive roots must be canonicalized after creation. Equal roots, or one root nested inside the other, are rejected to prevent self-archiving loops.

### 9.2 Worker behavior

The first version has exactly one archive worker. It claims one due DB row, processes the whole file, completes the state transition and then claims the next row.

Same-filesystem path:

```text
verify SSD source
-> non-replacing rename to archive final
-> fsync archive parent
-> mark archived
```

Cross-filesystem path:

```text
verify SSD source
-> copy sequentially to <archive-final>.moving
-> fsync and close .moving
-> verify exact size and SHA while/after copying
-> non-replacing atomic rename .moving -> archive final
-> fsync archive parent
-> mark archived in DB
-> delete SSD source
-> fsync SSD parent where supported
```

On a cross-filesystem copy, source deletion occurs only after the archive final is durable and the `archived` transition commits. If deletion fails, the archive remains successful and restart cleanup removes the duplicate; the only copy is never destroyed. A same-filesystem rename is one atomic ownership transfer rather than copy-plus-delete; if DB commit then fails, recovery verifies the archive final and completes the state transition.

### 9.3 Existing archive target

- Matching size and SHA: treat as idempotent archive success, then remove the verified SSD duplicate.
- Different size or SHA: set `conflict`, preserve the SSD source and archive target, and do not overwrite either.
- Destination directory unavailable or read-only: return to `pending`; preserve SSD source and download success.

## 10. Restart recovery matrix

Recovery runs before download admission and before the archive worker.

| DB state | SSD `.part` | SSD final | HDD `.moving` | HDD final | Recovery action |
|---|---|---|---|---|---|
| `downloading` | any | absent | n/a | n/a | remove/restart `.part`, set pending |
| `committing` with stored SHA | matching | absent | n/a | n/a | finish SSD rename, complete DB transaction |
| `committing` with stored SHA | absent | matching | n/a | n/a | complete DB transaction without network |
| `committing` | mismatched | absent | n/a | n/a | preserve evidence, typed collision/failure |
| `success`, archive disabled | absent | matching | n/a | n/a | no action |
| `success` + archive `pending` | absent | matching | absent | absent | enqueue/claim archive |
| archive `copying` | absent | matching | partial | absent | remove/truncate `.moving`, retry from source |
| archive `copying` | absent | matching | absent | matching | mark archived, delete SSD source |
| archive `pending`/`copying` | absent | absent | absent | matching | verify archive final and mark archived; no redownload |
| archive `archived` | absent | matching | absent | matching | delete verified SSD duplicate |
| archive `archived` | absent | absent | absent | matching | no action |
| any archive state | absent | matching | any | conflicting | mark conflict, preserve both finals |
| archive pending/copying | absent | absent | any | absent | record missing-source error; never claim success |

Recovery must use DB task/path/hash ownership. It must not promote arbitrary `.part` or `.moving` files based only on suffix and length.

## 11. One-time migration from the old architecture

Migration is an offline release step, not a permanent compatibility branch in runtime code.

1. Stop the old service and back up SQLite, old BufferDir and target directories.
2. Produce a dry-run inventory of `download_records`, `spool_attempts`, `spool_segments`, `target_commits`, `.part`, `.meta`, `.seg` and `.moving` artifacts.
3. For an old `success` record whose existing HDD file matches its recorded size/SHA, create an `archived` archive row so the new downloader does not fetch it again.
4. For `downloading` or `moving`, promote only a verified final file with authoritative commit proof. Otherwise reset the Telegram task to pending.
5. Do not import partial segments into the new downloader. They are quarantined in the migration backup and may be removed after acceptance.
6. Copy authoritative SHA values from `target_commits` into the simplified download/archive records.
7. Drop the obsolete Spool tables only after the migration report shows all authoritative terminal files accounted for and the backup is verified.
8. Start the new candidate with a new SSD download mount and the old final target mounted as `archive_dir`.

No `if legacySpool`, legacy filesystem scanner or old TargetSink remains in the normal process after migration.

## 12. Concrete code change map

### 12.1 Replace

| Current area | Final responsibility |
|---|---|
| `core/downloader` | thin gotd-based `core/transfer` manager and progress adapter |
| `pkg/sbe/gate` | simple data in-flight/DC cooldown gate under `core/transfer` |
| `pkg/sbe/atomic` | small cross-platform rename/fsync helpers under `internal/fscommit` |
| `app/daemon/files.go` | direct SSD `.part` writer and commit contract |
| `app/daemon/recovery.go` | direct SSD + whole-file archive reconciliation only |
| generic DB status updates | transition-specific download/archive repository methods |
| `GlobalSlotPool` | one active-file capacity owner exposed by `DownloadManager` |
| Web buffer metrics | network/SSD/archive owner metrics |

### 12.2 Delete after call-site migration

- `pkg/spool/`;
- `pkg/writeback/`;
- storage-related `pkg/sbe/` engine, coordinator, lease, meta and scheduler code;
- custom chunk scheduler/CDN/retry/reorder/disk-writer implementation in `core/downloader/`;
- `spoolWriterAt`, `spoolFileElement`, `AsyncMoving` and `SetSpool`/`SetTargetSink`;
- Spool DB methods and runtime schema creation;
- Buffer configuration and metrics;
- tests whose only purpose is the retired data path;
- dependencies used only by those packages;
- README claims about Dual-Lane, memory rectification or HDD streaming writeback.

### 12.3 Preserve

- update ingestion and monitored-target logic;
- typed peer identity and media resolution;
- authentication/session/UI control plane;
- SQLite download queue;
- proxy selection and connection health where independent of the old data path;
- fixed evaluation cohort/protocol concepts;
- final path naming and non-overwrite policy.

## 13. Implementation sequence

No intermediate phase is a release candidate. Only the final single-path branch may be deployed.

### Phase 0: freeze the starting point

- branch from `83c5e49`;
- restore the evaluation-only files listed in section 4.2;
- record source commit and clean/dirty state;
- capture a production migration dry-run without changing production data.

Exit condition: the branch contains the intended control plane and no unreviewed post-baseline downloader code.

### Phase 1: direct SSD lifecycle

- add transition-specific DB methods and committed SHA field;
- make `.part` a sibling of the SSD final;
- implement `downloading -> committing -> success` ordering;
- implement real cross-platform free-space query and whole-file reservation;
- remove `moving` from the download lifecycle;
- rewrite direct-file recovery and collision handling.

Exit condition: a fake transfer can commit, fail, cancel and restart without Spool, and no HDD/archive code is involved.

### Phase 2: native gotd network engine

- pin and compile gotd `v0.161.0`;
- implement the minimal master/CDN adapter and `DataGate`;
- implement file worker admission and the `file_threads` formula;
- route daemon and CLI through the official downloader;
- prove file errors do not escape to daemon `errgroup`;
- delete the custom downloader after every caller moves.

Exit condition: one real lifecycle uses gotd from media resolve through SSD commit, with no custom chunk planning or nested retry owner.

### Phase 3: independent archive lifecycle

- add `archive_jobs` and its repository;
- insert archive jobs in the download-completion transaction;
- add the single worker, rename/copy paths, collision logic and metrics;
- implement restart reconciliation and cleanup of duplicated SSD sources.

Exit condition: HDD can be disconnected for the entire download run without changing any successful download state; reconnecting it drains the durable backlog.

### Phase 4: delete and migrate

- remove all obsolete packages, flags, schema writers and UI fields;
- implement and run the offline migration in dry-run and sandbox modes;
- update Compose examples, README files, architecture docs and CLI help;
- update evaluation metric names once for the final architecture;
- run `go mod tidy` only after obsolete imports are gone.

Exit condition: source searches for retired runtime concepts are empty except historical migration documentation.

### Phase 5: fixed evaluation

- pass harness self-tests;
- seal one valid Baseline Cohort and independent TDL baseline if none exists;
- run TGX direct-SSD tests first;
- run archive tests separately on true SSD/HDD mounts;
- interpret immutable raw results with the frozen analysis policy.

Exit condition: all gates in section 15 are supported by raw artifacts, not by commit messages or unit tests alone.

## 14. Test plan

### 14.1 Static and build checks

Run after the complete feature implementation:

```text
gofmt -l <changed-go-files>  # must print nothing
go vet ./...
go test ./...
go test -race ./core/transfer ./app/daemon ./internal/fscommit
GOOS=linux GOARCH=amd64 go build ./...
GOOS=darwin GOARCH=arm64 go build ./...
GOOS=windows GOARCH=amd64 go build ./...
```

These checks establish compilation and isolated invariants. They do not establish NAS throughput or crash durability.

### 14.2 Unit invariant tests

#### Network planning and capacity

- boundary sizes `1`, `partSize-1`, `partSize`, `partSize+1`, `1.75 MiB`, `100 MiB` produce exact, gap-free payload coverage; bounded gotd EOF probes are recorded separately as physical attempts;
- changing worker limits changes execution timing only, never logical file size or task identity;
- `max_data_in_flight=40` is never exceeded across files and DCs;
- permits release once on success, error, timeout and cancel;
- 100 short RPCs reuse permits without a 40/100/160 requests-per-second ceiling;
- a per-DC FloodWait pauses that DC and preserves the original typed cause;
- no retry occurs in two different TGX layers;
- one failed file does not cancel or fail sibling files.

#### SSD commit

- exact file publishes with expected size/SHA and no `.part` residue;
- positive short read or missing range cannot publish a final file;
- `fsync`, close, hash, DB prepare and rename failures stop before false success;
- existing identical owned final is idempotent;
- existing conflicting final is never overwritten;
- paths cannot escape the SSD root;
- `.part` naming is valid on Linux, macOS and Windows;
- simultaneous reservations cannot admit more bytes than free minus minimum free space;
- every reservation returns to zero on all exits.

#### State transitions

- only legal download and archive transitions are accepted;
- download success and archive job insertion are one DB transaction;
- archive failure cannot update download status;
- duplicate completion and duplicate archive callbacks are idempotent;
- a stale attempt/generation cannot commit over a newer attempt;
- transient task retry is bounded and never turns into a seven-day freeze;
- typed unavailable errors are distinct from internal downloader failures.

#### Archive

- same-filesystem non-replacing rename path;
- cross-filesystem sequential copy path;
- target `.moving` is synced and verified before final rename;
- on cross-filesystem copy, the SSD source remains until archive durable state commits;
- matching target is idempotent; conflicting target preserves both files;
- worker concurrency is exactly one;
- restart reads DB authority and does not depend on an in-memory queue;
- file mode and media timestamps are preserved where the platform supports them.

### 14.3 Production-path component tests

Use gotd's public downloader with a deterministic fake RPC/CDN provider and the real TGX manager, SSD writer, DB and completion path.

| Scenario | Workload | Required fact |
|---|---|---|
| Historical boundary | one 1.75 MiB file | exact final hash; four 512 KiB logical payload chunks; bounded EOF probes are attempts, not extra TGX jobs |
| Small burst | 100 files below 1 MiB | all complete; active files continuously replenished; no global RPS cap |
| Large saturation | five files at least 100 MiB | each makes progress; aggregate gate reaches available capacity |
| Mixed | fixed small/medium/large distribution | small churn does not starve already active large files |
| Short response | non-final positive short response | no false SSD commit |
| CDN | redirect, reupload, hash mismatch | official gotd path used; mismatch retains original cause |
| SSD failure | short write, ENOSPC, fsync failure | one task fails/retries; daemon and siblings remain running |
| Shutdown | active files and queued files | contexts cancel; permits/FDs/reservations return to zero |

Tests must invoke the same constructor used by `daemon.Run`. A standalone helper that production never calls is not evidence.

### 14.4 Crash-point tests

Run each crash point in a separate process/container, kill without graceful shutdown, restart with the same DB and mounts, and verify the recovery matrix.

1. after `.part` creation;
2. after several out-of-order gotd writes;
3. after `.part` fsync but before DB commit intent;
4. after DB `committing` but before SSD rename;
5. after SSD rename but before DB success;
6. after DB success/archive enqueue but before worker claim;
7. halfway through HDD `.moving` copy;
8. after HDD `.moving` fsync but before rename;
9. after HDD final rename but before archive DB success;
10. after archive DB success but before SSD deletion;
11. after SSD deletion but before source-directory fsync.

For every point assert:

- at least one verified complete copy survives;
- no corrupt or incomplete final is reported as success;
- no successful Telegram file is downloaded again solely because archive was interrupted;
- no queue item, file permit, RPC permit or SSD reservation leaks;
- exactly the intentional stop/restart is observed, with no additional unexpected restart.

### 14.5 Synthetic throughput and resource test

This test proves the TGX code path has no sub-1-Gbps architectural ceiling without depending on Telegram or the external route.

- Use the production `DownloadManager`, official gotd downloader adapter and SSD writer with a local deterministic fake RPC source.
- Run five large files with 40 offered workers for at least 60 seconds or until a sufficiently large fixed payload completes.
- The SSD used for the test must first demonstrate sequential write capacity above 1 Gbps.
- Required result: active payload throughput above 1 Gbps, `active_rpc <= 40`, no RPS limiter event, bounded RSS, correct hashes and no unexplained zero-throughput interval over 10 seconds.

The result proves code-path capacity, not that a particular Telegram account, DC, proxy or WAN will deliver 1 Gbps.

### 14.6 NAS isolated functional evaluation

Use the frozen Protocol v1 workload identities and a new empty DB/output/log root per run.

#### Run A: direct SSD, archive disabled

- `P-S`: fixed 100-file burst;
- `P-SM`: duration-or-terminal;
- `P-LMS`: duration-or-terminal;
- `P-L`: duration-or-terminal;
- canonical comparison run remains comparable to the frozen TDL configuration;
- a product-capacity run uses the final `40/32/8` in-flight/file/file-thread settings.

#### Run B: direct SSD plus real HDD archive

- use distinct SSD and HDD filesystems so EXDEV copy is exercised;
- repeat `P-S` and `P-LMS` for network/archive overlap;
- stop or make HDD read-only during one run, then restore it;
- verify download success continues until the explicit SSD capacity boundary;
- verify the backlog subsequently drains with one archive writer.

The TDL baseline and TGX comparison run must use the same SSD target-storage identity. If no valid SSD-based Baseline Cohort exists, establish it once before interpreting TGX throughput.

#### Run C: restart recovery

- run a mixed workload;
- kill the TGX container at selected crash points;
- restart the same immutable artifact with the same sandbox mounts;
- verify task states, hashes, residues and container exit metadata.

### 14.7 Required evaluation output

Every run keeps the existing immutable raw contract:

```text
protocol.json
artifact.json
environment.json
run_spec.json
manifest.jsonl
events.jsonl
metrics.jsonl
task_results.jsonl
file_inventory.jsonl
hashes.jsonl
errors.jsonl
process.log
checksums.sha256
```

Update the storage metrics before execution:

```text
SSD download:
  ssd_free_bytes
  ssd_reserved_bytes
  ssd_part_bytes
  ssd_completed_pending_archive_bytes
  ssd_read_bps
  ssd_write_bps
  ssd_space_blocked

Archive:
  archive_backlog_files
  archive_backlog_bytes
  archive_active_workers
  archive_source_read_bytes
  archive_target_write_bytes
  archive_copy_bps
  archive_retry_count
  archive_conflict_count
  archive_moving_count
  archive_fsync_latency
```

Retire `active_segments`, `ready_bytes`, `writing_bytes`, `reclaimed_bytes` and `writeback_bps`; they describe a system that no longer exists. Missing required measurements are `null + collection_error`, never zero.

## 15. Release acceptance gates

All gates must pass on the same immutable candidate artifact.

### 15.1 Architecture deletion gate

- production code has one downloader and one SSD commit path;
- `pkg/spool`, `pkg/writeback` and storage SBE packages are absent;
- no buffer flags, `AsyncMoving`, Spool tables or download `moving` state remain;
- no small/large lane, whole-file small memory buffer or download disk-writer pool remains;
- no legacy/new path flag or runtime migration guess remains;
- gotd version and binary/image identity are pinned and recorded.

### 15.2 Correctness gate

- every completed Golden file has exact size and SHA-256 match;
- no unexpected final files and no unowned `.part`/`.moving` residue after drain;
- no false `success`, no overwrite and no source deletion before verified archive durability;
- every manifest case has an engine-owned task result.

### 15.3 Stability gate

- no unexpected process/container restart, OOM kill or daemon API disappearance;
- one file-level network/write/archive error never terminates the daemon or sibling files;
- no unexplained zero-throughput interval exceeds 10 seconds while eligible work, SSD space and a healthy Telegram route exist;
- all permits, reservations, FDs and goroutines return to their expected steady state after drain/cancel.

### 15.4 Performance gate

- when current control throughput is at least 250 Mbps, `P-S` active payload throughput is at least 200 Mbps;
- otherwise TGX reaches at least 75% of the current calibrated/frozen control according to the policy;
- direct-SSD large/mixed throughput reaches at least 75% of the frozen TDL payload baseline;
- unexplained zero-stall fraction is below 5%;
- the synthetic production path exceeds 1 Gbps on an SSD proven capable of it;
- before SSD capacity pressure, enabling archive does not reduce median network active throughput by more than 10% versus archive-disabled runs under comparable calibration;
- five active large files all make forward progress during a mixed run.

Absolute NAS speed remains an environment result. The synthetic gate and absence of an RPS ceiling establish whether the architecture itself can exceed 1 Gbps.

### 15.5 Archive independence gate

- SSD commit is the download success point;
- archive worker concurrency is one;
- HDD offline/read-only leaves completed downloads successful and retryable archive rows durable;
- SSD backlog grows only as whole completed files;
- when SSD reaches its configured admission boundary, only new downloads pause; active admitted files can finish with their reservations;
- after HDD recovery, archive drains and pending downloads resume without process restart or operator DB edits.

### 15.6 Diagnosability gate

Debug events for one item form an unbroken chain:

```text
item.ingested
item.admitted
item.resolved
download.started
rpc.retry (when applicable)
ssd.commit_prepared
ssd.committed
item.terminal
archive.queued
archive.started
archive.committed
archive.source_removed
```

Every error record contains `task_id`, `attempt_id`, `stage`, `op`, typed/stable error code, original wrapped cause, retry decision, DC/path where relevant and the next owner. Starting from one error record must identify the failing function boundary without timestamp archaeology.

Do not emit an Info log for every successful chunk. Normal ownership transitions are Debug; one authoritative final failure is Error.

## 16. Anti-patch rejection rules

The candidate is returned for rework if any of the following appears:

- old Spool/WriteBack remains and new flags merely bypass it;
- HDD completion still controls download success;
- archive errors are converted into download failures;
- another file-size threshold or small/large branch replaces the old one;
- a downstream size check is claimed to fix invalid chunk planning while the producer still emits invalid work;
- more retry/freeze layers are added around gotd;
- a file error can still escape into daemon-wide cancellation;
- recovery infers ownership only from `.part`/`.moving` suffixes;
- multiple modules can mark the same download/archive terminal state;
- tests exercise a helper that the production constructor does not use;
- performance is claimed from configuration, goroutine count or a unit benchmark instead of immutable raw results.

## 17. Definition of done

The refactor is complete only when:

```text
one ingress owner
+ one active-file owner
+ one physical-RPC owner
+ one SSD commit owner
+ one download terminal owner
+ one archive state/worker owner
+ one recovery owner
+ obsolete paths removed
+ fixed tests produce All GO evidence
```

Passing compilation while retaining the legacy data plane is not completion. Passing a benchmark while crash recovery, error causality or archive independence is unverified is not completion.

## 18. References

- [gotd official repository](https://github.com/gotd/td)
- [gotd v0.161.0 downloader API](https://pkg.go.dev/github.com/gotd/td@v0.161.0/telegram/downloader)

## 19. Short execution goal

```text
/goal 以 83c5e49 为控制面基线完成 TGX 数据面重构：使用官方 gotd 直接并发下载并原子提交到 SSD，彻底删除自研分片整流、内存/磁盘缓存、Spool/WriteBack、双通道和重复容量 owner；增加与下载状态完全解耦、可重启恢复的单 worker 整文件归档。修复必须落在唯一责任 owner，禁止保留旧链路、旁路开关或补丁式兼容。按 DIRECT_SSD_DOWNLOAD_ARCHIVE_REFACTOR_PLAN.md 实施，并以固定评测集、故障注入和 NAS 隔离实测全部通过作为完成条件。
```
