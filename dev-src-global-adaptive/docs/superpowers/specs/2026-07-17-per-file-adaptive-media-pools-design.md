# Per-File Adaptive Media Pools Design

## Decision

Replace the shared data-plane media session pool with one short-lived physical
media session pool per active file. Keep only a process-wide connection budget,
DC cooldown registry, and serialized Kurigram session factory. Sessions are not
shared across files.

The existing five database consumers remain the file-concurrency limit. Each
eligible file starts with up to four dedicated media sessions and may scale
through eight to a hard per-file limit of twelve. One session runs one stripe
at a time and remains bound to that file until completion, cancellation, or
failure.

## Goal

Maximize verified Telegram file-download goodput while avoiding the extreme
second-to-second variation and connection-reset rate observed with a single
shared pool.

With sufficient queued large-file work, the production acceptance target is:

- ten continuous post-warmup minutes;
- five-second committed-byte buckets with P10 at least 8 MiB/s;
- coefficient of variation no greater than 25 percent;
- stripe retry rate below 2 percent and no more than ten logged transport
  resets in the ten-minute acceptance window;
- 100 percent size and whole-file SHA-256 agreement;
- no container restart, OOM, stuck pool, or manual recovery.

These are measurements of durably committed bytes. UI smoothing cannot satisfy
the performance gate.

## Constraints

- NAS changes stay inside
  `/volume2/docker/telegram_media_downloader_us`.
- Continue using Kurigram for MTProto, media DC selection, file references,
  authorization transfer, CDN redirects, CDN decryption, Telegram errors, and
  reconnect behavior.
- Do not implement a raw MTProto or CDN client.
- Preserve the SQLite producer/consumer records, per-file SQLite chunk
  manifest, SSD positional writes, one-MiB chunk hashes, final SSD readback,
  whole-file SHA-256, and atomic HDD publication.
- Keep `max_download_task: 5`; connection tuning must not increase file-level
  consumer concurrency.
- Files at or below 5 MiB remain on the existing sequential Kurigram path.
- The current global-pool image and configuration remain an immediate rollback
  path during rollout.

## Alternatives Considered

### Per-File Physical Pools - Selected

Each active file owns its sessions and workers. This provides explicit file
isolation, stable session affinity, local failure control, and understandable
per-file metrics. It creates and closes connections as files enter and leave,
so connection setup must be staggered and globally bounded.

### Shared Pool With Per-File Quotas

This would reuse sessions efficiently, but file affinity, DC fairness,
contraction, replacement, and cancellation would remain coupled in one global
scheduler. The current implementation demonstrated that this state space is
difficult to tune and reason about under sustained load.

### Fixed Twelve Sessions Per File

This is simple but creates a 60-connection burst when five files start, wastes
connections on short files, and cannot react when additional sessions reduce
goodput or increase resets.

## Architecture

```text
five existing consumers
        |
        +-- file A -> FileMediaSessionPool A -> 4/8/12 dedicated sessions
        +-- file B -> FileMediaSessionPool B -> 4/8/12 dedicated sessions
        +-- ... up to five active file pools
                         |
             MediaTransferCoordinator
             - 60-session hard budget
             - staggered expansion grants
             - DC FloodWait cooldowns
             - serialized session factory
```

There is no process-wide session lease queue. The coordinator never assigns a
session from one file to another.

### MediaTransferCoordinator

One coordinator lives with the Kurigram client. It owns no media sessions. It
provides four shared services:

1. A hard budget of 60 media sessions. Creating, live, and draining sessions
   all consume a permit. A permit is released only after the session has
   stopped to completion.
2. A session factory using the existing Kurigram authorization lock and stale
   session cleanup. Session creation remains serialized where Kurigram requires
   it.
3. A DC cooldown registry. A FloodWait from one file pauses new media requests
   and session growth for every file pool using that DC until the wait expires.
4. An expansion arbiter. At most one file pool may begin a four-session
   expansion in a ten-second interval, preventing synchronized connection
   bursts.

Each worker media session owns at most one lazily created CDN transport per CDN
route. Kurigram still performs redirect handling, decryption, Telegram hash
verification, and RPC retries, but its per-`get_file()` close call sees a
non-owning view. The real CDN transport remains open across stripes and is
stopped with its media session. The 60-session budget applies to worker media
sessions; CDN transports are auxiliary physical connections and must be
reported separately.

### FileMediaSessionPool

One pool is created inside an eligible file's `ParallelDownloader` lifecycle.
It owns every temporary media session created for that file.

- Initial target: `min(4, missing_stripe_count)`.
- Allowed targets: 4, 8, and 12, bounded by remaining stripe count and the
  global budget.
- One worker owns one session for the file lifetime.
- One session executes one logical stripe at a time. Pipelining depth is one.
- A worker that completes a stripe immediately claims another stripe from the
  same file. It does not return the session to a shared pool or perform a new
  handshake.
- Contraction marks excess workers draining. They finish their current stripe,
  stop their sessions, and release budget permits without claiming more work.
- Completion, cancellation, fallback, and fatal failure stop every owned media
  session and its optional CDN transport exactly once.

Pools are memory-only. Resume state remains in the existing manifest rather
than pool metadata.

### File Work Queue

Telegram requests remain at most 1 MiB. The default logical work unit remains
five consecutive 1-MiB chunks, with a shorter final stripe.

- Durable completion is committed after each 1-MiB chunk.
- A transport failure requeues only uncommitted chunks from the stripe.
- Positional writes preserve correctness when stripes complete out of order.
- Workers claim work from only their owning file.
- The five-MiB boundary does not restart TCP or MTProto state because the
  worker keeps both its media session and any lazily created CDN session while
  claiming the next stripe.

Stripe size remains configurable for controlled 5/10/20-MiB experiments, but
the first implementation keeps 5 MiB so pool ownership is the only changed
performance variable.

## Adaptive Control

Each file pool measures complete ten-second windows of durably committed bytes,
active-worker utilization, stripe attempts, retries, unhealthy sessions, and
FloodWait state.

### Expansion

1. Start at four sessions, or fewer when less work remains.
2. Require queued work, at least 80 percent worker utilization, retry rate below
   2 percent, no DC cooldown, and two complete stable windows.
3. Ask the coordinator for the next tier. The coordinator may defer the grant
   to preserve the 60-session budget or stagger another pool's expansion.
4. Evaluate two complete post-expansion windows against the pre-expansion
   committed-byte baseline.
5. Keep the tier only when average goodput improves by at least 5 percent.
   Otherwise return to the prior tier and hold that ceiling for two minutes.

### Contraction

- A post-expansion plateau returns to the prior tier.
- Retry rate at or above 2 percent, two transport failures on one session, or
  more than 10 percent unhealthy sessions drops one tier and starts a
  two-minute growth hold.
- FloodWait freezes the DC and blocks growth; it does not launch replacement
  requests during the wait.
- Workers selected for contraction drain rather than cancelling an in-flight
  stripe.
- A file tail with fewer stripes than sessions drains unneeded workers without
  treating the lower utilization as a network fault.

The controller never raises file count or the 60-session hard budget.

## Data Flow

1. A database consumer claims one pending message using existing ordering and
   state transitions.
2. The consumer resolves the Telegram message and target path exactly as it
   does today.
3. Existing file presence and manifest identity checks run before network work.
4. A file above 5 MiB creates a file pool and plans missing five-MiB stripes.
5. The coordinator grants initial permits; the file pool creates sessions and
   starts one worker per session.
6. Each worker repeatedly downloads one stripe through its bound session,
   writes 1-MiB chunks positionally, hashes and commits each chunk, then claims
   the next stripe from the same file.
7. The controller may expand or drain the file pool while work remains.
8. Final readback and whole-file SHA-256 run unchanged.
9. Only a verified candidate replaces the HDD target and changes the database
   record to success.
10. The file pool stops all sessions and releases all coordinator permits.

## Failure And Recovery

- A retryable transport failure preserves committed 1-MiB chunks, requeues the
  remainder, and replaces only the affected session when demand remains.
- Two consecutive transport failures retire the media/CDN session pair and
  lower the file pool one tier. Replacement observes exponential backoff and
  the global creation budget.
- `AUTH_BYTES_INVALID` uses the existing fresh-authorization retry and stale
  cache cleanup. A partially created file pool closes all sessions on failure.
- FloodWait is enforced at DC scope across file pools.
- File-reference, DC migration, and CDN behavior continue through Kurigram.
- A remote-hash or final-hash integrity failure never publishes the candidate.
  Existing clean retry or sequential fallback behavior remains available.
- Cancellation stops new stripe claims, lets active cleanup complete, persists
  committed chunks, closes sessions, and releases every permit.
- Container restart reconstructs file pools from pending/processing database
  records and manifests. No session object is expected to survive restart.
- Failure in one file pool cannot disable or drain another file pool.

## Configuration

Add explicit per-file mode keys while preserving old keys for rollback:

```yaml
parallel_pool_mode: per_file
parallel_file_pool_threshold: 5242880
parallel_file_pool_stripe_size: 5242880
parallel_file_pool_initial_sessions: 4
parallel_file_pool_max_sessions: 12
parallel_file_pool_control_interval: 10
parallel_file_pool_growth_hold: 120
parallel_media_session_budget: 60
parallel_file_pool_pipeline_depth: 1
```

`max_download_task: 5` remains the active file-pool limit. Invalid values fail
closed to repository defaults. The implementation rejects per-file maxima over
12, budgets over 60, and pipeline depth other than one for this mode.

The existing global-pool mode remains compiled but disabled during the canary,
providing an image-level fallback without changing database or manifest data.

## Observability

Expose both raw evidence and a reader-friendly display:

- coordinator: used/60 permits, creating, live, draining, DC cooldowns, and
  expansion queue;
- each file: target/live/active/draining sessions, pending stripes, committed
  goodput, retries, resets, unhealthy sessions, current tier, and last scale
  reason;
- aggregate: raw one-second committed bytes, five-second rolling goodput, P10,
  mean, standard deviation, coefficient of variation, and retry rate for the
  current observation window.

The Web footer shows the five-second rolling speed and a compact
`Pools N/5 - Sessions X/60` status. Per-file rows use the same five-second
display window. Raw one-second samples remain available to logs and the status
endpoint so smoothing cannot hide stalls during validation.

## Testing

### Automated

Tests must prove:

- files at or below 5 MiB stay sequential;
- one eligible file creates one file pool and five consumers create at most
  five pools;
- sessions never move between files or DCs;
- one worker keeps one session across consecutive stripes;
- one session never executes two stripes concurrently;
- creating, live, and draining sessions never exceed the budget of 60;
- expansions are staggered and follow 4/8/12 tiers;
- growth is retained only after at least 5 percent goodput improvement;
- plateau, retry pressure, unhealthy sessions, and file tails drain safely;
- FloodWait pauses every file pool on the affected DC without pausing another
  DC;
- transport failure resumes uncommitted chunks on a replacement session;
- cancellation, fallback, startup failure, and shutdown stop sessions and
  release permits exactly once;
- restart resumes manifests without redownloading committed chunks;
- existing database ordering, presence checks, positional writes, hashes,
  Web status, and sequential fallback tests continue to pass.

### NAS Integrity Gate

Use immutable already-downloaded files in multiple size bands. Do not alter
production download records to manufacture work.

1. Download candidates into a project validation directory.
2. Test success, cancellation/resume, one forced session reset, repeated proxy
   interruption, and container restart.
3. Compare candidate size and whole-file SHA-256 with the existing HDD file on
   every run.
4. Confirm no candidate publishes before verification and no source file is
   modified.

### Performance Gate

1. Capture the current image baseline using committed-byte samples, resets,
   retries, CPU, memory, and session count.
2. Hold file set, proxy, DC mix, and stripe size constant while comparing fixed
   tiers 4, 8, and 12.
3. With the selected tier logic fixed, compare 5, 10, and 20 MiB stripe sizes.
4. Use the winning stable settings for a ten-minute sustained-load canary.
5. Exclude only the first 30 seconds, explicit DC FloodWait/proxy outage, and a
   file tail where fewer than eight workers have runnable stripes. Report every
   exclusion.
6. Require the goal metrics stated above before declaring performance success.

If the 8-MiB/s P10 target is not reached, preserve the best stable candidate
and report controlled evidence identifying proxy, DC, account, CPU, disk, or
protocol limits. Do not raise concurrency solely to improve a short peak.

## Rollout And Rollback

1. Build and test a candidate image without changing production Compose.
2. Run no-network unit/integration tests against the exact image.
3. Stop only this Compose service for account-safe live hash and recovery
   validation.
4. Deploy behind `parallel_pool_mode: per_file` with a stopped-state backup and
   rollback trap.
5. Observe integrity first, then tune 4/8/12, then run the ten-minute gate.
6. Roll back immediately on hash mismatch, repeated pool stall, auth-key error,
   container restart, Web unavailability, or materially worse stable goodput.

Rollback changes only this project's image/config and keeps database records,
manifests, partials, cursors, and Telegram sessions.

## Out Of Scope

- Increasing the five-file consumer count.
- Replacing Kurigram with tdl, TDLib, or a custom MTProto implementation.
- Changing producer scanning, target selection, cursors, SQLite task semantics,
  SSD/HDD paths, or NAS services.
- Claiming a stable speed from UI smoothing instead of committed-byte evidence.

## Acceptance Criteria

- Production data-plane sessions belong to exactly one file pool for their
  lifetime.
- One session runs at most one stripe concurrently and one file owns at most
  twelve media sessions.
- At most five file pools and sixty feature-owned media sessions are active.
- Pool growth is staggered and rolls back when measured goodput does not
  improve.
- Failures remain local to a session, file, or DC and never stall unrelated
  file pools.
- Every validation file and production candidate passes existing size, chunk,
  readback, and whole-file SHA-256 checks.
- Under sufficient sustained backlog, the ten-minute performance window meets
  P10 at least 8 MiB/s and coefficient of variation no greater than 25 percent,
  with fewer than 2 percent stripe retries and no more than ten transport
  resets.
- The service remains Web-responsive with restart count zero and OOM false.
