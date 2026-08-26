# Global Adaptive Media Session Pool Design

## Goal

Replace per-file temporary media sessions with a process-wide, reusable,
DC-aware transfer pool that can drive the available Telegram and proxy
bandwidth without sacrificing resumability or file integrity.

The pool has a hard limit of 48 media sessions. It starts empty, grows toward
a soft target of 16 only while eligible work exists, and may scale through 24,
32, 40, and 48 when measured aggregate goodput continues to improve.

## Constraints

- NAS changes remain inside
  `/volume2/docker/telegram_media_downloader_us`.
- Keep the existing five file consumers. Pool scheduling replaces per-file
  session ownership; it does not increase producer or consumer concurrency.
- Continue using Kurigram for MTProto, file references, DC migration, CDN
  redirects, CDN decryption, and Telegram error handling.
- Do not implement a raw MTProto or CDN client.
- Keep SQLite download records, the per-file SQLite chunk manifest, positional
  SSD writes, restart recovery, final SSD readback, and whole-file SHA-256.
- Never replace an HDD destination until the candidate is complete and its
  local integrity checks pass.
- Preserve the current `parallel-a9452ad` image and configuration as the
  immediate rollback path.

## Transfer Units

Telegram `upload.getFile` requests remain at most 1 MiB. The new scheduler
groups five consecutive protocol chunks into a 5 MiB logical stripe, except
for the final short stripe.

- Files at or below 5 MiB use the existing single-session Kurigram path.
- Files above 5 MiB register their missing 5 MiB stripes with the global
  scheduler.
- Durable completion remains recorded at 1 MiB granularity. A failure in the
  middle of a stripe therefore retries only uncommitted protocol chunks.
- Two stripe coroutines may be in flight through one healthy media session.
  This gives a maximum of 96 outstanding stripe streams at the 48-session hard
  limit, subject to adaptive scaling and available work.

## Components

### GlobalMediaSessionPool

One pool belongs to the running Kurigram client and lives until client
shutdown. It owns all temporary media sessions created for parallel file
transfers.

- Sessions are partitioned by Telegram DC ID and never leased across DCs.
- The global count of live plus creating sessions never exceeds 48.
- The pool starts empty. Demand for an active DC starts a background builder,
  so the first available session can transfer data while the pool continues
  warming.
- Session creation is serialized within each DC to avoid authorization import
  races. Builders for different DCs may proceed independently.
- Idle sessions are reused by later files in the same DC.
- Sessions idle for 10 minutes are stopped and removed.
- If the global limit is reached and a different DC needs capacity, the pool
  evicts least-recently-used idle sessions before creating sessions for the new
  DC. Active sessions finish their current stripes and may then be retired.
- Shutdown stops builders, rejects new leases, waits for active stripe cleanup,
  and stops every owned session exactly once before the Kurigram client stops.

### DownloadStripeScheduler

The scheduler owns logical stripe queues; files no longer own session queues.

- Eligible files register their media identity, DC ID, remaining stripes,
  manifest, output descriptor, and progress callback.
- Within a DC, deficit round-robin scheduling gives each active file one
  5 MiB quantum per turn.
- When compatible session capacity is available, an active file waiting for a
  stripe receives its next scheduling turn within at most the number of active
  files in that DC. A file whose DC has no healthy capacity waits for pool
  recovery rather than consuming another DC's turn.
- Scheduling is work-conserving. A single file may use all available slots up
  to the global limit. When more files arrive, new stripe assignments rotate
  fairly without cancelling stripes already in flight.
- DCs with runnable work receive capacity whenever a compatible healthy
  session is available. Idle capacity in one DC may be rebalanced through LRU
  retirement and replacement, not by reusing a session against another DC.
- A session worker owns one media session and runs at most two stripe
  coroutines through it. Each coroutine uses Kurigram's existing `get_file`
  path with a session-bound client adapter.
- Positional writes and manifest commits remain independent of completion
  order, so out-of-order stripe completion cannot reorder file bytes.

### AdaptivePoolController

The controller adjusts the desired number of sessions, not the hard limit.

- With queued eligible work, the initial desired size is 16.
- The control window is 60 seconds. When pending stripes exist, session
  utilization is at least 80 percent, no FloodWait is active, and the stripe
  retry rate is below 2 percent, the controller may increase the desired size
  by 8.
- After expansion, two complete windows are compared with the pre-expansion
  aggregate goodput. If improvement is below 5 percent, the controller returns
  to the previous target and treats it as the temporary ceiling for 10 minutes.
- A FloodWait freezes growth for the required wait. If more than 10 percent of
  the desired sessions become unhealthy in one window, the desired size drops
  by 8, never below the capacity currently required by active stripes.
- When no eligible stripes remain, idle timeout retirement is allowed to drain
  the pool to zero. Sixteen is a demand-time soft target, not a permanent
  connection floor.
- Scaling decisions use aggregate committed bytes, not socket receive counters,
  so reconnect traffic and discarded responses do not look like goodput.

## Failure Handling

- `AUTH_BYTES_INVALID` during creation immediately stops and evicts the failed
  session, including any stopped object Kurigram left in a client cache. The
  DC builder retries fresh authorization up to three times.
- A transport reset or timeout requeues only the missing portion of the stripe.
  A different healthy session may claim it. Durable chunks remain committed.
- Two consecutive transport failures on one session mark it unhealthy. It is
  removed, stopped, and replaced only if current demand justifies replacement.
- FloodWait is obeyed at DC scope. Existing work is paused for the Telegram
  delay rather than generating more requests.
- CDN requests continue through Kurigram's CDN session handling. CDN sessions
  are not substituted with media sessions and are not counted as reusable pool
  entries.
- If a DC cannot establish any healthy transfer session after three fresh
  authorization attempts, the affected file uses the existing sequential
  fallback. The failure does not disable the global pool or change later
  files' eligibility.
- Cancellation unregisters the file, requeues no new stripes, lets active
  stripe cleanup finish, and releases all session slots.
- An integrity mismatch never publishes the candidate. Existing manifest and
  fallback behavior remain responsible for a clean retry.

## Configuration

Add new settings rather than changing the meaning of the current canary keys:

```yaml
parallel_session_pool_enabled: true
parallel_pool_file_threshold: 5242880
parallel_pool_stripe_size: 5242880
parallel_pool_soft_sessions: 16
parallel_pool_max_sessions: 48
parallel_pool_pipeline_depth: 2
parallel_pool_idle_ttl: 600
parallel_pool_control_interval: 60
```

The existing `parallel_download_workers: 2` and
`parallel_download_min_size: 268435456` remain in production configuration so
the `parallel-a9452ad` rollback image retains its current behavior. The new
image uses the global pool settings when `parallel_session_pool_enabled` is
true and otherwise preserves the current per-file implementation.

Defaults in the repository remain disabled until validation passes.

## Observability

Emit one compact pool snapshot per control window and expose the same read-only
state through the existing web status data:

- desired and hard session counts;
- live, active, idle, creating, and unhealthy session counts;
- counts by DC;
- active files and pending stripes;
- pipeline depth and aggregate committed-byte goodput;
- last scale decision and reason;
- session creation, eviction, retry, FloodWait, and sequential fallback counts.

The mobile web view adds only a compact status row. It does not add pool
controls or require the user to tune live connections manually.

## Validation

### Automated Tests

Tests must prove:

- a 5 MiB file remains on the single-session path and a larger file registers
  stripes;
- logical stripes cover exact EOF while manifests retain 1 MiB records;
- the global hard limit includes both live and creating sessions;
- sessions never cross DCs;
- creation is serialized within one DC;
- LRU idle eviction makes capacity available to another DC;
- deficit round-robin is fair with multiple files and work-conserving with one;
- one session runs no more than the configured pipeline depth;
- failed stripes resume on another session without losing committed chunks;
- authorization and transport failures evict unhealthy sessions and recover;
- cancellation and shutdown close all owned sessions exactly once;
- existing resume, positional write, hash, database consumer, and sequential
  fallback tests continue to pass.

### NAS Integrity And Performance Gate

1. Build a candidate image without changing the running Compose image.
2. Stop production briefly so the Telegram account session file has one owner.
3. Re-download immutable archived samples around these sizes: below 5 MiB,
   5-20 MiB, 50-200 MiB, 200 MiB-1 GiB, and above 1 GiB.
4. Compare exact size and whole-file SHA-256 against each HDD baseline.
5. Inject cancellation and transport failures, resume from the SQLite
   manifest, and compare SHA-256 again.
6. Sweep session targets 8, 16, 24, 32, 40, and 48 on the same large sample.
   Compare pipeline depths 1 and 2. Record committed-byte goodput, retries,
   authorization errors, resets, FloodWaits, CPU, and memory.
7. Seed the controller's learned target with the best stable measured point.
   Keep the configured hard limit at 48, so later network conditions can still
   justify scaling higher. Do not force the live pool to 48 when throughput has
   plateaued.
8. Deploy only when every SHA matches, no unexplained integrity or CDN error
   occurs, restart recovery passes, and aggregate goodput is no worse than the
   current two-session implementation.
9. After deployment, observe at least two real files above 1 GiB, one proxy
   interruption or injected connection reset, web health, restart count, and
   final HDD hashes.

## Rollback

Before deployment, back up changed source, configuration, and Compose files
under the existing project `backups` directory. Rollback switches Compose to
`telegram_media_downloader_us:parallel-a9452ad`, keeps the old canary keys, and
recreates only `telegram_media_downloader_us`.

Pool sessions are memory-only. Rolling back does not migrate or delete download
records, manifests, partial files, cursors, or Telegram account sessions.

## Out Of Scope

- Increasing the five-file consumer count.
- Replacing Kurigram with tdl, TDLib, or a custom MTProto client.
- Changing producer scanning, cursors, target selection, or SQLite download
  record semantics.
- Moving SSD or HDD paths.
- Modifying NAS services or files outside the project directory.

## Acceptance Criteria

- Global media sessions never exceed 48.
- Files at or below 5 MiB are not split across media sessions.
- Larger files use reusable DC-specific sessions and 5 MiB logical stripes.
- A lone large file can consume all useful capacity; concurrent files continue
  to receive scheduling turns within the stated per-DC round-robin bound.
- Pool errors remain local to a session, stripe, DC, or file and never disable
  future parallel downloads globally.
- Every validation and production sample has an HDD SHA-256 equal to its
  verified candidate SHA-256.
- The web service remains responsive and the downloader survives proxy resets
  without requiring a container restart.
