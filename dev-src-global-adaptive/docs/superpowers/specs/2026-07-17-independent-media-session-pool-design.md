# Independent Media Session Pool Design

## Goal

Replace same-session range concurrency with true multi-session downloads for
large Telegram files, while keeping Kurigram responsible for Telegram file and
CDN protocol handling.

## Constraints

- Production changes on the NAS stay inside
  `/volume2/docker/telegram_media_downloader_us`.
- The existing five-file consumer concurrency remains unchanged.
- Start with two media sessions per eligible file. Four-session testing is out
  of scope until two sessions pass integrity and performance validation.
- Do not implement raw MTProto file or CDN handling.
- Keep the current SQLite chunk manifest, SSD positional writes, final chunk
  readback, whole-file SHA-256, retry behavior, and sequential fallback.
- A candidate must never replace the destination file until all local integrity
  checks pass.

## Architecture

`KurigramRangeSource` gains an explicit lifecycle:

1. `prepare(worker_count)` first warms Kurigram's cached media session, then
   creates `worker_count` temporary media sessions sequentially through
   `Client.get_session(..., is_media=True, temporary=True)`. Sequential setup
   avoids concurrent authorization import races.
2. Prepared sessions are stored in an `asyncio.Queue`. Each `iter_range` call
   leases one session for the full contiguous range and always returns it in a
   `finally` block.
3. A small session-bound client adapter delegates normal attributes to the real
   Kurigram client but returns the leased session for media file requests.
   Kurigram's own `Client.get_file` implementation remains the downloader, so
   its CDN redirect, decrypt, hash, progress, and error behavior is preserved.
4. CDN session requests are delegated to the real client. The adapter never
   substitutes a media session for a CDN session.
5. `close()` stops every temporary media session exactly once. The parallel
   downloader calls it after success, failure, or cancellation.

`ParallelDownloader` remains responsible for chunk planning, retries,
positional writes, manifests, final SSD readback, and whole-file SHA-256. It
only invokes the source lifecycle when the source provides it, preserving the
in-memory test source interface.

## Failure Handling

- If any temporary session cannot be prepared, already-created sessions are
  closed and the exception reaches the existing production fallback, which
  resumes through Kurigram's sequential downloader.
- A transport timeout retries the unfinished range without discarding durable
  chunks already recorded in SQLite.
- Cancellation closes the source sessions and does not start a sequential
  fallback.
- Session cleanup failures are logged without hiding an earlier download
  failure. A cleanup failure after an otherwise successful candidate is treated
  as a parallel-path failure and falls back safely.
- The existing media identity manifest prevents bytes from different Telegram
  media objects from sharing a partial file.

## Validation

Automated tests must prove:

- concurrent range calls use different media session objects;
- temporary sessions are created sequentially;
- a leased session is returned after a range error;
- all temporary sessions close on success, failure, and cancellation;
- CDN session requests still delegate to the real client;
- existing resume, timeout, positional write, and integrity tests continue to
  pass.

NAS validation uses an already-downloaded large message:

1. Stop the production container briefly so the Telegram account session file
   has one owner.
2. Download the same immutable media into the validation directory with two
   independent sessions.
3. Compare candidate size and whole-file SHA-256 with the HDD baseline.
4. Record manifest throughput and compare it with the measured same-session
   baseline.
5. Deploy only if hashes match, no authorization/CDN errors occur, and the
   independent-session run is faster. Otherwise restore the current image and
   configuration.

## Rollback

Before deployment, back up `config.yaml`, `docker-compose.yaml`, and changed
application files under the existing NAS `backups` directory. Rollback switches
Compose to `telegram_media_downloader_us:parallel-7632312`, keeps two workers,
and recreates only this Compose service.
