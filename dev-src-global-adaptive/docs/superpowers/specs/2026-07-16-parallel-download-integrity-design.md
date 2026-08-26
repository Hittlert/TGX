# Parallel Download Integrity Validation Design

## Goal

Add parallel chunk downloading for a single Telegram file without changing the
existing five-file consumer concurrency. Prove byte-for-byte correctness before
the implementation can be enabled for production downloads.

The first validation run uses files that have already been downloaded to HDD as
the local baseline. It does not redownload a second sequential copy.

## Scope

- Keep message scanning, task claiming, retry state, and final success records in
  the existing Python/Kurigram application.
- Keep SSD as the download staging area and HDD as the archive destination.
- Implement the candidate downloader behind a feature flag that is disabled by
  default.
- Store validation code, manifests, and candidate files only under
  `/volume2/docker/telegram_media_downloader_us`.
- Do not rename, move, overwrite, or delete baseline files on HDD.
- Do not update production download records during validation.

## Reference Model

Telegram supports range downloads through `upload.getFile` using byte offsets
and limits. It also exposes `upload.getFileHashes`, whose SHA-256 ranges are the
remote integrity reference:

- https://core.telegram.org/api/files
- https://core.telegram.org/method/upload.getFile

The scheduler and writer follow the mature gotd downloader model:

- Workers atomically claim non-overlapping offsets.
- Completed chunks are written at their absolute offsets, never through a shared
  seek position.
- Telegram hash ranges are verified before a file can be committed.
- A hash mismatch is a hard failure, not a retryable success.

Reference implementation:

- https://github.com/gotd/td/tree/main/telegram/downloader

## Candidate Download Algorithm

1. Resolve the message and capture an immutable media identity consisting of
   chat ID, message ID, media ID, DC ID, file unique ID when available, and
   declared file size.
2. Create a unique SSD `.part` file and truncate it to the exact declared size.
3. Start two range workers for the first validation version.
4. Allocate aligned chunks without gaps or overlaps. Each allocation records its
   offset and expected length in a validation manifest.
5. Download ranges through Kurigram's media session and write them with
   positional writes (`os.pwrite`, with a safe fallback where unavailable).
6. Treat short writes, unexpected lengths, changed media identity, expired file
   references, and connection failures as incomplete work. Never mark them as
   success.
7. Fetch every available Telegram hash range and compare its SHA-256 against the
   corresponding bytes in the SSD file.
8. Verify exact file size and complete interval coverage, then `fsync` the file.
9. Compute the candidate file's whole-file SHA-256 for comparison with the HDD
   baseline. The validation runner does not archive the candidate to HDD.

## Sample Selection

Select successful database records whose archived file still exists and whose
Telegram message remains accessible. Prefer different media types and DCs when
available.

The first run contains six samples:

| Declared size | Samples |
| --- | ---: |
| Less than 10 MB | 2 |
| 10 MB to 200 MB | 2 |
| 200 MB to 1 GB | 1 |
| Greater than 1 GB | 1 |

If a size bucket has no suitable record, the runner reports the gap instead of
silently substituting a different range.

## Three-Way Integrity Decision

For each sample, calculate the whole-file SHA-256 of the existing HDD baseline
and the SSD candidate. Independently verify both files against all Telegram hash
ranges.

| Baseline | Candidate | Telegram hashes | Result |
| --- | --- | --- | --- |
| Match | Match | Both pass | Candidate passes |
| Differ | Candidate passes | Baseline fails | Existing archive is suspect; candidate is not blamed |
| Differ | Baseline passes | Candidate fails | Candidate fails and parallel mode is blocked |
| Match | Both fail | Neither passes | Test is invalid or the remote identity changed |
| Differ | Both fail | Neither passes | Test is invalid and requires investigation |

No sample is considered passed from file size alone. Candidate rollout requires
all valid candidate samples to pass Telegram hash verification and requires no
unexplained baseline/candidate mismatch.

## Crash Test

During one file larger than 200 MB, terminate the isolated candidate container
after chunks have been written. Restart it with the same manifest and SSD file.
The downloader must reclaim only incomplete or unverified ranges and produce the
same final SHA-256 as the baseline and Telegram hashes.

The production container remains stopped while the validation client uses the
Telegram session. It is restarted on the old production image after validation,
whether the candidate passes or fails.

## Failure Handling

- Any candidate hash mismatch disables parallel mode for the run.
- Candidate files and manifests are retained for diagnosis on failure.
- A changed media identity invalidates the partial file; chunks from different
  media identities are never combined.
- Flood waits are honored globally. Transient range failures are retried at the
  same offset with a bounded attempt count.
- If Telegram hashes cannot be obtained, the sample is `unverified`, not passed.
- Production SQLite rows and HDD files remain untouched throughout validation.

## Test Strategy

Unit tests cover out-of-order completion, duplicate allocation, missing ranges,
short writes, corrupted bytes, exact chunk boundaries, final short chunks,
files larger than 2 GB, file-reference refresh, and process recovery.

Integration validation records, for every sample:

- media identity and declared size;
- baseline and candidate paths;
- baseline and candidate whole-file SHA-256;
- Telegram hash coverage and mismatch count;
- sequential historical file size versus candidate size;
- elapsed time, effective throughput, retries, and worker count;
- final decision and reason.

The candidate is eligible for a later production canary only when the first run
has six valid samples, a 100% candidate integrity pass rate, and zero unexplained
mismatches. Production enablement is a separate decision after reviewing the
validation report.
