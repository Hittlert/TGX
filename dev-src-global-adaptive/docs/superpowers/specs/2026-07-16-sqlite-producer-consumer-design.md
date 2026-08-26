# SQLite Producer Consumer Design

## Goal

Separate Telegram message scanning from media downloading. The producer owns the
scan cursor and persists work to SQLite. Consumers claim and complete work only
from SQLite.

## State Model

`download_records` is the durable queue. A record moves through `pending`,
`processing`, and one of `success`, `failed`, or `skipped`. A process restart
returns every `processing` row to `pending`. Failed rows are eligible again only
after a retry delay.

`chat_scan_cursors` stores the next Telegram message id for each configured chat.
The producer inserts or preserves the download record and advances this cursor in
one SQLite transaction. `config.yaml` mirrors the cursor for display and manual
editing. The cursor row also stores the last mirrored value, allowing startup to
distinguish an intentional config edit from a config file that lagged behind the
database during a crash.

## Producer

For each active target, resolve the cursor from SQLite, read Telegram history from
that position, and transactionally enqueue each message plus the next cursor. The
producer does not inspect files, retry old jobs, or wait for consumers. At the end
of a scan it mirrors the current cursor to `config.yaml` and sleeps for the
configured scan interval.

## Consumer

Each worker atomically claims one eligible row from SQLite. It fetches that
Telegram message, applies the configured media filter, checks for an existing
success record, then checks the expected target file. Existing files become
`success` without downloading. Downloads finish as `success` or `failed`.
Unavailable or filtered messages become `skipped`, so one broken target does not
stop other jobs.

The existing in-memory queue remains only for optional bot-triggered jobs that are
not configured scan targets.

## Migration

Existing `pending` and `failed` rows remain usable. New columns and cursor tables
are added with idempotent SQLite schema migration. The Panda target starts with a
producer cursor of zero; all other targets start from their current configured
cursors. Existing successful rows are preserved, and consumers retain the target
file existence check.

## Verification

Unit tests cover atomic enqueue/cursor advancement, success preservation, claim
and restart recovery, retry delay, and manual cursor overrides. Container checks
confirm a responsive web page, an advancing Panda producer cursor, database-only
consumer state transitions, empty YAML retry lists, and no startup directory scan.
