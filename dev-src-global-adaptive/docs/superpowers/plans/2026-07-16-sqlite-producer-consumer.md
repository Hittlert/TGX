# SQLite Producer Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Telegram scanning a cursor-driven SQLite producer and make download workers consume exclusively from SQLite.

**Architecture:** `DownloadRecordStore` owns durable queue claims and scan cursors. The configured-chat scanner only calls an atomic enqueue-and-advance API; workers claim database rows, fetch Telegram messages, check records/files, and write terminal status. YAML is a cursor mirror and manual override, not the retry store.

**Tech Stack:** Python 3.11, asyncio, sqlite3, Pyrogram, unittest, Docker Compose

## Global Constraints

- Modify only `/volume2/docker/telegram_media_downloader_us` on the NAS.
- Use only the existing Linux, Python standard library, and Docker runtime.
- Preserve the consumer-side target-file existence check.
- Reset only `熊猫原版VIP` to cursor `0`; preserve every other target cursor.
- Keep `data.yaml` retry lists empty.

---

### Task 1: Durable Queue And Cursor Store

**Files:**
- Modify: `module/download_records.py`
- Test: `tests/module/test_download_records.py`

**Interfaces:**
- Produces: `enqueue_and_advance_cursor(chat_id, message_id) -> int`
- Produces: `resolve_cursor(chat_id, configured_cursor) -> int`
- Produces: `override_cursor(chat_id, cursor) -> None`
- Produces: `mark_cursor_mirrored(chat_id, cursor) -> None`
- Produces: `claim_next(chat_ids) -> Optional[Dict[str, Any]]`
- Produces: `recover_processing() -> int`

- [ ] **Step 1: Write failing store tests**

```python
def test_enqueue_and_cursor_advance_are_durable():
    store.enqueue_and_advance_cursor("chat", 41)
    assert store.get_cursor("chat") == 42
    assert store.get_status("chat", 41) == "pending"

def test_claim_and_restart_recovery():
    store.enqueue_and_advance_cursor("chat", 41)
    assert store.claim_next(["chat"])["message_id"] == 41
    assert store.recover_processing() == 1
    assert store.claim_next(["chat"])["message_id"] == 41

def test_success_is_not_requeued_by_rescan():
    store.mark_success("chat", 41)
    store.enqueue_and_advance_cursor("chat", 41)
    assert store.get_status("chat", 41) == "success"

def test_manual_cursor_override_beats_mirrored_value():
    assert store.resolve_cursor("chat", 10) == 10
    store.enqueue_and_advance_cursor("chat", 20)
    assert store.resolve_cursor("chat", 10) == 21
    assert store.resolve_cursor("chat", 5) == 5
```

- [ ] **Step 2: Run the store tests and verify RED**

Run: `python3 -m unittest tests.module.test_download_records`

Expected: failures for the missing queue and cursor APIs.

- [ ] **Step 3: Implement schema migration and transactional APIs**

Add `processing_started_at`, `attempts`, and `next_retry_at` columns when absent,
plus `chat_scan_cursors(chat_id, cursor, mirrored_cursor, updated_at)`. Use
`BEGIN IMMEDIATE` for claim and enqueue/cursor transactions. Preserve terminal
`success` and `skipped` rows during producer rescans.

- [ ] **Step 4: Run the store tests and verify GREEN**

Run: `python3 -m unittest tests.module.test_download_records`

Expected: all download-record tests pass.

### Task 2: Application Cursor Ownership

**Files:**
- Modify: `module/app.py`
- Test: `tests/module/test_app.py`

**Interfaces:**
- Consumes: store cursor APIs from Task 1
- Produces: `initialize_scan_cursors() -> None`
- Produces: `record_scanned_message(chat_id, message_id) -> int`
- Produces: `claim_download_record() -> Optional[Dict[str, Any]]`

- [ ] **Step 1: Write failing application tests**

```python
def test_consumer_status_does_not_move_cursor():
    app.set_download_id(node, 41, DownloadStatus.SuccessDownload)
    assert config.last_read_message_id == 10

def test_producer_record_moves_cursor():
    assert app.record_scanned_message("chat", 41) == 42
    assert config.last_read_message_id == 42
```

- [ ] **Step 2: Run the application tests and verify RED**

Run inside the project image: `python -m unittest tests.module.test_app`

Expected: consumer cursor test fails and producer API is missing.

- [ ] **Step 3: Move cursor updates to producer-facing methods**

Initialize SQLite cursor rows during `pre_run`, mirror exact next-message cursors
to YAML, update the mirror marker after atomic YAML replacement, and make web
cursor edits call `override_cursor`. Remove cursor advancement and retry-list
mutation from `set_download_id`.

- [ ] **Step 4: Run application tests and verify GREEN**

Run inside the project image: `python -m unittest tests.module.test_app`

Expected: all application tests pass.

### Task 3: Database Producer And Consumers

**Files:**
- Modify: `media_downloader.py`
- Test: `tests/test_media_downloader.py`

**Interfaces:**
- Consumes: `record_scanned_message` and `claim_download_record`
- Produces: configured-chat producer without in-memory queue writes
- Produces: workers that claim configured-chat jobs from SQLite

- [ ] **Step 1: Write failing producer and consumer tests**

```python
async def test_configured_scan_persists_jobs_without_queue_put():
    await download_chat_task(client, config, TaskNode(chat_id=123))
    app.record_scanned_message.assert_called()
    queue.put.assert_not_called()

async def test_worker_claims_database_job_and_fetches_message():
    app.claim_download_record.return_value = {"chat_id": "123", "message_id": 41}
    await consume_one_database_task(client)
    client.get_messages.assert_awaited_with(chat_id=123, message_ids=41)
```

- [ ] **Step 2: Run media tests and verify RED**

Run inside the project image: `python -m unittest tests.test_media_downloader`

Expected: producer still writes the memory queue and database consumer API is missing.

- [ ] **Step 3: Implement producer and database consumer loops**

The producer persists every scanned message and cursor, then returns without
waiting for downloads. A database consumer claims one row, fetches the message,
applies metadata/filter handling, and delegates actual media work to
`download_task`. Empty messages become `skipped`; exceptions become `failed` with
the configured retry delay. Keep the memory queue path only for bot nodes.

- [ ] **Step 4: Run media tests and verify GREEN**

Run inside the project image: `python -m unittest tests.test_media_downloader`

Expected: producer and consumer tests pass.

### Task 4: Deploy And Verify Migration

**Files:**
- Modify on NAS: `app-src/module/download_records.py`
- Modify on NAS: `app-src/module/app.py`
- Modify on NAS: `app-src/media_downloader.py`
- Modify on NAS: `config.yaml`

**Interfaces:**
- Consumes: completed code from Tasks 1-3
- Produces: running `telegram_media_downloader_us` container

- [ ] **Step 1: Run local focused tests and syntax checks**

Run: `python3 -m unittest tests.module.test_download_records`

Run: `python3 -m py_compile module/download_records.py module/app.py media_downloader.py`

Expected: all commands exit zero.

- [ ] **Step 2: Copy only changed runtime files into the scoped NAS directory**

```bash
scp -O module/download_records.py module/app.py media_downloader.py \
  de1ta@192.168.79.37:/volume2/docker/telegram_media_downloader_us/app-src/
```

- [ ] **Step 3: Stop the service and set only Panda cursor to zero**

Stop the Compose service, back up `config.yaml` and the SQLite database inside the
project directory, set Panda `last_read_message_id` to `0`, and preserve all other
target values.

- [ ] **Step 4: Build and recreate the service**

Run in `/volume2/docker/telegram_media_downloader_us`:
`docker compose build telegram_media_downloader_us` then
`docker compose up -d --force-recreate telegram_media_downloader_us`.

Expected: image build and container start exit zero.

- [ ] **Step 5: Verify runtime behavior**

Confirm HTTP `200` on port `5875`, `data.yaml` retry count `0`, Panda SQLite cursor
advances from zero, pending rows are claimed as `processing`, consumers produce
`success` or delayed `failed`, existing-file logs do not produce downloads, and
consumer completions do not change the producer cursor.
