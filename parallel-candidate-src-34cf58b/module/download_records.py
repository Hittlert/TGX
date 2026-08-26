"""Persistent download records backed by SQLite."""

import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Union


class DownloadRecordStore:
    """Store per-message download state outside YAML config files."""

    RETRY_STATUSES = ("pending", "failed")

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._ensure_schema()

    @staticmethod
    def chat_key(chat_id: Union[int, str]) -> str:
        return str(chat_id)

    @staticmethod
    def message_key(message_id: Any) -> int:
        try:
            return max(int(message_id or 0), 0)
        except (TypeError, ValueError):
            return 0

    def _connect(self) -> sqlite3.Connection:
        parent = os.path.dirname(os.path.abspath(self.db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)

        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _ensure_schema(self):
        with self._lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS download_records (
                    chat_id TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    file_name TEXT,
                    save_path TEXT,
                    media_type TEXT,
                    file_size INTEGER,
                    error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    downloaded_at INTEGER,
                    processing_started_at INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry_at INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (chat_id, message_id)
                )
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(download_records)"
                ).fetchall()
            }
            if "processing_started_at" not in columns:
                connection.execute(
                    "ALTER TABLE download_records "
                    "ADD COLUMN processing_started_at INTEGER"
                )
            if "attempts" not in columns:
                connection.execute(
                    "ALTER TABLE download_records "
                    "ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
                )
            if "next_retry_at" not in columns:
                connection.execute(
                    "ALTER TABLE download_records "
                    "ADD COLUMN next_retry_at INTEGER NOT NULL DEFAULT 0"
                )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_download_records_retry
                ON download_records(chat_id, status, next_retry_at, message_id)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_scan_cursors (
                    chat_id TEXT PRIMARY KEY,
                    cursor INTEGER NOT NULL,
                    mirrored_cursor INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )

    def upsert(
        self,
        chat_id: Union[int, str],
        message_id: int,
        status: str,
        *,
        file_name: Optional[str] = None,
        save_path: Optional[str] = None,
        media_type: Optional[str] = None,
        file_size: Optional[int] = None,
        error: Optional[str] = None,
        downloaded_at: Optional[int] = None,
    ):
        """Insert or update one message record."""
        now = int(time.time())
        if downloaded_at is None and status == "success":
            downloaded_at = now

        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO download_records (
                    chat_id,
                    message_id,
                    status,
                    file_name,
                    save_path,
                    media_type,
                    file_size,
                    error,
                    created_at,
                    updated_at,
                    downloaded_at,
                    processing_started_at,
                    next_retry_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)
                ON CONFLICT(chat_id, message_id) DO UPDATE SET
                    status = excluded.status,
                    file_name = COALESCE(excluded.file_name, download_records.file_name),
                    save_path = COALESCE(excluded.save_path, download_records.save_path),
                    media_type = COALESCE(excluded.media_type, download_records.media_type),
                    file_size = COALESCE(excluded.file_size, download_records.file_size),
                    error = excluded.error,
                    updated_at = excluded.updated_at,
                    downloaded_at = COALESCE(
                        excluded.downloaded_at,
                        download_records.downloaded_at
                    ),
                    processing_started_at = NULL,
                    next_retry_at = 0
                """,
                (
                    self.chat_key(chat_id),
                    self.message_key(message_id),
                    status,
                    file_name,
                    save_path,
                    media_type,
                    file_size,
                    error,
                    now,
                    now,
                    downloaded_at,
                ),
            )

    def mark_pending(self, chat_id: Union[int, str], message_id: int):
        now = int(time.time())
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO download_records (
                    chat_id, message_id, status, created_at, updated_at
                )
                VALUES (?, ?, 'pending', ?, ?)
                ON CONFLICT(chat_id, message_id) DO UPDATE SET
                    status = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.status
                        ELSE excluded.status
                    END,
                    error = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.error
                        ELSE NULL
                    END,
                    updated_at = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.updated_at
                        ELSE excluded.updated_at
                    END,
                    processing_started_at = NULL,
                    next_retry_at = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.next_retry_at
                        ELSE 0
                    END
                """,
                (self.chat_key(chat_id), self.message_key(message_id), now, now),
            )

    def mark_failed(
        self,
        chat_id: Union[int, str],
        message_id: int,
        error: Optional[str] = None,
        retry_delay: int = 0,
    ):
        now = int(time.time())
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO download_records (
                    chat_id,
                    message_id,
                    status,
                    error,
                    created_at,
                    updated_at,
                    next_retry_at
                )
                VALUES (?, ?, 'failed', ?, ?, ?, ?)
                ON CONFLICT(chat_id, message_id) DO UPDATE SET
                    status = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.status
                        ELSE 'failed'
                    END,
                    error = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.error
                        ELSE excluded.error
                    END,
                    updated_at = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.updated_at
                        ELSE excluded.updated_at
                    END,
                    processing_started_at = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.processing_started_at
                        ELSE NULL
                    END,
                    next_retry_at = CASE
                        WHEN download_records.status = 'success'
                            THEN download_records.next_retry_at
                        ELSE excluded.next_retry_at
                    END
                """,
                (
                    self.chat_key(chat_id),
                    self.message_key(message_id),
                    error,
                    now,
                    now,
                    now + max(int(retry_delay or 0), 0),
                ),
            )

    def mark_skipped(self, chat_id: Union[int, str], message_id: int):
        if not self.has_success(chat_id, message_id):
            self.upsert(chat_id, message_id, "skipped")

    def mark_success(
        self,
        chat_id: Union[int, str],
        message_id: int,
        *,
        file_name: Optional[str] = None,
        save_path: Optional[str] = None,
        media_type: Optional[str] = None,
        file_size: Optional[int] = None,
    ):
        self.upsert(
            chat_id,
            message_id,
            "success",
            file_name=file_name,
            save_path=save_path,
            media_type=media_type,
            file_size=file_size,
            error="",
        )

    def has_success(self, chat_id: Union[int, str], message_id: int) -> bool:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM download_records
                WHERE chat_id = ? AND message_id = ? AND status = 'success'
                LIMIT 1
                """,
                (self.chat_key(chat_id), self.message_key(message_id)),
            ).fetchone()

        return row is not None

    def get_retry_ids(self, chat_id: Union[int, str]) -> List[int]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT message_id
                FROM download_records
                WHERE chat_id = ? AND status IN ('pending', 'failed')
                ORDER BY message_id
                """,
                (self.chat_key(chat_id),),
            ).fetchall()

        return [int(row["message_id"]) for row in rows]

    def get_record(
        self, chat_id: Union[int, str], message_id: int
    ) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM download_records
                WHERE chat_id = ? AND message_id = ?
                LIMIT 1
                """,
                (self.chat_key(chat_id), self.message_key(message_id)),
            ).fetchone()

        return dict(row) if row is not None else None

    def enqueue_and_advance_cursor(
        self, chat_id: Union[int, str], message_id: int
    ) -> int:
        """Persist a producer job and its next-message cursor atomically."""
        chat_key = self.chat_key(chat_id)
        message_key = self.message_key(message_id)
        next_cursor = message_key + 1
        now = int(time.time())

        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO download_records (
                    chat_id, message_id, status, created_at, updated_at
                )
                VALUES (?, ?, 'pending', ?, ?)
                ON CONFLICT(chat_id, message_id) DO UPDATE SET
                    status = CASE
                        WHEN download_records.status = 'skipped' THEN 'pending'
                        ELSE download_records.status
                    END,
                    error = CASE
                        WHEN download_records.status = 'skipped' THEN NULL
                        ELSE download_records.error
                    END,
                    updated_at = CASE
                        WHEN download_records.status = 'skipped'
                            THEN excluded.updated_at
                        ELSE download_records.updated_at
                    END,
                    processing_started_at = download_records.processing_started_at,
                    next_retry_at = CASE
                        WHEN download_records.status = 'skipped' THEN 0
                        ELSE download_records.next_retry_at
                    END
                """,
                (chat_key, message_key, now, now),
            )
            connection.execute(
                """
                INSERT INTO chat_scan_cursors (
                    chat_id, cursor, mirrored_cursor, updated_at
                )
                VALUES (?, ?, 0, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    cursor = MAX(chat_scan_cursors.cursor, excluded.cursor),
                    updated_at = excluded.updated_at
                """,
                (chat_key, next_cursor, now),
            )
            row = connection.execute(
                "SELECT cursor FROM chat_scan_cursors WHERE chat_id = ?",
                (chat_key,),
            ).fetchone()

        return int(row["cursor"])

    def resolve_cursor(
        self, chat_id: Union[int, str], configured_cursor: int
    ) -> int:
        """Resolve DB progress while honoring an intentional config edit."""
        chat_key = self.chat_key(chat_id)
        configured = self.message_key(configured_cursor)
        now = int(time.time())

        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT cursor, mirrored_cursor
                FROM chat_scan_cursors
                WHERE chat_id = ?
                """,
                (chat_key,),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO chat_scan_cursors (
                        chat_id, cursor, mirrored_cursor, updated_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (chat_key, configured, configured, now),
                )
                return configured

            if configured != int(row["mirrored_cursor"]):
                connection.execute(
                    """
                    UPDATE chat_scan_cursors
                    SET cursor = ?, mirrored_cursor = ?, updated_at = ?
                    WHERE chat_id = ?
                    """,
                    (configured, configured, now, chat_key),
                )
                return configured

            return int(row["cursor"])

    def override_cursor(self, chat_id: Union[int, str], cursor: int):
        chat_key = self.chat_key(chat_id)
        cursor_key = self.message_key(cursor)
        now = int(time.time())
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO chat_scan_cursors (
                    chat_id, cursor, mirrored_cursor, updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    cursor = excluded.cursor,
                    mirrored_cursor = excluded.mirrored_cursor,
                    updated_at = excluded.updated_at
                """,
                (chat_key, cursor_key, cursor_key, now),
            )

    def get_cursor(self, chat_id: Union[int, str]) -> Optional[int]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT cursor FROM chat_scan_cursors WHERE chat_id = ?",
                (self.chat_key(chat_id),),
            ).fetchone()

        return int(row["cursor"]) if row is not None else None

    def mark_cursor_mirrored(self, chat_id: Union[int, str], cursor: int):
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE chat_scan_cursors
                SET mirrored_cursor = ?, updated_at = ?
                WHERE chat_id = ?
                """,
                (
                    self.message_key(cursor),
                    int(time.time()),
                    self.chat_key(chat_id),
                ),
            )

    def claim_next(
        self, chat_ids: List[Union[int, str]]
    ) -> Optional[Dict[str, Any]]:
        chat_keys = [self.chat_key(chat_id) for chat_id in chat_ids]
        if not chat_keys:
            return None

        now = int(time.time())
        placeholders = ",".join("?" for _ in chat_keys)
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"""
                SELECT chat_id, message_id
                FROM download_records
                WHERE chat_id IN ({placeholders})
                  AND (
                    status = 'pending'
                    OR (status = 'failed' AND next_retry_at <= ?)
                  )
                ORDER BY message_id ASC
                LIMIT 1
                """,
                (*chat_keys, now),
            ).fetchone()
            if row is None:
                return None

            connection.execute(
                """
                UPDATE download_records
                SET status = 'processing',
                    processing_started_at = ?,
                    updated_at = ?,
                    attempts = attempts + 1
                WHERE chat_id = ? AND message_id = ?
                """,
                (now, now, row["chat_id"], row["message_id"]),
            )
            claimed = connection.execute(
                """
                SELECT *
                FROM download_records
                WHERE chat_id = ? AND message_id = ?
                """,
                (row["chat_id"], row["message_id"]),
            ).fetchone()

        return dict(claimed)

    def recover_processing(self) -> int:
        """Return jobs owned by a previous process to the pending queue."""
        now = int(time.time())
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE download_records
                SET status = 'pending',
                    processing_started_at = NULL,
                    updated_at = ?
                WHERE status = 'processing'
                """,
                (now,),
            )
            return int(cursor.rowcount)

    def count_by_status(self, chat_id: Union[int, str]) -> Dict[str, int]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, COUNT(*) AS total
                FROM download_records
                WHERE chat_id = ?
                GROUP BY status
                """,
                (self.chat_key(chat_id),),
            ).fetchall()

        return {str(row["status"]): int(row["total"]) for row in rows}
