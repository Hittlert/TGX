package daemon

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Database struct {
	db   *sql.DB
	lock sync.RWMutex
}

func NewDatabase(dbPath string) (*Database, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1) // SQLite single writer safety

	d := &Database{db: db}
	if err := d.initSchema(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return d, nil
}

func (d *Database) DB() *sql.DB {
	return d.db
}

func (d *Database) Close() error {
	return d.db.Close()
}

var (
	ErrStateConflict  = errors.New("state conflict: invalid state transition")
	ErrStaleAttempt   = errors.New("stale attempt: generation mismatch")
	ErrAlreadySuccess = errors.New("download already completed successfully")
)

func (d *Database) initSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS download_records (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			file_name TEXT,
			save_path TEXT,
			media_type TEXT,
			file_size INTEGER,
			sha256 TEXT,
			error TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			downloaded_at INTEGER,
			processing_started_at INTEGER,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_retry_at INTEGER NOT NULL DEFAULT 0,
			attempt_generation TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (chat_id, message_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_download_records_retry ON download_records(chat_id, status, next_retry_at, message_id)`,
		`CREATE INDEX IF NOT EXISTS idx_download_records_downloaded ON download_records(status, downloaded_at DESC, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS archive_jobs (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			relative_path TEXT NOT NULL,
			expected_size INTEGER NOT NULL,
			sha256 TEXT NOT NULL,
			state TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0,
			next_retry_at INTEGER NOT NULL DEFAULT 0,
			claim_id TEXT NOT NULL DEFAULT '',
			last_error TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (chat_id, message_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_archive_due ON archive_jobs(state, next_retry_at)`,
		`CREATE TABLE IF NOT EXISTS chat_scan_cursors (
			chat_id TEXT PRIMARY KEY,
			cursor INTEGER NOT NULL,
			mirrored_cursor INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS chat_messages (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			sender_id TEXT,
			sender_name TEXT,
			text TEXT,
			media_type TEXT,
			has_media INTEGER NOT NULL DEFAULT 0,
			reply_to_message_id INTEGER,
			date INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (chat_id, message_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_chat_messages_lookup ON chat_messages(chat_id, message_id)`,
		`CREATE TABLE IF NOT EXISTS listen_targets (
			chat_id TEXT NOT NULL PRIMARY KEY,
			enabled INTEGER NOT NULL DEFAULT 1,
			title TEXT NOT NULL DEFAULT '',
			username TEXT NOT NULL DEFAULT '',
			chat_type TEXT NOT NULL DEFAULT '',
			download_filter TEXT NOT NULL DEFAULT '',
			upload_telegram_chat_id TEXT NOT NULL DEFAULT '',
			priority INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1,
			peer_id TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS telegram_accounts (
			namespace TEXT PRIMARY KEY,
			user_id INTEGER NOT NULL DEFAULT 0,
			first_name TEXT NOT NULL DEFAULT '',
			last_name TEXT NOT NULL DEFAULT '',
			username TEXT NOT NULL DEFAULT '',
			phone TEXT NOT NULL DEFAULT '',
			is_premium INTEGER NOT NULL DEFAULT 0,
			is_active INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
	}

	for _, q := range queries {
		if _, err := d.db.Exec(q); err != nil {
			return err
		}
	}

	// Ensure sha256 column exists on download_records
	_, _ = d.db.Exec(`ALTER TABLE download_records ADD COLUMN sha256 TEXT`)
	// Ensure attempt_generation column exists on download_records
	_, _ = d.db.Exec(`ALTER TABLE download_records ADD COLUMN attempt_generation TEXT NOT NULL DEFAULT ''`)
	// Ensure claim_id column exists on archive_jobs
	_, _ = d.db.Exec(`ALTER TABLE archive_jobs ADD COLUMN claim_id TEXT NOT NULL DEFAULT ''`)

	// Automatically migrate legacy @username primary keys to canonical numeric IDs
	d.migrateLegacyUsernameIDs()

	return nil
}

func (d *Database) migrateLegacyUsernameIDs() {
	legacyMap := map[string]struct {
		NumericID string
		Title     string
		Username  string
		Type      string
	}{
		"@memento7711bot": {NumericID: "8844705144", Title: "纪念品bot", Username: "memento7711bot", Type: "bot"},
		"@Spjqr1_bot":     {NumericID: "8955155825", Title: "鱼哥原创视频机器人", Username: "Spjqr1_bot", Type: "bot"},
		"@XHDGZB521bot":   {NumericID: "7236297057", Title: "花卉市场高中部1", Username: "XHDGZB521bot", Type: "bot"},
		"@jinianpinbot":   {NumericID: "7377780474", Title: "纪念品bot", Username: "jinianpinbot", Type: "bot"},
	}

	for oldID, info := range legacyMap {
		var oldEnabled int
		err := d.db.QueryRow(`SELECT enabled FROM listen_targets WHERE chat_id = ?`, oldID).Scan(&oldEnabled)
		if err == nil {
			_, _ = d.db.Exec(`
				INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, upload_telegram_chat_id, priority, created_at, updated_at, revision)
				VALUES(?, ?, ?, ?, ?, '', '', 0, unixepoch(), unixepoch(), 1)
				ON CONFLICT(chat_id) DO UPDATE SET
					enabled = excluded.enabled,
					title = excluded.title,
					username = excluded.username,
					chat_type = excluded.chat_type,
					updated_at = excluded.updated_at
			`, info.NumericID, oldEnabled, info.Title, info.Username, info.Type)
			_, _ = d.db.Exec(`DELETE FROM listen_targets WHERE chat_id = ?`, oldID)
		}

		_, _ = d.db.Exec(`UPDATE OR IGNORE download_records SET chat_id = ? WHERE chat_id = ?`, info.NumericID, oldID)
		_, _ = d.db.Exec(`DELETE FROM download_records WHERE chat_id = ?`, oldID)

		_, _ = d.db.Exec(`UPDATE OR IGNORE chat_messages SET chat_id = ? WHERE chat_id = ?`, info.NumericID, oldID)
		_, _ = d.db.Exec(`DELETE FROM chat_messages WHERE chat_id = ?`, oldID)

		_, _ = d.db.Exec(`UPDATE OR IGNORE chat_scan_cursors SET chat_id = ? WHERE chat_id = ?`, info.NumericID, oldID)
		_, _ = d.db.Exec(`DELETE FROM chat_scan_cursors WHERE chat_id = ?`, oldID)
	}

	_, _ = d.db.Exec(`DELETE FROM listen_targets WHERE chat_id LIKE '@%' AND chat_id NOT IN (SELECT DISTINCT chat_id FROM download_records)`)
}

// Helper wrapper
func (d *Database) Execute(query string, args ...any) (sql.Result, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.db.Exec(query, args...)
}

func (d *Database) GetTargetTitle(chatID string) string {
	d.lock.RLock()
	defer d.lock.RUnlock()
	var title string
	_ = d.db.QueryRow(`SELECT title FROM listen_targets WHERE chat_id = ?`, chatID).Scan(&title)
	return title
}

func (d *Database) GetListenTargets() ([]ListenTarget, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	rows, err := d.db.Query(`
		SELECT t.chat_id, t.enabled, t.title, t.username, t.chat_type, t.download_filter, 
		       t.upload_telegram_chat_id, t.priority, COALESCE(c.cursor, 0), t.created_at, t.updated_at, t.revision
		FROM listen_targets t
		LEFT JOIN chat_scan_cursors c ON t.chat_id = c.chat_id
		ORDER BY t.priority DESC, t.updated_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var targets []ListenTarget
	for rows.Next() {
		var item ListenTarget
		var enabledInt int
		if err := rows.Scan(
			&item.ChatID, &enabledInt, &item.Title, &item.Username, &item.ChatType,
			&item.DownloadFilter, &item.UploadTelegramChatID, &item.Priority,
			&item.LastReadMessageID, &item.CreatedAt, &item.UpdatedAt, &item.Revision,
		); err != nil {
			return nil, err
		}
		item.Enabled = enabledInt == 1
		targets = append(targets, item)
	}
	return targets, nil
}

func (d *Database) SaveListenTargets(targets []ListenTarget) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, item := range targets {
		enabledInt := 0
		if item.Enabled {
			enabledInt = 1
		}
		_, err := tx.Exec(`
			INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, upload_telegram_chat_id, priority, created_at, updated_at, revision)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
			ON CONFLICT(chat_id) DO UPDATE SET
				enabled = excluded.enabled,
				title = CASE WHEN excluded.title != '' THEN excluded.title ELSE listen_targets.title END,
				username = CASE WHEN excluded.username != '' THEN excluded.username ELSE listen_targets.username END,
				chat_type = CASE WHEN excluded.chat_type != '' THEN excluded.chat_type ELSE listen_targets.chat_type END,
				download_filter = excluded.download_filter,
				upload_telegram_chat_id = excluded.upload_telegram_chat_id,
				priority = excluded.priority,
				updated_at = excluded.updated_at,
				revision = listen_targets.revision + 1
		`, item.ChatID, enabledInt, item.Title, item.Username, item.ChatType, item.DownloadFilter, item.UploadTelegramChatID, item.Priority, now, now)
		if err != nil {
			return err
		}

		if item.LastReadMessageID > 0 {
			_, err = tx.Exec(`
				INSERT INTO chat_scan_cursors(chat_id, cursor, mirrored_cursor, updated_at)
				VALUES(?, ?, 0, ?)
				ON CONFLICT(chat_id) DO UPDATE SET
					cursor = excluded.cursor,
					updated_at = excluded.updated_at
			`, item.ChatID, item.LastReadMessageID, now)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (d *Database) SaveDiscoveredDialogs(dialogs []ListenTarget) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, item := range dialogs {
		_, err := tx.Exec(`
			INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, upload_telegram_chat_id, priority, created_at, updated_at, revision)
			VALUES(?, 0, ?, ?, ?, '', '', 0, ?, ?, 1)
			ON CONFLICT(chat_id) DO UPDATE SET
				title = CASE WHEN excluded.title != '' THEN excluded.title ELSE listen_targets.title END,
				username = CASE WHEN excluded.username != '' THEN excluded.username ELSE listen_targets.username END,
				chat_type = CASE WHEN excluded.chat_type != '' THEN excluded.chat_type ELSE listen_targets.chat_type END,
				updated_at = excluded.updated_at
		`, item.ChatID, item.Title, item.Username, item.ChatType, now, now)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *Database) SaveSingleListenTarget(item ListenTarget) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	enabledInt := 0
	if item.Enabled {
		enabledInt = 1
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, upload_telegram_chat_id, priority, created_at, updated_at, revision)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		ON CONFLICT(chat_id) DO UPDATE SET
			enabled = excluded.enabled,
			title = CASE WHEN excluded.title != '' THEN excluded.title ELSE listen_targets.title END,
			username = CASE WHEN excluded.username != '' THEN excluded.username ELSE listen_targets.username END,
			chat_type = CASE WHEN excluded.chat_type != '' THEN excluded.chat_type ELSE listen_targets.chat_type END,
			download_filter = excluded.download_filter,
			upload_telegram_chat_id = excluded.upload_telegram_chat_id,
			priority = excluded.priority,
			updated_at = excluded.updated_at,
			revision = listen_targets.revision + 1
	`, item.ChatID, enabledInt, item.Title, item.Username, item.ChatType, item.DownloadFilter, item.UploadTelegramChatID, item.Priority, now, now)
	if err != nil {
		return err
	}

	if item.LastReadMessageID > 0 {
		_, err = tx.Exec(`
			INSERT INTO chat_scan_cursors(chat_id, cursor, mirrored_cursor, updated_at)
			VALUES(?, ?, 0, ?)
			ON CONFLICT(chat_id) DO UPDATE SET
				cursor = excluded.cursor,
				updated_at = excluded.updated_at
		`, item.ChatID, item.LastReadMessageID, now)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *Database) GetScanCursor(chatID string) (int, error) {
	cursor, _, err := d.GetScanCursorWithTime(chatID)
	return cursor, err
}

func (d *Database) GetScanCursorWithTime(chatID string) (int, int64, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var cursor int
	var updatedAt int64
	err := d.db.QueryRow(`SELECT cursor, updated_at FROM chat_scan_cursors WHERE chat_id = ?`, chatID).Scan(&cursor, &updatedAt)
	if err == sql.ErrNoRows {
		return 0, 0, nil
	}
	return cursor, updatedAt, err
}

func (d *Database) SaveScanCursor(chatID string, cursor int) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	_, err := d.db.Exec(`
		INSERT INTO chat_scan_cursors(chat_id, cursor, mirrored_cursor, updated_at)
		VALUES(?, ?, 0, ?)
		ON CONFLICT(chat_id) DO UPDATE SET
			cursor = excluded.cursor,
			updated_at = excluded.updated_at
	`, chatID, cursor, now)
	return err
}

func (d *Database) IngestMessage(msg ChatMessage) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	hasMediaInt := 0
	if msg.HasMedia {
		hasMediaInt = 1
	}

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin ingest tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	_, err = tx.Exec(`
		INSERT INTO chat_messages(chat_id, message_id, sender_id, sender_name, text, media_type, has_media, reply_to_message_id, date, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(chat_id, message_id) DO UPDATE SET
			text = excluded.text,
			media_type = excluded.media_type,
			has_media = excluded.has_media,
			updated_at = excluded.updated_at
	`, msg.ChatID, msg.MessageID, msg.SenderID, msg.SenderName, msg.Text, msg.MediaType, hasMediaInt, msg.ReplyToMessageID, msg.Date, now, now)
	if err != nil {
		return fmt.Errorf("insert chat_message: %w", err)
	}

	if msg.HasMedia {
		fileName := msg.FileName
		if fileName == "" || strings.HasSuffix(fileName, ".bin") || strings.HasSuffix(fileName, ".unknown") {
			ext := ".mp4"
			if msg.MediaType == "photo" {
				ext = ".jpg"
			} else if msg.MediaType == "audio" {
				ext = ".mp3"
			}
			fileName = fmt.Sprintf("%d%s", msg.MessageID, ext)
		}

		_, err = tx.Exec(`
			INSERT INTO download_records(chat_id, message_id, status, file_name, media_type, file_size, created_at, updated_at)
			VALUES(?, ?, 'pending', ?, ?, ?, ?, ?)
			ON CONFLICT(chat_id, message_id) DO UPDATE SET
				file_name = CASE WHEN download_records.file_name IS NULL OR download_records.file_name LIKE '%.bin' THEN excluded.file_name ELSE download_records.file_name END,
				media_type = excluded.media_type,
				file_size = excluded.file_size
		`, msg.ChatID, msg.MessageID, fileName, msg.MediaType, msg.FileSize, now, now)
		if err != nil {
			return fmt.Errorf("insert download_record: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ingest tx: %w", err)
	}
	tx = nil
	return nil
}

func (d *Database) GetPendingDownloads(limit int) ([]DownloadRecord, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	now := time.Now().Unix()
	rows, err := d.db.Query(`
		SELECT r.chat_id, r.message_id, r.status, COALESCE(r.file_name, ''), COALESCE(r.save_path, ''), 
		       COALESCE(r.media_type, ''), COALESCE(r.file_size, 0), COALESCE(r.error, ''), 
		       r.created_at, r.updated_at, r.attempts, r.next_retry_at, COALESCE(t.title, ''),
		       COALESCE(m.date, r.created_at)
		FROM download_records r
		INNER JOIN listen_targets t ON r.chat_id = t.chat_id
		LEFT JOIN chat_messages m ON r.chat_id = m.chat_id AND r.message_id = m.message_id
		WHERE t.enabled = 1 AND (r.status = 'pending' OR (r.status = 'failed' AND r.next_retry_at <= ?))
		ORDER BY t.priority DESC, r.message_id ASC
		LIMIT ?
	`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []DownloadRecord
	for rows.Next() {
		var rec DownloadRecord
		if err := rows.Scan(
			&rec.ChatID, &rec.MessageID, &rec.Status, &rec.FileName, &rec.SavePath,
			&rec.MediaType, &rec.FileSize, &rec.Error, &rec.CreatedAt, &rec.UpdatedAt,
			&rec.Attempts, &rec.NextRetryAt, &rec.TargetTitle, &rec.Date,
		); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// GetDownloadRecord retrieves a single download record by chatID and messageID.
func (d *Database) GetDownloadRecord(chatID string, messageID int) (*DownloadRecord, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var rec DownloadRecord
	var savePath, sha, attemptGen sql.NullString
	err := d.db.QueryRow(`
		SELECT chat_id, message_id, status, file_name, save_path, media_type, file_size, COALESCE(sha256, ''), attempts, COALESCE(attempt_generation, '')
		FROM download_records
		WHERE chat_id = ? AND message_id = ?
	`, chatID, messageID).Scan(
		&rec.ChatID, &rec.MessageID, &rec.Status, &rec.FileName, &savePath, &rec.MediaType, &rec.FileSize, &sha, &rec.Attempts, &attemptGen,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if savePath.Valid {
		rec.SavePath = savePath.String
	}
	if sha.Valid {
		rec.SHA256 = sha.String
	}
	if attemptGen.Valid {
		rec.AttemptGeneration = attemptGen.String
	}
	return &rec, nil
}

func (d *Database) UpdateDownloadStatus(chatID string, messageID int, status string, fileName, savePath, mediaType string, fileSize int64, errMsg string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	var downloadedAt *int64
	if status == "success" {
		downloadedAt = &now
	}

	// Check current status to ensure idempotency and prevent duplicate attempts increments
	var currentStatus string
	var currentAttempts int
	_ = d.db.QueryRow(`SELECT status, attempts FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, messageID).Scan(&currentStatus, &currentAttempts)

	if currentStatus == "success" {
		return nil
	}
	if currentStatus == "failed" && status == "failed" {
		return nil
	}

	var nextRetryAt int64 = 0
	if status == "failed" {
		lowerErr := strings.ToLower(errMsg)
		// Lifecycle/shutdown cancellation is NOT a download failure:
		// Reset status to 'pending', do NOT increment attempts, do NOT freeze for 7 days!
		if strings.Contains(lowerErr, "context canceled") ||
			strings.Contains(lowerErr, "context deadline exceeded") ||
			lowerErr == "canceled" || lowerErr == "task canceled" ||
			strings.Contains(lowerErr, "engine forcibly closed") {
			status = "pending"
			errMsg = ""
		} else if strings.Contains(lowerErr, "deleted") || strings.Contains(lowerErr, "unavailable") || strings.Contains(lowerErr, "message_id_invalid") {
			nextRetryAt = now + 86400*7
		} else {
			// Transient network / I/O error: exponential backoff capped at 30 minutes (1800s).
			// Do NOT freeze transient tasks for 7 days!
			backoff := int64(60 * (1 << currentAttempts))
			if backoff > 1800 {
				backoff = 1800
			}
			nextRetryAt = now + backoff
		}
	}

	initialAttempts := 0
	if status == "failed" {
		initialAttempts = 1
	}

	_, err := d.db.Exec(`
		INSERT INTO download_records (
			chat_id, message_id, status, file_name, save_path, media_type,
			file_size, error, attempts, next_retry_at, created_at, updated_at, downloaded_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(chat_id, message_id) DO UPDATE SET
			status = excluded.status,
			file_name = COALESCE(NULLIF(excluded.file_name, ''), download_records.file_name),
			save_path = COALESCE(NULLIF(excluded.save_path, ''), download_records.save_path),
			media_type = COALESCE(NULLIF(excluded.media_type, ''), download_records.media_type),
			file_size = CASE WHEN excluded.file_size > 0 THEN excluded.file_size ELSE download_records.file_size END,
			error = excluded.error,
			attempts = CASE WHEN excluded.status = 'failed' THEN download_records.attempts + 1 ELSE download_records.attempts END,
			next_retry_at = CASE WHEN excluded.status = 'failed' THEN excluded.next_retry_at ELSE download_records.next_retry_at END,
			updated_at = excluded.updated_at,
			downloaded_at = COALESCE(excluded.downloaded_at, download_records.downloaded_at)
	`, chatID, messageID, status, fileName, savePath, mediaType, fileSize, errMsg, initialAttempts, nextRetryAt, now, now, downloadedAt)
	return err
}

func (d *Database) GetDownloadedRecords(searchQuery string, limit, offset int) ([]DownloadRecord, int, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var total int
	var rows *sql.Rows
	var err error

	if searchQuery != "" {
		pattern := "%" + searchQuery + "%"
		_ = d.db.QueryRow(`
			SELECT COUNT(*) FROM download_records
			WHERE status = 'success' AND (file_name LIKE ? OR chat_id LIKE ? OR save_path LIKE ?)
		`, pattern, pattern, pattern).Scan(&total)

		rows, err = d.db.Query(`
			SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''),
			       COALESCE(media_type, ''), COALESCE(file_size, 0), created_at, updated_at, COALESCE(downloaded_at, 0)
			FROM download_records
			WHERE status = 'success' AND (file_name LIKE ? OR chat_id LIKE ? OR save_path LIKE ?)
			ORDER BY downloaded_at DESC, updated_at DESC
			LIMIT ? OFFSET ?
		`, pattern, pattern, pattern, limit, offset)
	} else {
		_ = d.db.QueryRow(`SELECT COUNT(*) FROM download_records WHERE status = 'success'`).Scan(&total)

		rows, err = d.db.Query(`
			SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''),
			       COALESCE(media_type, ''), COALESCE(file_size, 0), created_at, updated_at, COALESCE(downloaded_at, 0)
			FROM download_records
			WHERE status = 'success'
			ORDER BY downloaded_at DESC, updated_at DESC
			LIMIT ? OFFSET ?
		`, limit, offset)
	}

	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var records []DownloadRecord
	for rows.Next() {
		var rec DownloadRecord
		var dlAt int64
		if err := rows.Scan(
			&rec.ChatID, &rec.MessageID, &rec.Status, &rec.FileName, &rec.SavePath,
			&rec.MediaType, &rec.FileSize, &rec.CreatedAt, &rec.UpdatedAt, &dlAt,
		); err != nil {
			return nil, 0, err
		}
		rec.DownloadedAt = dlAt
		records = append(records, rec)
	}
	return records, total, nil
}

func (d *Database) GetChatMessagesAround(chatID string, targetMid, limitBefore, limitAfter int) ([]ChatMessage, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var msgs []ChatMessage

	// Before target message
	rowsBefore, err := d.db.Query(`
		SELECT chat_id, message_id, COALESCE(sender_id, ''), COALESCE(sender_name, ''),
		       COALESCE(text, ''), COALESCE(media_type, ''), has_media,
		       COALESCE(reply_to_message_id, 0), date
		FROM chat_messages
		WHERE chat_id = ? AND message_id < ?
		ORDER BY message_id DESC
		LIMIT ?
	`, chatID, targetMid, limitBefore)
	if err == nil {
		defer rowsBefore.Close()
		var beforeList []ChatMessage
		for rowsBefore.Next() {
			var m ChatMessage
			var hasMediaInt int
			if err := rowsBefore.Scan(
				&m.ChatID, &m.MessageID, &m.SenderID, &m.SenderName,
				&m.Text, &m.MediaType, &hasMediaInt, &m.ReplyToMessageID, &m.Date,
			); err == nil {
				m.HasMedia = hasMediaInt == 1
				beforeList = append(beforeList, m)
			}
		}
		// Reverse to ascending
		for i := len(beforeList) - 1; i >= 0; i-- {
			msgs = append(msgs, beforeList[i])
		}
	}

	// Target message and after
	rowsAfter, err := d.db.Query(`
		SELECT chat_id, message_id, COALESCE(sender_id, ''), COALESCE(sender_name, ''),
		       COALESCE(text, ''), COALESCE(media_type, ''), has_media,
		       COALESCE(reply_to_message_id, 0), date
		FROM chat_messages
		WHERE chat_id = ? AND message_id >= ?
		ORDER BY message_id ASC
		LIMIT ?
	`, chatID, targetMid, limitAfter)
	if err == nil {
		defer rowsAfter.Close()
		for rowsAfter.Next() {
			var m ChatMessage
			var hasMediaInt int
			if err := rowsAfter.Scan(
				&m.ChatID, &m.MessageID, &m.SenderID, &m.SenderName,
				&m.Text, &m.MediaType, &hasMediaInt, &m.ReplyToMessageID, &m.Date,
			); err == nil {
				m.HasMedia = hasMediaInt == 1
				msgs = append(msgs, m)
			}
		}
	}

	return msgs, nil
}

type TargetProgressStat struct {
	ChatID          string `json:"chat_id"`
	TotalFiles      int    `json:"total_files"`
	DownloadedFiles int    `json:"downloaded_files"`
	PendingFiles    int    `json:"pending_files"`
	ProcessingFiles int    `json:"processing_files"`
	FailedFiles     int    `json:"failed_files"`
	SkippedFiles    int    `json:"skipped_files"`
	DownloadedBytes int64  `json:"downloaded_bytes"`
}

func (d *Database) GetTargetProgressStats() (map[string]TargetProgressStat, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	rows, err := d.db.Query(`
		SELECT chat_id, status, count(*), COALESCE(sum(file_size), 0)
		FROM download_records
		GROUP BY chat_id, status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[string]TargetProgressStat)
	for rows.Next() {
		var chatID, status string
		var count int
		var bytes int64
		if err := rows.Scan(&chatID, &status, &count, &bytes); err != nil {
			return nil, err
		}

		stat := res[chatID]
		stat.ChatID = chatID
		stat.TotalFiles += count

		switch status {
		case "success":
			stat.DownloadedFiles += count
			stat.DownloadedBytes += bytes
		case "pending":
			stat.PendingFiles += count
		case "downloading", "processing":
			stat.ProcessingFiles += count
		case "failed":
			stat.FailedFiles += count
		case "skipped":
			stat.SkippedFiles += count
		}
		res[chatID] = stat
	}
	return res, nil
}

func (d *Database) Get24hSuccessBytes() int64 {
	d.lock.RLock()
	defer d.lock.RUnlock()

	cutoff := time.Now().Unix() - 86400
	var total sql.NullInt64
	_ = d.db.QueryRow(`
		SELECT sum(file_size) 
		FROM download_records 
		WHERE status = 'success' AND (downloaded_at >= ? OR updated_at >= ?)
	`, cutoff, cutoff).Scan(&total)

	return total.Int64
}

type TelegramAccount struct {
	Namespace string `json:"namespace"`
	UserID    int64  `json:"user_id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
	Phone     string `json:"phone"`
	IsPremium bool   `json:"is_premium"`
	IsActive  bool   `json:"is_active"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

func (d *Database) SaveAccount(acc TelegramAccount) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	if acc.CreatedAt == 0 {
		acc.CreatedAt = now
	}
	acc.UpdatedAt = now

	if acc.IsActive {
		_, _ = d.db.Exec(`UPDATE telegram_accounts SET is_active = 0`)
	}

	_, err := d.db.Exec(`
		INSERT INTO telegram_accounts (namespace, user_id, first_name, last_name, username, phone, is_premium, is_active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(namespace) DO UPDATE SET
			user_id = excluded.user_id,
			first_name = excluded.first_name,
			last_name = excluded.last_name,
			username = excluded.username,
			phone = excluded.phone,
			is_premium = excluded.is_premium,
			is_active = excluded.is_active,
			updated_at = excluded.updated_at
	`, acc.Namespace, acc.UserID, acc.FirstName, acc.LastName, acc.Username, acc.Phone, acc.IsPremium, acc.IsActive, acc.CreatedAt, acc.UpdatedAt)
	return err
}

func (d *Database) GetAccounts() ([]TelegramAccount, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	rows, err := d.db.Query(`
		SELECT namespace, user_id, first_name, last_name, username, phone, is_premium, is_active, created_at, updated_at
		FROM telegram_accounts
		ORDER BY is_active DESC, updated_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []TelegramAccount
	for rows.Next() {
		var a TelegramAccount
		if err := rows.Scan(&a.Namespace, &a.UserID, &a.FirstName, &a.LastName, &a.Username, &a.Phone, &a.IsPremium, &a.IsActive, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		list = append(list, a)
	}
	return list, rows.Err()
}

func (d *Database) GetActiveAccount() (*TelegramAccount, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var a TelegramAccount
	err := d.db.QueryRow(`
		SELECT namespace, user_id, first_name, last_name, username, phone, is_premium, is_active, created_at, updated_at
		FROM telegram_accounts
		WHERE is_active = 1
		LIMIT 1
	`).Scan(&a.Namespace, &a.UserID, &a.FirstName, &a.LastName, &a.Username, &a.Phone, &a.IsPremium, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (d *Database) SetActiveAccount(namespace string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	_, _ = d.db.Exec(`UPDATE telegram_accounts SET is_active = 0`)
	_, err := d.db.Exec(`UPDATE telegram_accounts SET is_active = 1, updated_at = ? WHERE namespace = ?`, time.Now().Unix(), namespace)
	return err
}

func (d *Database) DeleteAccount(namespace string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	_, err := d.db.Exec(`DELETE FROM telegram_accounts WHERE namespace = ?`, namespace)
	return err
}

// EnsureDownloadRecord guarantees that a row exists in download_records for this task.
func (d *Database) EnsureDownloadRecord(chatID string, messageID int, finalPath string, expectedSize int64) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	_, err := d.db.Exec(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, created_at, updated_at, attempt_generation)
		VALUES (?, ?, 'pending', ?, ?, ?, ?, '')
		ON CONFLICT(chat_id, message_id) DO NOTHING
	`, chatID, messageID, finalPath, expectedSize, now, now)
	return err
}

// BeginDownload transitions a record from 'pending' (or eligible retry) to 'downloading'
// and binds the current attempt generation.
func (d *Database) BeginDownload(chatID string, messageID int, generation string, fileName, savePath, mediaType string, fileSize int64) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	var currentStatus, currentGen string
	err := d.db.QueryRow(`SELECT status, COALESCE(attempt_generation, '') FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, messageID).Scan(&currentStatus, &currentGen)
	if err == sql.ErrNoRows {
		_, err = d.db.Exec(`
			INSERT INTO download_records (
				chat_id, message_id, status, file_name, save_path, media_type,
				file_size, attempt_generation, created_at, updated_at, processing_started_at
			) VALUES (?, ?, 'downloading', ?, ?, ?, ?, ?, ?, ?, ?)
		`, chatID, messageID, fileName, savePath, mediaType, fileSize, generation, now, now, now)
		return err
	} else if err != nil {
		return err
	}

	if currentStatus == "success" {
		return ErrAlreadySuccess
	}
	if currentStatus == "unavailable" {
		return fmt.Errorf("%w: cannot download unavailable message", ErrStateConflict)
	}
	if currentStatus == "committing" {
		return fmt.Errorf("%w: message is currently committing", ErrStateConflict)
	}
	if currentStatus == "downloading" {
		if currentGen == generation {
			return nil // idempotent for same active attempt
		}
		return fmt.Errorf("%w: concurrent download active with gen %q vs %q", ErrStateConflict, currentGen, generation)
	}

	res, err := d.db.Exec(`
		UPDATE download_records
		SET status = 'downloading',
			file_name = CASE WHEN ? != '' THEN ? ELSE file_name END,
			save_path = CASE WHEN ? != '' THEN ? ELSE save_path END,
			media_type = CASE WHEN ? != '' THEN ? ELSE media_type END,
			file_size = CASE WHEN ? > 0 THEN ? ELSE file_size END,
			attempt_generation = ?,
			updated_at = ?,
			processing_started_at = ?,
			error = ''
		WHERE chat_id = ? AND message_id = ?
		  AND status IN ('pending', 'failed')
	`, fileName, fileName, savePath, savePath, mediaType, mediaType, fileSize, fileSize, generation, now, now, chatID, messageID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: zero rows affected in BeginDownload", ErrStateConflict)
	}
	return nil
}

// PrepareDownloadCommit records durable intent to commit an SSD download before atomic rename.
// Strictly validates generation and state transitions: only 'downloading' + matching generation -> 'committing'.
func (d *Database) PrepareDownloadCommit(chatID string, messageID int, generation string, relPath string, size int64, sha256Hex string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	var currentStatus, currentGen, currentSHA, currentPath string
	var currentSize int64
	err := d.db.QueryRow(`
		SELECT status, COALESCE(attempt_generation, ''), COALESCE(sha256, ''), COALESCE(save_path, ''), COALESCE(file_size, 0)
		FROM download_records
		WHERE chat_id = ? AND message_id = ?
	`, chatID, messageID).Scan(&currentStatus, &currentGen, &currentSHA, &currentPath, &currentSize)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: record does not exist for PrepareDownloadCommit", ErrStateConflict)
	} else if err != nil {
		return err
	}

	// Idempotency: if already committing with same generation, path, size, and sha256
	if currentStatus == "committing" && currentGen == generation && currentSHA == sha256Hex && currentPath == relPath && currentSize == size {
		return nil
	}
	// If already success with same sha256
	if currentStatus == "success" && currentSHA == sha256Hex {
		return nil
	}
	// Generation guard: reject stale attempts
	if currentGen != generation {
		return fmt.Errorf("%w: record has gen %q, attempt has gen %q", ErrStaleAttempt, currentGen, generation)
	}
	// State guard: only downloading can transition to committing
	if currentStatus != "downloading" {
		return fmt.Errorf("%w: cannot transition from %q to committing", ErrStateConflict, currentStatus)
	}

	res, err := d.db.Exec(`
		UPDATE download_records
		SET status = 'committing',
			save_path = ?,
			file_size = ?,
			sha256 = ?,
			updated_at = ?
		WHERE chat_id = ? AND message_id = ?
		  AND status = 'downloading'
		  AND attempt_generation = ?
	`, relPath, size, sha256Hex, now, chatID, messageID, generation)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: zero rows affected in PrepareDownloadCommit", ErrStateConflict)
	}
	return nil
}

// CompleteDownloadAndQueueArchive atomically marks download_records.status='success'
// and enqueues an archive_job when archive is enabled.
// Strictly validates generation and state transitions: only 'committing' + current generation + matching proof -> 'success'.
func (d *Database) CompleteDownloadAndQueueArchive(chatID string, messageID int, generation string, relPath string, size int64, sha256Hex string, queueArchive bool) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin complete tx: %w", err)
	}
	defer tx.Rollback()

	var currentStatus, currentGen, currentSHA, currentPath string
	var currentSize int64
	err = tx.QueryRow(`
		SELECT status, COALESCE(attempt_generation, ''), COALESCE(sha256, ''), COALESCE(save_path, ''), COALESCE(file_size, 0)
		FROM download_records
		WHERE chat_id = ? AND message_id = ?
	`, chatID, messageID).Scan(&currentStatus, &currentGen, &currentSHA, &currentPath, &currentSize)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: record does not exist for CompleteDownload", ErrStateConflict)
	} else if err != nil {
		return err
	}

	if currentStatus == "success" {
		if currentSHA != sha256Hex || currentPath != relPath || currentSize != size {
			return fmt.Errorf("%w: already success with conflicting proof (sha: %q vs %q, size: %d vs %d, path: %q vs %q)",
				ErrStateConflict, currentSHA, sha256Hex, currentSize, size, currentPath, relPath)
		}
		if queueArchive {
			if queueErr := d.ensureArchiveJobLocked(tx, chatID, messageID, relPath, size, sha256Hex, now); queueErr != nil {
				return queueErr
			}
		}
		return tx.Commit()
	}

	// Generation guard
	if currentGen != generation {
		return fmt.Errorf("%w: record has gen %q, attempt has gen %q", ErrStaleAttempt, currentGen, generation)
	}

	// State guard: ONLY committing can transition to success!
	if currentStatus != "committing" {
		return fmt.Errorf("%w: cannot transition from %q to success (must be committing)", ErrStateConflict, currentStatus)
	}

	// Verify committed proof matches
	if currentSHA != "" && currentSHA != sha256Hex {
		return fmt.Errorf("%w: prepared sha %q does not match completion sha %q", ErrStateConflict, currentSHA, sha256Hex)
	}
	if currentPath != "" && currentPath != relPath {
		return fmt.Errorf("%w: prepared path %q does not match completion path %q", ErrStateConflict, currentPath, relPath)
	}
	if currentSize > 0 && currentSize != size {
		return fmt.Errorf("%w: prepared size %d does not match completion size %d", ErrStateConflict, currentSize, size)
	}

	res, err := tx.Exec(`
		UPDATE download_records
		SET status = 'success',
			save_path = ?,
			file_size = ?,
			sha256 = ?,
			downloaded_at = ?,
			updated_at = ?,
			error = ''
		WHERE chat_id = ? AND message_id = ?
		  AND status = 'committing'
		  AND attempt_generation = ?
	`, relPath, size, sha256Hex, now, now, chatID, messageID, generation)
	if err != nil {
		return fmt.Errorf("update download success: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: zero rows affected in CompleteDownload", ErrStateConflict)
	}

	if queueArchive {
		if queueErr := d.ensureArchiveJobLocked(tx, chatID, messageID, relPath, size, sha256Hex, now); queueErr != nil {
			return queueErr
		}
	}

	return tx.Commit()
}

// CompleteExistingDownload handles idempotent completion for pre-existing files on disk with verified SHA proof.
func (d *Database) CompleteExistingDownload(chatID string, messageID int, generation string, relPath string, size int64, sha256Hex string, queueArchive bool) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin complete existing tx: %w", err)
	}
	defer tx.Rollback()

	var currentStatus, currentGen, currentSHA, currentPath string
	var currentSize int64
	err = tx.QueryRow(`
		SELECT status, COALESCE(attempt_generation, ''), COALESCE(sha256, ''), COALESCE(save_path, ''), COALESCE(file_size, 0)
		FROM download_records
		WHERE chat_id = ? AND message_id = ?
	`, chatID, messageID).Scan(&currentStatus, &currentGen, &currentSHA, &currentPath, &currentSize)
	if err == sql.ErrNoRows {
		_, err = tx.Exec(`
			INSERT INTO download_records (
				chat_id, message_id, status, save_path, file_size, sha256,
				attempt_generation, downloaded_at, updated_at, error, created_at
			) VALUES (?, ?, 'success', ?, ?, ?, ?, ?, ?, '', ?)
		`, chatID, messageID, relPath, size, sha256Hex, generation, now, now, now)
		if err != nil {
			return fmt.Errorf("insert existing download success: %w", err)
		}
	} else if err != nil {
		return err
	} else {
		if currentStatus == "success" {
			if currentSHA != "" && currentSHA != sha256Hex {
				return fmt.Errorf("%w: already success with different sha %q vs %q", ErrStateConflict, currentSHA, sha256Hex)
			}
		} else if currentStatus == "committing" || currentStatus == "downloading" {
			if currentGen != "" && generation != "" && currentGen != generation {
				return fmt.Errorf("%w: record has gen %q, attempt has gen %q", ErrStaleAttempt, currentGen, generation)
			}
			res, err := tx.Exec(`
				UPDATE download_records
				SET status = 'success',
					save_path = ?,
					file_size = ?,
					sha256 = ?,
					downloaded_at = ?,
					updated_at = ?,
					error = ''
				WHERE chat_id = ? AND message_id = ?
				  AND status IN ('committing', 'downloading')
				  AND (attempt_generation = ? OR attempt_generation = '')
			`, relPath, size, sha256Hex, now, now, chatID, messageID, generation)
			if err != nil {
				return fmt.Errorf("update existing download success: %w", err)
			}
			n, _ := res.RowsAffected()
			if n == 0 {
				return fmt.Errorf("%w: zero rows affected in CompleteExistingDownload", ErrStateConflict)
			}
		} else {
			return fmt.Errorf("%w: cannot complete existing download from status %q", ErrStateConflict, currentStatus)
		}
	}

	if queueArchive {
		if queueErr := d.ensureArchiveJobLocked(tx, chatID, messageID, relPath, size, sha256Hex, now); queueErr != nil {
			return queueErr
		}
	}

	return tx.Commit()
}

func (d *Database) ensureArchiveJobLocked(tx *sql.Tx, chatID string, messageID int, relPath string, size int64, sha256Hex string, now int64) error {
	var arcState, arcSHA, arcPath, arcClaimID string
	var arcSize int64
	err := tx.QueryRow(`
		SELECT state, sha256, relative_path, expected_size, COALESCE(claim_id, '')
		FROM archive_jobs
		WHERE chat_id = ? AND message_id = ?
	`, chatID, messageID).Scan(&arcState, &arcSHA, &arcPath, &arcSize, &arcClaimID)
	if err == sql.ErrNoRows {
		_, err = tx.Exec(`
			INSERT INTO archive_jobs (
				chat_id, message_id, relative_path, expected_size, sha256,
				state, attempts, next_retry_at, claim_id, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, 'pending', 0, 0, '', ?, ?)
		`, chatID, messageID, relPath, size, sha256Hex, now, now)
		if err != nil {
			return fmt.Errorf("insert archive job: %w", err)
		}
		return nil
	} else if err != nil {
		return err
	}

	switch arcState {
	case "archived":
		if arcSHA != sha256Hex || arcPath != relPath || arcSize != size {
			_, err = tx.Exec(`
				UPDATE archive_jobs
				SET state = 'conflict',
					last_error = 'archive identity mismatch on duplicate complete',
					updated_at = ?
				WHERE chat_id = ? AND message_id = ?
			`, now, chatID, messageID)
			if err != nil {
				return fmt.Errorf("set archive conflict: %w", err)
			}
			return fmt.Errorf("%w: duplicate completion identity mismatch with archived job", ErrStateConflict)
		}
		// If sha, path, and size match, preserve terminal 'archived' state!
		return nil

	case "conflict":
		// Already in conflict state: do not overwrite
		if arcSHA != sha256Hex || arcPath != relPath || arcSize != size {
			return fmt.Errorf("%w: duplicate completion identity mismatch on conflicted archive job", ErrStateConflict)
		}
		return nil

	case "copying":
		// Strict guard: DO NOT mutate an active copy's identity!
		if arcSHA != sha256Hex || arcPath != relPath || arcSize != size {
			return fmt.Errorf("%w: duplicate completion identity mismatch while archive job is actively copying", ErrStateConflict)
		}
		// Matching identity: no-op, active copy will complete
		return nil

	default: // pending
		if arcSHA == sha256Hex && arcPath == relPath && arcSize == size {
			return nil
		}
		_, err = tx.Exec(`
			UPDATE archive_jobs
			SET state = 'conflict',
				last_error = 'archive identity mismatch on duplicate download complete',
				updated_at = ?
			WHERE chat_id = ? AND message_id = ?
		`, now, chatID, messageID)
		if err != nil {
			return fmt.Errorf("update pending archive job to conflict: %w", err)
		}
		return fmt.Errorf("%w: duplicate completion identity mismatch with pending archive job", ErrStateConflict)
	}
}

// GetDueArchiveJobs retrieves pending archive jobs that are ready to run.
func (d *Database) GetDueArchiveJobs(limit int) ([]ArchiveJob, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	now := time.Now().Unix()
	rows, err := d.db.Query(`
		SELECT chat_id, message_id, relative_path, expected_size, sha256,
		       state, attempts, next_retry_at, COALESCE(claim_id, ''), COALESCE(last_error, ''), created_at, updated_at
		FROM archive_jobs
		WHERE state = 'pending' AND next_retry_at <= ?
		ORDER BY created_at ASC
		LIMIT ?
	`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []ArchiveJob
	for rows.Next() {
		var j ArchiveJob
		if err := rows.Scan(
			&j.ChatID, &j.MessageID, &j.RelativePath, &j.ExpectedSize, &j.SHA256,
			&j.State, &j.Attempts, &j.NextRetryAt, &j.ClaimID, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// ClaimArchiveJob atomically transitions an archive job from 'pending' to 'copying' with a claim ID.
func (d *Database) ClaimArchiveJob(chatID string, messageID int, claimID string) (bool, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	res, err := d.db.Exec(`
		UPDATE archive_jobs
		SET state = 'copying', claim_id = ?, updated_at = ?
		WHERE chat_id = ? AND message_id = ? AND state = 'pending'
	`, claimID, now, chatID, messageID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// CompleteArchiveJob marks an archive job as 'archived' upon verified durability.
// Strict guard: requires matching claimID and expectedSHA, transitions only from 'copying'.
func (d *Database) CompleteArchiveJob(chatID string, messageID int, claimID string, expectedSHA string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	res, err := d.db.Exec(`
		UPDATE archive_jobs
		SET state = 'archived', last_error = '', claim_id = '', updated_at = ?
		WHERE chat_id = ? AND message_id = ? AND state = 'copying'
		  AND (claim_id = ? OR claim_id = '')
		  AND sha256 = ?
	`, now, chatID, messageID, claimID, expectedSHA)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var state, currentSHA, currentClaim string
		err := d.db.QueryRow(`SELECT state, sha256, COALESCE(claim_id, '') FROM archive_jobs WHERE chat_id = ? AND message_id = ?`, chatID, messageID).Scan(&state, &currentSHA, &currentClaim)
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: archive job does not exist", ErrStateConflict)
		} else if err != nil {
			return err
		}
		if state == "archived" {
			if currentSHA == expectedSHA {
				return nil // idempotent duplicate complete
			}
			return fmt.Errorf("%w: archive job already archived with conflicting sha (%q vs %q)", ErrStateConflict, currentSHA, expectedSHA)
		}
		if state == "conflict" {
			return fmt.Errorf("%w: archive job is in conflict state", ErrStateConflict)
		}
		if currentClaim != "" && claimID != "" && currentClaim != claimID {
			return fmt.Errorf("%w: stale archive claim %q (active is %q)", ErrStaleAttempt, claimID, currentClaim)
		}
		return fmt.Errorf("%w: archive job not in copying state (state=%q)", ErrStateConflict, state)
	}
	return nil
}

// FailArchiveJob returns a failed archive attempt to 'pending' with bounded exponential retry delay (capped at 30 min).
// Strict guard: only transitions from 'copying' with matching claimID, NEVER modifies 'archived' or 'conflict'.
func (d *Database) FailArchiveJob(chatID string, messageID int, claimID string, errStr string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	var state, currentClaim string
	var attempts int
	err := d.db.QueryRow(`SELECT state, attempts, COALESCE(claim_id, '') FROM archive_jobs WHERE chat_id = ? AND message_id = ?`, chatID, messageID).Scan(&state, &attempts, &currentClaim)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: archive job does not exist", ErrStateConflict)
	} else if err != nil {
		return err
	}

	if state == "archived" || state == "conflict" {
		return fmt.Errorf("%w: cannot fail archive job in terminal state %q", ErrStateConflict, state)
	}
	if state != "copying" {
		return fmt.Errorf("%w: cannot fail archive job from state %q", ErrStateConflict, state)
	}
	if currentClaim != "" && claimID != "" && currentClaim != claimID {
		return fmt.Errorf("%w: stale archive attempt %q (active is %q)", ErrStaleAttempt, claimID, currentClaim)
	}

	attempts++
	backoffSec := int64(5 * (1 << (attempts - 1)))
	if backoffSec > 1800 || backoffSec <= 0 {
		backoffSec = 1800
	}
	nextRetry := now + backoffSec

	res, err := d.db.Exec(`
		UPDATE archive_jobs
		SET state = 'pending', attempts = ?, next_retry_at = ?, last_error = ?, claim_id = '', updated_at = ?
		WHERE chat_id = ? AND message_id = ? AND state = 'copying'
		  AND (claim_id = ? OR claim_id = '')
	`, attempts, nextRetry, errStr, now, chatID, messageID, claimID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: zero rows affected in FailArchiveJob", ErrStateConflict)
	}
	return nil
}

// SetArchiveJobConflict marks an archive job as 'conflict' requiring operator intervention without overwrite.
// Strict guard: cannot overwrite 'archived', requires matching claimID if active.
func (d *Database) SetArchiveJobConflict(chatID string, messageID int, claimID string, errStr string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	var state, currentClaim string
	err := d.db.QueryRow(`SELECT state, COALESCE(claim_id, '') FROM archive_jobs WHERE chat_id = ? AND message_id = ?`, chatID, messageID).Scan(&state, &currentClaim)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: archive job does not exist", ErrStateConflict)
	} else if err != nil {
		return err
	}

	if state == "archived" {
		return fmt.Errorf("%w: cannot set conflict on already archived job", ErrStateConflict)
	}
	if state == "conflict" {
		return nil // idempotent conflict
	}
	if currentClaim != "" && claimID != "" && currentClaim != claimID {
		return fmt.Errorf("%w: stale archive claim %q (active is %q)", ErrStaleAttempt, claimID, currentClaim)
	}

	res, err := d.db.Exec(`
		UPDATE archive_jobs
		SET state = 'conflict', last_error = ?, claim_id = '', updated_at = ?
		WHERE chat_id = ? AND message_id = ? AND state IN ('pending', 'copying')
		  AND (claim_id = ? OR claim_id = '')
	`, errStr, now, chatID, messageID, claimID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: zero rows affected in SetArchiveJobConflict", ErrStateConflict)
	}
	return nil
}

// FailDownloadDisposition records download failure or unavailability using a structured FailureDisposition,
// preventing confusion over positional boolean parameters.
func (d *Database) FailDownloadDisposition(
	chatID string, messageID int, generation string,
	fileName, savePath, mediaType string, fileSize int64,
	disp FailureDisposition,
) error {
	return d.FailDownload(chatID, messageID, generation, fileName, savePath, mediaType, fileSize, disp.Error(), disp.Unavailable)
}

// FailDownload records download failure or unavailability, strictly honoring attempt generation
// and never overwriting terminal 'success'.
func (d *Database) FailDownload(chatID string, messageID int, generation string, fileName, savePath, mediaType string, fileSize int64, errMsg string, isUnavailable bool) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	var currentStatus, currentGen string
	var currentAttempts int
	err := d.db.QueryRow(`
		SELECT status, COALESCE(attempt_generation, ''), attempts
		FROM download_records
		WHERE chat_id = ? AND message_id = ?
	`, chatID, messageID).Scan(&currentStatus, &currentGen, &currentAttempts)
	if err == sql.ErrNoRows {
		status := "failed"
		if isUnavailable {
			status = "unavailable"
		}
		_, err = d.db.Exec(`
			INSERT INTO download_records (
				chat_id, message_id, status, file_name, save_path, media_type,
				file_size, error, attempts, next_retry_at, attempt_generation, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
		`, chatID, messageID, status, fileName, savePath, mediaType, fileSize, errMsg, now+60, generation, now, now)
		return err
	} else if err != nil {
		return err
	}

	if currentStatus == "success" {
		return nil
	}
	if currentGen != "" && generation != "" && currentGen != generation {
		return ErrStaleAttempt
	}

	status := "failed"
	if isUnavailable {
		status = "unavailable"
	}

	lowerErr := strings.ToLower(errMsg)
	var nextRetryAt int64 = 0
	if status == "failed" {
		if strings.Contains(lowerErr, "context canceled") ||
			strings.Contains(lowerErr, "context deadline exceeded") ||
			lowerErr == "canceled" || lowerErr == "task canceled" ||
			strings.Contains(lowerErr, "engine forcibly closed") {
			status = "pending"
			errMsg = ""
		} else {
			backoff := int64(60 * (1 << currentAttempts))
			if backoff > 1800 {
				backoff = 1800
			}
			nextRetryAt = now + backoff
		}
	} else if status == "unavailable" {
		nextRetryAt = now + 86400*7
	}

	res, err := d.db.Exec(`
		UPDATE download_records
		SET status = ?,
			file_name = CASE WHEN ? != '' THEN ? ELSE file_name END,
			save_path = CASE WHEN ? != '' THEN ? ELSE save_path END,
			media_type = CASE WHEN ? != '' THEN ? ELSE media_type END,
			file_size = CASE WHEN ? > 0 THEN ? ELSE file_size END,
			error = ?,
			attempts = CASE WHEN ? = 'failed' THEN attempts + 1 ELSE attempts END,
			next_retry_at = ?,
			updated_at = ?
		WHERE chat_id = ? AND message_id = ?
		  AND status != 'success'
		  AND (attempt_generation = ? OR attempt_generation = '')
	`, status, fileName, fileName, savePath, savePath, mediaType, mediaType, fileSize, fileSize,
		errMsg, status, nextRetryAt, now, chatID, messageID, generation)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: zero rows affected in FailDownload", ErrStaleAttempt)
	}
	return nil
}

// CancelDownload transitions an active download to 'failed' due to cancellation,
// conditional on the matching attempt generation and current non-terminal state.
func (d *Database) CancelDownload(chatID string, messageID int, generation string, reason string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	if reason == "" {
		reason = "task canceled"
	}
	res, err := d.db.Exec(`
		UPDATE download_records
		SET status = 'failed',
			error = ?,
			updated_at = ?
		WHERE chat_id = ? AND message_id = ?
		  AND status IN ('pending', 'downloading', 'committing')
		  AND (attempt_generation = ? OR attempt_generation = '')
	`, reason, now, chatID, messageID, generation)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: zero rows affected in CancelDownload", ErrStateConflict)
	}
	return nil
}

// GetArchiveStats returns the aggregate archive queue status.
func (d *Database) GetArchiveStats() (ArchiveStats, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var s ArchiveStats
	err := d.db.QueryRow(`
		SELECT 
			COALESCE(SUM(CASE WHEN state = 'pending' OR state = 'copying' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'pending' OR state = 'copying' THEN expected_size ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'copying' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'archived' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'conflict' THEN 1 ELSE 0 END), 0)
		FROM archive_jobs
	`).Scan(&s.BacklogFiles, &s.BacklogBytes, &s.ActiveWorkers, &s.ArchivedFiles, &s.ConflictCount)
	return s, err
}

// GetPendingCommittingDownloads returns records in 'committing' status for restart recovery.
func (d *Database) GetPendingCommittingDownloads() ([]DownloadRecord, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	rows, err := d.db.Query(`
		SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''),
		       COALESCE(media_type, ''), COALESCE(file_size, 0), COALESCE(sha256, ''),
		       created_at, updated_at, attempts, next_retry_at, COALESCE(attempt_generation, '')
		FROM download_records
		WHERE status = 'committing'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []DownloadRecord
	for rows.Next() {
		var rec DownloadRecord
		if err := rows.Scan(
			&rec.ChatID, &rec.MessageID, &rec.Status, &rec.FileName, &rec.SavePath,
			&rec.MediaType, &rec.FileSize, &rec.SHA256,
			&rec.CreatedAt, &rec.UpdatedAt, &rec.Attempts, &rec.NextRetryAt, &rec.AttemptGeneration,
		); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// GetStaleDownloadingRecords returns records in 'downloading' status that were interrupted by crash.
func (d *Database) GetStaleDownloadingRecords() ([]DownloadRecord, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	rows, err := d.db.Query(`
		SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''),
		       COALESCE(media_type, ''), COALESCE(file_size, 0), COALESCE(sha256, ''),
		       created_at, updated_at, attempts, next_retry_at, COALESCE(attempt_generation, '')
		FROM download_records
		WHERE status = 'downloading'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []DownloadRecord
	for rows.Next() {
		var rec DownloadRecord
		if err := rows.Scan(
			&rec.ChatID, &rec.MessageID, &rec.Status, &rec.FileName, &rec.SavePath,
			&rec.MediaType, &rec.FileSize, &rec.SHA256,
			&rec.CreatedAt, &rec.UpdatedAt, &rec.Attempts, &rec.NextRetryAt, &rec.AttemptGeneration,
		); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// GetStaleCopyingArchiveJobs returns archive jobs in 'copying' status that were interrupted by crash.
func (d *Database) GetStaleCopyingArchiveJobs() ([]ArchiveJob, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	rows, err := d.db.Query(`
		SELECT chat_id, message_id, relative_path, expected_size, sha256,
		       state, attempts, next_retry_at, COALESCE(claim_id, ''), COALESCE(last_error, ''), created_at, updated_at
		FROM archive_jobs
		WHERE state = 'copying'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []ArchiveJob
	for rows.Next() {
		var j ArchiveJob
		if err := rows.Scan(
			&j.ChatID, &j.MessageID, &j.RelativePath, &j.ExpectedSize, &j.SHA256,
			&j.State, &j.Attempts, &j.NextRetryAt, &j.ClaimID, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// RecoverStaleArchiveJob resets an interrupted 'copying' archive job back to 'pending'.
// Strict guard: ONLY resets from 'copying' state, NEVER mutates 'archived' or 'conflict'.
func (d *Database) RecoverStaleArchiveJob(chatID string, messageID int, staleClaimID string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	res, err := d.db.Exec(`
		UPDATE archive_jobs
		SET state = 'pending', claim_id = '', updated_at = ?
		WHERE chat_id = ? AND message_id = ? AND state = 'copying'
		  AND (claim_id = ? OR claim_id = '')
	`, now, chatID, messageID, staleClaimID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var state string
		_ = d.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = ? AND message_id = ?`, chatID, messageID).Scan(&state)
		return fmt.Errorf("%w: cannot recover archive job from non-copying state %q", ErrStateConflict, state)
	}
	return nil
}

// RecoverArchiveJobComplete marks an interrupted 'copying' archive job as 'archived'
// if the final destination file already exists and matches expected SHA.
func (d *Database) RecoverArchiveJobComplete(chatID string, messageID int, staleClaimID string, expectedSHA string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	res, err := d.db.Exec(`
		UPDATE archive_jobs
		SET state = 'archived', last_error = '', claim_id = '', updated_at = ?
		WHERE chat_id = ? AND message_id = ? AND state = 'copying'
		  AND (claim_id = ? OR claim_id = '')
		  AND sha256 = ?
	`, now, chatID, messageID, staleClaimID, expectedSHA)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var state, currentSHA string
		_ = d.db.QueryRow(`SELECT state, sha256 FROM archive_jobs WHERE chat_id = ? AND message_id = ?`, chatID, messageID).Scan(&state, &currentSHA)
		if state == "archived" && currentSHA == expectedSHA {
			return nil
		}
		return fmt.Errorf("%w: cannot complete recovered archive job from state %q", ErrStateConflict, state)
	}
	return nil
}
