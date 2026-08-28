package daemon

import (
	"database/sql"
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
			error TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			downloaded_at INTEGER,
			processing_started_at INTEGER,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_retry_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (chat_id, message_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_download_records_retry ON download_records(chat_id, status, next_retry_at, message_id)`,
		`CREATE INDEX IF NOT EXISTS idx_download_records_downloaded ON download_records(status, downloaded_at DESC, updated_at DESC)`,
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

	// Clean any dangling 'downloading' records from crashes back to 'pending'
	_, _ = d.db.Exec(`UPDATE download_records SET status = 'pending' WHERE status = 'downloading'`)
	return nil
}

// Helper wrapper
func (d *Database) Execute(query string, args ...any) (sql.Result, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.db.Exec(query, args...)
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

	_, err := d.db.Exec(`
		INSERT INTO chat_messages(chat_id, message_id, sender_id, sender_name, text, media_type, has_media, reply_to_message_id, date, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(chat_id, message_id) DO UPDATE SET
			text = excluded.text,
			media_type = excluded.media_type,
			has_media = excluded.has_media,
			updated_at = excluded.updated_at
	`, msg.ChatID, msg.MessageID, msg.SenderID, msg.SenderName, msg.Text, msg.MediaType, hasMediaInt, msg.ReplyToMessageID, msg.Date, now, now)
	if err != nil {
		return err
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

		_, err = d.db.Exec(`
			INSERT INTO download_records(chat_id, message_id, status, file_name, media_type, file_size, created_at, updated_at)
			VALUES(?, ?, 'pending', ?, ?, ?, ?, ?)
			ON CONFLICT(chat_id, message_id) DO UPDATE SET
				file_name = CASE WHEN download_records.file_name IS NULL OR download_records.file_name LIKE '%.bin' THEN excluded.file_name ELSE download_records.file_name END,
				media_type = excluded.media_type,
				file_size = excluded.file_size
		`, msg.ChatID, msg.MessageID, fileName, msg.MediaType, msg.FileSize, now, now)
	}

	return err
}

func (d *Database) GetPendingDownloads(limit int) ([]DownloadRecord, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	now := time.Now().Unix()
	rows, err := d.db.Query(`
		SELECT r.chat_id, r.message_id, r.status, COALESCE(r.file_name, ''), COALESCE(r.save_path, ''), 
		       COALESCE(r.media_type, ''), COALESCE(r.file_size, 0), COALESCE(r.error, ''), 
		       r.created_at, r.updated_at, r.attempts, r.next_retry_at, COALESCE(t.title, '')
		FROM download_records r
		INNER JOIN listen_targets t ON r.chat_id = t.chat_id
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
			&rec.Attempts, &rec.NextRetryAt, &rec.TargetTitle,
		); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func (d *Database) UpdateDownloadStatus(chatID string, messageID int, status string, fileName, savePath, mediaType string, fileSize int64, errMsg string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	now := time.Now().Unix()
	var downloadedAt *int64
	if status == "success" {
		downloadedAt = &now
	}

	_, err := d.db.Exec(`
		UPDATE download_records
		SET status = ?, file_name = COALESCE(NULLIF(?, ''), file_name), 
		    save_path = COALESCE(NULLIF(?, ''), save_path),
		    media_type = COALESCE(NULLIF(?, ''), media_type),
		    file_size = CASE WHEN ? > 0 THEN ? ELSE file_size END,
		    error = ?, updated_at = ?, downloaded_at = COALESCE(?, downloaded_at)
		WHERE chat_id = ? AND message_id = ?
	`, status, fileName, savePath, mediaType, fileSize, fileSize, errMsg, now, downloadedAt, chatID, messageID)
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
