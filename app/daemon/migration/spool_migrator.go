package migration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// LegacyDisposition describes the disposition action determined for a legacy record or artifact.
type LegacyDisposition string

const (
	DispositionImportSuccess   LegacyDisposition = "IMPORT_SUCCESS"
	DispositionResetPending    LegacyDisposition = "RESET_PENDING"
	DispositionQuarantine      LegacyDisposition = "QUARANTINE"
	DispositionAlreadyMigrated LegacyDisposition = "ALREADY_MIGRATED"
	DispositionTableDrop       LegacyDisposition = "DROP_TABLE"
	DispositionBufferClean     LegacyDisposition = "BUFFER_CLEAN"
)

// MigrationOptions specifies parameters for the offline Spool migration tool.
type MigrationOptions struct {
	DBPath           string
	TargetDir        string
	ArchiveDir       string
	BufferDir        string
	DryRun           bool
	CreateBackup     bool
	DropLegacyTables bool
}

// ItemReport details the disposition decision for an individual legacy row or artifact.
type ItemReport struct {
	SourceTable string            `json:"source_table"`
	ChatID      string            `json:"chat_id,omitempty"`
	MessageID   int               `json:"message_id,omitempty"`
	FileName    string            `json:"file_name,omitempty"`
	SavePath    string            `json:"save_path,omitempty"`
	Size        int64             `json:"size,omitempty"`
	SHA256      string            `json:"sha256,omitempty"`
	LegacyState string            `json:"legacy_state,omitempty"`
	Disposition LegacyDisposition `json:"disposition"`
	Reason      string            `json:"reason"`
}

// MigrationReport summarizes the entire migration inventory and applied actions.
type MigrationReport struct {
	DBPath            string       `json:"db_path"`
	BackupPath        string       `json:"backup_path,omitempty"`
	DryRun            bool         `json:"dry_run"`
	LegacyTablesFound []string     `json:"legacy_tables_found"`
	TotalLegacyRows   int          `json:"total_legacy_rows"`
	ImportedSuccess   int          `json:"imported_success"`
	ResetPending      int          `json:"reset_pending"`
	Quarantined       int          `json:"quarantined"`
	AlreadyMigrated   int          `json:"already_migrated"`
	DroppedTables     []string     `json:"dropped_tables,omitempty"`
	PlannedCleanFiles []string     `json:"planned_clean_files,omitempty"`
	CleanedFiles      []string     `json:"cleaned_files,omitempty"`
	Items             []ItemReport `json:"items"`
}

// Run executes the migration evaluation and optional mutation.
func Run(ctx context.Context, opts MigrationOptions) (*MigrationReport, error) {
	if opts.DBPath == "" {
		return nil, fmt.Errorf("db-path must be specified")
	}

	report := &MigrationReport{
		DBPath:            opts.DBPath,
		DryRun:            opts.DryRun,
		PlannedCleanFiles: make([]string, 0),
		CleanedFiles:      make([]string, 0),
		Items:             make([]ItemReport, 0),
	}

	// 1. Check SQLite database existence
	if _, err := os.Stat(opts.DBPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", opts.DBPath)
	}

	db, err := sql.Open("sqlite", opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	defer db.Close()

	// 2. Discover legacy tables
	legacyTableCandidates := []string{"target_commits", "spool_attempts", "spool_segments", "spool_cleanup"}
	for _, tbl := range legacyTableCandidates {
		var exists int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", tbl).Scan(&exists)
		if err != nil {
			return report, fmt.Errorf("check legacy table %s existence: %w", tbl, err)
		}
		if exists > 0 {
			report.LegacyTablesFound = append(report.LegacyTablesFound, tbl)
		}
	}

	// Check if download_records exists
	var drExists int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='download_records'").Scan(&drExists)
	if err != nil {
		return report, fmt.Errorf("check download_records existence: %w", err)
	}

	// 3. Inventory target_commits if present
	type commitEntry struct {
		chatID      string
		messageID   int
		savePath    string
		fileSize    int64
		sha256Hex   string
		committedAt int64
	}
	var commits []commitEntry
	if contains(report.LegacyTablesFound, "target_commits") {
		rows, err := db.QueryContext(ctx, "SELECT chat_id, message_id, COALESCE(save_path, ''), COALESCE(file_size, 0), COALESCE(sha256, ''), COALESCE(committed_at, 0) FROM target_commits")
		if err != nil {
			return report, fmt.Errorf("query target_commits: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var c commitEntry
			if err := rows.Scan(&c.chatID, &c.messageID, &c.savePath, &c.fileSize, &c.sha256Hex, &c.committedAt); err != nil {
				return report, fmt.Errorf("scan target_commits row: %w", err)
			}
			commits = append(commits, c)
		}
		if err := rows.Err(); err != nil {
			return report, fmt.Errorf("iterate target_commits: %w", err)
		}
	}

	// 4. Inventory spool_attempts if present
	type attemptEntry struct {
		chatID    string
		messageID int
		state     string
		updatedAt int64
	}
	var attempts []attemptEntry
	if contains(report.LegacyTablesFound, "spool_attempts") {
		rows, err := db.QueryContext(ctx, "SELECT chat_id, message_id, COALESCE(state, ''), COALESCE(updated_at, 0) FROM spool_attempts")
		if err != nil {
			return report, fmt.Errorf("query spool_attempts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var a attemptEntry
			if err := rows.Scan(&a.chatID, &a.messageID, &a.state, &a.updatedAt); err != nil {
				return report, fmt.Errorf("scan spool_attempts row: %w", err)
			}
			attempts = append(attempts, a)
		}
		if err := rows.Err(); err != nil {
			return report, fmt.Errorf("iterate spool_attempts: %w", err)
		}
	}

	// 5. Inventory legacy rows in download_records (e.g. status in 'moving', 'segmenting', 'spooling', 'transferring')
	type legacyRecordEntry struct {
		chatID    string
		messageID int
		status    string
		fileName  string
		savePath  string
		fileSize  int64
		sha256Hex string
		createdAt int64
		updatedAt int64
	}
	var legacyDR []legacyRecordEntry
	if drExists > 0 {
		rows, err := db.QueryContext(ctx, "SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''), COALESCE(file_size, 0), COALESCE(sha256, ''), created_at, updated_at FROM download_records WHERE status IN ('moving', 'segmenting', 'spooling', 'transferring')")
		if err != nil {
			return report, fmt.Errorf("query legacy download_records: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var r legacyRecordEntry
			if err := rows.Scan(&r.chatID, &r.messageID, &r.status, &r.fileName, &r.savePath, &r.fileSize, &r.sha256Hex, &r.createdAt, &r.updatedAt); err != nil {
				return report, fmt.Errorf("scan legacy download_records row: %w", err)
			}
			legacyDR = append(legacyDR, r)
		}
		if err := rows.Err(); err != nil {
			return report, fmt.Errorf("iterate legacy download_records: %w", err)
		}
	}

	report.TotalLegacyRows = len(commits) + len(attempts) + len(legacyDR)

	// 6. Process target_commits
	committedKeys := make(map[string]bool)
	for _, c := range commits {
		key := fmt.Sprintf("%s:%d", c.chatID, c.messageID)
		committedKeys[key] = true

		item := ItemReport{
			SourceTable: "target_commits",
			ChatID:      c.chatID,
			MessageID:   c.messageID,
			SavePath:    c.savePath,
			Size:        c.fileSize,
			SHA256:      c.sha256Hex,
			LegacyState: "committed",
		}

		// Verify on disk in targetDir or archiveDir
		diskPath, exists, valid, reason := verifyDiskFile(c.savePath, c.fileSize, c.sha256Hex, opts.TargetDir, opts.ArchiveDir)
		if exists && valid {
			item.Disposition = DispositionImportSuccess
			item.Reason = fmt.Sprintf("Verified authoritative commit on disk (%s): %s", diskPath, reason)
			report.ImportedSuccess++
		} else {
			// Incomplete or corrupted -> explicit reset to pending without guessing success
			item.Disposition = DispositionResetPending
			item.Reason = fmt.Sprintf("Authoritative file missing or invalid on disk: %s", reason)
			report.ResetPending++
		}
		report.Items = append(report.Items, item)
	}

	// 7. Process spool_attempts (skip if already handled by commit)
	for _, a := range attempts {
		key := fmt.Sprintf("%s:%d", a.chatID, a.messageID)
		if committedKeys[key] {
			continue // already handled by target_commits
		}

		item := ItemReport{
			SourceTable: "spool_attempts",
			ChatID:      a.chatID,
			MessageID:   a.messageID,
			LegacyState: a.state,
		}

		// Unfinished spool work must NEVER be guessed as success!
		item.Disposition = DispositionResetPending
		item.Reason = fmt.Sprintf("Legacy in-flight spool attempt (state=%s) reset to pending", a.state)
		report.ResetPending++
		report.Items = append(report.Items, item)
	}

	// 8. Process legacy download_records
	for _, r := range legacyDR {
		key := fmt.Sprintf("%s:%d", r.chatID, r.messageID)
		if committedKeys[key] {
			continue
		}

		item := ItemReport{
			SourceTable: "download_records",
			ChatID:      r.chatID,
			MessageID:   r.messageID,
			FileName:    r.fileName,
			SavePath:    r.savePath,
			Size:        r.fileSize,
			SHA256:      r.sha256Hex,
			LegacyState: r.status,
		}

		diskPath, exists, valid, reason := verifyDiskFile(r.savePath, r.fileSize, r.sha256Hex, opts.TargetDir, opts.ArchiveDir)
		if exists && valid && r.fileSize > 0 {
			item.Disposition = DispositionImportSuccess
			item.Reason = fmt.Sprintf("Verified legacy file on disk (%s): %s", diskPath, reason)
			report.ImportedSuccess++
		} else {
			item.Disposition = DispositionResetPending
			item.Reason = fmt.Sprintf("Incomplete legacy download record (status=%s) reset to pending: %s", r.status, reason)
			report.ResetPending++
		}
		report.Items = append(report.Items, item)
	}

	// 9. Inspect BufferDir for orphaned spool/segment artifacts
	if opts.BufferDir != "" {
		if fi, err := os.Stat(opts.BufferDir); err != nil {
			if !os.IsNotExist(err) {
				return report, fmt.Errorf("stat buffer directory: %w", err)
			}
		} else if fi.IsDir() {
			err := filepath.Walk(opts.BufferDir, func(p string, info os.FileInfo, err error) error {
				if err != nil {
					return fmt.Errorf("walk buffer path %s: %w", p, err)
				}
				if info.IsDir() {
					return nil
				}
				name := info.Name()
				if strings.HasSuffix(name, ".spool") || strings.HasSuffix(name, ".part") || strings.Contains(name, "segment") {
					report.PlannedCleanFiles = append(report.PlannedCleanFiles, p)
					report.Items = append(report.Items, ItemReport{
						SourceTable: "buffer_filesystem",
						SavePath:    p,
						Size:        info.Size(),
						Disposition: DispositionBufferClean,
						Reason:      "Orphaned legacy spool/segment file",
					})
				}
				return nil
			})
			if err != nil {
				return report, fmt.Errorf("inspect buffer directory: %w", err)
			}
		}
	}

	// If dry-run, stop before any mutation
	if opts.DryRun {
		return report, nil
	}

	// 10. Create Verified Backup if requested or if legacy items exist
	if opts.CreateBackup && report.TotalLegacyRows > 0 {
		backupPath, err := createVerifiedBackup(opts.DBPath)
		if err != nil {
			return report, fmt.Errorf("create verified backup: %w", err)
		}
		report.BackupPath = backupPath
	}

	// 11. Apply Changes in Transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return report, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()

	// Ensure download_records schema exists
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS download_records (
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
		)
	`)
	if err != nil {
		return report, fmt.Errorf("ensure download_records: %w", err)
	}

	// Ensure legacy download_records has all necessary columns if it already existed
	_, _ = tx.ExecContext(ctx, "ALTER TABLE download_records ADD COLUMN error TEXT")
	_, _ = tx.ExecContext(ctx, "ALTER TABLE download_records ADD COLUMN media_type TEXT")
	_, _ = tx.ExecContext(ctx, "ALTER TABLE download_records ADD COLUMN attempt_generation TEXT NOT NULL DEFAULT ''")
	_, _ = tx.ExecContext(ctx, "ALTER TABLE download_records ADD COLUMN downloaded_at INTEGER")
	_, _ = tx.ExecContext(ctx, "ALTER TABLE download_records ADD COLUMN processing_started_at INTEGER")

	for _, item := range report.Items {
		switch item.Disposition {
		case DispositionImportSuccess:
			_, err = tx.ExecContext(ctx, `
				INSERT INTO download_records (
					chat_id, message_id, status, file_name, save_path, media_type, file_size, sha256,
					created_at, updated_at, downloaded_at, attempts, next_retry_at
				) VALUES (?, ?, 'success', ?, ?, '', ?, ?, ?, ?, ?, 0, 0)
				ON CONFLICT(chat_id, message_id) DO UPDATE SET
					status = 'success',
					save_path = excluded.save_path,
					file_size = excluded.file_size,
					sha256 = excluded.sha256,
					updated_at = excluded.updated_at
				WHERE download_records.status != 'archived'
			`, item.ChatID, item.MessageID, filepath.Base(item.SavePath), item.SavePath, item.Size, item.SHA256, now, now, now)
			if err != nil {
				return report, fmt.Errorf("import success record (%s:%d): %w", item.ChatID, item.MessageID, err)
			}

		case DispositionResetPending:
			_, err = tx.ExecContext(ctx, `
				INSERT INTO download_records (
					chat_id, message_id, status, file_name, save_path, media_type, file_size, error,
					created_at, updated_at, attempts, next_retry_at
				) VALUES (?, ?, 'pending', ?, ?, '', ?, 'legacy_incomplete_reset', ?, ?, 0, 0)
				ON CONFLICT(chat_id, message_id) DO UPDATE SET
					status = CASE WHEN download_records.status IN ('success', 'archived') THEN download_records.status ELSE 'pending' END,
					error = 'legacy_incomplete_reset',
					updated_at = excluded.updated_at
				WHERE download_records.status NOT IN ('success', 'archived')
			`, item.ChatID, item.MessageID, filepath.Base(item.SavePath), item.SavePath, item.Size, now, now)
			if err != nil {
				return report, fmt.Errorf("reset pending record (%s:%d): %w", item.ChatID, item.MessageID, err)
			}
		}
	}

	// 12. Clean orphaned buffer files on disk BEFORE dropping legacy tables and committing
	for _, p := range report.PlannedCleanFiles {
		if err := os.Remove(p); err != nil {
			return report, fmt.Errorf("clean buffer file %s: %w", p, err)
		}
		report.CleanedFiles = append(report.CleanedFiles, p)
	}

	// 13. Drop legacy tables if requested
	if opts.DropLegacyTables && len(report.LegacyTablesFound) > 0 {
		for _, tbl := range report.LegacyTablesFound {
			_, err = tx.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
			if err != nil {
				return report, fmt.Errorf("drop legacy table %s: %w", tbl, err)
			}
			report.DroppedTables = append(report.DroppedTables, tbl)
		}
	}

	if err := tx.Commit(); err != nil {
		return report, fmt.Errorf("commit migration transaction: %w", err)
	}

	return report, nil
}

func verifyDiskFile(relPath string, expectedSize int64, expectedSHA string, targetDir string, archiveDir string) (diskPath string, exists bool, valid bool, reason string) {
	if relPath == "" {
		return "", false, false, "empty relative path"
	}

	candidates := []string{}
	if targetDir != "" {
		candidates = append(candidates, filepath.Join(targetDir, filepath.FromSlash(relPath)))
	}
	if archiveDir != "" {
		candidates = append(candidates, filepath.Join(archiveDir, filepath.FromSlash(relPath)))
	}

	for _, cand := range candidates {
		fi, err := os.Stat(cand)
		if err == nil && !fi.IsDir() {
			if expectedSize > 0 && fi.Size() != expectedSize {
				return cand, true, false, fmt.Sprintf("size mismatch: disk %d != expected %d", fi.Size(), expectedSize)
			}
			if expectedSHA != "" {
				actualSHA, err := computeFileSHA256(cand)
				if err != nil {
					return cand, true, false, fmt.Sprintf("sha compute failed: %v", err)
				}
				if !strings.EqualFold(actualSHA, expectedSHA) {
					return cand, true, false, fmt.Sprintf("sha mismatch: disk %s != expected %s", actualSHA, expectedSHA)
				}
			}
			return cand, true, true, "exact size and hash verified"
		}
	}

	return "", false, false, "file not found in target or archive directory"
}

func computeFileSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func createVerifiedBackup(dbPath string) (string, error) {
	timestamp := time.Now().Format("20060102_150405")
	backupPath := fmt.Sprintf("%s.migrate.%s.bak", dbPath, timestamp)

	src, err := os.Open(dbPath)
	if err != nil {
		return "", fmt.Errorf("open src db: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(backupPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", fmt.Errorf("create dst backup: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return "", fmt.Errorf("copy db: %w", err)
	}
	_ = dst.Sync()

	// Verify backup with integrity_check
	backupDB, err := sql.Open("sqlite", backupPath)
	if err != nil {
		return "", fmt.Errorf("open backup for verification: %w", err)
	}
	defer backupDB.Close()

	var integrity string
	if err := backupDB.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		return "", fmt.Errorf("backup integrity check failed: %s (err: %v)", integrity, err)
	}

	return backupPath, nil
}

func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
