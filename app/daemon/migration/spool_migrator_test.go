package migration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/Hittlert/TGX/app/daemon"
	"github.com/Hittlert/TGX/app/daemon/migration"
	"github.com/Hittlert/TGX/cmd"
)

// 1. Acceptance Test: Migration dry-run reports every legacy row/artifact disposition without mutating DB.
func TestMigration_DryRunReportsAllDispositions(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "legacy.sqlite3")
	targetDir := filepath.Join(tempDir, "downloads")
	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(targetDir, 0o755)
	_ = os.MkdirAll(bufferDir, 0o755)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	// Setup legacy tables and data
	_, err = db.Exec(`
		CREATE TABLE target_commits (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			save_path TEXT,
			file_size INTEGER,
			sha256 TEXT,
			committed_at INTEGER,
			PRIMARY KEY (chat_id, message_id)
		);
		CREATE TABLE spool_attempts (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			state TEXT,
			updated_at INTEGER,
			PRIMARY KEY (chat_id, message_id)
		);
	`)
	if err != nil {
		t.Fatalf("failed to create legacy tables: %v", err)
	}

	// 1.1 Verified file on disk
	validContent := []byte("hello legacy world verified")
	validHash := sha256.Sum256(validContent)
	validHex := hex.EncodeToString(validHash[:])
	validRelPath := "Channel_1/valid.mp4"
	validDiskPath := filepath.Join(targetDir, filepath.FromSlash(validRelPath))
	_ = os.MkdirAll(filepath.Dir(validDiskPath), 0o755)
	if err := os.WriteFile(validDiskPath, validContent, 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, _ = db.Exec(`
		INSERT INTO target_commits (chat_id, message_id, save_path, file_size, sha256, committed_at)
		VALUES ('-1001', 1, ?, ?, ?, 1700000000)
	`, validRelPath, len(validContent), validHex)

	// 1.2 Missing file in target_commits
	_, _ = db.Exec(`
		INSERT INTO target_commits (chat_id, message_id, save_path, file_size, sha256, committed_at)
		VALUES ('-1001', 2, 'Channel_1/missing.mp4', 1024, 'dummy', 1700000000)
	`)

	// 1.3 Incomplete spool_attempt
	_, _ = db.Exec(`
		INSERT INTO spool_attempts (chat_id, message_id, state, updated_at)
		VALUES ('-1001', 3, 'spooling', 1700000000)
	`)
	db.Close()

	// 1.4 Orphaned buffer file
	_ = os.WriteFile(filepath.Join(bufferDir, "temp_chunk.spool"), []byte("orphan"), 0o644)

	// Run migration in DryRun mode
	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		TargetDir:        targetDir,
		BufferDir:        bufferDir,
		DryRun:           true,
		CreateBackup:     false,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("migration dry-run failed: %v", err)
	}

	if !report.DryRun {
		t.Fatal("expected report.DryRun == true")
	}
	if report.TotalLegacyRows != 3 {
		t.Fatalf("expected 3 legacy rows, got %d", report.TotalLegacyRows)
	}
	if report.ImportedSuccess != 1 {
		t.Fatalf("expected 1 imported success, got %d", report.ImportedSuccess)
	}
	if report.ResetPending != 2 {
		t.Fatalf("expected 2 reset pending, got %d", report.ResetPending)
	}
	if len(report.PlannedCleanFiles) != 1 {
		t.Fatalf("expected 1 planned clean file, got %d", len(report.PlannedCleanFiles))
	}
	if len(report.CleanedFiles) != 0 {
		t.Fatalf("expected 0 cleaned buffer files in dry run, got %d", len(report.CleanedFiles))
	}

	// Verify DB is UNTOUCHED
	checkDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to re-open sqlite: %v", err)
	}
	defer checkDB.Close()

	var tblCount int
	_ = checkDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('target_commits', 'spool_attempts')").Scan(&tblCount)
	if tblCount != 2 {
		t.Fatalf("expected 2 legacy tables still existing in dry run, got %d", tblCount)
	}
}

// 2. Acceptance Test: Verified historical final files are imported to success and not downloaded again.
func TestMigration_VerifiedFinalFilesNotRedownloaded(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "legacy.sqlite3")
	targetDir := filepath.Join(tempDir, "downloads")
	_ = os.MkdirAll(targetDir, 0o755)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	_, _ = db.Exec(`
		CREATE TABLE target_commits (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			save_path TEXT,
			file_size INTEGER,
			sha256 TEXT,
			committed_at INTEGER,
			PRIMARY KEY (chat_id, message_id)
		);
	`)

	validContent := []byte("authoritative verified data payload")
	validHash := sha256.Sum256(validContent)
	validHex := hex.EncodeToString(validHash[:])
	relPath := "ArchiveChannel/2024_01/100 - clip.mp4"
	diskPath := filepath.Join(targetDir, filepath.FromSlash(relPath))
	_ = os.MkdirAll(filepath.Dir(diskPath), 0o755)
	_ = os.WriteFile(diskPath, validContent, 0o644)

	_, _ = db.Exec(`
		INSERT INTO target_commits (chat_id, message_id, save_path, file_size, sha256, committed_at)
		VALUES ('-100888', 100, ?, ?, ?, 1700000000)
	`, relPath, len(validContent), validHex)
	db.Close()

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		TargetDir:        targetDir,
		DryRun:           false,
		CreateBackup:     true,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if report.ImportedSuccess != 1 {
		t.Fatalf("expected 1 imported success, got %d", report.ImportedSuccess)
	}
	if report.BackupPath == "" {
		t.Fatal("expected backup path to be created and verified")
	}

	// Verify download_records has the record in 'success' state with exact SHA and size
	daemonDB, err := daemon.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to open daemon DB: %v", err)
	}
	defer daemonDB.Close()

	rec, err := daemonDB.GetDownloadRecord("-100888", 100)
	if err != nil || rec == nil {
		t.Fatalf("failed to get migrated download record: %v", err)
	}
	if rec.Status != "success" {
		t.Fatalf("expected status 'success', got: %s", rec.Status)
	}
	if rec.SavePath != relPath {
		t.Fatalf("expected save path %s, got %s", relPath, rec.SavePath)
	}
	if rec.FileSize != int64(len(validContent)) {
		t.Fatalf("expected size %d, got %d", len(validContent), rec.FileSize)
	}
	if rec.SHA256 != validHex {
		t.Fatalf("expected sha256 %s, got %s", validHex, rec.SHA256)
	}
}

// 3. Acceptance Test: Incomplete legacy moving/segment state becomes explicit pending/quarantine.
func TestMigration_IncompleteWorkBecomesPendingOrQuarantine(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "legacy.sqlite3")
	targetDir := filepath.Join(tempDir, "downloads")
	_ = os.MkdirAll(targetDir, 0o755)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	_, _ = db.Exec(`
		CREATE TABLE spool_attempts (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			state TEXT,
			updated_at INTEGER,
			PRIMARY KEY (chat_id, message_id)
		);
		CREATE TABLE download_records (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			file_name TEXT,
			save_path TEXT,
			file_size INTEGER,
			sha256 TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_retry_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (chat_id, message_id)
		);
		INSERT INTO spool_attempts (chat_id, message_id, state, updated_at)
		VALUES ('-100999', 201, 'segmenting', 1700000000);
		INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size, created_at, updated_at)
		VALUES ('-100999', 202, 'moving', 'corrupt.mp4', 'Channel/corrupt.mp4', 5000, 1700000000, 1700000000);
	`)
	db.Close()

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		TargetDir:        targetDir,
		DryRun:           false,
		CreateBackup:     true,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if report.ResetPending != 2 {
		t.Fatalf("expected 2 records reset to pending, got %d", report.ResetPending)
	}
	if report.ImportedSuccess != 0 {
		t.Fatalf("expected 0 imported success, got %d", report.ImportedSuccess)
	}

	daemonDB, err := daemon.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to open daemon DB: %v", err)
	}
	defer daemonDB.Close()

	// Both records must be pending and never success
	for _, msgID := range []int{201, 202} {
		rec, err := daemonDB.GetDownloadRecord("-100999", msgID)
		if err != nil || rec == nil {
			t.Fatalf("failed to get record %d: %v", msgID, err)
		}
		if rec.Status != "pending" {
			t.Fatalf("expected record %d to be 'pending', got: %s", msgID, rec.Status)
		}
	}
}

// 4. Acceptance Test: A fresh database contains no retired Spool tables.
func TestMigration_FreshDatabaseHasNoSpoolTables(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "fresh.sqlite3")

	db, err := daemon.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to init fresh database: %v", err)
	}
	defer db.Close()

	sqlDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open fresh sqlite: %v", err)
	}
	defer sqlDB.Close()

	var spoolTableCount int
	err = sqlDB.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='table' AND (name LIKE 'spool_%' OR name = 'target_commits')
	`).Scan(&spoolTableCount)
	if err != nil {
		t.Fatalf("failed to query tables: %v", err)
	}
	if spoolTableCount != 0 {
		t.Fatalf("expected 0 retired spool tables in fresh database, got %d", spoolTableCount)
	}
}

// 5. Acceptance Test: CLI help contains no buffer/temp compatibility flags for daemon.
func TestMigration_CLIHelpHasNoBufferFlags(t *testing.T) {
	daemonCmd := cmd.NewDaemon()
	flags := daemonCmd.Flags()

	forbiddenFlags := []string{
		"buffer-dir",
		"buffer-type",
		"buffer-size",
		"temp-dir",
		"legacy-spool",
	}

	for _, f := range forbiddenFlags {
		if flags.Lookup(f) != nil {
			t.Fatalf("forbidden compatibility flag %q still present in daemon CLI flags", f)
		}
	}
}

// 6. Acceptance Test: Source deletion gate finds no retired runtime symbols outside migration.
func TestMigration_SourceDeletionGate(t *testing.T) {
	retiredSymbols := []string{
		"GlobalSlotPool",
		"AsyncMoving",
		"spool_attempts",
		"spool_segments",
	}

	walkRoots := []string{"../../daemon", "../../../core"}
	for _, root := range walkRoots {
		absRoot, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		_ = filepath.Walk(absRoot, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			// Only check production Go source files (skip tests and migration package itself)
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.Contains(path, "migration") {
				return nil
			}

			content, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			contentStr := string(content)
			for _, sym := range retiredSymbols {
				if strings.Contains(contentStr, sym) {
					t.Fatalf("retired runtime symbol %q found in production source: %s", sym, path)
				}
			}
			return nil
		})
	}
}

// 7. Acceptance Test: Malformed legacy schema aborts without dropping evidence.
func TestMigration_MalformedLegacySchemaAbortsWithoutDroppingEvidence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "malformed.sqlite3")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	// Incompatible target_commits schema missing chat_id / message_id
	_, err = db.Exec(`CREATE TABLE target_commits (bad_col TEXT NOT NULL);`)
	db.Close()
	if err != nil {
		t.Fatalf("failed to create malformed table: %v", err)
	}

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error due to malformed schema, got nil")
	}
	_ = report

	// Verify evidence was NOT dropped: target_commits must still exist
	verifyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite for verification: %v", err)
	}
	defer verifyDB.Close()

	var tblCount int
	err = verifyDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='target_commits'").Scan(&tblCount)
	if err != nil || tblCount != 1 {
		t.Fatalf("legacy evidence was dropped! target_commits count: %d, err: %v", tblCount, err)
	}
}

// 8. Acceptance Test: Row scan failure aborts without dropping evidence.
func TestMigration_RowScanFailureAbortsWithoutDroppingEvidence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scan_fail.sqlite3")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	// Normal schema but SQLite dynamic typing allows string in integer column
	_, err = db.Exec(`
		CREATE TABLE spool_attempts (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			state TEXT,
			updated_at INTEGER,
			PRIMARY KEY (chat_id, message_id)
		);
		INSERT INTO spool_attempts (chat_id, message_id, state, updated_at)
		VALUES ('-1001', 'corrupt_non_int_message_id', 'spooling', 1700000000);
	`)
	db.Close()
	if err != nil {
		t.Fatalf("failed to setup scan fail table: %v", err)
	}

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error due to row scan failure, got nil")
	}
	_ = report

	// Verify evidence was NOT dropped: spool_attempts must still exist
	verifyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite for verification: %v", err)
	}
	defer verifyDB.Close()

	var tblCount int
	err = verifyDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='spool_attempts'").Scan(&tblCount)
	if err != nil || tblCount != 1 {
		t.Fatalf("legacy evidence was dropped! spool_attempts count: %d, err: %v", tblCount, err)
	}
}

// 9. Acceptance Test: Traversal failure in BufferDir aborts without dropping evidence.
func TestMigration_TraversalFailureAbortsWithoutDroppingEvidence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "traversal_fail.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	unreadableDir := filepath.Join(bufferDir, "unreadable_sub")
	if err := os.MkdirAll(unreadableDir, 0o000); err != nil {
		t.Fatalf("failed to create unreadable dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(unreadableDir, 0o755)
	})

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE target_commits (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			save_path TEXT,
			file_size INTEGER,
			sha256 TEXT,
			committed_at INTEGER,
			PRIMARY KEY (chat_id, message_id)
		);
	`)
	db.Close()
	if err != nil {
		t.Fatalf("failed to setup legacy table: %v", err)
	}

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		BufferDir:        bufferDir,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error due to traversal failure, got nil")
	}
	_ = report

	// Verify evidence was NOT dropped: target_commits must still exist
	verifyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite for verification: %v", err)
	}
	defer verifyDB.Close()

	var tblCount int
	err = verifyDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='target_commits'").Scan(&tblCount)
	if err != nil || tblCount != 1 {
		t.Fatalf("legacy evidence was dropped! target_commits count: %d, err: %v", tblCount, err)
	}
}

// 10. Acceptance Test: Removal failure of buffer file aborts transaction without dropping evidence.
func TestMigration_RemovalFailureAbortsWithoutDroppingEvidence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "removal_fail.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	roSubDir := filepath.Join(bufferDir, "ro_sub")
	if err := os.MkdirAll(roSubDir, 0o755); err != nil {
		t.Fatalf("failed to create ro subdir: %v", err)
	}

	spoolFile := filepath.Join(roSubDir, "test.spool")
	if err := os.WriteFile(spoolFile, []byte("unremovable spool data"), 0o644); err != nil {
		t.Fatalf("failed to write spool file: %v", err)
	}

	// Make parent directory read-only so removing files inside it fails with permission denied
	if err := os.Chmod(roSubDir, 0o555); err != nil {
		t.Fatalf("failed to chmod ro subdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(roSubDir, 0o755)
	})

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE target_commits (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			save_path TEXT,
			file_size INTEGER,
			sha256 TEXT,
			committed_at INTEGER,
			PRIMARY KEY (chat_id, message_id)
		);
	`)
	db.Close()
	if err != nil {
		t.Fatalf("failed to setup legacy table: %v", err)
	}

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		BufferDir:        bufferDir,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error due to buffer removal failure, got nil")
	}
	_ = report

	// Verify evidence was NOT dropped: target_commits must still exist in sqlite
	verifyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite for verification: %v", err)
	}
	defer verifyDB.Close()

	var tblCount int
	err = verifyDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='target_commits'").Scan(&tblCount)
	if err != nil || tblCount != 1 {
		t.Fatalf("legacy evidence was dropped! target_commits count: %d, err: %v", tblCount, err)
	}
}
