package migration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// 11. Acceptance Test: Database DROP/commit failure after staging restores buffer files and preserves evidence.
func TestMigration_DropOrCommitFailureAfterStagingPreservesFilesystemEvidence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "drop_fail.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	spoolFile := filepath.Join(bufferDir, "critical_evidence.spool")
	evidenceContent := []byte("irreplaceable buffer chunk evidence")
	if err := os.WriteFile(spoolFile, evidenceContent, 0o644); err != nil {
		t.Fatalf("failed to write spool file: %v", err)
	}

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
		INSERT INTO target_commits VALUES ('-10099', 1, 'vid.mp4', 100, 'hash', 1000);
	`)
	if err != nil {
		t.Fatalf("failed to setup legacy table: %v", err)
	}

	// Lock table target_commits by holding an active read query in an explicit transaction
	lockingDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open locking db: %v", err)
	}
	defer lockingDB.Close()

	lockTx, err := lockingDB.Begin()
	if err != nil {
		t.Fatalf("failed to begin lock transaction: %v", err)
	}
	rows, err := lockTx.Query("SELECT * FROM target_commits")
	if err != nil {
		t.Fatalf("failed to query for lock: %v", err)
	}
	// Do not close rows or commit lockTx until migration run finishes

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		BufferDir:        bufferDir,
		DropLegacyTables: true,
	}

	// Running migration should stage files, but DROP TABLE target_commits will fail due to table lock!
	report, runErr := migration.Run(context.Background(), opts)
	_ = report

	// Release lock now so we can verify DB
	_ = rows.Close()
	_ = lockTx.Rollback()
	db.Close()

	if runErr == nil {
		t.Fatal("expected migration to fail when DROP TABLE is locked, got nil")
	}

	// Assert cross-resource failure atomicity:
	// 1. Filesystem: The critical buffer file MUST still exist at its original path with matching content!
	content, err := os.ReadFile(spoolFile)
	if err != nil {
		t.Fatalf("critical filesystem evidence was destroyed or missing after DB failure: %v", err)
	}
	if string(content) != string(evidenceContent) {
		t.Fatalf("filesystem evidence corrupted! expected %q, got %q", string(evidenceContent), string(content))
	}

	// 2. Database: The legacy table MUST still exist!
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

// 12. Acceptance Test: Crash before database commit restores all quarantined files from manifest across restart.
func TestMigration_QuarantineManifest_CrashBeforeCommitRestoresFilesOnRestart(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "crash_before.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	// Create SQLite DB without any verdict
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INTEGER)")
	db.Close()

	// Simulate crash state: quarantined files staged in directory with manifest, but DB never committed
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_1234567")
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		t.Fatalf("failed to create qDir: %v", err)
	}

	origFile1 := filepath.Join(bufferDir, "in_flight_1.spool")
	stagedFile1 := filepath.Join(qDir, "staged_0_in_flight_1.spool")
	content1 := []byte("valuable chunk data 1")
	if err := os.WriteFile(stagedFile1, content1, 0o644); err != nil {
		t.Fatalf("failed to write staged file: %v", err)
	}

	origFile2 := filepath.Join(bufferDir, "subdir", "in_flight_2.part")
	stagedFile2 := filepath.Join(qDir, "staged_1_in_flight_2.part")
	content2 := []byte("valuable chunk data 2")
	if err := os.WriteFile(stagedFile2, content2, 0o644); err != nil {
		t.Fatalf("failed to write staged file: %v", err)
	}

	h1 := sha256.Sum256(content1)
	h2 := sha256.Sum256(content2)
	manifest := migration.QuarantineManifest{
		MigrationID:   "mig_crash_before_test",
		QuarantineDir: qDir,
		CreatedAt:     1234567,
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile1,
				StagedName:   "staged_0_in_flight_1.spool",
				StagedPath:   stagedFile1,
				Size:         int64(len(content1)),
				SHA256:       hex.EncodeToString(h1[:]),
			},
			{
				OriginalPath: origFile2,
				StagedName:   "staged_1_in_flight_2.part",
				StagedPath:   stagedFile2,
				Size:         int64(len(content2)),
				SHA256:       hex.EncodeToString(h2[:]),
			},
		},
	}
	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(qDir, "quarantine_manifest.json"), manifestBytes, 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	// Verify original files do NOT exist yet
	if _, err := os.Stat(origFile1); !os.IsNotExist(err) {
		t.Fatal("origFile1 should not exist before reconcile")
	}
	if _, err := os.Stat(origFile2); !os.IsNotExist(err) {
		t.Fatal("origFile2 should not exist before reconcile")
	}

	// Trigger reconciliation on restart
	recReport, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("reconcile pending quarantines failed: %v", err)
	}

	if recReport.QuarantineDirsScanned != 1 {
		t.Fatalf("expected 1 quarantine dir scanned, got %d", recReport.QuarantineDirsScanned)
	}
	if len(recReport.RestoredFiles) != 2 {
		t.Fatalf("expected 2 restored files, got %d", len(recReport.RestoredFiles))
	}

	// Verify original files restored with exact content
	read1, err := os.ReadFile(origFile1)
	if err != nil {
		t.Fatalf("origFile1 not restored: %v", err)
	}
	if string(read1) != string(content1) {
		t.Fatalf("content mismatch for origFile1")
	}

	read2, err := os.ReadFile(origFile2)
	if err != nil {
		t.Fatalf("origFile2 not restored: %v", err)
	}
	if string(read2) != string(content2) {
		t.Fatalf("content mismatch for origFile2")
	}

	// Verify quarantine directory removed
	if _, err := os.Stat(qDir); !os.IsNotExist(err) {
		t.Fatalf("quarantine dir should be cleaned up after full restoration")
	}
}

// 13. Acceptance Test: Crash after database commit finalizes purge of quarantined files across restart.
func TestMigration_QuarantineManifest_CrashAfterCommitFinalizesPurgeOnRestart(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "crash_after.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	migrationID := "mig_crash_after_test"

	// Create SQLite DB with COMMITTED verdict
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE _tgx_migration_verdicts (
			migration_id TEXT PRIMARY KEY,
			committed_at INTEGER NOT NULL,
			status TEXT NOT NULL
		);
		INSERT INTO _tgx_migration_verdicts VALUES (?, 1700000000, 'COMMITTED');
	`, migrationID)
	db.Close()
	if err != nil {
		t.Fatalf("failed to insert verdict: %v", err)
	}

	// Quarantine directory with staged file
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_7654321")
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		t.Fatalf("failed to create qDir: %v", err)
	}

	origPath := filepath.Join(bufferDir, "orphaned.spool")
	stagedPath := filepath.Join(qDir, "staged_0_orphaned.spool")
	if err := os.WriteFile(stagedPath, []byte("junk"), 0o644); err != nil {
		t.Fatalf("failed to write staged file: %v", err)
	}

	manifest := migration.QuarantineManifest{
		MigrationID:   migrationID,
		QuarantineDir: qDir,
		CreatedAt:     7654321,
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origPath,
				StagedName:   "staged_0_orphaned.spool",
				StagedPath:   stagedPath,
			},
		},
	}
	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(qDir, "quarantine_manifest.json"), manifestBytes, 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	// Trigger reconciliation on restart
	recReport, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("reconcile pending quarantines failed: %v", err)
	}

	if len(recReport.CleanedFiles) != 1 {
		t.Fatalf("expected 1 cleaned file, got %d", len(recReport.CleanedFiles))
	}
	if len(recReport.RestoredFiles) != 0 {
		t.Fatalf("expected 0 restored files, got %d", len(recReport.RestoredFiles))
	}

	// Staged file and quarantine dir should both be purged
	if _, err := os.Stat(stagedPath); !os.IsNotExist(err) {
		t.Fatalf("staged file should be deleted")
	}
	if _, err := os.Stat(qDir); !os.IsNotExist(err) {
		t.Fatalf("quarantine dir should be deleted")
	}
	if _, err := os.Stat(origPath); !os.IsNotExist(err) {
		t.Fatalf("origPath should NOT be restored since commit succeeded")
	}
}

// 14. Acceptance Test: Normal migration run records durable verdict and cleans up quarantine.
func TestMigration_QuarantineManifest_NormalRunRecordsVerdictAndPurgesQuarantine(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "normal.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	spoolFile := filepath.Join(bufferDir, "test.spool")
	if err := os.WriteFile(spoolFile, []byte("legacy buffer data"), 0o644); err != nil {
		t.Fatalf("failed to write spool file: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE target_commits (chat_id TEXT, message_id INTEGER, save_path TEXT, file_size INTEGER, sha256 TEXT, committed_at INTEGER, PRIMARY KEY(chat_id, message_id))")
	db.Close()

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		BufferDir:        bufferDir,
		DropLegacyTables: true,
	}

	report, err := migration.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("migration.Run failed: %v", err)
	}

	if len(report.CleanedFiles) != 1 {
		t.Fatalf("expected 1 cleaned file, got %d", len(report.CleanedFiles))
	}

	// Verify _tgx_migration_verdicts table has COMMITTED verdict
	verifyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite for verification: %v", err)
	}
	defer verifyDB.Close()

	var verdictCount int
	err = verifyDB.QueryRow("SELECT COUNT(*) FROM _tgx_migration_verdicts WHERE status = 'COMMITTED'").Scan(&verdictCount)
	if err != nil || verdictCount != 1 {
		t.Fatalf("expected 1 committed migration verdict in DB, got count=%d, err=%v", verdictCount, err)
	}

	// Verify no remaining quarantine directories
	matches, _ := filepath.Glob(filepath.Join(bufferDir, ".migrator_quarantine_*"))
	if len(matches) != 0 {
		t.Fatalf("expected 0 quarantine directories remaining, found: %v", matches)
	}
}

// 15. Acceptance Test: Crash during staging (partial rename) restores staged files and verifies intact unstaged files.
func TestMigration_QuarantineManifest_CrashDuringStagingPartialRenameRestoresCorrectly(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "partial_crash.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	// Create SQLite DB without COMMITTED verdict (simulating crash before commit)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE _tgx_migration_verdicts (migration_id TEXT PRIMARY KEY, committed_at INTEGER, status TEXT);")
	db.Close()

	qDir := filepath.Join(bufferDir, ".migrator_quarantine_987654")
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		t.Fatalf("failed to create qDir: %v", err)
	}

	// File 1 was staged before crash
	origFile1 := filepath.Join(bufferDir, "partially_staged_1.spool")
	stagedFile1 := filepath.Join(qDir, "staged_0_partially_staged_1.spool")
	content1 := []byte("already staged before crash")
	if err := os.WriteFile(stagedFile1, content1, 0o644); err != nil {
		t.Fatalf("failed to write staged file 1: %v", err)
	}

	// File 2 was planned in manifest, but crash occurred before it was renamed! So it remains at origFile2
	origFile2 := filepath.Join(bufferDir, "not_yet_staged_2.spool")
	stagedFile2 := filepath.Join(qDir, "staged_1_not_yet_staged_2.spool")
	content2 := []byte("crash occurred before rename - still at original path")
	if err := os.WriteFile(origFile2, content2, 0o644); err != nil {
		t.Fatalf("failed to write original file 2: %v", err)
	}

	h1 := sha256.Sum256(content1)
	h2 := sha256.Sum256(content2)
	manifest := migration.QuarantineManifest{
		MigrationID:   "mig_partial_staging_crash",
		QuarantineDir: qDir,
		CreatedAt:     987654,
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile1,
				StagedName:   "staged_0_partially_staged_1.spool",
				StagedPath:   stagedFile1,
				Size:         int64(len(content1)),
				SHA256:       hex.EncodeToString(h1[:]),
			},
			{
				OriginalPath: origFile2,
				StagedName:   "staged_1_not_yet_staged_2.spool",
				StagedPath:   stagedFile2,
				Size:         int64(len(content2)),
				SHA256:       hex.EncodeToString(h2[:]),
			},
		},
	}
	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(qDir, "quarantine_manifest.json"), manifestBytes, 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	// Reconcile pending quarantines
	recReport, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("reconcile pending quarantines failed: %v", err)
	}

	// File 1 should be restored to original path
	if len(recReport.RestoredFiles) != 1 || recReport.RestoredFiles[0] != origFile1 {
		t.Fatalf("expected restored files to contain origFile1, got: %v", recReport.RestoredFiles)
	}

	read1, err := os.ReadFile(origFile1)
	if err != nil || string(read1) != string(content1) {
		t.Fatalf("origFile1 content mismatch: %v, err: %v", string(read1), err)
	}

	// File 2 should remain intact at its original path
	read2, err := os.ReadFile(origFile2)
	if err != nil || string(read2) != string(content2) {
		t.Fatalf("origFile2 content mismatch: %v, err: %v", string(read2), err)
	}

	// Quarantine directory should be cleaned up
	if _, err := os.Stat(qDir); !os.IsNotExist(err) {
		t.Fatalf("quarantine dir should be cleaned up after successful reconcile")
	}
}

// 16. Acceptance Test: Failure during file restoration fails closed and preserves quarantine directory and files without data loss.
func TestMigration_QuarantineManifest_RestoreFailureFailsClosedAndRetainsData(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "restore_fail.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	qDir := filepath.Join(bufferDir, ".migrator_quarantine_112233")
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		t.Fatalf("failed to create qDir: %v", err)
	}

	// Original path is inside a read-only directory to force rename failure
	blockedDir := filepath.Join(bufferDir, "blocked_dir")
	if err := os.MkdirAll(blockedDir, 0o555); err != nil {
		t.Fatalf("failed to create blocked dir: %v", err)
	}
	defer func() {
		_ = os.Chmod(blockedDir, 0o755)
	}()

	origFile := filepath.Join(blockedDir, "cannot_restore_here.spool")
	stagedFile := filepath.Join(qDir, "staged_0_file.spool")
	valuableData := []byte("irreplaceable data that must not be deleted on restore failure")
	if err := os.WriteFile(stagedFile, valuableData, 0o644); err != nil {
		t.Fatalf("failed to write staged file: %v", err)
	}

	hVal := sha256.Sum256(valuableData)
	manifest := migration.QuarantineManifest{
		MigrationID:   "mig_restore_fail_test",
		QuarantineDir: qDir,
		CreatedAt:     112233,
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_0_file.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(valuableData)),
				SHA256:       hex.EncodeToString(hVal[:]),
			},
		},
	}
	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(qDir, "quarantine_manifest.json"), manifestBytes, 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	// Reconcile pending quarantines must FAIL CLOSED because restore to read-only dir fails
	_, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err == nil {
		t.Fatal("expected error from ReconcilePendingQuarantines when restore fails, got nil")
	}

	// Quarantine directory and staged file MUST still exist!
	if _, err := os.Stat(stagedFile); os.IsNotExist(err) {
		t.Fatal("staged file was unexpectedly deleted after restore failure! Data loss occurred!")
	}
	readStaged, err := os.ReadFile(stagedFile)
	if err != nil || string(readStaged) != string(valuableData) {
		t.Fatalf("staged data damaged: %v, err: %v", string(readStaged), err)
	}
	if _, err := os.Stat(qDir); os.IsNotExist(err) {
		t.Fatal("quarantine directory was unexpectedly deleted after restore failure!")
	}
}

// 17. Acceptance Test: Non-empty quarantine directory without manifest fails closed and is never silently deleted.
func TestMigration_QuarantineManifest_NonEmptyOrphanDirFailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "orphan.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	// 1. Non-empty quarantine directory without manifest
	nonEmptyQDir := filepath.Join(bufferDir, ".migrator_quarantine_non_empty")
	if err := os.MkdirAll(nonEmptyQDir, 0o755); err != nil {
		t.Fatalf("failed to create non-empty qDir: %v", err)
	}
	orphanFilePath := filepath.Join(nonEmptyQDir, "unmanifested_leftover.spool")
	orphanData := []byte("unmanifested data that must be preserved")
	if err := os.WriteFile(orphanFilePath, orphanData, 0o644); err != nil {
		t.Fatalf("failed to write orphan file: %v", err)
	}

	// Reconcile should FAIL-CLOSED with an error
	_, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err == nil {
		t.Fatal("expected error for non-empty quarantine directory without manifest, got nil")
	}

	// The orphan file and directory must NOT be removed
	if _, err := os.Stat(orphanFilePath); os.IsNotExist(err) {
		t.Fatal("orphan file was unexpectedly deleted!")
	}
	if _, err := os.Stat(nonEmptyQDir); os.IsNotExist(err) {
		t.Fatal("non-empty orphan quarantine dir was unexpectedly deleted!")
	}

	// 2. An empty orphan directory SHOULD be safely pruned
	_ = os.Remove(orphanFilePath)
	_ = os.Remove(nonEmptyQDir)

	emptyQDir := filepath.Join(bufferDir, ".migrator_quarantine_empty")
	if err := os.MkdirAll(emptyQDir, 0o755); err != nil {
		t.Fatalf("failed to create empty qDir: %v", err)
	}

	recReport, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("expected no error for empty quarantine dir, got: %v", err)
	}
	if len(recReport.CleanedDirs) != 1 || recReport.CleanedDirs[0] != emptyQDir {
		t.Fatalf("expected emptyQDir in CleanedDirs, got: %v", recReport.CleanedDirs)
	}
	if _, err := os.Stat(emptyQDir); !os.IsNotExist(err) {
		t.Fatal("empty orphan quarantine dir should have been removed")
	}
}

// 18. Acceptance Test: When a COMMITTED verdict exists but verdict lookup fails (e.g. exclusive DB lock),
// reconciliation treats verdict as UNKNOWN, fails closed, and leaves every staged file and manifest completely untouched.
func TestMigration_QuarantineManifest_VerdictLookupFailureFailsClosedAndLeavesFilesUntouched(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "lookup_fail.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	migrationID := "mig_committed_but_locked_test"

	// 1. Create SQLite DB with COMMITTED verdict
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE _tgx_migration_verdicts (
			migration_id TEXT PRIMARY KEY,
			committed_at INTEGER NOT NULL,
			status TEXT NOT NULL
		);
		INSERT INTO _tgx_migration_verdicts VALUES (?, 1700000000, 'COMMITTED');
	`, migrationID)
	if err != nil {
		t.Fatalf("failed to insert verdict: %v", err)
	}
	db.Close()

	// 2. Setup quarantine directory, staged file, and manifest
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_554433")
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		t.Fatalf("failed to create qDir: %v", err)
	}

	origFile := filepath.Join(bufferDir, "critical_spool.data")
	stagedFile := filepath.Join(qDir, "staged_0_critical_spool.data")
	criticalData := []byte("critical data that must not be deleted or incorrectly restored")
	if err := os.WriteFile(stagedFile, criticalData, 0o644); err != nil {
		t.Fatalf("failed to write staged file: %v", err)
	}

	hCrit := sha256.Sum256(criticalData)
	manifest := migration.QuarantineManifest{
		MigrationID:   migrationID,
		QuarantineDir: qDir,
		CreatedAt:     554433,
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_0_critical_spool.data",
				StagedPath:   stagedFile,
				Size:         int64(len(criticalData)),
				SHA256:       hex.EncodeToString(hCrit[:]),
			},
		},
	}
	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	if err := os.WriteFile(manifestPath, manifestBytes, 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	// 3. Hold an EXCLUSIVE write lock on the database to force verdict lookup to fail with 'database is locked'
	lockDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open lock db: %v", err)
	}
	defer lockDB.Close()

	// Begin immediate/exclusive write transaction
	_, err = lockDB.Exec("BEGIN EXCLUSIVE")
	if err != nil {
		t.Fatalf("failed to acquire exclusive transaction lock: %v", err)
	}
	defer func() {
		_, _ = lockDB.Exec("ROLLBACK")
	}()

	// 4. Trigger reconciliation with a context timeout so it fails quickly on lock
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = migration.ReconcilePendingQuarantines(ctx, dbPath, bufferDir)
	if err == nil {
		t.Fatal("expected error from ReconcilePendingQuarantines when verdict query fails, got nil")
	}

	// 5. Fail-Closed Verification:
	// The staged file MUST still exist with exact original content!
	stagedRead, readErr := os.ReadFile(stagedFile)
	if readErr != nil {
		t.Fatalf("staged file missing or unreadable after query failure: %v (data loss!)", readErr)
	}
	if string(stagedRead) != string(criticalData) {
		t.Fatal("staged file content corrupted!")
	}

	// The manifest MUST still exist!
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("quarantine manifest was deleted after verdict lookup failure! Fail-closed violated!")
	}

	// The quarantine directory MUST NOT have been removed!
	if _, statErr := os.Stat(qDir); os.IsNotExist(statErr) {
		t.Fatal("quarantine directory was deleted after verdict lookup failure! Fail-closed violated!")
	}

	// The original file MUST NOT have been created by a mistaken pre-commit restore!
	if _, statErr := os.Stat(origFile); !os.IsNotExist(statErr) {
		t.Fatal("origFile was unexpectedly restored while verdict was unknown!")
	}

	// 6. Also verify that when dbPath is unopenable/inaccessible, it fails closed
	unopenableDBPath := filepath.Join(tempDir, "inaccessible.sqlite3")
	_ = os.WriteFile(unopenableDBPath, []byte("not a sqlite db"), 0o000)
	defer func() { _ = os.Chmod(unopenableDBPath, 0o644) }()

	_, err = migration.ReconcilePendingQuarantines(context.Background(), unopenableDBPath, bufferDir)
	if err == nil {
		t.Fatal("expected error for inaccessible dbPath, got nil")
	}

	// Again, verify staged file, manifest, and qDir remain untouched
	if _, statErr := os.Stat(stagedFile); os.IsNotExist(statErr) {
		t.Fatal("staged file deleted when dbPath is inaccessible!")
	}
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest deleted when dbPath is inaccessible!")
	}
}

// 19. Acceptance Test: Post-commit cleanup failure preserves COMMITTED manifest and never restores originals.
func TestMigration_PostCommitCleanupFailure_LeavesCommittedManifestUntouchedAndNeverRestoresOriginals(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "post_commit_failure.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	targetDir := filepath.Join(tempDir, "target")
	_ = os.MkdirAll(bufferDir, 0o755)
	_ = os.MkdirAll(targetDir, 0o755)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// Create legacy schema and legacy records
	_, _ = db.Exec(`
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
			PRIMARY KEY (chat_id, message_id)
		);
		CREATE TABLE target_commits (commit_id TEXT PRIMARY KEY, chat_id TEXT, message_id INTEGER, save_path TEXT, file_size INTEGER, sha256 TEXT, committed_at INTEGER);
	`)
	db.Close()

	// Place an uncommitted buffer file
	spoolFile := filepath.Join(bufferDir, "in_flight.spool")
	spoolContent := []byte("spool in flight data to be cleaned")
	if err := os.WriteFile(spoolFile, spoolContent, 0o644); err != nil {
		t.Fatalf("write spool file: %v", err)
	}

	var capturedQDir string
	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		TargetDir:        targetDir,
		BufferDir:        bufferDir,
		DryRun:           false,
		DropLegacyTables: true,
		HookPostCommitCleanup: func(qDir string) error {
			capturedQDir = qDir
			return errors.New("injected post-commit cleanup failure (e.g. disk read-only or permission error)")
		},
	}

	_, err = migration.Run(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error from migration.Run on post-commit cleanup failure, got nil")
	}
	t.Logf("migration returned error: %v", err)

	// 1. Assert DB transaction was durably COMMITTED!
	verifyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer verifyDB.Close()

	var verdictStatus string
	err = verifyDB.QueryRow("SELECT status FROM _tgx_migration_verdicts").Scan(&verdictStatus)
	if err != nil || verdictStatus != "COMMITTED" {
		t.Fatalf("expected COMMITTED verdict in DB, got status=%q, err=%v", verdictStatus, err)
	}

	// 2. Assert quarantine manifest STILL EXISTS (not deleted by post-commit failure!)
	manifestPath := filepath.Join(capturedQDir, "quarantine_manifest.json")
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("quarantine manifest was deleted after post-commit cleanup error! Disarm failure!")
	}

	// 3. Assert original file was NOT restored back! (It committed, so original should remain absent!)
	if _, statErr := os.Stat(spoolFile); !os.IsNotExist(statErr) {
		t.Fatal("original file was mistakenly restored after DB committed! Pre-commit rollback was not disarmed!")
	}

	// 4. Assert staged file exists in quarantine!
	entries, _ := os.ReadDir(capturedQDir)
	foundStaged := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "staged_") {
			foundStaged = true
			break
		}
	}
	if !foundStaged {
		t.Fatal("no staged files found in quarantine directory!")
	}

	// 5. Now execute ReconcilePendingQuarantines across restart:
	// It must see the COMMITTED verdict and complete the deletion cleanly!
	reconcileReport, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("reconciliation failed to finalize committed quarantine: %v", err)
	}
	if len(reconcileReport.CleanedFiles) == 0 {
		t.Fatal("expected reconcile to clean staged files for COMMITTED migration")
	}
	if len(reconcileReport.RestoredFiles) != 0 {
		t.Fatalf("expected 0 restored files for COMMITTED migration, got %d", len(reconcileReport.RestoredFiles))
	}
	if _, statErr := os.Stat(capturedQDir); !os.IsNotExist(statErr) {
		t.Fatal("quarantine directory was not cleaned up after reconcile!")
	}
}

// 20. Acceptance Test: Reconcile restore fails closed if file is missing from both staged and original path.
func TestMigration_StagedAndOriginalBothMissing_FailsClosedAndRetainsManifest(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "double_missing.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)

	// DB without verdict
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INTEGER)")
	db.Close()

	// Create quarantine dir with manifest pointing to non-existent staged AND original file
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_999999")
	_ = os.MkdirAll(qDir, 0o755)
	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")

	manifest := migration.QuarantineManifest{
		MigrationID:   "mig_double_missing",
		QuarantineDir: qDir,
		CreatedAt:     999999,
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: filepath.Join(bufferDir, "non_existent_original.spool"),
				StagedName:   "staged_0_non_existent.spool",
				StagedPath:   filepath.Join(qDir, "staged_0_non_existent.spool"),
				Size:         100,
				SHA256:       "sha_double_missing",
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	// Call ReconcilePendingQuarantines: must fail-closed because file is missing from both locations!
	t.Logf("[Test 20: DOUBLE_MISSING] Invoking ReconcilePendingQuarantines with missing staged and original files...")
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	t.Logf("[Test 20: DOUBLE_MISSING] ReconcilePendingQuarantines returned err: %v", err)
	if err == nil {
		t.Fatal("expected error when file is missing from both staged and original, got nil")
	}

	// Manifest and quarantine directory MUST NOT be deleted!
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest was deleted despite double missing file! Fail-closed violated!")
	}
	if _, statErr := os.Stat(qDir); os.IsNotExist(statErr) {
		t.Fatal("quarantine directory was deleted despite double missing file! Fail-closed violated!")
	}
	t.Logf("[Test 20: DOUBLE_MISSING] Verified fail-closed: manifest and quarantine dir preserved intact")
}

// 21. Acceptance Test: Reconcile restore fails closed if staged file is corrupted.
func TestMigration_Reconcile_StagedCorrupted_FailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_corrupt_staged")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "orig_corrupt_test.spool")
	stagedFile := filepath.Join(qDir, "staged_corrupt.spool")

	// Write corrupted content to staged file
	corruptContent := []byte("corrupted staged data")
	_ = os.WriteFile(stagedFile, corruptContent, 0o644)
	t.Logf("[Test 21: STAGED_CORRUPTED] Created staged file with %d bytes corrupt content", len(corruptContent))

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_corrupt_staged",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_corrupt.spool",
				StagedPath:   stagedFile,
				Size:         int64(len("authentic expected data")),
				SHA256:       "authentic_hash_expected_123",
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 21: STAGED_CORRUPTED] Invoking ReconcilePendingQuarantines...")
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	t.Logf("[Test 21: STAGED_CORRUPTED] ReconcilePendingQuarantines returned err: %v", err)
	if err == nil {
		t.Fatal("expected error from ReconcilePendingQuarantines when staged file is corrupted, got nil")
	}

	// Fail-closed verification
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest was deleted despite corrupted staged file! Fail-closed violated!")
	}
	if _, statErr := os.Stat(stagedFile); os.IsNotExist(statErr) {
		t.Fatal("staged file was deleted despite being corrupted! Fail-closed violated!")
	}
	t.Logf("[Test 21: STAGED_CORRUPTED] Verified fail-closed: manifest and corrupt staged file retained")
}

// 22. Acceptance Test: Reconcile restore fails closed if original file is corrupted.
func TestMigration_Reconcile_OriginalCorrupted_FailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_corrupt_orig")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "orig_corrupt.spool")
	stagedFile := filepath.Join(qDir, "staged_not_here.spool")

	// Original exists but has corrupted content
	corruptOrig := []byte("corrupted original content")
	_ = os.WriteFile(origFile, corruptOrig, 0o644)
	t.Logf("[Test 22: ORIG_CORRUPTED] Created original file with %d bytes corrupt content", len(corruptOrig))

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_corrupt_orig",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_not_here.spool",
				StagedPath:   stagedFile,
				Size:         int64(len("authentic expected data")),
				SHA256:       "authentic_hash_expected_456",
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 22: ORIG_CORRUPTED] Invoking ReconcilePendingQuarantines...")
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	t.Logf("[Test 22: ORIG_CORRUPTED] ReconcilePendingQuarantines returned err: %v", err)
	if err == nil {
		t.Fatal("expected error from ReconcilePendingQuarantines when original file is corrupted, got nil")
	}

	// Fail-closed verification
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest was deleted despite corrupted original file! Fail-closed violated!")
	}
	t.Logf("[Test 22: ORIG_CORRUPTED] Verified fail-closed: manifest retained")
}

// 23. Acceptance Test: Reconcile restore safely deduplicates when both copies exist and are byte-for-byte identical.
func TestMigration_Reconcile_BothExistIdentical_Deduplicates(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_both_identical")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "orig_identical.spool")
	stagedFile := filepath.Join(qDir, "staged_identical.spool")

	payload := []byte("identical authentic data 123456789")
	hash := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(hash[:])

	// Both files exist with exact authentic payload
	_ = os.WriteFile(origFile, payload, 0o644)
	_ = os.WriteFile(stagedFile, payload, 0o644)
	t.Logf("[Test 23: DEDUPLICATE] Created identical copies at orig (%s) and staged (%s), sha=%s", origFile, stagedFile, shaHex)

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_both_identical",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_identical.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(payload)),
				SHA256:       shaHex,
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 23: DEDUPLICATE] Invoking ReconcilePendingQuarantines...")
	report, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("ReconcilePendingQuarantines failed on identical copies: %v", err)
	}
	t.Logf("[Test 23: DEDUPLICATE] Reconcile report: RestoredFiles=%v, CleanedDirs=%v", report.RestoredFiles, report.CleanedDirs)
	if len(report.RestoredFiles) != 1 || report.RestoredFiles[0] != origFile {
		t.Fatalf("unexpected RestoredFiles: %+v", report.RestoredFiles)
	}

	// Verify deduplication: staged file removed, original file intact
	if _, statErr := os.Stat(stagedFile); !os.IsNotExist(statErr) {
		t.Fatalf("staged file should be removed after deduplication, but exists: %v", statErr)
	}
	origBytes, readErr := os.ReadFile(origFile)
	if readErr != nil || string(origBytes) != string(payload) {
		t.Fatalf("original file was corrupted or missing: %v", readErr)
	}
	// Quarantine directory and manifest cleaned
	if _, statErr := os.Stat(manifestPath); !os.IsNotExist(statErr) {
		t.Fatalf("manifest should be removed after successful deduplication: %v", statErr)
	}
	t.Logf("[Test 23: DEDUPLICATE] Verified: staged file deduplicated, original file intact with verified payload")
}

// 24. Acceptance Test: Reconcile restore fails closed without overwriting when both copies exist but conflict.
func TestMigration_Reconcile_BothExistConflicting_FailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_both_conflict")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "orig_conflict.spool")
	stagedFile := filepath.Join(qDir, "staged_conflict.spool")

	payloadOrig := []byte("original content version A")
	payloadStaged := []byte("staged content version B differs!!")

	// Both files exist but have conflicting content
	_ = os.WriteFile(origFile, payloadOrig, 0o644)
	_ = os.WriteFile(stagedFile, payloadStaged, 0o644)
	t.Logf("[Test 24: CONFLICT] Created conflicting copies: orig=%q, staged=%q", string(payloadOrig), string(payloadStaged))

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_both_conflict",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_conflict.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(payloadStaged)),
				SHA256:       "some_hash_that_does_not_match_orig",
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 24: CONFLICT] Invoking ReconcilePendingQuarantines...")
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	t.Logf("[Test 24: CONFLICT] ReconcilePendingQuarantines returned err: %v", err)
	if err == nil {
		t.Fatal("expected error from ReconcilePendingQuarantines when both copies conflict, got nil")
	}

	// Crucial assertion: original file MUST NOT be overwritten!
	origContent, readErr := os.ReadFile(origFile)
	if readErr != nil || string(origContent) != string(payloadOrig) {
		t.Fatalf("original file was overwritten or corrupted! got %q, want %q", string(origContent), string(payloadOrig))
	}
	// Manifest and staged file retained
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest was deleted despite conflict! Fail-closed violated!")
	}
	if _, statErr := os.Stat(stagedFile); os.IsNotExist(statErr) {
		t.Fatal("staged file was deleted despite conflict! Fail-closed violated!")
	}
	t.Logf("[Test 24: CONFLICT] Verified fail-closed: original file untouched, manifest and staged retained")
}

// 25. Acceptance Test: Reconcile restore safely restores valid staged file when original is missing.
func TestMigration_Reconcile_StagedValid_OriginalMissing_RestoresSuccessfully(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_single_valid")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "orig_restored.spool")
	stagedFile := filepath.Join(qDir, "staged_restored.spool")

	payload := []byte("valid staged payload awaiting restore 987654321")
	hash := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(hash[:])

	_ = os.WriteFile(stagedFile, payload, 0o644)
	t.Logf("[Test 25: RESTORE_VALID] Created valid staged file (%d bytes, sha=%s), original missing", len(payload), shaHex)

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_single_valid",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_restored.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(payload)),
				SHA256:       shaHex,
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 25: RESTORE_VALID] Invoking ReconcilePendingQuarantines...")
	report, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("ReconcilePendingQuarantines failed on valid staged copy: %v", err)
	}
	t.Logf("[Test 25: RESTORE_VALID] Reconcile report: RestoredFiles=%v, CleanedDirs=%v", report.RestoredFiles, report.CleanedDirs)
	if len(report.RestoredFiles) != 1 || report.RestoredFiles[0] != origFile {
		t.Fatalf("unexpected RestoredFiles: %+v", report.RestoredFiles)
	}

	// Verify original file exists and has correct payload
	origBytes, readErr := os.ReadFile(origFile)
	if readErr != nil || string(origBytes) != string(payload) {
		t.Fatalf("original file was not correctly restored: %v", readErr)
	}
	// Staged file should be moved
	if _, statErr := os.Stat(stagedFile); !os.IsNotExist(statErr) {
		t.Fatalf("staged file still exists after restore: %v", statErr)
	}
	// Quarantine directory and manifest cleaned
	if _, statErr := os.Stat(manifestPath); !os.IsNotExist(statErr) {
		t.Fatalf("manifest should be removed after restore: %v", statErr)
	}
	t.Logf("[Test 25: RESTORE_VALID] Verified: original file restored byte-for-byte, staged file and manifest cleaned")
}

// 26. Acceptance Test: Reconcile restore fails closed when manifest specifies absolute path outside bufferDir or outside quarantineDir.
func TestMigration_Reconcile_AbsoluteOutsidePath_FailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	outsideDir := filepath.Join(tempDir, "outside")
	_ = os.MkdirAll(bufferDir, 0o755)
	_ = os.MkdirAll(outsideDir, 0o755)

	qDir := filepath.Join(bufferDir, ".migrator_quarantine_outside_escape")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	escapedOrigFile := filepath.Join(outsideDir, "escaped.spool")
	stagedFile := filepath.Join(qDir, "staged_outside.spool")

	_ = os.WriteFile(stagedFile, []byte("some data"), 0o644)

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_outside_escape",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: escapedOrigFile, // points outside bufferDir!
				StagedName:   "staged_outside.spool",
				StagedPath:   stagedFile,
				Size:         int64(len("some data")),
				SHA256:       "dummy_hash",
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 26: OUTSIDE_ESCAPE] Invoking ReconcilePendingQuarantines with outside path %s...", escapedOrigFile)
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	t.Logf("[Test 26: OUTSIDE_ESCAPE] ReconcilePendingQuarantines returned err: %v", err)
	if err == nil {
		t.Fatal("expected error when manifest contains outside path escaping bufferDir, got nil")
	}

	// Fail-closed verification: manifest and quarantine intact, escaped file never created
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest was deleted despite path escape! Fail-closed violated!")
	}
	if _, statErr := os.Stat(escapedOrigFile); !os.IsNotExist(statErr) {
		t.Fatalf("escaped file was created outside bufferDir: %v", statErr)
	}
	t.Logf("[Test 26: OUTSIDE_ESCAPE] Verified fail-closed: outside escape blocked, manifest retained")
}

// 27. Acceptance Test: Reconcile restore accepts legal double dots in filename (e.g. video..spool) and restores safely.
func TestMigration_Reconcile_LegalDoubleDotsInFilename_AcceptedAndRestored(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_double_dots")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	// Legal filename containing ".." in the name (not as a directory traversal)
	origFile := filepath.Join(bufferDir, "video..spool")
	stagedFile := filepath.Join(qDir, "staged_video..spool")

	payload := []byte("legal double dots video content 12345")
	hash := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(hash[:])

	_ = os.WriteFile(stagedFile, payload, 0o644)
	t.Logf("[Test 27: LEGAL_DOUBLE_DOTS] Created staged file with legal double-dots in name: %s", stagedFile)

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_double_dots",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_video..spool",
				StagedPath:   stagedFile,
				Size:         int64(len(payload)),
				SHA256:       shaHex,
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 27: LEGAL_DOUBLE_DOTS] Invoking ReconcilePendingQuarantines...")
	report, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("ReconcilePendingQuarantines failed on legal double-dots filename: %v", err)
	}
	t.Logf("[Test 27: LEGAL_DOUBLE_DOTS] Reconcile report: RestoredFiles=%v, CleanedDirs=%v", report.RestoredFiles, report.CleanedDirs)
	if len(report.RestoredFiles) != 1 || report.RestoredFiles[0] != origFile {
		t.Fatalf("unexpected RestoredFiles: %+v", report.RestoredFiles)
	}

	// Verify restored file exists and has correct payload
	origBytes, readErr := os.ReadFile(origFile)
	if readErr != nil || string(origBytes) != string(payload) {
		t.Fatalf("restored file with double-dots was corrupted or missing: %v", readErr)
	}
	if _, statErr := os.Stat(stagedFile); !os.IsNotExist(statErr) {
		t.Fatalf("staged file still exists after restore: %v", statErr)
	}
	t.Logf("[Test 27: LEGAL_DOUBLE_DOTS] Verified: legal filename with double-dots restored successfully")
}

// 28. Acceptance Test: Target appears between Phase 1 and Phase 2 (TOCTOU race), safe link/move fails closed without overwriting.
func TestMigration_Reconcile_TargetAppearsBetweenPhases_FailsClosedWithoutOverwrite(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_toctou")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "target_race.spool")
	stagedFile := filepath.Join(qDir, "staged_race.spool")

	stagedPayload := []byte("authentic staged content")
	hash := sha256.Sum256(stagedPayload)
	shaHex := hex.EncodeToString(hash[:])
	_ = os.WriteFile(stagedFile, stagedPayload, 0o644)

	racePayload := []byte("concurrently created target file during race window")

	// In BeforeRestoreMove hook (fired right before physical link/move in Phase 2),
	// simulate a race condition where another process creates the target file!
	migration.SetTestHooks(migration.MigratorTestHooks{
		BeforeRestoreMove: func(stagedPath, origPath string) {
			t.Logf("[HOOK: BeforeRestoreMove] Simulating TOCTOU race: creating %s before restore...", origPath)
			_ = os.WriteFile(origPath, racePayload, 0o644)
		},
	})
	defer migration.SetTestHooks(migration.MigratorTestHooks{})

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_toctou_race",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_race.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(stagedPayload)),
				SHA256:       shaHex,
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 28: TOCTOU_RACE] Invoking ReconcilePendingQuarantines...")
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	t.Logf("[Test 28: TOCTOU_RACE] ReconcilePendingQuarantines returned err: %v", err)
	if err == nil {
		t.Fatal("expected error when target appears between Phase 1 and Phase 2, got nil")
	}

	// Crucial assertion: the concurrently created file MUST NOT be overwritten!
	currentContent, readErr := os.ReadFile(origFile)
	if readErr != nil || string(currentContent) != string(racePayload) {
		t.Fatalf("concurrently created file was overwritten! got %q, want %q", string(currentContent), string(racePayload))
	}
	// Staged file and manifest retained
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest was deleted despite TOCTOU collision! Fail-closed violated!")
	}
	if _, statErr := os.Stat(stagedFile); os.IsNotExist(statErr) {
		t.Fatal("staged file was deleted despite TOCTOU collision! Fail-closed violated!")
	}
	t.Logf("[Test 28: TOCTOU_RACE] Verified fail-closed: concurrent target file preserved without overwrite, manifest retained")
}

// 29. Acceptance Test: Parent directory Sync failure prevents manifest removal and retains quarantine.
func TestMigration_Reconcile_ParentDirSyncFailure_RetainsManifest(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_sync_fail")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "sync_fail.spool")
	stagedFile := filepath.Join(qDir, "staged_sync_fail.spool")

	payload := []byte("payload for sync failure test")
	hash := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(hash[:])
	_ = os.WriteFile(stagedFile, payload, 0o644)

	// Inject parent directory sync error
	migration.SetTestHooks(migration.MigratorTestHooks{
		ParentDirSyncHook: func(dir string) error {
			t.Logf("[HOOK: ParentDirSyncHook] Simulating parent dir sync error for %s", dir)
			return errors.New("simulated parent directory sync failure")
		},
	})
	defer migration.SetTestHooks(migration.MigratorTestHooks{})

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_sync_fail",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_sync_fail.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(payload)),
				SHA256:       shaHex,
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 29: DIR_SYNC_FAIL] Invoking ReconcilePendingQuarantines...")
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	t.Logf("[Test 29: DIR_SYNC_FAIL] ReconcilePendingQuarantines returned err: %v", err)
	if err == nil {
		t.Fatal("expected error when parent directory sync fails, got nil")
	}

	// Crucial assertion: manifest MUST NOT be deleted if durability check fails!
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("manifest was deleted despite parent directory sync failure! Durability violated!")
	}
	t.Logf("[Test 29: DIR_SYNC_FAIL] Verified fail-closed: manifest retained when parent dir sync fails")
}

// 30. Acceptance Test: safeNoReplaceLinkOrMove copy fallback (cross-device) verifies size and SHA before source deletion.
func TestMigration_Reconcile_CopyFallback_VerifiedAndRestored(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_copy_fallback")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "copy_fallback.spool")
	stagedFile := filepath.Join(qDir, "staged_copy_fallback.spool")

	payload := []byte("copy fallback authentic data payload 999888777")
	hash := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(hash[:])
	_ = os.WriteFile(stagedFile, payload, 0o644)

	// Force copy fallback path (simulating cross-device EXDEV / no-hardlink filesystem)
	migration.SetTestHooks(migration.MigratorTestHooks{
		ForceCopyFallback: true,
	})
	defer migration.SetTestHooks(migration.MigratorTestHooks{})

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_copy_fallback",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_copy_fallback.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(payload)),
				SHA256:       shaHex,
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 30: COPY_FALLBACK] Invoking ReconcilePendingQuarantines with ForceCopyFallback=true...")
	report, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("ReconcilePendingQuarantines failed with copy fallback: %v", err)
	}
	t.Logf("[Test 30: COPY_FALLBACK] Reconcile report: RestoredFiles=%v, CleanedDirs=%v", report.RestoredFiles, report.CleanedDirs)
	if len(report.RestoredFiles) != 1 || report.RestoredFiles[0] != origFile {
		t.Fatalf("unexpected RestoredFiles: %+v", report.RestoredFiles)
	}

	// Verify restored file exists with matching payload and staged file is deleted
	content, readErr := os.ReadFile(origFile)
	if readErr != nil || string(content) != string(payload) {
		t.Fatalf("restored file was corrupted or missing: %v", readErr)
	}
	if _, statErr := os.Stat(stagedFile); !os.IsNotExist(statErr) {
		t.Fatalf("staged file still exists after copy fallback restore: %v", statErr)
	}
	if _, statErr := os.Stat(manifestPath); !os.IsNotExist(statErr) {
		t.Fatalf("manifest should be removed after successful copy fallback restore: %v", statErr)
	}
	t.Logf("[Test 30: COPY_FALLBACK] Verified: copy fallback safely verified integrity and restored file")
}

// 31. Acceptance Test: Deferred rollback reconciliation failure during Run preserves durable quarantine evidence.
func TestMigration_Run_DeferredRollbackReconcileFailure_PreservesQuarantineEvidence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "defer_fail.sqlite3")
	bufferDir := filepath.Join(tempDir, "buffer")
	if err := os.MkdirAll(bufferDir, 0o755); err != nil {
		t.Fatalf("failed to create buffer dir: %v", err)
	}

	spoolFile := filepath.Join(bufferDir, "defer_evidence.spool")
	evidenceContent := []byte("defer evidence content for failure injection")
	if err := os.WriteFile(spoolFile, evidenceContent, 0o644); err != nil {
		t.Fatalf("failed to write spool file: %v", err)
	}

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
		INSERT INTO target_commits VALUES ('-10099', 1, 'vid.mp4', 100, 'hash', 1000);
	`)
	if err != nil {
		t.Fatalf("failed to setup legacy table: %v", err)
	}

	// Lock table target_commits by holding an active read query in an explicit transaction
	lockingDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open locking db: %v", err)
	}
	defer lockingDB.Close()

	lockTx, err := lockingDB.Begin()
	if err != nil {
		t.Fatalf("failed to begin lock transaction: %v", err)
	}
	rows, err := lockTx.Query("SELECT * FROM target_commits")
	if err != nil {
		t.Fatalf("failed to query for lock: %v", err)
	}

	// Inject parent directory sync failure into the reconciliation primitive
	migration.SetTestHooks(migration.MigratorTestHooks{
		ParentDirSyncHook: func(dir string) error {
			if filepath.Clean(dir) == filepath.Clean(bufferDir) {
				return errors.New("injected defer sync failure")
			}
			return nil
		},
	})
	defer migration.SetTestHooks(migration.MigratorTestHooks{})

	opts := migration.MigrationOptions{
		DBPath:           dbPath,
		BufferDir:        bufferDir,
		DropLegacyTables: true,
	}

	// Run should stage files, fail on DROP TABLE, trigger defer -> ReconcilePendingQuarantines -> fail on dir sync!
	report, runErr := migration.Run(context.Background(), opts)
	_ = report
	_ = rows.Close()
	_ = lockTx.Rollback()
	db.Close()

	if runErr == nil {
		t.Fatal("expected migration Run to fail, got nil")
	}

	// Verify fail-closed behavior: quarantine directory and manifest MUST be preserved intact!
	matches, globErr := filepath.Glob(filepath.Join(bufferDir, ".migrator_quarantine_*"))
	if globErr != nil || len(matches) != 1 {
		t.Fatalf("expected 1 quarantine dir preserved on defer failure, got %v (err: %v)", matches, globErr)
	}
	manifestPath := filepath.Join(matches[0], "quarantine_manifest.json")
	if _, statErr := os.Stat(manifestPath); statErr != nil {
		t.Fatalf("quarantine manifest must be preserved on defer failure: %v", statErr)
	}

	// Now clear the hook to simulate next restart reconciliation
	migration.SetTestHooks(migration.MigratorTestHooks{})

	reconcileReport, recErr := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if recErr != nil {
		t.Fatalf("restart reconciliation failed after clearing hook: %v", recErr)
	}
	if len(reconcileReport.CleanedDirs) != 1 {
		t.Fatalf("expected 1 quarantine dir cleaned on restart, got: %+v", reconcileReport.CleanedDirs)
	}

	// Verify final restoration
	restoredContent, readErr := os.ReadFile(spoolFile)
	if readErr != nil || string(restoredContent) != string(evidenceContent) {
		t.Fatalf("evidence corrupted after restart reconciliation: %v", readErr)
	}
	if _, statErr := os.Stat(manifestPath); !os.IsNotExist(statErr) {
		t.Fatalf("quarantine manifest should be deleted after successful restart reconciliation")
	}
	t.Logf("[Test 31: DEFER_RECONCILE_FAIL] Verified: defer failure preserves durable manifest, restart successfully restores")
}

// 32. Acceptance Test: safeNoReplaceLinkOrMove copy fallback detects partial/corrupt copy, preserving source file and manifest.
func TestMigration_Reconcile_CopyFallback_PartialCopyDetected_PreservesSourceAndManifest(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	_, _ = db.Exec("CREATE TABLE dummy (id INT)")
	defer db.Close()

	bufferDir := filepath.Join(tempDir, "buffer")
	_ = os.MkdirAll(bufferDir, 0o755)
	qDir := filepath.Join(bufferDir, ".migrator_quarantine_copy_corrupt")
	_ = os.MkdirAll(qDir, 0o755)

	manifestPath := filepath.Join(qDir, "quarantine_manifest.json")
	origFile := filepath.Join(bufferDir, "copy_corrupt.spool")
	stagedFile := filepath.Join(qDir, "staged_copy_corrupt.spool")

	payload := []byte("authentic original payload before partial copy failure 123456789")
	hash := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(hash[:])
	_ = os.WriteFile(stagedFile, payload, 0o644)

	// Simulate cross-device copy fallback with partial/corrupted write
	migration.SetTestHooks(migration.MigratorTestHooks{
		ForceCopyFallback:   true,
		CorruptCopyFallback: true,
	})
	defer migration.SetTestHooks(migration.MigratorTestHooks{})

	manifest := migration.QuarantineManifest{
		MigrationID: "mig_copy_corrupt",
		CreatedAt:   time.Now().Unix(),
		Files: []migration.QuarantineFileEntry{
			{
				OriginalPath: origFile,
				StagedName:   "staged_copy_corrupt.spool",
				StagedPath:   stagedFile,
				Size:         int64(len(payload)),
				SHA256:       shaHex,
			},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(manifestPath, data, 0o644)

	t.Logf("[Test 32: COPY_PARTIAL] Invoking ReconcilePendingQuarantines with CorruptCopyFallback=true...")
	_, err = migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err == nil {
		t.Fatal("expected ReconcilePendingQuarantines to fail when copy fallback is corrupted, got nil")
	}
	t.Logf("[Test 32: COPY_PARTIAL] Reconcile correctly returned error: %v", err)

	// Assert fail-closed: staged source must be preserved intact!
	srcContent, readErr := os.ReadFile(stagedFile)
	if readErr != nil || string(srcContent) != string(payload) {
		t.Fatalf("staged source was deleted or corrupted on copy failure: %v", readErr)
	}

	// Corrupted destination must be cleaned up to prevent half-baked artifacts
	if _, statErr := os.Stat(origFile); !os.IsNotExist(statErr) {
		t.Fatalf("corrupted destination file was left on disk: %v", statErr)
	}

	// Manifest must be preserved
	if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
		t.Fatal("quarantine manifest was deleted despite copy failure")
	}

	// Now clear the corruption hook and retry (simulating subsequent reconcile)
	migration.SetTestHooks(migration.MigratorTestHooks{
		ForceCopyFallback: true,
	})

	report, err := migration.ReconcilePendingQuarantines(context.Background(), dbPath, bufferDir)
	if err != nil {
		t.Fatalf("ReconcilePendingQuarantines failed after clearing corrupt hook: %v", err)
	}
	if len(report.RestoredFiles) != 1 || report.RestoredFiles[0] != origFile {
		t.Fatalf("unexpected RestoredFiles: %+v", report.RestoredFiles)
	}

	// Verify successful restore
	dstContent, readErr := os.ReadFile(origFile)
	if readErr != nil || string(dstContent) != string(payload) {
		t.Fatalf("restored file was corrupted: %v", readErr)
	}
	if _, statErr := os.Stat(stagedFile); !os.IsNotExist(statErr) {
		t.Fatalf("staged file still exists after successful restore")
	}
	if _, statErr := os.Stat(manifestPath); !os.IsNotExist(statErr) {
		t.Fatalf("manifest should be removed after successful restore")
	}
	t.Logf("[Test 32: COPY_PARTIAL] Verified: partial copy failed-closed, source preserved, subsequent restore succeeded")
}
