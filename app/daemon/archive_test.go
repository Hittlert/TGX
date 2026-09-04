package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestArchiveWorker_DirValidation(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	logger := zap.NewNop()

	// Same dir rejected
	_, err = NewArchiveWorker(db, tempDir, tempDir, logger)
	if err == nil {
		t.Fatal("expected error for identical download and archive dirs")
	}

	// Nested dir rejected
	subDir := filepath.Join(tempDir, "archive")
	_ = os.MkdirAll(subDir, 0o755)
	_, err = NewArchiveWorker(db, tempDir, subDir, logger)
	if err == nil {
		t.Fatal("expected error for nested archive dir inside download dir")
	}
}

func TestArchiveWorker_ProcessJobSuccess(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	downloadDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "hdd")
	_ = os.MkdirAll(downloadDir, 0o755)
	_ = os.MkdirAll(archiveDir, 0o755)

	logger := zap.NewNop()
	worker, err := NewArchiveWorker(db, downloadDir, archiveDir, logger)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	// Create test file on SSD
	relPath := "test_chat/video.mp4"
	ssdFilePath := filepath.Join(downloadDir, relPath)
	_ = os.MkdirAll(filepath.Dir(ssdFilePath), 0o755)

	payload := []byte("important video content for archive")
	if err := os.WriteFile(ssdFilePath, payload, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	h := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(h[:])

	// Enqueue archive job
	now := time.Now().Unix()
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 'pending', 0, 0, ?, ?)
	`, "12345", 1, relPath, int64(len(payload)), shaHex, now, now)
	if err != nil {
		t.Fatalf("enqueue archive job: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Process job
	claimed, err := db.ClaimArchiveJob("12345", 1, "claim-1")
	if err != nil || !claimed {
		t.Fatalf("claim failed: claimed=%v, err=%v", claimed, err)
	}

	job := ArchiveJob{
		ChatID:       "12345",
		MessageID:    1,
		RelativePath: relPath,
		ExpectedSize: int64(len(payload)),
		SHA256:       shaHex,
		ClaimID:      "claim-1",
		State:        "copying",
	}
	worker.processJob(ctx, job)

	// Verify archive target exists and matches
	archiveFilePath := filepath.Join(archiveDir, relPath)
	readData, err := os.ReadFile(archiveFilePath)
	if err != nil {
		t.Fatalf("archive file not found: %v", err)
	}
	if string(readData) != string(payload) {
		t.Fatalf("archive content mismatch")
	}

	// Verify SSD source removed
	if _, err := os.Stat(ssdFilePath); !os.IsNotExist(err) {
		t.Fatalf("expected SSD source to be deleted, got err: %v", err)
	}

	// Verify job is marked archived
	var state string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = ? AND message_id = ?`, "12345", 1).Scan(&state)
	if state != "archived" {
		t.Fatalf("expected state 'archived', got %q", state)
	}
}

func TestArchiveWorker_ConflictHandling(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	downloadDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "hdd")
	_ = os.MkdirAll(downloadDir, 0o755)
	_ = os.MkdirAll(archiveDir, 0o755)

	worker, _ := NewArchiveWorker(db, downloadDir, archiveDir, zap.NewNop())

	relPath := "conflict_chat/file.txt"
	ssdPath := filepath.Join(downloadDir, relPath)
	archivePath := filepath.Join(archiveDir, relPath)
	_ = os.MkdirAll(filepath.Dir(ssdPath), 0o755)
	_ = os.MkdirAll(filepath.Dir(archivePath), 0o755)

	_ = os.WriteFile(ssdPath, []byte("ssd source data"), 0o644)
	_ = os.WriteFile(archivePath, []byte("pre-existing conflicting data"), 0o644)

	now := time.Now().Unix()
	_, _ = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('999', 42, ?, ?, 'fake_sha', 'pending', 0, 0, '', ?, ?)
	`, relPath, int64(len("ssd source data")), now, now)

	job := ArchiveJob{
		ChatID:       "999",
		MessageID:    42,
		RelativePath: relPath,
		ExpectedSize: int64(len("ssd source data")),
		SHA256:       "fake_sha",
		ClaimID:      "claim-conflict",
		State:        "copying",
	}
	_, _ = db.ClaimArchiveJob("999", 42, "claim-conflict")

	worker.processJob(context.Background(), job)

	// Verify job marked conflict
	var state string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = ? AND message_id = ?`, "999", 42).Scan(&state)
	if state != "conflict" {
		t.Fatalf("expected state 'conflict', got %q", state)
	}

	// Verify BOTH files preserved intact
	if _, err := os.Stat(ssdPath); err != nil {
		t.Fatalf("SSD file must be preserved: %v", err)
	}
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("Archive file must be preserved: %v", err)
	}
}

// Issue #6 Acceptance Matrix

func TestArchive_DuplicateCompletion_PreservesArchived(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	relPath := "chat/video.mp4"
	shaHex := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	size := int64(1024)

	// Seed archived record
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('100', 1, ?, ?, ?, 'archived', 1, 0, '', ?, ?)
	`, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("insert archived: %v", err)
	}

	// Also seed download_records in committing state to test CompleteDownloadAndQueueArchive
	_, _ = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, sha256, attempt_generation, created_at, updated_at)
		VALUES ('100', 1, 'committing', ?, ?, ?, 'gen1', ?, ?)
	`, relPath, size, shaHex, now, now)

	// Duplicate completion with matching identity
	err = db.CompleteDownloadAndQueueArchive("100", 1, "gen1", relPath, size, shaHex, true)
	if err != nil {
		t.Fatalf("expected success on matching duplicate completion, got: %v", err)
	}

	// Verify state is still 'archived'
	var state string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = '100' AND message_id = 1`).Scan(&state)
	if state != "archived" {
		t.Fatalf("expected state 'archived' to be preserved, got %q", state)
	}
}

func TestArchive_DuplicateCompletion_DifferentIdentity_ReturnsConflict(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	relPath := "chat/video.mp4"
	shaHex := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	size := int64(1024)

	// Seed archived record
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('101', 1, ?, ?, ?, 'archived', 1, 0, '', ?, ?)
	`, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("insert archived: %v", err)
	}

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	// Duplicate completion with different SHA
	differentSHA := "9999999999999999999999999999999999999999999999999999999999999999"
	err = db.ensureArchiveJobLocked(tx, "101", 1, relPath, size, differentSHA, now)
	if err == nil {
		t.Fatal("expected error on duplicate completion with different SHA")
	}
	_ = tx.Commit()

	// Verify state is marked 'conflict'
	var state string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = '101' AND message_id = 1`).Scan(&state)
	if state != "conflict" {
		t.Fatalf("expected state 'conflict', got %q", state)
	}
}

func TestArchive_LateCopyFailure_CannotRevertArchivedToPending(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	relPath := "chat/file.bin"
	shaHex := "1111111111111111111111111111111111111111111111111111111111111111"
	size := int64(2048)

	// 1. Seed pending job
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('102', 1, ?, ?, ?, 'pending', 0, 0, '', ?, ?)
	`, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("insert pending: %v", err)
	}

	// 2. Claim job with claimA
	claimed, err := db.ClaimArchiveJob("102", 1, "claimA")
	if !claimed || err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	// 3. Complete job -> archived
	err = db.CompleteArchiveJob("102", 1, "claimA", shaHex)
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	// 4. Late failure from claimA arrives
	err = db.FailArchiveJob("102", 1, "claimA", "late network timeout")
	if err == nil {
		t.Fatal("expected error when failing an already archived job")
	}

	// 5. Verify state is still archived
	var state string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = '102' AND message_id = 1`).Scan(&state)
	if state != "archived" {
		t.Fatalf("state must remain 'archived', got %q", state)
	}
}

func TestArchive_LateRecovery_CannotOverwriteConflict(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	relPath := "chat/conflict.bin"
	shaHex := "2222222222222222222222222222222222222222222222222222222222222222"
	size := int64(4096)

	// Seed conflict job
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('103', 1, ?, ?, ?, 'conflict', 2, 0, '', ?, ?)
	`, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("insert conflict: %v", err)
	}

	// Recovery attempts to reset stale job to pending
	err = db.RecoverStaleArchiveJob("103", 1, "staleClaim")
	if err == nil {
		t.Fatal("expected error when recovering a conflict job to pending")
	}

	// Recovery attempts to complete conflict job
	err = db.RecoverArchiveJobComplete("103", 1, "staleClaim", shaHex)
	if err == nil {
		t.Fatal("expected error when recovering a conflict job to archived")
	}

	// Verify state is still conflict
	var state string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = '103' AND message_id = 1`).Scan(&state)
	if state != "conflict" {
		t.Fatalf("state must remain 'conflict', got %q", state)
	}
}

func TestArchive_DuplicateArchiveSuccess_Idempotent(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	relPath := "chat/file.iso"
	shaHex := "3333333333333333333333333333333333333333333333333333333333333333"
	size := int64(8192)

	// Seed copying job
	_, _ = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('104', 1, ?, ?, ?, 'copying', 1, 0, 'claimX', ?, ?)
	`, relPath, size, shaHex, now, now)

	// First complete: success
	err = db.CompleteArchiveJob("104", 1, "claimX", shaHex)
	if err != nil {
		t.Fatalf("first complete failed: %v", err)
	}

	// Duplicate complete with identical proof: idempotent success (nil err)
	err = db.CompleteArchiveJob("104", 1, "claimX", shaHex)
	if err != nil {
		t.Fatalf("duplicate complete with identical proof must return nil, got: %v", err)
	}

	// Duplicate complete with conflicting proof: error
	err = db.CompleteArchiveJob("104", 1, "claimX", "different_sha_here")
	if err == nil {
		t.Fatal("duplicate complete with conflicting proof must return error")
	}
}

func TestArchive_ActiveCopy_IdentityNotMutatedByDuplicateDownload(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	relPath1 := "chat/file1.mp4"
	shaHex1 := "4444444444444444444444444444444444444444444444444444444444444444"
	size1 := int64(5000)

	// Active copying job
	_, _ = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('105', 1, ?, ?, ?, 'copying', 1, 0, 'claimActive', ?, ?)
	`, relPath1, size1, shaHex1, now, now)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	// Conflicting duplicate download completion arrives
	relPath2 := "chat/file2.mp4"
	shaHex2 := "5555555555555555555555555555555555555555555555555555555555555555"
	size2 := int64(9000)

	err = db.ensureArchiveJobLocked(tx, "105", 1, relPath2, size2, shaHex2, now)
	if err == nil {
		t.Fatal("expected conflict error when duplicate download arrives with conflicting identity during active copy")
	}
	_ = tx.Rollback()

	// Verify active copying job was NOT mutated in DB
	var path, sha string
	var size int64
	_ = db.db.QueryRow(`SELECT relative_path, file_size, sha256 FROM archive_jobs WHERE chat_id = '105' AND message_id = 1`).Scan(&path, &size, &sha)
	// Query archive_jobs columns correctly
	_ = db.db.QueryRow(`SELECT relative_path, expected_size, sha256 FROM archive_jobs WHERE chat_id = '105' AND message_id = 1`).Scan(&path, &size, &sha)
	if path != relPath1 || sha != shaHex1 || size != size1 {
		t.Fatalf("active copying identity was corrupted! got path=%q size=%d sha=%q", path, size, sha)
	}
}

func TestArchive_SourceDeletion_OnlyAfterAcceptedArchived(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	downloadDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "hdd")
	_ = os.MkdirAll(downloadDir, 0o755)
	_ = os.MkdirAll(archiveDir, 0o755)

	worker, _ := NewArchiveWorker(db, downloadDir, archiveDir, zap.NewNop())

	relPath := "chat/safety.bin"
	ssdPath := filepath.Join(downloadDir, relPath)
	_ = os.MkdirAll(filepath.Dir(ssdPath), 0o755)
	payload := []byte("critical SSD source content")
	_ = os.WriteFile(ssdPath, payload, 0o644)
	h := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(h[:])

	now := time.Now().Unix()
	// Job is seeded with pending, but when claimed, use claimA
	_, _ = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, claim_id, created_at, updated_at)
		VALUES ('106', 1, ?, ?, ?, 'copying', 0, 0, 'claimA', ?, ?)
	`, relPath, int64(len(payload)), shaHex, now, now)

	// Worker processJob is called with an invalid/mismatched claimID "claimStale"
	job := ArchiveJob{
		ChatID:       "106",
		MessageID:    1,
		RelativePath: relPath,
		ExpectedSize: int64(len(payload)),
		SHA256:       shaHex,
		ClaimID:      "claimStale", // mismatch!
		State:        "copying",
	}

	worker.processJob(context.Background(), job)

	// Because DB rejected completion (stale claim), SSD source MUST NOT BE DELETED!
	if _, err := os.Stat(ssdPath); os.IsNotExist(err) {
		t.Fatal("SSD source was deleted even though CompleteArchiveJob failed!")
	}
}
