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
	claimed, err := db.ClaimArchiveJob("12345", 1)
	if err != nil || !claimed {
		t.Fatalf("claim failed: claimed=%v, err=%v", claimed, err)
	}

	jobs, _ := db.GetDueArchiveJobs(1)
	job := ArchiveJob{
		ChatID:       "12345",
		MessageID:    1,
		RelativePath: relPath,
		ExpectedSize: int64(len(payload)),
		SHA256:       shaHex,
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
	_ = jobs
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
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, created_at, updated_at)
		VALUES ('999', 42, ?, ?, 'fake_sha', 'pending', 0, 0, ?, ?)
	`, relPath, int64(len("ssd source data")), now, now)

	job := ArchiveJob{
		ChatID:       "999",
		MessageID:    42,
		RelativePath: relPath,
		ExpectedSize: int64(len("ssd source data")),
		SHA256:       "fake_sha",
		State:        "copying",
	}
	_, _ = db.ClaimArchiveJob("999", 42)

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
