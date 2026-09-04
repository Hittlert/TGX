package daemon

import (
	"errors"
	"path/filepath"
	"testing"
)

func setupTestDB(t *testing.T) (*Database, func()) {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	cleanup := func() {
		_ = db.Close()
	}
	return db, cleanup
}

// 1. Failed -> success is rejected.
func TestDB_FailedToSuccess_Rejected(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 101
	gen := "gen_1"

	// Create a failed record
	disp := FailureDisposition{
		Stage:       "transfer",
		Op:          "download",
		Class:       "network",
		Unavailable: false,
		Retryable:   true,
		Message:     "connection reset",
	}
	if err := db.FailDownloadDisposition(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024, disp); err != nil {
		t.Fatalf("failed to insert failed record: %v", err)
	}

	// Attempting direct completion from failed must be strictly rejected
	err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", false)
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when completing failed record, got: %v", err)
	}

	// Verify record status is still failed
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil {
		t.Fatalf("failed to get record: %v", err)
	}
	if rec.Status != "failed" {
		t.Fatalf("expected status failed, got: %s", rec.Status)
	}
}

// 2. Unavailable -> success is rejected.
func TestDB_UnavailableToSuccess_Rejected(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 102
	gen := "gen_1"

	// Create an unavailable record
	disp := FailureDisposition{
		Stage:       "resolve",
		Op:          "get_message",
		Class:       "unavailable",
		Unavailable: true,
		Retryable:   false,
		Message:     "message deleted",
	}
	if err := db.FailDownloadDisposition(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024, disp); err != nil {
		t.Fatalf("failed to insert unavailable record: %v", err)
	}

	// BeginDownload must be rejected for unavailable record
	if err := db.BeginDownload(chatID, msgID, "gen_2", "test.mp4", "path/test.mp4", "video", 1024); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict on BeginDownload for unavailable record, got: %v", err)
	}

	// CompleteDownload must be rejected for unavailable record
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict on CompleteDownload for unavailable record, got: %v", err)
	}

	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil {
		t.Fatalf("failed to get record: %v", err)
	}
	if rec.Status != "unavailable" {
		t.Fatalf("expected status unavailable, got: %s", rec.Status)
	}
}

// 3. Old attempt A cannot complete after retry B becomes current.
func TestDB_StaleAttemptCannotComplete(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 103
	genA := "gen_A"
	genB := "gen_B"

	// Attempt A begins
	if err := db.BeginDownload(chatID, msgID, genA, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload genA failed: %v", err)
	}

	// Attempt A fails transiently
	disp := FailureDisposition{
		Stage:       "transfer",
		Op:          "chunk",
		Class:       "network",
		Unavailable: false,
		Retryable:   true,
		Message:     "timeout",
	}
	if err := db.FailDownloadDisposition(chatID, msgID, genA, "test.mp4", "path/test.mp4", "video", 1024, disp); err != nil {
		t.Fatalf("FailDownload genA failed: %v", err)
	}

	// Attempt B begins with new generation
	if err := db.BeginDownload(chatID, msgID, genB, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload genB failed: %v", err)
	}

	// Old attempt A tries to prepare commit -> rejected!
	if err := db.PrepareDownloadCommit(chatID, msgID, genA, "path/test.mp4", 1024, "aabbcc"); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("expected ErrStaleAttempt for stale genA PrepareCommit, got: %v", err)
	}

	// Old attempt A tries to complete -> rejected!
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, genA, "path/test.mp4", 1024, "aabbcc", false); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("expected ErrStaleAttempt for stale genA CompleteDownload, got: %v", err)
	}

	// Attempt B prepares commit -> succeeds!
	if err := db.PrepareDownloadCommit(chatID, msgID, genB, "path/test.mp4", 1024, "aabbcc"); err != nil {
		t.Fatalf("expected genB PrepareCommit to succeed, got: %v", err)
	}

	// Attempt B completes -> succeeds!
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, genB, "path/test.mp4", 1024, "aabbcc", false); err != nil {
		t.Fatalf("expected genB CompleteDownload to succeed, got: %v", err)
	}
}

// 4. Cancel during Sync/Hash/rename cannot be overwritten by the canceled attempt.
func TestDB_CancelDuringCommitCannotBeOverwritten(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 104
	gen := "gen_1"

	// Begin download and prepare commit
	if err := db.BeginDownload(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc"); err != nil {
		t.Fatalf("PrepareDownloadCommit failed: %v", err)
	}

	// User cancels task while worker is computing hash
	if err := db.CancelDownload(chatID, msgID, gen, "user canceled"); err != nil {
		t.Fatalf("CancelDownload failed: %v", err)
	}

	// Canceled worker finishes hash and tries to complete -> rejected!
	err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", false)
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when completing canceled task, got: %v", err)
	}

	// Status must remain failed
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil {
		t.Fatalf("failed to get record: %v", err)
	}
	if rec.Status != "failed" {
		t.Fatalf("expected status failed, got: %s", rec.Status)
	}
}

// 5. Wrong generation and wrong path/size/SHA are rejected before terminal mutation.
func TestDB_WrongProof_Rejected(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 105
	gen := "gen_1"

	if err := db.BeginDownload(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc"); err != nil {
		t.Fatalf("PrepareDownloadCommit failed: %v", err)
	}

	// Wrong SHA
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "wrong_sha", false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict on wrong SHA, got: %v", err)
	}

	// Wrong size
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 2048, "aabbcc", false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict on wrong size, got: %v", err)
	}

	// Wrong path
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "other/path.mp4", 1024, "aabbcc", false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict on wrong path, got: %v", err)
	}

	// Correct proof succeeds
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", false); err != nil {
		t.Fatalf("expected correct proof to succeed, got: %v", err)
	}
}

// 6. Duplicate completion with identical current proof is idempotent.
func TestDB_DuplicateCompletion_Idempotent(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 106
	gen := "gen_1"

	if err := db.BeginDownload(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc"); err != nil {
		t.Fatalf("PrepareDownloadCommit failed: %v", err)
	}
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", true); err != nil {
		t.Fatalf("CompleteDownloadAndQueueArchive failed: %v", err)
	}

	// Duplicate call with identical proof must return nil
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", true); err != nil {
		t.Fatalf("expected duplicate completion to be idempotent, got: %v", err)
	}

	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil {
		t.Fatalf("failed to get record: %v", err)
	}
	if rec.Status != "success" {
		t.Fatalf("expected status success, got: %s", rec.Status)
	}
}

// 7. Existing file recovery uses explicit API and rejects DB conflicts.
func TestDB_CompleteExistingDownload_Guards(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 107
	gen := "gen_1"

	// A failed record cannot be directly transitioned by CompleteExistingDownload
	disp := FailureDisposition{
		Stage:       "transfer",
		Op:          "download",
		Class:       "corrupt",
		Unavailable: false,
		Message:     "corrupted",
	}
	if err := db.FailDownloadDisposition(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024, disp); err != nil {
		t.Fatalf("FailDownloadDisposition failed: %v", err)
	}

	// CompleteExistingDownload rejects completing from failed status
	if err := db.CompleteExistingDownload(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when completing existing download for failed record, got: %v", err)
	}

	// New record with no prior conflict succeeds
	if err := db.CompleteExistingDownload("-1001234567", 108, "gen_new", "path/new.mp4", 2048, "ddeeff", false); err != nil {
		t.Fatalf("expected CompleteExistingDownload on fresh record to succeed, got: %v", err)
	}
}
