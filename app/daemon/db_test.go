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

// 4a. Cancel during downloading cannot be overwritten by late commit/complete.
func TestDB_CancelDuringDownloadingCannotBeOverwritten(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 104
	gen := "gen_1"

	// Begin download
	if err := db.BeginDownload(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}

	// User cancels task while worker is still downloading
	if err := db.CancelDownload(chatID, msgID, gen, "user canceled"); err != nil {
		t.Fatalf("CancelDownload failed: %v", err)
	}

	// Canceled worker tries to prepare commit -> rejected!
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when preparing commit on canceled task, got: %v", err)
	}

	// Canceled worker tries to complete -> rejected!
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

// 4b. Once PrepareDownloadCommit succeeds (status=committing), durable publish intent is established; late cancel is rejected.
func TestDB_CancelDuringCommit_RejectedDueToAuthoritativePublishIntent(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 1042
	gen := "gen_1"

	if err := db.BeginDownload(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc"); err != nil {
		t.Fatalf("PrepareDownloadCommit failed: %v", err)
	}

	// Late cancel must be rejected by DB to protect publishing window
	err := db.CancelDownload(chatID, msgID, gen, "late cancel")
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when canceling committing task, got: %v", err)
	}

	// Worker proceeds to complete -> succeeds!
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "aabbcc", false); err != nil {
		t.Fatalf("CompleteDownloadAndQueueArchive failed: %v", err)
	}

	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil {
		t.Fatalf("failed to get record: %v", err)
	}
	if rec.Status != "success" {
		t.Fatalf("expected status success, got: %s", rec.Status)
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

// 8. Idempotent success checks SHA, path, and size (full three-way proof).
func TestDB_IdempotentSuccess_RejectsConflictingPathOrSize(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 109
	gen := "gen_1"

	// Transition to success with genuine proof
	if err := db.BeginDownload(chatID, msgID, gen, "test.mp4", "path/test.mp4", "video", 1024); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "path/test.mp4", 1024, "hash123"); err != nil {
		t.Fatalf("PrepareDownloadCommit failed: %v", err)
	}
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "path/test.mp4", 1024, "hash123", false); err != nil {
		t.Fatalf("CompleteDownloadAndQueueArchive failed: %v", err)
	}

	// 1. PrepareDownloadCommit called with same SHA but conflicting path must return ErrStateConflict
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "conflicting/path.mp4", 1024, "hash123"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when PrepareDownloadCommit has conflicting path on success, got: %v", err)
	}

	// 2. PrepareDownloadCommit called with same SHA but conflicting size must return ErrStateConflict
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "path/test.mp4", 2048, "hash123"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when PrepareDownloadCommit has conflicting size on success, got: %v", err)
	}

	// 3. CompleteExistingDownload called with same SHA but conflicting path must return ErrStateConflict
	if err := db.CompleteExistingDownload(chatID, msgID, "gen_2", "conflicting/path.mp4", 1024, "hash123", false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when CompleteExistingDownload has conflicting path on success, got: %v", err)
	}

	// 4. CompleteExistingDownload called with same SHA but conflicting size must return ErrStateConflict
	if err := db.CompleteExistingDownload(chatID, msgID, "gen_2", "path/test.mp4", 2048, "hash123", false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when CompleteExistingDownload has conflicting size on success, got: %v", err)
	}

	// 5. CompleteExistingDownload called with identical proof must succeed (idempotent)
	if err := db.CompleteExistingDownload(chatID, msgID, "gen_2", "path/test.mp4", 1024, "hash123", false); err != nil {
		t.Fatalf("expected identical CompleteExistingDownload on success to succeed, got: %v", err)
	}
}

// 9. BeginDownload returns authoritative proof on matching success and rejects conflicts / missing proofs.
func TestDB_BeginDownload_AuthoritativeProofAndGuards(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 110
	gen := "gen_1"

	// Complete to success with valid proof
	if err := db.BeginDownload(chatID, msgID, gen, "clip.mp4", "videos/clip.mp4", "video", 5000); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}
	if err := db.PrepareDownloadCommit(chatID, msgID, gen, "videos/clip.mp4", 5000, "sha_5000"); err != nil {
		t.Fatalf("PrepareDownloadCommit failed: %v", err)
	}
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, "videos/clip.mp4", 5000, "sha_5000", false); err != nil {
		t.Fatalf("CompleteDownloadAndQueueArchive failed: %v", err)
	}

	// 1. Calling BeginDownload with matching proof returns AlreadySuccessError containing the proof
	beginErr := db.BeginDownload(chatID, msgID, "gen_2", "clip.mp4", "videos/clip.mp4", "video", 5000)
	if !errors.Is(beginErr, ErrAlreadySuccess) {
		t.Fatalf("expected ErrAlreadySuccess, got: %v", beginErr)
	}
	var alreadyErr *AlreadySuccessError
	if !errors.As(beginErr, &alreadyErr) {
		t.Fatalf("expected AlreadySuccessError, got: %T", beginErr)
	}
	if alreadyErr.Proof.SavePath != "videos/clip.mp4" || alreadyErr.Proof.FileSize != 5000 || alreadyErr.Proof.SHA256 != "sha_5000" {
		t.Fatalf("unexpected proof: %+v", alreadyErr.Proof)
	}

	// 2. Calling BeginDownload with conflicting path rejects with ErrStateConflict
	confPathErr := db.BeginDownload(chatID, msgID, "gen_3", "clip.mp4", "other/clip.mp4", "video", 5000)
	if !errors.Is(confPathErr, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict for conflicting path, got: %v", confPathErr)
	}

	// 3. Calling BeginDownload with conflicting size rejects with ErrStateConflict
	confSizeErr := db.BeginDownload(chatID, msgID, "gen_4", "clip.mp4", "videos/clip.mp4", "video", 9999)
	if !errors.Is(confSizeErr, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict for conflicting size, got: %v", confSizeErr)
	}

	// 4. Missing proof on success row (e.g. empty sha256) is rejected as ErrStateConflict, not treated as wildcard
	_, err := db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, sha256, attempt_generation, created_at, updated_at)
		VALUES ('-100999', 1, 'success', 'path/legacy.bin', 100, '', 'gen_leg', 1000, 1000)
	`)
	if err != nil {
		t.Fatalf("insert legacy row failed: %v", err)
	}
	legErr := db.BeginDownload("-100999", 1, "gen_new", "legacy.bin", "path/legacy.bin", "document", 100)
	if !errors.Is(legErr, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when existing success record is missing SHA proof, got: %v", legErr)
	}
}

// 10. CompleteExistingDownload rejects success rows that lack required proof.
func TestDB_CompleteExistingDownload_MissingProofSuccessRejected(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Seed success row missing SHA
	_, err := db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, sha256, attempt_generation, created_at, updated_at)
		VALUES ('-100888', 2, 'success', 'path/empty_sha.bin', 200, '', 'gen_old', 1000, 1000)
	`)
	if err != nil {
		t.Fatalf("insert row failed: %v", err)
	}

	err = db.CompleteExistingDownload("-100888", 2, "gen_new", "path/empty_sha.bin", 200, "new_sha", false)
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when existing success row has empty SHA proof, got: %v", err)
	}
}

// 11. CancelDownload transitions matching generation and rejects stale or success rows.
func TestDB_CancelDownload_LifecycleAndGuards(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 111
	gen := "gen_1"

	// 1. Begin download (status = downloading, generation = gen_1)
	if err := db.BeginDownload(chatID, msgID, gen, "cancel_test.mp4", "", "video", 0); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}

	// 2. Cancel with mismatching generation is rejected
	if err := db.CancelDownload(chatID, msgID, "gen_wrong", "user cancel"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict for mismatching generation, got: %v", err)
	}

	// 3. Cancel with matching generation succeeds and sets status to failed
	if err := db.CancelDownload(chatID, msgID, gen, "target disabled"); err != nil {
		t.Fatalf("CancelDownload failed: %v", err)
	}
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil || rec.Status != "failed" {
		t.Fatalf("expected status failed after cancel, got: %+v, err: %v", rec, err)
	}

	// 4. Cancel on completed success record is rejected
	msgSuccess := 112
	if err := db.BeginDownload(chatID, msgSuccess, gen, "success.mp4", "path/success.mp4", "video", 100); err != nil {
		t.Fatalf("BeginDownload failed: %v", err)
	}
	if err := db.PrepareDownloadCommit(chatID, msgSuccess, gen, "path/success.mp4", 100, "sha_ok"); err != nil {
		t.Fatalf("PrepareDownloadCommit failed: %v", err)
	}
	if err := db.CompleteDownloadAndQueueArchive(chatID, msgSuccess, gen, "path/success.mp4", 100, "sha_ok", false); err != nil {
		t.Fatalf("CompleteDownloadAndQueueArchive failed: %v", err)
	}
	if err := db.CancelDownload(chatID, msgSuccess, gen, "cancel late"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict when cancelling already completed success record, got: %v", err)
	}
}

// 12. BeginDownload updates metadata for same-generation calls without silent no-op.
func TestDB_BeginDownload_UpdatesMetadataForSameGeneration(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	chatID := "-1001234567"
	msgID := 113
	gen := "gen_planned"

	// 1. First call in admission stage (path empty, size 0)
	if err := db.BeginDownload(chatID, msgID, gen, "", "", "", 0); err != nil {
		t.Fatalf("initial BeginDownload failed: %v", err)
	}

	// 2. Second call after PathPlanner (canonical path and size provided)
	plannedPath := "Channel/2026_09/113 - movie.mp4"
	if err := db.BeginDownload(chatID, msgID, gen, "movie.mp4", plannedPath, "video", 8192); err != nil {
		t.Fatalf("second BeginDownload failed: %v", err)
	}

	// 3. Assert DB record now has updated metadata
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("GetDownloadRecord failed: %v", err)
	}
	if rec.SavePath != plannedPath {
		t.Fatalf("expected SavePath %q, got: %q", plannedPath, rec.SavePath)
	}
	if rec.FileSize != 8192 {
		t.Fatalf("expected FileSize 8192, got: %d", rec.FileSize)
	}
	if rec.FileName != "movie.mp4" {
		t.Fatalf("expected FileName 'movie.mp4', got: %q", rec.FileName)
	}
}

