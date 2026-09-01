package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		CREATE TABLE download_records (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			file_name TEXT,
			save_path TEXT,
			media_type TEXT,
			file_size INTEGER,
			error TEXT,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_retry_at INTEGER NOT NULL DEFAULT 0,
			downloaded_at INTEGER,
			created_at INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (chat_id, message_id)
		);
	`)
	require.NoError(t, err)
	return db
}

func TestReconciler_PromotesExistingFileToSuccess(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	// Final file exists with exact size
	finalPath := filepath.Join(outDir, "test.mp4")
	data := []byte("data")
	require.NoError(t, os.WriteFile(finalPath, data, 0644))

	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 1, 'downloading', 'test.mp4', 'test.mp4', 4)`)
	require.NoError(t, err)

	r := NewReconcilerWithBuffer(db, outDir, tempDir, "memory", nil, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "success", results[0].NextState)
	assert.Equal(t, "FINAL_FILE_COMMITTED_PROMOTED_TO_SUCCESS", results[0].ActionTaken)

	var newStatus string
	err = db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '123' AND message_id = 1`).Scan(&newStatus)
	require.NoError(t, err)
	assert.Equal(t, "success", newStatus)
}

func TestReconciler_MemoryBufferVolatileReset(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 2, 'downloading', 'missing.mp4', 'missing.mp4', 100)`)
	require.NoError(t, err)

	r := NewReconcilerWithBuffer(db, outDir, tempDir, "memory", nil, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "pending", results[0].NextState)
	assert.Equal(t, "MEMORY_BUFFER_VOLATILE_RESET_TO_PENDING", results[0].ActionTaken)

	var newStatus string
	err = db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '123' AND message_id = 2`).Scan(&newStatus)
	require.NoError(t, err)
	assert.Equal(t, "pending", newStatus)
}

func TestTask_StaleAttemptCannotTerminateNewAttempt(t *testing.T) {
	r := NewRegistry(10, 100, time.Now)
	req := TaskRequest{ID: "chat1:100", Peer: "chat1", MessageID: 100, FinalPath: "chat1/file.bin", ExpectedSize: 1024}

	// 1. Submit first attempt (Gen = "1")
	_, ok, err := r.Submit(req)
	require.NoError(t, err)
	require.True(t, ok)

	task1, err := r.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "1", task1.AttemptGen())

	// Simulate task1 failure
	task1.Fail("test_error", "network drop", false)
	snap1 := task1.Snapshot()
	assert.Equal(t, StateFailed, snap1.State)

	// 2. Retry task -> creates new attempt with Gen = "retry_..."
	req.Retry = true
	_, ok, err = r.Submit(req)
	require.NoError(t, err)
	require.True(t, ok)

	task2, err := r.Next(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, "1", task2.AttemptGen())
	assert.Equal(t, StateResolving, task2.Snapshot().State)

	// 3. Late sibling call from old task1 object: should be rejected
	task1.Succeed("some/path", false)

	// Verify task2 was NOT terminated by stale task1 call
	snap2 := task2.Snapshot()
	assert.Equal(t, StateResolving, snap2.State)
	assert.False(t, task2.IsTerminal())
}

func TestTask_ImmutableAttemptPointers(t *testing.T) {
	r := NewRegistry(10, 100, time.Now)
	req1 := TaskRequest{ID: "chat1:100", Peer: "chat1", MessageID: 100, FinalPath: "chat1/file1.bin", ExpectedSize: 1024}

	// 1. Submit first attempt
	_, ok, err := r.Submit(req1)
	require.NoError(t, err)
	require.True(t, ok)

	task1, err := r.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "1", task1.AttemptGen())

	// Fail attempt 1
	task1.Fail("error", "net drop", false)
	require.True(t, task1.IsTerminal())
	require.Error(t, task1.Context().Err())

	// 2. Submit retry
	req1.Retry = true
	_, ok, err = r.Submit(req1)
	require.NoError(t, err)
	require.True(t, ok)

	task2, err := r.Next(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, "1", task2.AttemptGen())
	require.NoError(t, task2.Context().Err())

	// 3. Verify task1 still holds old canceled context and old request
	require.Error(t, task1.Context().Err())
	require.Equal(t, StateFailed, task1.Snapshot().State)
	require.Equal(t, StateResolving, task2.Snapshot().State)
}

func TestOrchestrator_StartupRecoveryCallbackUpdatesDB(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()

	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 99, 'moving', 'rec.bin', 'rec.bin', 1024)`)
	require.NoError(t, err)

	r := NewRegistry(10, 100, time.Now)
	// Register recovered task into Registry
	r.RegisterRecoveredTask("123:99", "1", "rec.bin", 1024)

	// Callback logic
	completeCallback := func(taskID, gen, finalPath, shaHash string) {
		res := r.FinishTask(taskID, gen, StateSuccess, "", "", finalPath, false, shaHash)
		if res == FinishAcceptedNewTerminal {
			parts := strings.Split(taskID, ":")
			if len(parts) == 2 {
				_, _ = db.Exec(`UPDATE download_records SET status = 'success' WHERE chat_id = ? AND message_id = ?`, parts[0], 99)
			}
		}
	}

	completeCallback("123:99", "1", filepath.Join(outDir, "rec.bin"), "dummy_sha")

	var status string
	err = db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '123' AND message_id = 99`).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "success", status)
}

func TestOrchestrator_RejectsConflictingAndStaleCallbacks(t *testing.T) {
	r := NewRegistry(10, 100, time.Now)
	req := TaskRequest{ID: "chat1:50", Peer: "chat1", MessageID: 50, FinalPath: "file.bin", ExpectedSize: 100}
	_, _, _ = r.Submit(req)
	_, _ = r.Next(context.Background())

	// 1. Stale generation callback: rejected
	res := r.FinishTask("chat1:50", "stale_gen", StateSuccess, "", "", "file.bin", false, "sha")
	assert.Equal(t, FinishRejectedStale, res)

	// 2. Valid first terminal callback: accepted
	res = r.FinishTask("chat1:50", "1", StateSuccess, "", "", "file.bin", false, "sha")
	assert.Equal(t, FinishAcceptedNewTerminal, res)

	// 3. Duplicate same terminal callback: already same terminal
	res = r.FinishTask("chat1:50", "1", StateSuccess, "", "", "file.bin", false, "sha")
	assert.Equal(t, FinishAlreadySameTerminal, res)

	// 4. Conflicting callback: conflicting terminal
	res = r.FinishTask("chat1:50", "1", StateFailed, "err", "msg", "", false, "")
	assert.Equal(t, FinishConflictingTerminal, res)

	// 5. Unknown task: not found
	res = r.FinishTask("unknown:999", "1", StateSuccess, "", "", "file.bin", false, "sha")
	assert.Equal(t, FinishNotFound, res)
}

func TestRecovery_UnfreezesCanceledFailedTasks(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	// Insert record that failed due to shutdown cancellation with attempts=4 and next_retry_at 7 days out
	futureRetry := time.Now().Add(7 * 24 * time.Hour).Unix()
	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size, error, attempts, next_retry_at) VALUES ('-1002313319912', 101, 'failed', 'video.mp4', 'video.mp4', 50000000, 'context canceled', 4, ?)`, futureRetry)
	require.NoError(t, err)

	reconciler := NewReconcilerWithBuffer(db, outDir, tempDir, "memory", nil, zap.NewNop())
	results, err := reconciler.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	assert.Equal(t, "pending", results[0].NextState)

	// Verify DB record is reset to pending with attempts=0 and next_retry_at=0
	var status string
	var attempts int
	var nextRetry int64
	err = db.QueryRow(`SELECT status, attempts, next_retry_at FROM download_records WHERE chat_id = '-1002313319912' AND message_id = 101`).Scan(&status, &attempts, &nextRetry)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)
	assert.Equal(t, 0, attempts)
	assert.Equal(t, int64(0), nextRetry)
}

func TestDatabase_UpdateDownloadStatus_ContextCanceledDoesNotIncrementAttemptsOrFreeze(t *testing.T) {
	rawDB := setupTestDB(t)
	defer rawDB.Close()

	d := &Database{db: rawDB}

	// Insert initial record
	_, err := rawDB.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size, attempts) VALUES ('chat1', 10, 'downloading', 'file.bin', 'file.bin', 1024, 0)`)
	require.NoError(t, err)

	// Simulate shutdown / context cancellation
	err = d.UpdateDownloadStatus("chat1", 10, "failed", "file.bin", "file.bin", "", 1024, "context canceled")
	require.NoError(t, err)

	// Verify status became 'pending', attempts remains 0, next_retry_at is 0
	var status string
	var attempts int
	var nextRetry int64
	err = rawDB.QueryRow(`SELECT status, attempts, next_retry_at FROM download_records WHERE chat_id = 'chat1' AND message_id = 10`).Scan(&status, &attempts, &nextRetry)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)
	assert.Equal(t, 0, attempts)
	assert.Equal(t, int64(0), nextRetry)
}

func TestVerifyFinalFileIdentity_CalculatesSHA256(t *testing.T) {
	tempDir := t.TempDir()
	finalPath := filepath.Join(tempDir, "verified.bin")
	data := []byte("hello world 1234567890")
	require.NoError(t, os.WriteFile(finalPath, data, 0644))

	sum := sha256.Sum256(data)
	expectedSHA := hex.EncodeToString(sum[:])

	// 1. Exact size and empty expectedSHA -> returns computed SHA
	sha, err := verifyFinalFileIdentity(finalPath, int64(len(data)), "", "task-1")
	assert.NoError(t, err)
	assert.Equal(t, expectedSHA, sha)

	// 2. Exact size and matching expectedSHA -> returns computed SHA
	sha, err = verifyFinalFileIdentity(finalPath, int64(len(data)), expectedSHA, "task-1")
	assert.NoError(t, err)
	assert.Equal(t, expectedSHA, sha)

	// 3. Mismatching expectedSHA -> returns error
	_, err = verifyFinalFileIdentity(finalPath, int64(len(data)), "wrong_sha", "task-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "content conflict")

	// 4. Size mismatch -> returns error
	_, err = verifyFinalFileIdentity(finalPath, int64(len(data))+1, "", "task-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "size mismatch")
}
