package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"

	"github.com/Hittlert/TGX/core/bucket"
	"github.com/Hittlert/TGX/core/mover"
	"github.com/Hittlert/TGX/core/targetwriter"
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

	sum := sha256.Sum256(data)
	proof := targetwriter.CommitProof{
		TaskID:       CanonicalTaskID("123", 1),
		Gen:          "1",
		FinalPath:    "test.mp4",
		ExpectedSize: 4,
		SHA256:       hex.EncodeToString(sum[:]),
		CommittedAt:  time.Now().Unix(),
	}
	proofData, _ := json.Marshal(proof)
	require.NoError(t, os.WriteFile(finalPath+".tgx_commit", proofData, 0644))

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

func TestReconciler_SSDBufferPartialRetainedForResume(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	// Write partial .part file in SSD tempDir using canonical naming
	taskID := CanonicalTaskID("123", 3)
	partPath := CanonicalPartPath(tempDir, taskID)
	require.NoError(t, os.WriteFile(partPath, make([]byte, 50), 0644))

	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 3, 'downloading', 'resume.mp4', 'resume.mp4', 100)`)
	require.NoError(t, err)

	r := NewReconcilerWithBuffer(db, outDir, tempDir, "ssd", nil, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "pending", results[0].NextState)
	assert.Equal(t, "SSD_BUFFER_PARTIAL_RETAINED_FOR_RESUME", results[0].ActionTaken)

	// Verify partial file was NOT deleted
	_, err = os.Stat(partPath)
	assert.NoError(t, err)
}

func TestReconciler_SSDBufferCompletedRequeuedInMover(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	// Write completed .part file in SSD tempDir using canonical naming
	taskID := CanonicalTaskID("123", 4)
	partPath := CanonicalPartPath(tempDir, taskID)
	testContent := []byte("completed-file-content")
	require.NoError(t, os.WriteFile(partPath, testContent, 0644))

	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 4, 'moving', 'completed.mp4', 'completed.mp4', ?)`, len(testContent))
	require.NoError(t, err)

	m := mover.New(1, 100*1024*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.Start(ctx)
	defer m.Close()

	r := NewReconcilerWithBuffer(db, outDir, tempDir, "ssd", m, zap.NewNop())
	results, err := r.ReconcileAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "moving", results[0].NextState)
	assert.Equal(t, "SSD_BUFFER_COMPLETED_REQUEUED_IN_MOVER", results[0].ActionTaken)

	// Wait for mover to finish moving
	require.Eventually(t, func() bool {
		var status string
		_ = db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '123' AND message_id = 4`).Scan(&status)
		return status == "success"
	}, 3*time.Second, 50*time.Millisecond)

	// Verify destination file exists
	finalPath := filepath.Join(outDir, "completed.mp4")
	data, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, testContent, data)
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

	// Orchestrator callback logic
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

func TestRecovery_PartialSidecarDoesNotRegisterPublishingTask(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	fileKey := CanonicalTaskID("123", 55)
	finalPath := filepath.Join(outDir, "partial.bin")
	movingPath := finalPath + ".moving"
	metaPath := finalPath + ".moving.meta"

	// Create incomplete .moving (size 500) and sidecar with ExpectedSize 1000 and range [0, 500)
	testContent := make([]byte, 500)
	require.NoError(t, os.WriteFile(movingPath, testContent, 0644))

	manifest := targetwriter.TaskManifest{
		Version:      targetwriter.SidecarVersion,
		TaskID:       fileKey,
		FinalPath:    "partial.bin",
		ExpectedSize: 1000,
		Gen:          "1",
		Ranges: []targetwriter.Range{
			{Start: 0, End: 500},
		},
	}
	metaBytes, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(metaPath, metaBytes, 0644))

	_, err = db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 55, 'downloading', 'partial.bin', 'partial.bin', 1000)`)
	require.NoError(t, err)

	reg := NewRegistry(10, 100, time.Now)
	reconciler := NewReconcilerWithBuffer(db, outDir, tempDir, "ssd", nil, zap.NewNop())
	reconciler.SetRegistry(reg)

	results, err := reconciler.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	assert.Equal(t, "pending", results[0].NextState)
	assert.Equal(t, "SSD_BUFFER_PARTIAL_RESET_TO_PENDING", results[0].ActionTaken)

	// Verify Registry has NO synthetic Publishing task left behind
	_, exists := reg.Task(fileKey)
	assert.False(t, exists)
	assert.Equal(t, 0, len(reg.Tasks()))

	// Verify pending scanner can cleanly submit real TaskRequest with 0 ID conflict
	realReq := TaskRequest{
		ID:           fileKey,
		Peer:         "123",
		MessageID:    55,
		FinalPath:    "partial.bin",
		ExpectedSize: 1000,
	}
	snap, ok, err := reg.Submit(realReq)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, StateQueued, snap.State)
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

func TestRecovery_MemoryModeRecoversTargetStorageSidecars(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	fileKey := CanonicalTaskID("123", 77)
	finalPath := filepath.Join(outDir, "complete.bin")
	movingPath := finalPath + ".moving"
	metaPath := finalPath + ".moving.meta"

	// Create complete .moving (size 1000) and sidecar with ExpectedSize 1000 and range [0, 1000)
	testContent := make([]byte, 1000)
	require.NoError(t, os.WriteFile(movingPath, testContent, 0644))

	manifest := targetwriter.TaskManifest{
		Version:      targetwriter.SidecarVersion,
		TaskID:       fileKey,
		FinalPath:    "complete.bin",
		ExpectedSize: 1000,
		Gen:          "1",
		Ranges: []targetwriter.Range{
			{Start: 0, End: 1000},
		},
	}
	metaBytes, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(metaPath, metaBytes, 0644))

	_, err = db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 77, 'moving', 'complete.bin', 'complete.bin', 1000)`)
	require.NoError(t, err)

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := targetwriter.New(bkt, outDir)
	reconciler := NewReconcilerWithBuffer(db, outDir, tempDir, "memory", nil, zap.NewNop())
	reconciler.SetTargetWriter(tw)

	results, err := reconciler.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	assert.Equal(t, "moving", results[0].NextState)
	assert.Equal(t, "SSD_BUFFER_COMPLETE_FINALIZE_PENDING", results[0].ActionTaken)

	// Verify .moving and .moving.meta were NOT destroyed in memory buffer mode
	_, err = os.Stat(movingPath)
	assert.NoError(t, err)
	_, err = os.Stat(metaPath)
	assert.NoError(t, err)
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

func TestRecovery_LegacyPart_ContentConflictDifferentSHA(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	fileKeyConflict := CanonicalTaskID("chat_conf", 1)
	finalPathConf := filepath.Join(outDir, "conf.bin")
	tempPartPathConf := CanonicalPartPath(tempDir, fileKeyConflict)

	// Create target and temp part with SAME SIZE but DIFFERENT CONTENT
	partData := []byte("part-data-12345")
	diffData := []byte("diff-data-67890")
	require.Equal(t, len(partData), len(diffData))

	require.NoError(t, os.WriteFile(finalPathConf, diffData, 0644))
	require.NoError(t, os.WriteFile(tempPartPathConf, partData, 0644))

	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('chat_conf', 1, 'moving', 'conf.bin', 'conf.bin', ?)`, len(partData))
	require.NoError(t, err)

	rec := NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := rec.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Result should be reset to pending because SHAs do not match
	assert.Equal(t, "pending", results[0].NextState)
	assert.Equal(t, "LEGACY_PART_RESET_TO_PENDING", results[0].ActionTaken)

	// Temp part must NOT have been deleted
	_, err = os.Stat(tempPartPathConf)
	assert.NoError(t, err)

	// Now test identical SHA:
	fileKeyIdentical := CanonicalTaskID("chat_ident", 2)
	finalPathIdent := filepath.Join(outDir, "ident.bin")
	tempPartPathIdent := CanonicalPartPath(tempDir, fileKeyIdentical)

	require.NoError(t, os.WriteFile(finalPathIdent, partData, 0644))
	require.NoError(t, os.WriteFile(tempPartPathIdent, partData, 0644))

	_, err = db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('chat_ident', 2, 'moving', 'ident.bin', 'ident.bin', ?)`, len(partData))
	require.NoError(t, err)

	results, err = rec.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, fileKeyIdentical, results[0].FileKey)
	assert.Equal(t, "success", results[0].NextState)
	assert.Equal(t, "LEGACY_PART_TARGET_ALREADY_EXISTS_PROMOTED", results[0].ActionTaken)

	// Temp part should be cleaned up on verified identical content
	_, err = os.Stat(tempPartPathIdent)
	assert.True(t, os.IsNotExist(err))
}

func TestReconciler_RejectsSameSizeWrongHashFile(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir := t.TempDir()
	tempDir := t.TempDir()

	fileKey := CanonicalTaskID("chat_wrong", 1)
	finalPath := filepath.Join(outDir, "wrong.bin")

	// Final file has different content with the same length
	require.NoError(t, os.WriteFile(finalPath, []byte("bad content 1234"), 0644))

	// Commit proof specifies a different SHA
	proof := targetwriter.CommitProof{
		TaskID:       fileKey,
		Gen:          "1",
		FinalPath:    "wrong.bin",
		ExpectedSize: 16,
		SHA256:       "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		CommittedAt:  time.Now().Unix(),
	}
	proofData, _ := json.Marshal(proof)
	require.NoError(t, os.WriteFile(finalPath+".tgx_commit", proofData, 0644))

	_, err := db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('chat_wrong', 1, 'downloading', 'wrong.bin', 'wrong.bin', 16)`)
	require.NoError(t, err)

	rec := NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := rec.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Must NOT promote to success because proof SHA does not match actual file SHA!
	assert.Equal(t, "pending", results[0].NextState)
}
