package targetwriter

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Hittlert/TGX/core/bucket"
)

func TestTargetWriter_OutOfOrderAndContiguous(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 50 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	completeChan := make(chan string, 1)
	tw.SetCallbacks(func(taskID, gen, finalPath, shaHash string) {
		completeChan <- finalPath
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	// 1. Prepare 3 chunks for a 3MB file
	totalSize := int64(3 * 1024 * 1024)
	fullData := make([]byte, totalSize)
	_, _ = rand.Read(fullData)

	manifest := TaskManifest{
		TaskID:       "task-oo",
		FinalPath:    "Videos/movie.mp4",
		ExpectedSize: totalSize,
		Gen:          "g1",
	}
	tw.RegisterTask(manifest)

	chunkSize := int64(1024 * 1024)

	// 2. Put chunks in OUT-OF-ORDER: Chunk 1 (offset 1MB), then Chunk 0 (offset 0), then Chunk 2 (offset 2MB)
	key1 := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "g1", Offset: 1 * chunkSize, Length: chunkSize}
	require.NoError(t, bkt.Reserve(ctx, chunkSize))
	require.NoError(t, bkt.PutObject(key1, fullData[1*chunkSize:2*chunkSize]))

	key0 := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "g1", Offset: 0, Length: chunkSize}
	require.NoError(t, bkt.Reserve(ctx, chunkSize))
	require.NoError(t, bkt.PutObject(key0, fullData[0:chunkSize]))

	key2 := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "g1", Offset: 2 * chunkSize, Length: chunkSize}
	require.NoError(t, bkt.Reserve(ctx, chunkSize))
	require.NoError(t, bkt.PutObject(key2, fullData[2*chunkSize:3*chunkSize]))

	// 3. Wait for TargetWriter to process all chunks and finalize file
	select {
	case finalRel := <-completeChan:
		assert.Equal(t, manifest.FinalPath, finalRel)
	case <-time.After(3 * time.Second):
		t.Fatal("target writer did not complete task in time")
	}

	// 4. Verify physical destination file content
	finalPath := filepath.Join(outDir, manifest.FinalPath)
	content, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fullData, content))

	// 5. Verify metrics
	m := tw.Metrics()
	assert.Equal(t, totalSize, m.TotalBytesWritten)
	assert.Empty(t, m.LastError)
}

func TestTargetWriter_LeftoverObjectAfterComplete(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	completeChan := make(chan string, 1)
	tw.SetCallbacks(func(taskID, gen, finalPath, shaHash string) {
		completeChan <- finalPath
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	totalSize := int64(1024 * 1024)
	data := make([]byte, totalSize)
	_, _ = rand.Read(data)

	manifest := TaskManifest{
		TaskID:       "task-completed",
		FinalPath:    "doc.bin",
		ExpectedSize: totalSize,
		Gen:          "1",
	}
	tw.RegisterTask(manifest)

	key0 := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "1", Offset: 0, Length: totalSize}
	require.NoError(t, bkt.Reserve(ctx, totalSize))
	require.NoError(t, bkt.PutObject(key0, data))

	select {
	case <-completeChan:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not complete in time")
	}

	// Now put a leftover object for the completed task
	leftoverData := make([]byte, 512)
	keyLeftover := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "1", Offset: 0, Length: int64(len(leftoverData))}
	require.NoError(t, bkt.Reserve(ctx, keyLeftover.Length))
	require.NoError(t, bkt.PutObject(keyLeftover, leftoverData))

	// Leftover object should be consumed and deleted by TargetWriter without blocking
	require.Eventually(t, func() bool {
		m := bkt.Metrics()
		return m.ReadyBytes == 0 && m.PendingDeleteBytes == 0
	}, 2*time.Second, 20*time.Millisecond)
}

func TestTargetWriter_ContentConflict(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	// Pre-create destination file with different content
	finalPath := filepath.Join(outDir, "conflict.bin")
	require.NoError(t, os.WriteFile(finalPath, []byte("existing-different-content"), 0644))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	errChan := make(chan error, 1)
	tw.SetCallbacks(nil, nil, func(taskID, gen string, err error) {
		errChan <- err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	data := []byte("new-different-content-12345")
	manifest := TaskManifest{
		TaskID:       "task-conflict",
		FinalPath:    "conflict.bin",
		ExpectedSize: int64(len(data)),
		Gen:          "1",
	}
	tw.RegisterTask(manifest)

	key := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "1", Offset: 0, Length: int64(len(data))}
	require.NoError(t, bkt.Reserve(ctx, key.Length))
	require.NoError(t, bkt.PutObject(key, data))

	select {
	case err := <-errChan:
		require.ErrorIs(t, err, ErrContentConflict)
	case <-time.After(3 * time.Second):
		t.Fatal("expected content conflict error callback")
	}
}

func TestTargetWriter_RegisterTaskRejectsOlderGenerationRollback(t *testing.T) {
	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	outDir := t.TempDir()
	tw := New(bkt, outDir)

	manifestNew := TaskManifest{
		TaskID:       "task-tw-rollback",
		FinalPath:    "file.bin",
		ExpectedSize: 1024,
		Gen:          "retry_2000",
		Ranges: []Range{
			{Start: 0, End: 512},
		},
	}
	tw.RegisterTask(manifestNew)

	// Verify bitmap has range [0, 512)
	bm, ok := tw.TaskBitmap("task-tw-rollback")
	require.True(t, ok)
	assert.Equal(t, int64(512), bm.DurableBytes())

	// Stale resolver attempts to register older generation "1" or "retry_1000" with empty range
	manifestOld := TaskManifest{
		TaskID:       "task-tw-rollback",
		FinalPath:    "file.bin",
		ExpectedSize: 1024,
		Gen:          "1",
	}
	tw.RegisterTask(manifestOld)

	// Verify bitmap was NOT wiped or replaced
	bm, ok = tw.TaskBitmap("task-tw-rollback")
	require.True(t, ok)
	assert.Equal(t, int64(512), bm.DurableBytes())
}

func TestTargetWriter_CompletedTaskRejectsLateResolver(t *testing.T) {
	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	outDir := t.TempDir()
	tw := New(bkt, outDir)

	// Mark task completed with generation retry_2000
	tw.MarkTaskCompleted("task-comp", "retry_2000")

	// Late resolver attempts to re-register with older generation "1" or same generation "retry_2000"
	manifestStale := TaskManifest{
		TaskID:       "task-comp",
		FinalPath:    "file.bin",
		ExpectedSize: 1024,
		Gen:          "1",
	}
	tw.RegisterTask(manifestStale)

	// Verify completed tombstone was NOT deleted and task was NOT registered
	compGen, ok := tw.TaskCompleted("task-comp")
	require.True(t, ok)
	assert.Equal(t, "retry_2000", compGen)

	_, bmExists := tw.TaskBitmap("task-comp")
	assert.False(t, bmExists)
}

func TestTargetWriter_RegisterResultEnum(t *testing.T) {
	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	outDir := t.TempDir()
	tw := New(bkt, outDir)

	manifest1 := TaskManifest{
		TaskID:       "task-enum",
		FinalPath:    "file.bin",
		ExpectedSize: 1024,
		Gen:          "retry_100",
	}
	res1 := tw.RegisterTask(manifest1)
	assert.Equal(t, RegisterAccepted, res1)

	// Stale registration with older generation "1"
	manifestStale := TaskManifest{
		TaskID:       "task-enum",
		FinalPath:    "file.bin",
		ExpectedSize: 1024,
		Gen:          "1",
	}
	resStale := tw.RegisterTask(manifestStale)
	assert.Equal(t, RegisterStale, resStale)

	// Conflict registration: same gen but different path
	manifestConflict := TaskManifest{
		TaskID:       "task-enum",
		FinalPath:    "different.bin",
		ExpectedSize: 1024,
		Gen:          "retry_100",
	}
	resConflict := tw.RegisterTask(manifestConflict)
	assert.Equal(t, RegisterConflict, resConflict)

	// Mark completed
	tw.MarkTaskCompleted("task-enum", "retry_100")
	resAfterComp := tw.RegisterTask(manifest1)
	assert.Equal(t, RegisterAlreadyFinalized, resAfterComp)
}

func TestTargetWriter_CompletedLeftoverOnlyAcksMatchingGen(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	completeChan := make(chan string, 1)
	tw.SetCallbacks(func(taskID, gen, finalPath, shaHash string) {
		completeChan <- finalPath
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	totalSize := int64(1024)
	data := make([]byte, totalSize)
	_, _ = rand.Read(data)

	manifest := TaskManifest{
		TaskID:       "task-match-gen",
		FinalPath:    "doc.bin",
		ExpectedSize: totalSize,
		Gen:          "retry_200",
	}
	require.Equal(t, RegisterAccepted, tw.RegisterTask(manifest))

	key0 := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "retry_200", Offset: 0, Length: totalSize}
	require.NoError(t, bkt.Reserve(ctx, totalSize))
	require.NoError(t, bkt.PutObject(key0, data))

	select {
	case <-completeChan:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not complete in time")
	}

	// Put leftover with matching generation -> should be Acked and cleaned up
	keyMatching := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "retry_200", Offset: 0, Length: 512}
	require.NoError(t, bkt.Reserve(ctx, 512))
	require.NoError(t, bkt.PutObject(keyMatching, data[:512]))

	require.Eventually(t, func() bool {
		m := bkt.Metrics()
		return m.ReadyBytes == 0 && m.PendingDeleteBytes == 0
	}, 2*time.Second, 20*time.Millisecond)
}

func TestTargetWriter_TombstoneReleasesHeavyMemory(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	completeChan := make(chan string, 1)
	tw.SetCallbacks(func(taskID, gen, finalPath, shaHash string) {
		completeChan <- finalPath
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	totalSize := int64(1024)
	data := make([]byte, totalSize)
	_, _ = rand.Read(data)

	manifest := TaskManifest{
		TaskID:       "task-tombstone-gc",
		FinalPath:    "tombstone.bin",
		ExpectedSize: totalSize,
		Gen:          "1",
	}
	tw.RegisterTask(manifest)

	key0 := bucket.ObjectKey{TaskID: manifest.TaskID, Gen: "1", Offset: 0, Length: totalSize}
	require.NoError(t, bkt.Reserve(ctx, totalSize))
	require.NoError(t, bkt.PutObject(key0, data))

	select {
	case <-completeChan:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not complete in time")
	}

	// Verify bitmap and ranges are nil in tombstone state
	_, bmExists := tw.TaskBitmap("task-tombstone-gc")
	assert.False(t, bmExists)

	compGen, ok := tw.TaskCompleted("task-tombstone-gc")
	require.True(t, ok)
	assert.Equal(t, "1", compGen)
}

func TestTargetWriter_ActiveOpsBlocksCutoverUntilDrained(t *testing.T) {
	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	outDir := t.TempDir()
	tw := New(bkt, outDir)

	manifest1 := TaskManifest{
		TaskID:       "task-lease-drain",
		FinalPath:    "file.bin",
		ExpectedSize: 1024,
		Gen:          "1",
	}
	require.Equal(t, RegisterAccepted, tw.RegisterTask(manifest1))

	tw.stateMu.RLock()
	state1 := tw.tasks["task-lease-drain"]
	tw.stateMu.RUnlock()
	require.NotNil(t, state1)

	// Acquire operation lease on attempt 1
	require.True(t, state1.AcquireOp())

	cutoverDone := make(chan RegisterResult, 1)
	go func() {
		manifest2 := TaskManifest{
			TaskID:       "task-lease-drain",
			FinalPath:    "file.bin",
			ExpectedSize: 1024,
			Gen:          "retry_100",
		}
		res := tw.RegisterTask(manifest2)
		cutoverDone <- res
	}()

	// Cutover must be waiting for state1.activeOps == 0!
	select {
	case <-cutoverDone:
		t.Fatal("cutover should have blocked while activeOps > 0")
	case <-time.After(100 * time.Millisecond):
		// Expected: cutover is actively waiting for drain
	}

	// Now release operation lease on attempt 1
	state1.ReleaseOp()

	select {
	case res := <-cutoverDone:
		assert.Equal(t, RegisterAccepted, res)
	case <-time.After(2 * time.Second):
		t.Fatal("cutover did not unblock after activeOps reached 0")
	}

	// Verify that the task in tw is now attempt 2
	tw.stateMu.RLock()
	state2 := tw.tasks["task-lease-drain"]
	tw.stateMu.RUnlock()
	require.NotNil(t, state2)
	assert.Equal(t, "retry_100", state2.manifest.Gen)
}

func TestTargetWriter_MismatchedCompletedLeftoverRequeued(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	// Mark task completed with generation "1"
	tw.MarkTaskCompleted("task-mismatch-requeue", "1")

	// Object with NEWER generation "retry_500" arrives
	keyNewer := bucket.ObjectKey{TaskID: "task-mismatch-requeue", Gen: "retry_500", Offset: 0, Length: 512}
	obj := &bucket.BufferObject{
		Key:  keyNewer,
		Data: make([]byte, 512),
	}

	// processObject must return phaseObjectRetryable so it is requeued and NOT dropped in pending-delete
	res := tw.processObject(obj, false)
	assert.Equal(t, phaseObjectRetryable, res.phase)
}

func TestTargetWriter_TombstoneBoundedGC(t *testing.T) {
	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	outDir := t.TempDir()
	tw := New(bkt, outDir)

	// Mark 1200 tasks completed (exceeding maxTombstoneEntries = 1000)
	for i := 0; i < 1200; i++ {
		tw.MarkTaskCompleted(fmt.Sprintf("task-%d", i), "1")
	}

	tw.stateMu.RLock()
	taskCount := len(tw.tasks)
	tombstoneCount := len(tw.tombstoneOrder)
	tw.stateMu.RUnlock()

	// Tombstone cache must be bounded at 1000 entries!
	assert.LessOrEqual(t, taskCount, 1000)
	assert.LessOrEqual(t, tombstoneCount, 1000)
}

func TestTargetWriter_TaskCompletedConcurrentRead(t *testing.T) {
	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	outDir := t.TempDir()
	tw := New(bkt, outDir)

	manifest := TaskManifest{
		TaskID:       "task-concurrent-read",
		FinalPath:    "file.bin",
		ExpectedSize: 1024,
		Gen:          "1",
	}
	tw.RegisterTask(manifest)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_, _ = tw.TaskCompleted("task-concurrent-read")
					_, _ = tw.TaskBitmap("task-concurrent-read")
				}
			}
		}()
	}

	// Mark completed concurrently
	time.Sleep(50 * time.Millisecond)
	tw.MarkTaskCompleted("task-concurrent-read", "1")
	wg.Wait()

	gen, ok := tw.TaskCompleted("task-concurrent-read")
	require.True(t, ok)
	assert.Equal(t, "1", gen)
}

func TestTargetWriter_CallbackReentryNoDeadlock(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	// Callback that re-enters RegisterTask during onProgress
	tw.SetCallbacks(nil, func(taskID string, movedBytes, totalBytes int64) {
		_ = tw.RegisterTask(TaskManifest{
			TaskID:       taskID,
			FinalPath:    "reentry.bin",
			ExpectedSize: totalBytes,
			Gen:          "retry_new",
		})
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	manifest := TaskManifest{
		TaskID:       "task-reentry",
		FinalPath:    "reentry.bin",
		ExpectedSize: 1024,
		Gen:          "1",
	}
	require.Equal(t, RegisterAccepted, tw.RegisterTask(manifest))

	key := bucket.ObjectKey{TaskID: "task-reentry", Gen: "1", Offset: 0, Length: 512}
	require.NoError(t, bkt.Reserve(ctx, 512))
	require.NoError(t, bkt.PutObject(key, make([]byte, 512)))

	// Should not deadlock and consume cleanly
	time.Sleep(100 * time.Millisecond)
}

func TestTargetWriter_TaskFinalInfo(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	completeChan := make(chan string, 1)
	tw.SetCallbacks(func(taskID, gen, finalPath, shaHash string) {
		completeChan <- shaHash
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	data := []byte("hello final info verification")
	manifest := TaskManifest{
		TaskID:       "task-final-info",
		FinalPath:    "info.txt",
		ExpectedSize: int64(len(data)),
		Gen:          "1",
	}
	require.Equal(t, RegisterAccepted, tw.RegisterTask(manifest))

	key := bucket.ObjectKey{TaskID: "task-final-info", Gen: "1", Offset: 0, Length: int64(len(data))}
	require.NoError(t, bkt.Reserve(ctx, int64(len(data))))
	require.NoError(t, bkt.PutObject(key, data))

	var cbSHA string
	select {
	case cbSHA = <-completeChan:
	case <-time.After(3 * time.Second):
		t.Fatal("finalize did not complete in time")
	}

	gen, finalPath, sha, ok := tw.TaskFinalInfo("task-final-info")
	require.True(t, ok)
	assert.Equal(t, "1", gen)
	assert.Equal(t, "info.txt", finalPath)
	assert.Equal(t, cbSHA, sha)
	assert.NotEmpty(t, sha)
}

func TestTargetWriter_OnCompleteReentryNoDeadlock(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "output")
	require.NoError(t, os.MkdirAll(outDir, 0755))

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := New(bkt, outDir)

	// Callback that re-registers the next gen inside onComplete
	reentryDone := make(chan struct{})
	tw.SetCallbacks(func(taskID, gen, finalPath, shaHash string) {
		res := tw.RegisterTask(TaskManifest{
			TaskID:       taskID,
			FinalPath:    finalPath,
			ExpectedSize: 1024,
			Gen:          "retry_from_complete",
		})
		assert.Equal(t, RegisterAccepted, res)
		close(reentryDone)
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	data := []byte("complete reentry test data")
	manifest := TaskManifest{
		TaskID:       "task-complete-reentry",
		FinalPath:    "reentry_complete.bin",
		ExpectedSize: int64(len(data)),
		Gen:          "1",
	}
	require.Equal(t, RegisterAccepted, tw.RegisterTask(manifest))

	key := bucket.ObjectKey{TaskID: "task-complete-reentry", Gen: "1", Offset: 0, Length: int64(len(data))}
	require.NoError(t, bkt.Reserve(ctx, int64(len(data))))
	require.NoError(t, bkt.PutObject(key, data))

	select {
	case <-reentryDone:
	case <-time.After(3 * time.Second):
		t.Fatal("onComplete reentry deadlocked or timed out")
	}
}

func TestTargetWriter_WaitForDrainingCancel(t *testing.T) {
	state := newAttemptWriteState(TaskManifest{TaskID: "t1", Gen: "1"}, nil)
	state.AcquireOp()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Should unblock promptly with ctx.Err() without hanging
	err := state.WaitForDraining(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Now release and verify draining succeeds
	state.ReleaseOp()
	assert.NoError(t, state.WaitForDraining(context.Background()))
}
