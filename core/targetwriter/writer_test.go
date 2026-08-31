package targetwriter

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
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
