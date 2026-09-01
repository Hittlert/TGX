package writeback

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Hittlert/TGX/pkg/spool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestQueue_PrioritiesAndOrdering(t *testing.T) {
	q := NewQueue()
	defer q.Close()

	// 1. Large file segment 1
	largeItem1 := &Item{
		Key:              spool.SegmentKey{TaskID: "large-1", Gen: "1", SegmentIndex: 1, StartOffset: 32 * 1024 * 1024, Length: 32 * 1024 * 1024},
		ExpectedFileSize: 100 * 1024 * 1024,
		AddedAt:          time.Now(),
	}
	// 2. Large file segment 0
	largeItem0 := &Item{
		Key:              spool.SegmentKey{TaskID: "large-1", Gen: "1", SegmentIndex: 0, StartOffset: 0, Length: 32 * 1024 * 1024},
		ExpectedFileSize: 100 * 1024 * 1024,
		AddedAt:          time.Now().Add(1 * time.Millisecond),
	}
	// 3. Small whole file
	smallItem := &Item{
		Key:              spool.SegmentKey{TaskID: "small-1", Gen: "1", SegmentIndex: 0, StartOffset: 0, Length: 2 * 1024 * 1024},
		ExpectedFileSize: 2 * 1024 * 1024,
		AddedAt:          time.Now().Add(2 * time.Millisecond),
	}

	q.Enqueue(largeItem1)
	q.Enqueue(largeItem0)
	q.Enqueue(smallItem)

	ctx := context.Background()

	// Small item should come out first (top priority)
	deq1, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "small-1", deq1.Key.TaskID)

	// Large item segment 0 should come out before segment 1
	deq2, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "large-1", deq2.Key.TaskID)
	assert.Equal(t, 0, deq2.Key.SegmentIndex)

	deq3, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "large-1", deq3.Key.TaskID)
	assert.Equal(t, 1, deq3.Key.SegmentIndex)
}

func TestTargetSink_EndToEndStreamingAndReclaim(t *testing.T) {
	tempSpoolDir := t.TempDir()
	tempTargetDir := t.TempDir()

	store, err := spool.NewFileStore(tempSpoolDir, 50*1024*1024)
	require.NoError(t, err)
	defer store.Close()

	queue := NewQueue()

	var finalSHA string
	var finalSize int64
	var finalizedErr error
	var wg sync.WaitGroup
	wg.Add(1)

	cb := Callbacks{
		OnTaskFinalized: func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error) {
			finalSHA = sha256Hex
			finalSize = size
			finalizedErr = err
			wg.Done()
		},
	}

	cfg := DefaultConfig(tempTargetDir)
	cfg.Concurrency = 2
	sink := NewTargetSink(cfg, store, queue, cb, zap.NewNop())
	defer sink.Close()

	taskID := "task-e2e-1"
	gen := "1"
	finalRelPath := "videos/test_movie.mp4"

	// Create 2 segments of 1 MiB each (total 2 MiB file)
	seg0Data := make([]byte, 1024*1024)
	for i := range seg0Data {
		seg0Data[i] = byte(i % 256)
	}
	seg1Data := make([]byte, 1024*1024)
	for i := range seg1Data {
		seg1Data[i] = byte((i + 13) % 256)
	}

	fullPayload := append(seg0Data, seg1Data...)
	expectedSum := sha256.Sum256(fullPayload)
	expectedSHAHex := hex.EncodeToString(expectedSum[:])

	seg0Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 0, StartOffset: 0, Length: int64(len(seg0Data))}
	seg1Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 1, StartOffset: int64(len(seg0Data)), Length: int64(len(seg1Data))}

	ctx := context.Background()
	require.NoError(t, store.Reserve(ctx, seg0Key.Length+seg1Key.Length))

	// Write seg 0
	_, err = store.CreateSegment(seg0Key)
	require.NoError(t, err)
	_, err = store.WriteAt(seg0Key, 0, seg0Data)
	require.NoError(t, err)
	require.NoError(t, store.MarkReady(seg0Key))

	// Write seg 1
	_, err = store.CreateSegment(seg1Key)
	require.NoError(t, err)
	_, err = store.WriteAt(seg1Key, 0, seg1Data)
	require.NoError(t, err)
	require.NoError(t, store.MarkReady(seg1Key))

	// Enqueue to write-back
	queue.Enqueue(&Item{
		Key:              seg0Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: int64(len(fullPayload)),
		IsLastSegment:    false,
		AddedAt:          time.Now(),
	})
	queue.Enqueue(&Item{
		Key:              seg1Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: int64(len(fullPayload)),
		IsLastSegment:    true,
		AddedAt:          time.Now(),
	})

	// Wait for finalization
	wg.Wait()
	require.NoError(t, finalizedErr)
	assert.Equal(t, int64(len(fullPayload)), finalSize)
	assert.Equal(t, expectedSHAHex, finalSHA)

	// Verify target file exists and content matches
	finalPath := filepath.Join(tempTargetDir, finalRelPath)
	diskData, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, fullPayload, diskData)

	// Verify segments in Spool store were reclaimed
	_, exists0 := store.GetItem(seg0Key)
	assert.False(t, exists0)
	_, exists1 := store.GetItem(seg1Key)
	assert.False(t, exists1)
}
