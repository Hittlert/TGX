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

func TestTargetSink_OutOfOrderSegments_StrictSequentialSHAAndNoHoles(t *testing.T) {
	tempSpoolDir := t.TempDir()
	tempTargetDir := t.TempDir()

	store, err := spool.NewFileStore(tempSpoolDir, 50*1024*1024)
	require.NoError(t, err)
	defer store.Close()

	queue := NewQueue()

	var finalSHA string
	var finalizedErr error
	var wg sync.WaitGroup
	wg.Add(1)

	cb := Callbacks{
		OnTaskFinalized: func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error) {
			finalSHA = sha256Hex
			finalizedErr = err
			wg.Done()
		},
	}

	cfg := DefaultConfig(tempTargetDir)
	cfg.Concurrency = 2
	sink := NewTargetSink(cfg, store, queue, cb, zap.NewNop())
	defer sink.Close()

	taskID := "task-ooo-1"
	gen := "1"
	finalRelPath := "videos/out_of_order.mp4"

	// 2 segments: seg 0 (1MB), seg 1 (1MB)
	seg0Data := []byte("SEGMENT_0_DATA_CONTENT_12345678")
	seg1Data := []byte("SEGMENT_1_DATA_CONTENT_87654321")

	fullPayload := append(seg0Data, seg1Data...)
	expectedSum := sha256.Sum256(fullPayload)
	expectedSHAHex := hex.EncodeToString(expectedSum[:])

	seg0Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 0, StartOffset: 0, Length: int64(len(seg0Data))}
	seg1Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 1, StartOffset: int64(len(seg0Data)), Length: int64(len(seg1Data))}

	ctx := context.Background()
	require.NoError(t, store.Reserve(ctx, seg0Key.Length+seg1Key.Length))

	_, _ = store.CreateSegment(seg0Key)
	_, _ = store.WriteAt(seg0Key, 0, seg0Data)
	_ = store.MarkReady(seg0Key)

	_, _ = store.CreateSegment(seg1Key)
	_, _ = store.WriteAt(seg1Key, 0, seg1Data)
	_ = store.MarkReady(seg1Key)

	// Intentionally enqueue SEGMENT 1 FIRST into the queue!
	queue.Enqueue(&Item{
		Key:              seg1Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: int64(len(fullPayload)),
		IsLastSegment:    true,
		AddedAt:          time.Now(),
	})

	time.Sleep(50 * time.Millisecond)

	// Enqueue SEGMENT 0 SECOND
	queue.Enqueue(&Item{
		Key:              seg0Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: int64(len(fullPayload)),
		IsLastSegment:    false,
		AddedAt:          time.Now(),
	})

	wg.Wait()
	require.NoError(t, finalizedErr)
	assert.Equal(t, expectedSHAHex, finalSHA)

	finalPath := filepath.Join(tempTargetDir, finalRelPath)
	diskData, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, fullPayload, diskData)
}

func TestTargetSink_MissingMiddle_NeverFinalizesPrematurely(t *testing.T) {
	tempSpoolDir := t.TempDir()
	tempTargetDir := t.TempDir()

	store, err := spool.NewFileStore(tempSpoolDir, 50*1024*1024)
	require.NoError(t, err)
	defer store.Close()

	queue := NewQueue()

	finalizedChan := make(chan struct{}, 1)
	cb := Callbacks{
		OnTaskFinalized: func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error) {
			finalizedChan <- struct{}{}
		},
	}

	cfg := DefaultConfig(tempTargetDir)
	cfg.Concurrency = 2
	sink := NewTargetSink(cfg, store, queue, cb, zap.NewNop())
	defer sink.Close()

	taskID := "task-missing-middle"
	gen := "1"
	finalRelPath := "videos/sparse.mp4"

	// 3 segments total expected: 0 (10B), 1 (10B), 2 (10B), total 30B
	seg0Data := []byte("0123456789")
	seg2Data := []byte("abcdefghij")

	seg0Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 0, StartOffset: 0, Length: 10}
	seg2Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 2, StartOffset: 20, Length: 10}

	ctx := context.Background()
	require.NoError(t, store.Reserve(ctx, 20))

	_, _ = store.CreateSegment(seg0Key)
	_, _ = store.WriteAt(seg0Key, 0, seg0Data)
	_ = store.MarkReady(seg0Key)

	_, _ = store.CreateSegment(seg2Key)
	_, _ = store.WriteAt(seg2Key, 0, seg2Data)
	_ = store.MarkReady(seg2Key)

	// Enqueue seg 0 and seg 2 (seg 1 is missing!)
	queue.Enqueue(&Item{
		Key:              seg0Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: 30,
		IsLastSegment:    false,
		AddedAt:          time.Now(),
	})
	queue.Enqueue(&Item{
		Key:              seg2Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: 30,
		IsLastSegment:    true, // Last segment arriving, but middle is missing!
		AddedAt:          time.Now(),
	})

	select {
	case <-finalizedChan:
		t.Fatal("task must NOT be finalized prematurely when middle segment is missing!")
	case <-time.After(200 * time.Millisecond):
		// Success: correctly waiting for segment 1!
	}
}

func TestTargetSink_TargetExists_ConflictFails(t *testing.T) {
	tempSpoolDir := t.TempDir()
	tempTargetDir := t.TempDir()

	store, err := spool.NewFileStore(tempSpoolDir, 50*1024*1024)
	require.NoError(t, err)
	defer store.Close()

	queue := NewQueue()

	var finalizedErr error
	var wg sync.WaitGroup
	wg.Add(1)

	cb := Callbacks{
		OnTaskFinalized: func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error) {
			finalizedErr = err
			wg.Done()
		},
	}

	cfg := DefaultConfig(tempTargetDir)
	cfg.Concurrency = 1
	sink := NewTargetSink(cfg, store, queue, cb, zap.NewNop())
	defer sink.Close()

	taskID := "task-conflict"
	gen := "1"
	finalRelPath := "videos/conflict.mp4"

	// Pre-create target file with DIFFERENT content
	finalPath := filepath.Join(tempTargetDir, finalRelPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(finalPath), 0755))
	require.NoError(t, os.WriteFile(finalPath, []byte("preexisting_different_content"), 0644))

	segData := []byte("new_incoming_content_123456789")
	segKey := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 0, StartOffset: 0, Length: int64(len(segData))}

	ctx := context.Background()
	require.NoError(t, store.Reserve(ctx, segKey.Length))
	_, _ = store.CreateSegment(segKey)
	_, _ = store.WriteAt(segKey, 0, segData)
	_ = store.MarkReady(segKey)

	queue.Enqueue(&Item{
		Key:              segKey,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: int64(len(segData)),
		IsLastSegment:    true,
		AddedAt:          time.Now(),
	})

	wg.Wait()
	require.Error(t, finalizedErr)
	assert.ErrorIs(t, finalizedErr, ErrTargetConflict)
}

func TestTargetSink_IdenticalTarget_IdempotentSuccess(t *testing.T) {
	tempSpoolDir := t.TempDir()
	tempTargetDir := t.TempDir()

	store, err := spool.NewFileStore(tempSpoolDir, 50*1024*1024)
	require.NoError(t, err)
	defer store.Close()

	queue := NewQueue()

	var finalizedErr error
	var wg sync.WaitGroup
	wg.Add(1)

	cb := Callbacks{
		OnTaskFinalized: func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error) {
			finalizedErr = err
			wg.Done()
		},
	}

	cfg := DefaultConfig(tempTargetDir)
	cfg.Concurrency = 1
	sink := NewTargetSink(cfg, store, queue, cb, zap.NewNop())
	defer sink.Close()

	taskID := "task-identical"
	gen := "1"
	finalRelPath := "videos/identical.mp4"

	segData := []byte("identical_content_123456789")

	// Pre-create target file with EXACT IDENTICAL content
	finalPath := filepath.Join(tempTargetDir, finalRelPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(finalPath), 0755))
	require.NoError(t, os.WriteFile(finalPath, segData, 0644))

	segKey := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 0, StartOffset: 0, Length: int64(len(segData))}

	ctx := context.Background()
	require.NoError(t, store.Reserve(ctx, segKey.Length))
	_, _ = store.CreateSegment(segKey)
	_, _ = store.WriteAt(segKey, 0, segData)
	_ = store.MarkReady(segKey)

	queue.Enqueue(&Item{
		Key:              segKey,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: int64(len(segData)),
		IsLastSegment:    true,
		AddedAt:          time.Now(),
	})

	wg.Wait()
	require.NoError(t, finalizedErr)
}

func TestTargetSink_CapacityRelease_ZeroDeadlock(t *testing.T) {
	tempSpoolDir := t.TempDir()
	tempTargetDir := t.TempDir()

	// Small store capacity: only 150 bytes total capacity
	store, err := spool.NewFileStore(tempSpoolDir, 150)
	require.NoError(t, err)
	defer store.Close()

	queue := NewQueue()

	var wg sync.WaitGroup
	wg.Add(1)
	var finalizedErr error

	cb := Callbacks{
		OnTaskFinalized: func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error) {
			finalizedErr = err
			wg.Done()
		},
	}

	cfg := DefaultConfig(tempTargetDir)
	cfg.Concurrency = 2
	sink := NewTargetSink(cfg, store, queue, cb, zap.NewNop())
	defer sink.Close()

	taskID := "task-cap-deadlock-free"
	gen := "1"
	finalRelPath := "videos/cap_test.mp4"

	// 2 segments: seg0 (100B), seg1 (100B), total 200B > 150B max spool capacity!
	seg0Data := make([]byte, 100)
	for i := range seg0Data {
		seg0Data[i] = 'A'
	}
	seg1Data := make([]byte, 100)
	for i := range seg1Data {
		seg1Data[i] = 'B'
	}

	seg0Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 0, StartOffset: 0, Length: 100}
	seg1Key := spool.SegmentKey{TaskID: taskID, Gen: gen, SegmentIndex: 1, StartOffset: 100, Length: 100}

	ctx := context.Background()

	// Step 1: Write seg1 first (out of order) occupying 100 bytes of spool
	require.NoError(t, store.Reserve(ctx, 100))
	_, _ = store.CreateSegment(seg1Key)
	_, _ = store.WriteAt(seg1Key, 0, seg1Data)
	require.NoError(t, store.MarkReady(seg1Key))

	// Enqueue seg1
	queue.Enqueue(&Item{
		Key:              seg1Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: 200,
		IsLastSegment:    true,
		AddedAt:          time.Now(),
	})

	// Wait for seg1 to be written back and reclaimed from Spool
	time.Sleep(100 * time.Millisecond)

	// Step 2: Now write seg0 (capacity was freed by writeback of seg1!)
	require.NoError(t, store.Reserve(ctx, 100))
	_, _ = store.CreateSegment(seg0Key)
	_, _ = store.WriteAt(seg0Key, 0, seg0Data)
	require.NoError(t, store.MarkReady(seg0Key))

	// Enqueue seg0
	queue.Enqueue(&Item{
		Key:              seg0Key,
		FinalRelPath:     finalRelPath,
		ExpectedFileSize: 200,
		IsLastSegment:    false,
		AddedAt:          time.Now(),
	})

	wg.Wait()
	require.NoError(t, finalizedErr)

	// Verify whole file on disk
	finalPath := filepath.Join(tempTargetDir, finalRelPath)
	data, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, 200, len(data))
	assert.Equal(t, append(seg0Data, seg1Data...), data)
}
