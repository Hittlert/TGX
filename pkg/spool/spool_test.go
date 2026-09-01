package spool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeSet_MergingAndCompleteness(t *testing.T) {
	rs := NewRangeSet()
	assert.Equal(t, int64(0), rs.TotalCovered())
	assert.False(t, rs.IsComplete(1000))

	// Write chunk 1: [0, 500)
	rs.Add(0, 500)
	assert.Equal(t, int64(500), rs.TotalCovered())
	assert.False(t, rs.IsComplete(1000))

	// Missing should be [500, 1000)
	missing := rs.MissingRanges(1000)
	require.Len(t, missing, 1)
	assert.Equal(t, int64(500), missing[0].Start)
	assert.Equal(t, int64(1000), missing[0].End)

	// Write chunk 2: [500, 1000) -> contiguous merge
	rs.Add(500, 1000)
	assert.Equal(t, int64(1000), rs.TotalCovered())
	assert.True(t, rs.IsComplete(1000))
	assert.Empty(t, rs.MissingRanges(1000))

	// Overlapping redundant chunk [200, 700)
	rs.Add(200, 700)
	assert.Equal(t, int64(1000), rs.TotalCovered())
	assert.True(t, rs.IsComplete(1000))
}

func TestCapacityManager_BoundedReservationAndReclaim(t *testing.T) {
	cm := NewCapacityManager(1000) // 1000 bytes max

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Reserve 600 bytes
	require.NoError(t, cm.Reserve(ctx, 600))
	cm.ConvertReservationToUsed(600)

	// Reserve 400 bytes
	require.NoError(t, cm.Reserve(ctx, 400))
	cm.ConvertReservationToUsed(400)

	// Now 1000 bytes used. Next reserve should block until space is reclaimed
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer blockedCancel()
	err := cm.Reserve(blockedCtx, 200)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Reclaim 500 bytes
	cm.Reclaim(500)

	// Now reserve should succeed immediately
	require.NoError(t, cm.Reserve(ctx, 500))
	cm.ReleaseReservation(500)
}

func TestFileStore_LifecycleAndWriteAt(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileStore(tempDir, 100*1024*1024)
	require.NoError(t, err)
	defer store.Close()

	key := SegmentKey{
		TaskID:       "task-test-1",
		Gen:          "1",
		SegmentIndex: 0,
		StartOffset:  0,
		Length:       1024 * 1024, // 1 MiB
	}

	ctx := context.Background()
	require.NoError(t, store.Reserve(ctx, key.Length))

	item, err := store.CreateSegment(key)
	require.NoError(t, err)
	assert.Equal(t, StateReceiving, item.State)

	// Write chunks out of order: Chunk 2 first, Chunk 1 second
	chunk2Data := []byte("WORLD-DATA-CHUNK-2")
	chunk1Data := []byte("HELLO-DATA-CHUNK-1")

	n, err := store.WriteAt(key, 512, chunk2Data)
	require.NoError(t, err)
	assert.Equal(t, len(chunk2Data), n)

	n, err = store.WriteAt(key, 0, chunk1Data)
	require.NoError(t, err)
	assert.Equal(t, len(chunk1Data), n)

	// Verify ReadAt
	readBuf := make([]byte, len(chunk1Data))
	n, err = store.ReadAt(key, 0, readBuf)
	require.NoError(t, err)
	assert.Equal(t, chunk1Data, readBuf)

	readBuf2 := make([]byte, len(chunk2Data))
	n, err = store.ReadAt(key, 512, readBuf2)
	require.NoError(t, err)
	assert.Equal(t, chunk2Data, readBuf2)

	// Mark ready
	require.NoError(t, store.MarkReady(key))
	readyList := store.ListReadySegments()
	require.Len(t, readyList, 1)
	assert.Equal(t, StateReady, readyList[0].State)

	// Reclaim segment
	require.NoError(t, store.Reclaim(key))
	_, exists := store.GetItem(key)
	assert.False(t, exists)
}

func TestMemoryStore_LifecycleAndWriteAt(t *testing.T) {
	store := NewMemoryStore(10 * 1024 * 1024)
	defer store.Close()

	key := SegmentKey{
		TaskID:       "task-mem-1",
		Gen:          "1",
		SegmentIndex: 0,
		StartOffset:  0,
		Length:       4096,
	}

	ctx := context.Background()
	require.NoError(t, store.Reserve(ctx, key.Length))

	_, err := store.CreateSegment(key)
	require.NoError(t, err)

	data := []byte("IN_MEMORY_PAYLOAD_TEST")
	n, err := store.WriteAt(key, 100, data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	readBuf := make([]byte, len(data))
	n, err = store.ReadAt(key, 100, readBuf)
	require.NoError(t, err)
	assert.Equal(t, data, readBuf)

	require.NoError(t, store.MarkReady(key))
	require.Len(t, store.ListReadySegments(), 1)

	require.NoError(t, store.Reclaim(key))
	_, exists := store.GetItem(key)
	assert.False(t, exists)
}
