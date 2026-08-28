package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDRR_InflightFormula(t *testing.T) {
	assert.Equal(t, 16, CalculateInflightCap(0))
	assert.Equal(t, 16, CalculateInflightCap(1))
	assert.Equal(t, 16, CalculateInflightCap(2))
	assert.Equal(t, 16, CalculateInflightCap(4))
	assert.Equal(t, 8, CalculateInflightCap(8))
	assert.Equal(t, 5, CalculateInflightCap(12))
	assert.Equal(t, 4, CalculateInflightCap(16))
	assert.Equal(t, 4, CalculateInflightCap(20)) // Capped at min 4
}

func TestDRR_WorkConservingRatio(t *testing.T) {
	sched := NewDRRScheduler()

	// Enqueue 6 small chunks and 6 large chunks
	for i := 0; i < 6; i++ {
		sched.Enqueue(ChunkTask{
			FileKey:    fmt.Sprintf("small_%d", i),
			TotalSize:  2 * 1024 * 1024, // 2MB <= 10MB
			BlockIndex: uint32(i),
		})
	}
	for i := 0; i < 6; i++ {
		sched.Enqueue(ChunkTask{
			FileKey:    fmt.Sprintf("large_%d", i),
			TotalSize:  50 * 1024 * 1024, // 50MB > 10MB
			BlockIndex: uint32(i),
		})
	}

	var order []string
	for i := 0; i < 8; i++ {
		task, ok := sched.NextChunk()
		require.True(t, ok)
		if IsSmallFile(task.TotalSize) {
			order = append(order, "S")
		} else {
			order = append(order, "L")
		}
	}

	// First round: S, S, S, L (3:1)
	// Second round: S, S, S, L (3:1)
	expected := []string{"S", "S", "S", "L", "S", "S", "S", "L"}
	assert.Equal(t, expected, order)
}

func TestDRR_SingleLaneWorkConserving(t *testing.T) {
	sched := NewDRRScheduler()

	// Enqueue ONLY large tasks
	for i := 0; i < 5; i++ {
		sched.Enqueue(ChunkTask{
			FileKey:    fmt.Sprintf("large_file_%d", i),
			TotalSize:  20 * 1024 * 1024,
			BlockIndex: uint32(i),
		})
	}

	for i := 0; i < 5; i++ {
		task, ok := sched.NextChunk()
		require.True(t, ok)
		assert.False(t, IsSmallFile(task.TotalSize))
	}

	_, ok := sched.NextChunk()
	assert.False(t, ok)
}

func TestDRR_TimerQueueFloodWait(t *testing.T) {
	sched := NewDRRScheduler()

	// Enqueue delayed chunk for 50ms in future
	sched.EnqueueDelay(ChunkTask{
		FileKey:    "delayed_file",
		TotalSize:  5 * 1024 * 1024,
		BlockIndex: 0,
	}, time.Now().Add(50*time.Millisecond))

	// Immediately popping should yield false
	_, ok := sched.NextChunk()
	assert.False(t, ok)

	// Wait 60ms
	time.Sleep(60 * time.Millisecond)

	task, ok := sched.NextChunk()
	require.True(t, ok)
	assert.Equal(t, "delayed_file", task.FileKey)
}
