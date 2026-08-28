package lease

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool_BasicAcquireRelease(t *testing.T) {
	p := NewPool(Config{
		BufferBudget: 4 * 1024 * 1024, // 4MB
		DirtyBudget:  2 * 1024 * 1024, // 2MB
	})

	ctx := context.Background()

	// 1. Acquire Buffer
	b1, err := p.AcquireBuffer(ctx, 2*1024*1024)
	require.NoError(t, err)
	assert.Equal(t, int64(2*1024*1024), b1.Size())

	stats := p.Stats()
	assert.Equal(t, int64(2*1024*1024), stats.BufferUsed)
	assert.Equal(t, 0.5, stats.BufferUtil)

	// 2. Acquire Dirty
	d1, err := p.AcquireDirty(ctx, 1*1024*1024)
	require.NoError(t, err)
	assert.Equal(t, int64(1*1024*1024), d1.Size())

	stats = p.Stats()
	assert.Equal(t, int64(1*1024*1024), stats.DirtyUsed)
	assert.Equal(t, 0.5, stats.DirtyUtil)

	// 3. Release
	b1.Release()
	d1.Release()

	// Repeated release is idempotent
	b1.Release()
	d1.Release()

	stats = p.Stats()
	assert.Equal(t, int64(0), stats.BufferUsed)
	assert.Equal(t, int64(0), stats.DirtyUsed)
}

func TestPool_BackpressureAndTimeout(t *testing.T) {
	p := NewPool(Config{
		BufferBudget: 2 * 1024 * 1024, // 2MB
		DirtyBudget:  2 * 1024 * 1024,
	})

	ctx := context.Background()
	b1, err := p.AcquireBuffer(ctx, 2*1024*1024)
	require.NoError(t, err)

	// Try acquiring another block with timeout context
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = p.AcquireBuffer(ctxTimeout, 1*1024*1024)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.True(t, elapsed >= 45*time.Millisecond)

	// Release b1 in goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		b1.Release()
	}()

	ctxAcquire, cancelAcquire := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelAcquire()

	b2, err := p.AcquireBuffer(ctxAcquire, 2*1024*1024)
	require.NoError(t, err)
	assert.NotNil(t, b2)
	b2.Release()
	wg.Wait()
}

func TestPool_DirectDirtyRelease(t *testing.T) {
	p := NewPool(Config{
		BufferBudget: 4 * 1024 * 1024,
		DirtyBudget:  4 * 1024 * 1024,
	})

	ctx := context.Background()
	_, err := p.AcquireDirty(ctx, 2*1024*1024)
	require.NoError(t, err)

	assert.Equal(t, int64(2*1024*1024), p.Stats().DirtyUsed)

	// CheckpointLoop releases dirty bytes directly
	p.ReleaseDirtyBytes(2 * 1024 * 1024)
	assert.Equal(t, int64(0), p.Stats().DirtyUsed)
}
