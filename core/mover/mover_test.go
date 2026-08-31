package mover

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMover_SingleFileMove(t *testing.T) {
	tempDir := t.TempDir()
	bufDir := filepath.Join(tempDir, "buffer")
	targetDir := filepath.Join(tempDir, "target")
	require.NoError(t, os.MkdirAll(bufDir, 0755))
	require.NoError(t, os.MkdirAll(targetDir, 0755))

	// Create 10MB test file in buffer
	testData := make([]byte, 10*1024*1024)
	_, _ = rand.Read(testData)
	srcPath := filepath.Join(bufDir, "movie.mp4")
	require.NoError(t, os.WriteFile(srcPath, testData, 0666))

	dstPath := filepath.Join(targetDir, "sub", "movie.mp4")

	m := New(1, 100*1024*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m.Start(ctx)
	defer m.Close()

	// Pre-reserve
	require.True(t, m.Reserve(int64(len(testData))))
	assert.Equal(t, int64(len(testData)), m.UsedBytes())

	done := make(chan error, 1)
	var progressCalled bool
	var lastMoved int64
	var mu sync.Mutex

	job := &MoveJob{
		ID:      "task-1",
		SrcPath: srcPath,
		DstPath: dstPath,
		Size:    int64(len(testData)),
		OnProgress: func(bytesMoved, totalBytes int64) {
			mu.Lock()
			progressCalled = true
			lastMoved = bytesMoved
			mu.Unlock()
		},
		OnDone: func(err error) {
			done <- err
		},
	}

	err := m.Enqueue(job)
	require.NoError(t, err)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("mover timeout")
	}

	// Verify destination file
	dstContent, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(testData, dstContent))

	// Verify source was removed
	_, err = os.Stat(srcPath)
	assert.True(t, os.IsNotExist(err))

	// Verify capacity was automatically released
	assert.Equal(t, int64(0), m.UsedBytes())

	mu.Lock()
	assert.True(t, progressCalled)
	assert.Equal(t, int64(len(testData)), lastMoved)
	mu.Unlock()
}

func TestMover_MemorySmallFileMove(t *testing.T) {
	targetDir := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.MkdirAll(targetDir, 0755))

	testData := []byte("small image payload")
	dstPath := filepath.Join(targetDir, "img.jpg")

	m := New(1, 10*1024*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m.Start(ctx)
	defer m.Close()

	done := make(chan error, 1)
	job := &MoveJob{
		ID:      "task-img",
		SrcData: testData,
		DstPath: dstPath,
		Size:    int64(len(testData)),
		OnDone: func(err error) {
			done <- err
		},
	}

	require.NoError(t, m.Enqueue(job))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("mover timeout")
	}

	dstContent, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(testData, dstContent))
}

func TestMover_BackpressureWaiting(t *testing.T) {
	m := New(1, 10*1024*1024) // 10MB capacity

	// 1. Reserve 8MB -> OK
	assert.True(t, m.Reserve(8*1024*1024))
	assert.Equal(t, int64(8*1024*1024), m.UsedBytes())

	// 2. Reserve another 5MB (total 13MB > 10MB) -> Fails
	assert.False(t, m.Reserve(5*1024*1024))

	// 3. WaitBackpressure in goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	wakeChan := make(chan struct{})
	go func() {
		err := m.WaitBackpressure(ctx, 5*1024*1024)
		assert.NoError(t, err)
		close(wakeChan)
	}()

	// Simulate mover releasing 8MB after 50ms
	time.Sleep(50 * time.Millisecond)
	m.Release(8 * 1024 * 1024)

	select {
	case <-wakeChan:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatal("backpressure did not wake up")
	}

	assert.Equal(t, int64(5*1024*1024), m.UsedBytes())
}
