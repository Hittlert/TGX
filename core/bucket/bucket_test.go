package bucket

import (
	"context"
	"crypto/rand"
	"hash/crc32"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucket_MemoryPutReadTake(t *testing.T) {
	b, err := New(Config{Mode: ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer b.Close()

	ctx := context.Background()

	// 1. Reserve 1MB
	require.NoError(t, b.Reserve(ctx, 1024*1024))
	m := b.Metrics()
	assert.Equal(t, int64(1024*1024), m.ReservedBytes)
	assert.Equal(t, int64(0), m.ReadyBytes)

	data := make([]byte, 1024*1024)
	_, _ = rand.Read(data)
	checksum := crc32.ChecksumIEEE(data)

	key := ObjectKey{
		TaskID:   "task-1",
		Gen:      "g1",
		Offset:   0,
		Length:   1024 * 1024,
		Checksum: checksum,
	}

	// 2. PutObject
	require.NoError(t, b.PutObject(key, data))
	m = b.Metrics()
	assert.Equal(t, int64(0), m.ReservedBytes)
	assert.Equal(t, int64(1024*1024), m.ReadyBytes)
	assert.Equal(t, int64(1), m.ObjectCount)

	// 3. ReadObject
	readData, err := b.ReadObject(key)
	require.NoError(t, err)
	assert.Equal(t, data, readData)

	// 4. TryTakeNext
	obj, ok := b.TryTakeNext("task-1", 0)
	require.True(t, ok)
	assert.Equal(t, key, obj.Key)
	assert.Equal(t, data, obj.Data)

	m = b.Metrics()
	assert.Equal(t, int64(1024*1024), m.PendingDeleteBytes)
	assert.Equal(t, int64(0), m.ReadyBytes)

	// 5. AckDurable
	require.NoError(t, b.AckDurable([]ObjectKey{key}))
	m = b.Metrics()
	assert.Equal(t, int64(0), m.PendingDeleteBytes)
	assert.Equal(t, int64(0), m.UsedBytes)
	assert.Equal(t, int64(0), m.ObjectCount)
}

func TestBucket_SSDPutReadAndRecovery(t *testing.T) {
	tempDir := t.TempDir()
	b, err := New(Config{Mode: ModeSSD, RootDir: tempDir, MaxCapacity: 50 * 1024 * 1024})
	require.NoError(t, err)

	ctx := context.Background()
	chunkData := []byte("hello-ssd-chunk-buffer-data")
	key := ObjectKey{
		TaskID:   "task-ssd",
		Gen:      "g1",
		Offset:   0,
		Length:   int64(len(chunkData)),
		Checksum: crc32.ChecksumIEEE(chunkData),
	}

	require.NoError(t, b.Reserve(ctx, key.Length))
	require.NoError(t, b.PutObject(key, chunkData))

	// Close bucket to simulate crash
	require.NoError(t, b.Close())

	// Create new bucket and recover
	b2, err := New(Config{Mode: ModeSSD, RootDir: tempDir, MaxCapacity: 50 * 1024 * 1024})
	require.NoError(t, err)
	defer b2.Close()

	require.NoError(t, b2.Recover(ctx))
	m := b2.Metrics()
	assert.Equal(t, int64(1), m.ObjectCount)
	assert.Equal(t, key.Length, m.ReadyBytes)

	obj, ok := b2.TakeReady()
	require.True(t, ok)
	assert.Equal(t, key.TaskID, obj.Key.TaskID)
	assert.Equal(t, key.Offset, obj.Key.Offset)

	data, err := b2.ReadObject(obj.Key)
	require.NoError(t, err)
	assert.Equal(t, chunkData, data)
}

func TestBucket_CapacityBackpressure(t *testing.T) {
	b, err := New(Config{Mode: ModeMemory, MaxCapacity: 2 * 1024 * 1024}) // 2MB
	require.NoError(t, err)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Reserve 2MB -> OK
	require.NoError(t, b.Reserve(ctx, 2*1024*1024))

	// Next reserve blocks
	wakeChan := make(chan error)
	go func() {
		err := b.Reserve(ctx, 1024*1024)
		wakeChan <- err
	}()

	time.Sleep(50 * time.Millisecond)
	// Release 1MB
	b.ReleaseReservation(1024 * 1024)

	select {
	case err := <-wakeChan:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("backpressure reserve did not unblock")
	}
}
