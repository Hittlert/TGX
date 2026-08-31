package bucket

import (
	"context"
	"crypto/rand"
	"hash/crc32"
	"os"
	"path/filepath"
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

func TestBucket_LateOlderGenerationRejected(t *testing.T) {
	b, err := New(Config{Mode: ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer b.Close()

	ctx := context.Background()
	dataNew := []byte("new-generation-data")
	dataOld := []byte("old-generation-data")

	keyNew := ObjectKey{
		TaskID:   "task-gen",
		Gen:      "retry_2000",
		Offset:   0,
		Length:   int64(len(dataNew)),
		Checksum: crc32.ChecksumIEEE(dataNew),
	}

	keyOld := ObjectKey{
		TaskID:   "task-gen",
		Gen:      "retry_1000",
		Offset:   0,
		Length:   int64(len(dataOld)),
		Checksum: crc32.ChecksumIEEE(dataOld),
	}

	b.SetTaskGeneration("task-gen", "retry_2000")

	// 1. Put newer generation object first
	require.NoError(t, b.Reserve(ctx, keyNew.Length))
	require.NoError(t, b.PutObject(keyNew, dataNew))

	m := b.Metrics()
	assert.Equal(t, int64(0), m.ReservedBytes)
	assert.Equal(t, keyNew.Length, m.ReadyBytes)
	assert.Equal(t, int64(1), m.ObjectCount)

	// 2. Late older generation object arrives
	require.NoError(t, b.Reserve(ctx, keyOld.Length))
	require.NoError(t, b.PutObject(keyOld, dataOld))

	// Metrics should show reserved bytes released, and keyNew still in place
	m = b.Metrics()
	assert.Equal(t, int64(0), m.ReservedBytes)
	assert.Equal(t, keyNew.Length, m.ReadyBytes)
	assert.Equal(t, int64(1), m.ObjectCount)

	// 3. TakeReady should return the newer object
	obj, ok := b.TakeReady()
	require.True(t, ok)
	assert.Equal(t, keyNew.Gen, obj.Key.Gen)
	assert.Equal(t, dataNew, obj.Data)
}

func TestBucket_MultiGenRecovery(t *testing.T) {
	tempDir := t.TempDir()

	data1 := []byte("chunk-gen-1")
	key1 := ObjectKey{
		TaskID:   "task-multigen",
		Gen:      "1",
		Offset:   0,
		Length:   int64(len(data1)),
		Checksum: crc32.ChecksumIEEE(data1),
	}

	data2 := []byte("chunk-gen-retry")
	key2 := ObjectKey{
		TaskID:   "task-multigen",
		Gen:      "retry_5000",
		Offset:   0,
		Length:   int64(len(data2)),
		Checksum: crc32.ChecksumIEEE(data2),
	}

	// Write both files directly onto disk to simulate real crash with multi-generation files on disk
	path1 := filepath.Join(tempDir, key1.RelPath(".ready"))
	require.NoError(t, os.MkdirAll(filepath.Dir(path1), 0755))
	require.NoError(t, os.WriteFile(path1, data1, 0644))

	path2 := filepath.Join(tempDir, key2.RelPath(".ready"))
	require.NoError(t, os.MkdirAll(filepath.Dir(path2), 0755))
	require.NoError(t, os.WriteFile(path2, data2, 0644))

	// Recover with new bucket
	b2, err := New(Config{Mode: ModeSSD, RootDir: tempDir, MaxCapacity: 50 * 1024 * 1024})
	require.NoError(t, err)
	defer b2.Close()

	require.NoError(t, b2.Recover(context.Background()))
	m := b2.Metrics()
	assert.Equal(t, int64(1), m.ObjectCount)
	assert.Equal(t, key2.Length, m.ReadyBytes)

	obj, ok := b2.TakeReady()
	require.True(t, ok)
	assert.Equal(t, key2.Gen, obj.Key.Gen)
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

func TestBucket_PutObjectSecondGuardReleasesReservation(t *testing.T) {
	b, err := New(Config{Mode: ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer b.Close()

	ctx := context.Background()
	data := []byte("hello-data")
	key := ObjectKey{
		TaskID:   "task-guard",
		Gen:      "1",
		Offset:   0,
		Length:   int64(len(data)),
		Checksum: crc32.ChecksumIEEE(data),
	}

	// 1. Initially active gen is "1"
	b.SetTaskGeneration("task-guard", "1")
	require.NoError(t, b.Reserve(ctx, key.Length))
	assert.Equal(t, key.Length, b.Metrics().ReservedBytes)

	// 2. Cutover active gen to "2" before PutObject completes Phase 2
	b.SetTaskGeneration("task-guard", "2")

	// 3. PutObject with Gen "1" arrives
	require.NoError(t, b.PutObject(key, data))

	// Verify reservation was released (no leak) and no object added
	m := b.Metrics()
	assert.Equal(t, int64(0), m.ReservedBytes)
	assert.Equal(t, int64(0), m.ReadyBytes)
	assert.Equal(t, int64(0), m.ObjectCount)
}

func TestBucket_RequeueStaleGenerationDropped(t *testing.T) {
	b, err := New(Config{Mode: ModeMemory, MaxCapacity: 10 * 1024 * 1024})
	require.NoError(t, err)
	defer b.Close()

	ctx := context.Background()
	data1 := []byte("old-attempt-chunk")
	key1 := ObjectKey{
		TaskID:   "task-requeue",
		Gen:      "1",
		Offset:   0,
		Length:   int64(len(data1)),
		Checksum: crc32.ChecksumIEEE(data1),
	}

	b.SetTaskGeneration("task-requeue", "1")
	require.NoError(t, b.Reserve(ctx, key1.Length))
	require.NoError(t, b.PutObject(key1, data1))

	// TakeReady moves key1 to pending-delete
	obj, ok := b.TakeReady()
	require.True(t, ok)
	assert.Equal(t, key1.Length, b.Metrics().PendingDeleteBytes)

	// Cutover to generation "retry_1"
	b.SetTaskGeneration("task-requeue", "retry_1")

	// Writer fails and calls Requeue on the old generation object
	b.Requeue(obj)

	// Verify old object was dropped, PendingDeleteBytes released, and not in ready queue
	m := b.Metrics()
	assert.Equal(t, int64(0), m.PendingDeleteBytes)
	assert.Equal(t, int64(0), m.ReadyBytes)
	assert.Equal(t, int64(0), m.ObjectCount)

	_, ok = b.TakeReady()
	assert.False(t, ok)
}
