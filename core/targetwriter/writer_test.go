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
	tw.SetCallbacks(func(taskID, finalPath, shaHash string) {
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
