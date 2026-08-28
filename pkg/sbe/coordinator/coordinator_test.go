package coordinator

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/Hittlert/TGX/pkg/sbe/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileCoordinator_DownloadLifecycle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_coord_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p := lease.NewPool(lease.Config{
		BufferBudget: 8 * 1024 * 1024,
		DirtyBudget:  8 * 1024 * 1024,
	})
	defer p.Close()

	totalSize := int64(4 * 1024 * 1024) // 4MB = 2 blocks of 2MB
	var attemptID [16]byte
	copy(attemptID[:], []byte("attempt_test_001"))

	fc, rec, err := NewFileCoordinator(Config{
		FileKey:           "test_file_key",
		AttemptID:         attemptID,
		TargetDir:         tmpDir,
		FileName:          "video.mp4",
		TotalSize:         totalSize,
		BlockSize:         2 * 1024 * 1024,
		SourceFingerprint: 12345678,
		Pool:              p,
	})
	require.NoError(t, err)
	require.NotNil(t, fc)
	assert.Equal(t, uint32(2), fc.TotalBlocks())
	assert.Equal(t, uint(0), rec.DurableBitmap.Count())

	ctx := context.Background()

	// 1. Download block 0
	off0, len0, err := fc.BeginChunk(0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), off0)
	assert.Equal(t, int64(2*1024*1024), len0)

	bufLease0, err := p.AcquireBuffer(ctx, len0)
	require.NoError(t, err)

	data0 := bytes.Repeat([]byte("A"), int(len0))
	err = fc.WriteBlock(ctx, 0, data0, bufLease0)
	require.NoError(t, err)

	// 2. Download block 1
	off1, len1, err := fc.BeginChunk(1)
	require.NoError(t, err)
	assert.Equal(t, int64(2*1024*1024), off1)
	assert.Equal(t, int64(2*1024*1024), len1)

	bufLease1, err := p.AcquireBuffer(ctx, len1)
	require.NoError(t, err)

	data1 := bytes.Repeat([]byte("B"), int(len1))
	err = fc.WriteBlock(ctx, 1, data1, bufLease1)
	require.NoError(t, err)

	// 3. Finalize
	ctxFin, cancelFin := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFin()

	err = fc.Finalize(ctxFin)
	require.NoError(t, err)
	assert.True(t, fc.IsComplete())

	// 4. Verify physical part file contents
	partData, err := os.ReadFile(fc.PartPath())
	require.NoError(t, err)
	assert.Equal(t, totalSize, int64(len(partData)))
	assert.Equal(t, byte('A'), partData[0])
	assert.Equal(t, byte('B'), partData[2*1024*1024])
}

func TestFileCoordinator_AbortAndRetry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_coord_abort_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p := lease.NewPool(lease.Config{})
	defer p.Close()

	var attemptID [16]byte
	copy(attemptID[:], []byte("attempt_abort_01"))

	fc, _, err := NewFileCoordinator(Config{
		FileKey:   "abort_file",
		AttemptID: attemptID,
		TargetDir: tmpDir,
		FileName:  "sample.bin",
		TotalSize: 4 * 1024 * 1024,
		BlockSize: 2 * 1024 * 1024,
		Pool:      p,
	})
	require.NoError(t, err)
	defer fc.Close()

	// Begin chunk 0
	_, _, err = fc.BeginChunk(0)
	require.NoError(t, err)

	// Second begin chunk 0 should fail
	_, _, err = fc.BeginChunk(0)
	assert.Equal(t, ErrBlockNotMissing, err)

	// Abort chunk 0
	fc.AbortChunk(0)

	// Re-claim chunk 0 should succeed
	_, _, err = fc.BeginChunk(0)
	assert.NoError(t, err)
}
