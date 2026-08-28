package sbe

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hittlert/TGX/pkg/sbe/coordinator"
	"github.com/Hittlert/TGX/pkg/sbe/meta"
	"github.com/Hittlert/TGX/pkg/sbe/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_ConcurrentDownloadingAndCommit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_engine_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	mockFetcher := func(ctx context.Context, task scheduler.ChunkTask, buf []byte) (int64, error) {
		// Fill buffer with block index byte
		val := byte('A' + task.BlockIndex)
		for i := range buf {
			buf[i] = val
		}
		return int64(len(buf)), nil
	}

	engine := NewEngine(EngineConfig{
		NetworkWorkers: 8,
		DiskWorkers:    2,
		BufferBudget:   16 * 1024 * 1024,
		DirtyBudget:    16 * 1024 * 1024,
		BlockFetcher:   mockFetcher,
	})
	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = engine.Shutdown(ctx)
	}()

	// Register File 1: 6MB (3 blocks)
	var attempt1 [16]byte
	copy(attempt1[:], []byte("att_001"))
	fc1, _, err := engine.RegisterFile(coordinator.Config{
		FileKey:   "file_1",
		AttemptID: attempt1,
		TargetDir: tmpDir,
		FileName:  "movie1.mp4",
		TotalSize: 6 * 1024 * 1024,
		BlockSize: meta.StandardBlockSize,
	})
	require.NoError(t, err)

	// Register File 2: 4MB (2 blocks)
	var attempt2 [16]byte
	copy(attempt2[:], []byte("att_002"))
	fc2, _, err := engine.RegisterFile(coordinator.Config{
		FileKey:   "file_2",
		AttemptID: attempt2,
		TargetDir: tmpDir,
		FileName:  "movie2.mp4",
		TotalSize: 4 * 1024 * 1024,
		BlockSize: meta.StandardBlockSize,
	})
	require.NoError(t, err)

	// Schedule both files
	require.NoError(t, engine.ScheduleFile(fc1))
	require.NoError(t, engine.ScheduleFile(fc2))

	// Wait for completion (max 5s)
	require.Eventually(t, func() bool {
		return fc1.IsComplete() && fc2.IsComplete()
	}, 5*time.Second, 50*time.Millisecond)

	ctxCommit := context.Background()

	// Commit File 1
	err = engine.CommitCompletedFile(ctxCommit, fc1)
	require.NoError(t, err)

	// Commit File 2
	err = engine.CommitCompletedFile(ctxCommit, fc2)
	require.NoError(t, err)

	// Verify physical destination files
	dest1 := filepath.Join(tmpDir, "movie1.mp4")
	dest2 := filepath.Join(tmpDir, "movie2.mp4")

	assert.FileExists(t, dest1)
	assert.FileExists(t, dest2)

	data1, err := os.ReadFile(dest1)
	require.NoError(t, err)
	assert.Equal(t, 6*1024*1024, len(data1))
	assert.Equal(t, byte('A'), data1[0])
	assert.Equal(t, byte('B'), data1[2*1024*1024])
	assert.Equal(t, byte('C'), data1[4*1024*1024])

	data2, err := os.ReadFile(dest2)
	require.NoError(t, err)
	assert.Equal(t, 4*1024*1024, len(data2))
	assert.Equal(t, byte('A'), data2[0])
	assert.Equal(t, byte('B'), data2[2*1024*1024])
}

func TestEngine_ShutdownGracefulDrain(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_shutdown_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engine := NewEngine(EngineConfig{
		NetworkWorkers: 4,
		DiskWorkers:    2,
	})
	engine.Start()

	var attempt [16]byte
	copy(attempt[:], []byte("shutdown_att"))
	_, _, err = engine.RegisterFile(coordinator.Config{
		FileKey:   "shutdown_file",
		AttemptID: attempt,
		TargetDir: tmpDir,
		FileName:  "test.dat",
		TotalSize: 2 * 1024 * 1024,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = engine.Shutdown(ctx)
	assert.NoError(t, err)
}
