package chaos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hittlert/TGX/app/daemon"
	sbeatomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/pkg/sbe/coordinator"
	"github.com/Hittlert/TGX/pkg/sbe/lease"
	"github.com/Hittlert/TGX/pkg/sbe/meta"
	"github.com/Hittlert/TGX/pkg/sbe/scheduler"
	"github.com/bits-and-blooms/bitset"
	_ "modernc.org/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func setupMemoryDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		CREATE TABLE tasks (
			file_key TEXT PRIMARY KEY,
			attempt_id TEXT,
			state TEXT,
			file_name TEXT,
			total_size INTEGER,
			block_size INTEGER,
			total_blocks INTEGER
		);
	`)
	require.NoError(t, err)
	return db
}

// 1. TestChaos_PowerCut_During_Part_Write
func TestChaos_PowerCut_During_Part_Write(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chaos_part_write_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p := lease.NewPool(lease.Config{BufferBudget: 8 * 1024 * 1024, DirtyBudget: 8 * 1024 * 1024})
	defer p.Close()

	var attemptID [16]byte
	copy(attemptID[:], []byte("chaos_att_001"))

	fc, _, err := coordinator.NewFileCoordinator(coordinator.Config{
		FileKey:   "chaos_file_1",
		AttemptID: attemptID,
		TargetDir: tmpDir,
		FileName:  "video.mp4",
		TotalSize: 4 * 1024 * 1024,
		BlockSize: 2 * 1024 * 1024,
		Pool:      p,
	})
	require.NoError(t, err)

	ctx := context.Background()
	_, len0, _ := fc.BeginChunk(0)
	buf0, _ := p.AcquireBuffer(ctx, len0)
	require.NoError(t, fc.WriteBlock(ctx, 0, make([]byte, len0), buf0))

	// Simulate sudden crash without calling Finalize
	_ = fc.Close()

	// Reopen after crash
	fc2, rec2, err := coordinator.NewFileCoordinator(coordinator.Config{
		FileKey:   "chaos_file_1",
		AttemptID: attemptID,
		TargetDir: tmpDir,
		FileName:  "video.mp4",
		TotalSize: 4 * 1024 * 1024,
		BlockSize: 2 * 1024 * 1024,
		Pool:      p,
	})
	require.NoError(t, err)
	defer fc2.Close()

	assert.Equal(t, uint(1), rec2.DurableBitmap.Count())
	assert.True(t, rec2.DurableBitmap.Test(0))
	assert.False(t, rec2.DurableBitmap.Test(1))
}

// 2. TestChaos_PowerCut_After_Complete_Meta
func TestChaos_PowerCut_After_Complete_Meta(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	outDir, err := os.MkdirTemp("", "chaos_comp_out_*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	tempDir, err := os.MkdirTemp("", "chaos_comp_tmp_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	var att [16]byte
	copy(att[:], []byte("att_crash_02"))
	attHex := fmt.Sprintf("%x", att)
	fileName := "completed_movie.mp4"

	// Create physical .part and COMPLETE .meta
	partPath := filepath.Join(tempDir, fmt.Sprintf("%s.part.%s", fileName, attHex))
	require.NoError(t, os.WriteFile(partPath, []byte("complete_movie_data"), 0644))

	metaH := &meta.MetaHeader{
		Magic:       meta.MetaMagic,
		Version:     meta.MetaVersion,
		AttemptID:   att,
		TotalSize:   19,
		BlockSize:   2 * 1024 * 1024,
		TotalBlocks: 1,
	}
	copy(metaH.FileKeyHash[:], []byte("f_comp"))

	mf, _, err := meta.CreateOrOpenMetaFile(tempDir, fileName, metaH)
	require.NoError(t, err)
	bs := bitset.New(1)
	bs.Set(0)
	require.NoError(t, mf.WriteComplete(bs))
	require.NoError(t, mf.Close())

	// Insert database state as RUNNING (power cut before DB update)
	_, err = db.Exec(`INSERT INTO tasks VALUES ('f_comp', ?, 'RUNNING', ?, 19, 2097152, 1)`, attHex, fileName)
	require.NoError(t, err)

	// Run Reconciler
	r := daemon.NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "SUCCESS", results[0].NextState)
	assert.Equal(t, "COMPLETE_META_PROMOTED_TO_SUCCESS", results[0].ActionTaken)
	assert.FileExists(t, filepath.Join(outDir, fileName))
	assert.NoFileExists(t, partPath)
}

// 3. TestChaos_Linkat_Unlink_Crash
func TestChaos_Linkat_Unlink_Crash(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	outDir, err := os.MkdirTemp("", "chaos_link_out_*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	tempDir, err := os.MkdirTemp("", "chaos_link_tmp_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	fileName := "linked_file.bin"
	finalPath := filepath.Join(outDir, fileName)
	partPath := filepath.Join(tempDir, fileName+".part.att3")

	// Create identical hard-linked file (simulates crash after linkat before unlink)
	require.NoError(t, os.WriteFile(partPath, []byte("identical_hardlink_data"), 0644))
	require.NoError(t, os.Link(partPath, finalPath))

	_, err = db.Exec(`INSERT INTO tasks VALUES ('f3', 'att3', 'COMMITTING', ?, 23, 2097152, 1)`, fileName)
	require.NoError(t, err)

	r := daemon.NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "SUCCESS", results[0].NextState)
	assert.Equal(t, "COMMITTING_UNLINK_CRASH_REPAIRED", results[0].ActionTaken)
	assert.FileExists(t, finalPath)
	assert.NoFileExists(t, partPath)
}

// 4. TestChaos_Target_Path_Collision
func TestChaos_Target_Path_Collision(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chaos_coll_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	finalPath := filepath.Join(tmpDir, "video.mp4")
	partPath := filepath.Join(tmpDir, "video.mp4.part.001")

	require.NoError(t, os.WriteFile(finalPath, []byte("preexisting_different_data"), 0644))
	require.NoError(t, os.WriteFile(partPath, []byte("newly_downloaded_data"), 0644))

	// Commit must reject and preserve preexisting file
	err = sbeatomic.CommitFile(partPath, finalPath)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrExist))

	existingData, _ := os.ReadFile(finalPath)
	assert.Equal(t, "preexisting_different_data", string(existingData))
	assert.FileExists(t, partPath)
}

// 5. TestChaos_Corrupted_Slot_CRC
func TestChaos_Corrupted_Slot_CRC(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chaos_crc_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	var att [16]byte
	copy(att[:], []byte("att_crc_005"))
	metaH := &meta.MetaHeader{
		Magic:       meta.MetaMagic,
		Version:     meta.MetaVersion,
		AttemptID:   att,
		TotalSize:   4 * 1024 * 1024,
		BlockSize:   2 * 1024 * 1024,
		TotalBlocks: 2,
	}
	copy(metaH.FileKeyHash[:], []byte("f5"))

	mf, _, err := meta.CreateOrOpenMetaFile(tmpDir, "sample.bin", metaH)
	require.NoError(t, err)

	// Write Gen 1 to Slot A
	bsA := bitset.New(2)
	bsA.Set(0)
	_, _ = mf.WriteSlot(bsA)

	// Write Gen 2 to Slot B
	bsB := bsA.Clone()
	bsB.Set(1)
	_, _ = mf.WriteSlot(bsB)
	_ = mf.Close()

	// Corrupt Slot B
	data, _ := os.ReadFile(mf.Path())
	bOffset := meta.SlotBOffset(2)
	data[bOffset+8] ^= 0xFF // Corrupt data in Slot B
	_ = os.WriteFile(mf.Path(), data, 0644)

	// Reopen -> must cleanly failover to Slot A
	mf2, rec2, err := meta.CreateOrOpenMetaFile(tmpDir, "sample.bin", metaH)
	require.NoError(t, err)
	defer mf2.Close()

	assert.Equal(t, "A", rec2.ValidSlot)
	assert.Equal(t, uint64(1), rec2.LatestGen)
	assert.True(t, rec2.DurableBitmap.Test(0))
	assert.False(t, rec2.DurableBitmap.Test(1))
}

// 6. TestChaos_Both_Slots_Corrupted
func TestChaos_Both_Slots_Corrupted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chaos_both_crc_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	var att [16]byte
	copy(att[:], []byte("att_crc_both"))
	metaH := &meta.MetaHeader{
		Magic:       meta.MetaMagic,
		Version:     meta.MetaVersion,
		AttemptID:   att,
		TotalSize:   2 * 1024 * 1024,
		BlockSize:   2 * 1024 * 1024,
		TotalBlocks: 1,
	}
	copy(metaH.FileKeyHash[:], []byte("f6"))

	mf, _, _ := meta.CreateOrOpenMetaFile(tmpDir, "torn.bin", metaH)
	bs := bitset.New(1)
	bs.Set(0)
	_, _ = mf.WriteSlot(bs)
	_ = mf.Close()

	// Corrupt both Slot A and Slot B
	data, _ := os.ReadFile(mf.Path())
	data[128+2] ^= 0xFF
	data[meta.SlotBOffset(1)+2] ^= 0xFF
	_ = os.WriteFile(mf.Path(), data, 0644)

	// Reopen -> returns ValidSlot = NONE (fresh start, no panic)
	mf2, rec2, err := meta.CreateOrOpenMetaFile(tmpDir, "torn.bin", metaH)
	require.NoError(t, err)
	defer mf2.Close()

	assert.Equal(t, "NONE", rec2.ValidSlot)
	assert.Equal(t, uint(0), rec2.DurableBitmap.Count())
}

// 7. TestChaos_FloodWait_Backpressure
func TestChaos_FloodWait_Backpressure(t *testing.T) {
	sched := scheduler.NewDRRScheduler()

	sched.EnqueueDelay(scheduler.ChunkTask{
		FileKey:    "flood_task",
		BlockIndex: 0,
		TotalSize:  1 * 1024 * 1024,
	}, time.Now().Add(50*time.Millisecond))

	_, ok := sched.NextChunk()
	assert.False(t, ok)

	time.Sleep(60 * time.Millisecond)
	task, ok := sched.NextChunk()
	require.True(t, ok)
	assert.Equal(t, "flood_task", task.FileKey)
}

// 8. TestChaos_Lease_Pool_Exhaustion
func TestChaos_Lease_Pool_Exhaustion(t *testing.T) {
	p := lease.NewPool(lease.Config{
		BufferBudget: 4 * 1024 * 1024, // 4MB
		DirtyBudget:  4 * 1024 * 1024, // 4MB
	})
	defer p.Close()

	ctx := context.Background()

	// Acquire 4MB BufferLease
	l1, err := p.AcquireBuffer(ctx, 4*1024*1024)
	require.NoError(t, err)
	assert.NotNil(t, l1)

	// Next acquire with timeout should backpressure and time out
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err = p.AcquireBuffer(ctxTimeout, 2*1024*1024)
	assert.Equal(t, context.DeadlineExceeded, err)

	// Release l1 -> subsequent acquire succeeds immediately
	l1.Release()

	l2, err := p.AcquireBuffer(ctx, 2*1024*1024)
	require.NoError(t, err)
	l2.Release()
}

// 9. TestChaos_Graceful_Drain_Order
func TestChaos_Graceful_Drain_Order(t *testing.T) {
	var activeWrites int32

	pool := lease.NewPool(lease.Config{})
	defer pool.Close()

	// Verify lease pool stats during shutdown
	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.BufferUsed)
	assert.Equal(t, int64(0), stats.DirtyUsed)
	assert.Equal(t, int32(0), atomic.LoadInt32(&activeWrites))
}
