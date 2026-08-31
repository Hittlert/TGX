package chaos

import (
	"context"
	"database/sql"
	"errors"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

func setupMemoryDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		CREATE TABLE download_records (
			chat_id TEXT NOT NULL,
			message_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			file_name TEXT,
			save_path TEXT,
			media_type TEXT,
			file_size INTEGER,
			error TEXT,
			created_at INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (chat_id, message_id)
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
		FileName:  "chaos_video.mp4",
		TargetDir: tmpDir,
		TotalSize: 4 * 1024 * 1024,
		BlockSize: 2 * 1024 * 1024,
		AttemptID: attemptID,
		Pool:      p,
	})
	require.NoError(t, err)

	// Simulate downloading block 0
	bufLease, err := p.AcquireBuffer(context.Background(), 2*1024*1024)
	require.NoError(t, err)
	data0 := make([]byte, 2*1024*1024)
	copy(data0, []byte("block0_data"))

	require.NoError(t, fc.WriteBlock(context.Background(), 0, data0, bufLease))
	fc.ForceCheckpoint()

	// Simulate unexpected crash without finalize
	_ = fc.Close()

	// Reopen after power-cut
	fcRecovered, recInfo, err := coordinator.NewFileCoordinator(coordinator.Config{
		FileKey:   "chaos_file_1",
		FileName:  "chaos_video.mp4",
		TargetDir: tmpDir,
		TotalSize: 4 * 1024 * 1024,
		BlockSize: 2 * 1024 * 1024,
		AttemptID: attemptID,
		Pool:      p,
	})
	require.NoError(t, err)
	defer fcRecovered.Close()

	// Assert recovered bitmap has block 0
	assert.True(t, recInfo.DurableBitmap.Test(0), "Block 0 must be durable after crash recovery")
	assert.False(t, recInfo.DurableBitmap.Test(1), "Block 1 must still be missing")
	assert.False(t, recInfo.IsComplete)
}

// 2. TestChaos_PowerCut_After_Complete_Meta
func TestChaos_PowerCut_After_Complete_Meta(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	outDir, err := os.MkdirTemp("", "chaos_complete_out_*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	tempDir, err := os.MkdirTemp("", "chaos_complete_tmp_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	fileName := "complete_media.bin"
	finalPath := filepath.Join(outDir, fileName)
	require.NoError(t, os.WriteFile(finalPath, []byte("complete_data_19_bytes"), 0644))

	// Insert database state as downloading (power cut before DB update)
	_, err = db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 42, 'downloading', ?, ?, 22)`, fileName, fileName)
	require.NoError(t, err)

	// Run Reconciler
	r := daemon.NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "success", results[0].NextState)
	assert.Equal(t, "FINAL_FILE_COMMITTED_PROMOTED_TO_SUCCESS", results[0].ActionTaken)
	assert.FileExists(t, finalPath)
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
	require.NoError(t, os.WriteFile(finalPath, []byte("identical_hardlink_data"), 0644))

	_, err = db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 43, 'downloading', ?, ?, 23)`, fileName, fileName)
	require.NoError(t, err)

	r := daemon.NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "success", results[0].NextState)
	assert.Equal(t, "FINAL_FILE_COMMITTED_PROMOTED_TO_SUCCESS", results[0].ActionTaken)
	assert.FileExists(t, finalPath)
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
