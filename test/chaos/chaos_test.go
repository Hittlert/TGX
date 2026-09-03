package chaos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/app/daemon"
	"github.com/Hittlert/TGX/internal/fscommit"
)

// TestChaos_CrashDuringPartWrite ensures an interrupted download leaves no dirty files
// and safely resets to pending.
func TestChaos_CrashDuringPartWrite(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "chaos.db")
	db, err := daemon.NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "archive")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	require.NoError(t, os.MkdirAll(archiveDir, 0o755))

	now := time.Now().Unix()
	relPath := "channel/interrupted.mp4"
	finalPath := filepath.Join(ssdDir, relPath)
	partPath := finalPath + ".part"
	require.NoError(t, os.MkdirAll(filepath.Dir(partPath), 0o755))
	require.NoError(t, os.WriteFile(partPath, []byte("partial_chunk_1"), 0o644))

	_, err = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, created_at, updated_at)
		VALUES ('100', 1, 'downloading', ?, 1024, ?, ?)
	`, relPath, now, now)
	require.NoError(t, err)

	// Simulate daemon restart crash recovery
	err = daemon.ReconcileOnStartup(context.Background(), db, ssdDir, archiveDir, zap.NewNop())
	require.NoError(t, err)

	// .part must be deleted
	_, err = os.Stat(partPath)
	assert.True(t, os.IsNotExist(err), "dirty .part must be cleaned up on crash recovery")

	// status must be reset to pending
	var status string
	_ = db.DB().QueryRow(`SELECT status FROM download_records WHERE chat_id = '100' AND message_id = 1`).Scan(&status)
	assert.Equal(t, "pending", status)
}

// TestChaos_CrashDuringCommittingWithValidPart ensures an atomic commit completes on restart.
func TestChaos_CrashDuringCommittingWithValidPart(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "chaos.db")
	db, err := daemon.NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "archive")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	require.NoError(t, os.MkdirAll(archiveDir, 0o755))

	now := time.Now().Unix()
	relPath := "channel/complete.mp4"
	finalPath := filepath.Join(ssdDir, relPath)
	partPath := finalPath + ".part"
	require.NoError(t, os.MkdirAll(filepath.Dir(partPath), 0o755))
	payload := []byte("fully downloaded payload data")
	require.NoError(t, os.WriteFile(partPath, payload, 0o644))
	shaHex := hex.EncodeToString(func() []byte { h := sha256.Sum256(payload); return h[:] }())

	// State was committed to DB right before rename
	_, err = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, sha256, created_at, updated_at)
		VALUES ('200', 2, 'committing', ?, ?, ?, ?, ?)
	`, relPath, int64(len(payload)), shaHex, now, now)
	require.NoError(t, err)

	// Simulate restart recovery
	err = daemon.ReconcileOnStartup(context.Background(), db, ssdDir, archiveDir, zap.NewNop())
	require.NoError(t, err)

	// .part must be atomically renamed to final
	_, err = os.Stat(partPath)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(finalPath)
	assert.NoError(t, err, "final file must exist after recovery")

	var status string
	_ = db.DB().QueryRow(`SELECT status FROM download_records WHERE chat_id = '200' AND message_id = 2`).Scan(&status)
	assert.Equal(t, "success", status)

	// Archive job must be queued
	var arcCount int
	_ = db.DB().QueryRow(`SELECT COUNT(*) FROM archive_jobs WHERE chat_id = '200' AND message_id = 2`).Scan(&arcCount)
	assert.Equal(t, 1, arcCount)
}

// TestChaos_CrashDuringArchiveCopy ensures moving residue is removed and job reset to pending.
func TestChaos_CrashDuringArchiveCopy(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "chaos.db")
	db, err := daemon.NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "archive")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	require.NoError(t, os.MkdirAll(archiveDir, 0o755))

	now := time.Now().Unix()
	relPath := "channel/video.mp4"
	dstFinal := filepath.Join(archiveDir, relPath)
	dstMoving := dstFinal + ".moving"
	require.NoError(t, os.MkdirAll(filepath.Dir(dstMoving), 0o755))
	require.NoError(t, os.WriteFile(dstMoving, []byte("halfway copied data"), 0o644))

	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, created_at, updated_at)
		VALUES ('300', 3, ?, 1024, 'dummy_sha', 'copying', 1, 0, ?, ?)
	`, relPath, now, now)
	require.NoError(t, err)

	err = daemon.ReconcileOnStartup(context.Background(), db, ssdDir, archiveDir, zap.NewNop())
	require.NoError(t, err)

	// .moving must be deleted
	_, err = os.Stat(dstMoving)
	assert.True(t, os.IsNotExist(err), "incomplete .moving file must be deleted")

	// Job state must be reset to pending
	var state string
	_ = db.DB().QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = '300' AND message_id = 3`).Scan(&state)
	assert.Equal(t, "pending", state)
}

// TestChaos_ArchiveDestinationConflict ensures conflicting destination content is never overwritten.
func TestChaos_ArchiveDestinationConflict(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "chaos.db")
	db, err := daemon.NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "archive")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	require.NoError(t, os.MkdirAll(archiveDir, 0o755))

	worker, err := daemon.NewArchiveWorker(db, ssdDir, archiveDir, zap.NewNop())
	require.NoError(t, err)

	relPath := "conflict/target.txt"
	ssdPath := filepath.Join(ssdDir, relPath)
	archivePath := filepath.Join(archiveDir, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(ssdPath), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(archivePath), 0o755))

	ssdData := []byte("brand new download data on ssd")
	arcData := []byte("pre-existing different data on hdd")
	require.NoError(t, os.WriteFile(ssdPath, ssdData, 0o644))
	require.NoError(t, os.WriteFile(archivePath, arcData, 0o644))

	now := time.Now().Unix()
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, created_at, updated_at)
		VALUES ('400', 4, ?, ?, 'ssd_sha', 'pending', 0, 0, ?, ?)
	`, relPath, int64(len(ssdData)), now, now)
	require.NoError(t, err)

	// Process archive job
	worker.Wake()
	time.Sleep(100 * time.Millisecond)

	// Verify both files are preserved intact
	currentSSD, err := os.ReadFile(ssdPath)
	require.NoError(t, err)
	assert.Equal(t, ssdData, currentSSD)

	currentArc, err := os.ReadFile(archivePath)
	require.NoError(t, err)
	assert.Equal(t, arcData, currentArc)
}

// TestChaos_SSDAdmissionCapacityRejection ensures requests exceeding available space are rejected cleanly.
func TestChaos_SSDAdmissionCapacityRejection(t *testing.T) {
	tempDir := t.TempDir()
	adm := fscommit.NewSSDAdmission(tempDir, 5*1024*1024*1024)

	// Attempt reservation of 100 PB (way beyond disk capacity)
	hugeSize := int64(100 * 1024 * 1024 * 1024 * 1024 * 1024)
	release, err := adm.Reserve("chaos:huge", hugeSize)
	assert.Error(t, err, "huge reservation must be rejected by admission owner")
	assert.Nil(t, release)
	assert.Equal(t, int64(0), adm.ReservedBytes())
}
