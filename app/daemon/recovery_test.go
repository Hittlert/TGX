package daemon

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
)

func TestReconcileOnStartup_Matrix(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "rec.db")
	db, err := NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "archive")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	require.NoError(t, os.MkdirAll(archiveDir, 0o755))

	logger := zap.NewNop()
	now := time.Now().Unix()

	// Case 1: downloading + .part exists + no final -> delete .part, reset to pending
	case1Rel := "chat1/file1.mp4"
	case1Final := filepath.Join(ssdDir, case1Rel)
	case1Part := case1Final + ".part"
	require.NoError(t, os.MkdirAll(filepath.Dir(case1Part), 0o755))
	require.NoError(t, os.WriteFile(case1Part, []byte("partial"), 0o644))
	_, err = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, created_at, updated_at)
		VALUES ('1', 1, 'downloading', ?, 100, ?, ?)
	`, case1Rel, now, now)
	require.NoError(t, err)

	// Case 2: downloading + final exists with matching size/sha -> success
	case2Rel := "chat2/file2.mp4"
	case2Final := filepath.Join(ssdDir, case2Rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(case2Final), 0o755))
	case2Payload := []byte("complete file 2 data")
	require.NoError(t, os.WriteFile(case2Final, case2Payload, 0o644))
	case2SHA := hex.EncodeToString(func() []byte { h := sha256.Sum256(case2Payload); return h[:] }())
	_, err = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, sha256, created_at, updated_at)
		VALUES ('2', 2, 'downloading', ?, ?, ?, ?, ?)
	`, case2Rel, int64(len(case2Payload)), case2SHA, now, now)
	require.NoError(t, err)

	// Case 3: committing + .part exists with matching sha -> atomic rename to final -> success
	case3Rel := "chat3/file3.mp4"
	case3Final := filepath.Join(ssdDir, case3Rel)
	case3Part := case3Final + ".part"
	require.NoError(t, os.MkdirAll(filepath.Dir(case3Part), 0o755))
	case3Payload := []byte("committing part file 3 data")
	require.NoError(t, os.WriteFile(case3Part, case3Payload, 0o644))
	case3SHA := hex.EncodeToString(func() []byte { h := sha256.Sum256(case3Payload); return h[:] }())
	_, err = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, save_path, file_size, sha256, created_at, updated_at)
		VALUES ('3', 3, 'committing', ?, ?, ?, ?, ?)
	`, case3Rel, int64(len(case3Payload)), case3SHA, now, now)
	require.NoError(t, err)

	// Case 4: archive copying + .moving exists, no archive final -> delete .moving, state pending
	case4Rel := "chat4/file4.mp4"
	case4DstFinal := filepath.Join(archiveDir, case4Rel)
	case4DstMoving := case4DstFinal + ".moving"
	require.NoError(t, os.MkdirAll(filepath.Dir(case4DstMoving), 0o755))
	require.NoError(t, os.WriteFile(case4DstMoving, []byte("moving data"), 0o644))
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, created_at, updated_at)
		VALUES ('4', 4, ?, 100, 'sha', 'copying', 0, 0, ?, ?)
	`, case4Rel, now, now)
	require.NoError(t, err)

	// Case 5: archive copying + archive final exists & verified -> mark archived, delete SSD duplicate
	case5Rel := "chat5/file5.mp4"
	case5SSDFinal := filepath.Join(ssdDir, case5Rel)
	case5ArcFinal := filepath.Join(archiveDir, case5Rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(case5SSDFinal), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(case5ArcFinal), 0o755))
	case5Payload := []byte("verified archive file 5")
	require.NoError(t, os.WriteFile(case5SSDFinal, case5Payload, 0o644))
	require.NoError(t, os.WriteFile(case5ArcFinal, case5Payload, 0o644))
	case5SHA := hex.EncodeToString(func() []byte { h := sha256.Sum256(case5Payload); return h[:] }())
	_, err = db.Execute(`
		INSERT INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, created_at, updated_at)
		VALUES ('5', 5, ?, ?, ?, 'copying', 0, 0, ?, ?)
	`, case5Rel, int64(len(case5Payload)), case5SHA, now, now)
	require.NoError(t, err)

	// Run startup reconciliation with typed storage meter
	meter := NewStorageIOMeter()
	err = ReconcileOnStartup(context.Background(), db, ssdDir, archiveDir, logger, meter)
	require.NoError(t, err)

	// Verify Case 1: .part deleted, status is pending
	_, err = os.Stat(case1Part)
	assert.True(t, os.IsNotExist(err), "case 1 part file should be deleted")
	var status1 string
	_ = db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '1' AND message_id = 1`).Scan(&status1)
	assert.Equal(t, "pending", status1)

	// Verify Case 2: status is reset to pending (downloading without commit intent)
	var status2 string
	_ = db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '2' AND message_id = 2`).Scan(&status2)
	assert.Equal(t, "pending", status2)

	// Verify Case 3: .part renamed to final, status is success
	_, err = os.Stat(case3Part)
	assert.True(t, os.IsNotExist(err), "case 3 part file should be renamed")
	_, err = os.Stat(case3Final)
	assert.NoError(t, err, "case 3 final file must exist")
	var status3 string
	_ = db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '3' AND message_id = 3`).Scan(&status3)
	assert.Equal(t, "success", status3)

	// Verify Case 4: .moving deleted, state is pending
	_, err = os.Stat(case4DstMoving)
	assert.True(t, os.IsNotExist(err), "case 4 moving file should be deleted")
	var state4 string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = '4' AND message_id = 4`).Scan(&state4)
	assert.Equal(t, "pending", state4)

	// Verify Case 5: state is archived, SSD duplicate deleted
	var state5 string
	_ = db.db.QueryRow(`SELECT state FROM archive_jobs WHERE chat_id = '5' AND message_id = 5`).Scan(&state5)
	assert.Equal(t, "archived", state5)
	_, err = os.Stat(case5SSDFinal)
	assert.True(t, os.IsNotExist(err), "case 5 SSD duplicate must be deleted after archive verified")

	// Verify strict two-tier storage meter separation during recovery
	assert.Equal(t, int64(len(case3Payload)), meter.SSDReadBytes(), "SSD recovery reads must strictly match SSD part file size")
	assert.Equal(t, int64(len(case5Payload)), meter.ArchiveReadBytes(), "Archive recovery reads must strictly match Archive final file size")
	assert.Equal(t, int64(0), meter.SSDWriteBytes(), "SSD recovery must not incur physical writes")
	assert.Equal(t, int64(0), meter.ArchiveWriteBytes(), "Archive recovery must not incur physical writes")
}
