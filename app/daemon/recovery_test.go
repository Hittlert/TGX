package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/Hittlert/TGX/pkg/sbe/meta"
	"github.com/bits-and-blooms/bitset"
	_ "modernc.org/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func setupTestDB(t *testing.T) *sql.DB {
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

func TestReconciler_CommittingCrashPromotion(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir, err := os.MkdirTemp("", "sbe_rec_out_*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	tempDir, err := os.MkdirTemp("", "sbe_rec_tmp_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Final file exists
	finalPath := filepath.Join(outDir, "test.mp4")
	require.NoError(t, os.WriteFile(finalPath, []byte("data"), 0644))

	_, err = db.Exec(`INSERT INTO tasks VALUES ('f1', 'att1', 'COMMITTING', 'test.mp4', 4, 2, 2)`)
	require.NoError(t, err)

	r := NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "SUCCESS", results[0].NextState)
	assert.Equal(t, "COMMITTING_RENAME_ALREADY_DONE_PROMOTED", results[0].ActionTaken)

	var newState string
	err = db.QueryRow(`SELECT state FROM tasks WHERE file_key = 'f1'`).Scan(&newState)
	require.NoError(t, err)
	assert.Equal(t, "SUCCESS", newState)
}

func TestReconciler_RunningCompleteMetaPromotion(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir, err := os.MkdirTemp("", "sbe_rec_out2_*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	tempDir, err := os.MkdirTemp("", "sbe_rec_tmp2_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	var attempt [16]byte
	copy(attempt[:], []byte("att2"))
	fileName := "complete.bin"

	// Create .part file
	partPath := filepath.Join(tempDir, fileName+".part.61747432000000000000000000000000")
	require.NoError(t, os.WriteFile(partPath, []byte("complete_data"), 0644))

	// Create COMPLETE .meta file
	metaH := &meta.MetaHeader{
		Magic:       meta.MetaMagic,
		Version:     meta.MetaVersion,
		AttemptID:   attempt,
		TotalSize:   13,
		BlockSize:   2 * 1024 * 1024,
		TotalBlocks: 1,
	}
	copy(metaH.FileKeyHash[:], []byte("f2"))

	mf, _, err := meta.CreateOrOpenMetaFile(tempDir, fileName, metaH)
	require.NoError(t, err)

	bs := bitset.New(1)
	bs.Set(0)
	require.NoError(t, mf.WriteComplete(bs))
	require.NoError(t, mf.Close())

	// Insert task with RUNNING state
	_, err = db.Exec(`INSERT INTO tasks VALUES ('f2', '61747432000000000000000000000000', 'RUNNING', 'complete.bin', 13, 2097152, 1)`)
	require.NoError(t, err)

	r := NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "SUCCESS", results[0].NextState)
	assert.Equal(t, "COMPLETE_META_PROMOTED_TO_SUCCESS", results[0].ActionTaken)

	// Verify final file was created
	finalPath := filepath.Join(outDir, fileName)
	assert.FileExists(t, finalPath)
}
