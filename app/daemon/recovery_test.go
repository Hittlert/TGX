package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

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

func TestReconciler_PromotesExistingFileToSuccess(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir, err := os.MkdirTemp("", "sbe_rec_out_*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	tempDir, err := os.MkdirTemp("", "sbe_rec_tmp_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Final file exists with exact size
	finalPath := filepath.Join(outDir, "test.mp4")
	require.NoError(t, os.WriteFile(finalPath, []byte("data"), 0644))

	_, err = db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 1, 'downloading', 'test.mp4', 'test.mp4', 4)`)
	require.NoError(t, err)

	r := NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "success", results[0].NextState)
	assert.Equal(t, "FINAL_FILE_EXISTS_PROMOTED_TO_SUCCESS", results[0].ActionTaken)

	var newStatus string
	err = db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '123' AND message_id = 1`).Scan(&newStatus)
	require.NoError(t, err)
	assert.Equal(t, "success", newStatus)
}

func TestReconciler_ResetsIncompleteFileToPending(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	outDir, err := os.MkdirTemp("", "sbe_rec_out2_*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	tempDir, err := os.MkdirTemp("", "sbe_rec_tmp2_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Incomplete file missing on disk
	_, err = db.Exec(`INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, file_size) VALUES ('123', 2, 'downloading', 'missing.mp4', 'missing.mp4', 100)`)
	require.NoError(t, err)

	r := NewReconciler(db, outDir, tempDir, zap.NewNop())
	results, err := r.ReconcileAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	assert.Equal(t, "pending", results[0].NextState)
	assert.Equal(t, "INCOMPLETE_RESET_TO_PENDING", results[0].ActionTaken)

	var newStatus string
	err = db.QueryRow(`SELECT status FROM download_records WHERE chat_id = '123' AND message_id = 2`).Scan(&newStatus)
	require.NoError(t, err)
	assert.Equal(t, "pending", newStatus)
}
