package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// TaskRecoveryResult describes the action taken for a task during startup reconciliation.
type TaskRecoveryResult struct {
	FileKey     string
	AttemptID   string
	PrevState   string
	NextState   string
	ActionTaken string
	Error       error
}

// Reconciler performs startup crash recovery against SQLite and physical filesystem state.
type Reconciler struct {
	db        *sql.DB
	outputDir string
	tempDir   string
	logger    *zap.Logger
}

// NewReconciler creates a startup reconciler.
func NewReconciler(db *sql.DB, outputDir, tempDir string, logger *zap.Logger) *Reconciler {
	return &Reconciler{
		db:        db,
		outputDir: outputDir,
		tempDir:   tempDir,
		logger:    logger,
	}
}

type taskRecord struct {
	fileKey     string
	attemptID   string
	state       string
	fileName    string
	totalSize   int64
	blockSize   uint32
	totalBlocks uint32
}

// ReconcileAll runs the full crash recovery matrix on all non-terminal tasks.
func (r *Reconciler) ReconcileAll(ctx context.Context) ([]TaskRecoveryResult, error) {
	query := `SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''), COALESCE(file_size, 0)
	          FROM download_records WHERE status = 'downloading'`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query download_records for recovery: %w", err)
	}
	defer rows.Close()

	var records []struct {
		ChatID    string
		MessageID int
		Status    string
		FileName  string
		SavePath  string
		FileSize  int64
	}
	for rows.Next() {
		var rec struct {
			ChatID    string
			MessageID int
			Status    string
			FileName  string
			SavePath  string
			FileSize  int64
		}
		if err := rows.Scan(&rec.ChatID, &rec.MessageID, &rec.Status, &rec.FileName, &rec.SavePath, &rec.FileSize); err != nil {
			continue
		}
		records = append(records, rec)
	}

	var results []TaskRecoveryResult
	for _, rec := range records {
		fileKey := fmt.Sprintf("%s:%d", rec.ChatID, rec.MessageID)
		finalPath := filepath.Join(r.outputDir, rec.SavePath)
		stat, err := os.Stat(finalPath)
		if err == nil && stat.Size() == rec.FileSize && rec.FileSize > 0 {
			// Final file exists with exact size -> promote to success
			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'success', error = '' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "success",
				ActionTaken: "FINAL_FILE_EXISTS_PROMOTED_TO_SUCCESS",
			})
		} else {
			// Reset back to pending so orchestrator re-queues it
			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "pending",
				ActionTaken: "INCOMPLETE_RESET_TO_PENDING",
			})
		}
	}

	return results, nil
}
