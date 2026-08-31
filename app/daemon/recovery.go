package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/mover"
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
	db         *sql.DB
	outputDir  string
	tempDir    string
	bufferType string
	mover      *mover.Mover
	logger     *zap.Logger
}

// NewReconciler creates a startup reconciler with buffer awareness.
func NewReconciler(db *sql.DB, outputDir, tempDir string, logger *zap.Logger) *Reconciler {
	return NewReconcilerWithBuffer(db, outputDir, tempDir, "memory", nil, logger)
}

// NewReconcilerWithBuffer creates a startup reconciler with specific buffer type and mover.
func NewReconcilerWithBuffer(db *sql.DB, outputDir, tempDir, bufferType string, seqMover *mover.Mover, logger *zap.Logger) *Reconciler {
	if bufferType == "" {
		bufferType = "memory"
	}
	return &Reconciler{
		db:         db,
		outputDir:  outputDir,
		tempDir:    tempDir,
		bufferType: bufferType,
		mover:      seqMover,
		logger:     logger,
	}
}

// ReconcileAll runs the differential crash recovery matrix on all non-terminal tasks.
func (r *Reconciler) ReconcileAll(ctx context.Context) ([]TaskRecoveryResult, error) {
	// Clean up any stale .moving leftover files in outputDir
	r.cleanStaleMovingFiles(r.outputDir)

	query := `SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''), COALESCE(file_size, 0)
	          FROM download_records WHERE status IN ('downloading', 'moving')`
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
		fileKey := CanonicalTaskID(rec.ChatID, rec.MessageID)
		finalPath := filepath.Join(r.outputDir, rec.SavePath)

		// 1. Check if final file already exists with exact size in target directory
		stat, err := os.Stat(finalPath)
		if err == nil && stat.Size() == rec.FileSize && rec.FileSize > 0 {
			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'success', error = '' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "success",
				ActionTaken: "FINAL_FILE_EXISTS_PROMOTED_TO_SUCCESS",
			})
			continue
		}

		// 2. Compute canonical part file path in tempDir or outputDir
		tempPartPath := CanonicalPartPath(r.tempDir, fileKey)
		if r.bufferType == "none" {
			tempPartPath = CanonicalPartPath(filepath.Dir(finalPath), fileKey)
		}

		// 3. Medium-aware recovery logic
		switch r.bufferType {
		case "memory":
			// Volatile memory: wipe any stale temp reference and reset to pending for full fresh download
			_ = os.Remove(tempPartPath)
			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "pending",
				ActionTaken: "MEMORY_BUFFER_VOLATILE_RESET_TO_PENDING",
			})

		case "ssd":
			// Non-volatile persistent SSD: check if temp file exists
			partStat, partErr := os.Stat(tempPartPath)
			if partErr == nil && partStat.Size() == rec.FileSize && rec.FileSize > 0 {
				// Completed on SSD buffer: if mover is available, re-enqueue for zero-network moving!
				if r.mover != nil {
					chatID := rec.ChatID
					msgID := rec.MessageID
					fileSize := rec.FileSize
					job := &mover.MoveJob{
						ID:      fileKey,
						SrcPath: tempPartPath,
						DstPath: finalPath,
						Size:    fileSize,
						OnDone: func(moveErr error) {
							if moveErr == nil {
								_, _ = r.db.Exec(`UPDATE download_records SET status = 'success', error = '' WHERE chat_id = ? AND message_id = ?`, chatID, msgID)
							} else {
								_, _ = r.db.Exec(`UPDATE download_records SET status = 'failed', error = ? WHERE chat_id = ? AND message_id = ?`, moveErr.Error(), chatID, msgID)
							}
						},
					}
					if err := r.mover.Enqueue(job); err == nil {
						_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'moving' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
						results = append(results, TaskRecoveryResult{
							FileKey:     fileKey,
							PrevState:   rec.Status,
							NextState:   "moving",
							ActionTaken: "SSD_BUFFER_COMPLETED_REQUEUED_IN_MOVER",
						})
						continue
					}
				}
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "pending",
					ActionTaken: "SSD_BUFFER_COMPLETED_RETAINED_FOR_MOVING",
				})
			} else if partErr == nil && partStat.Size() > 0 {
				// Partial .part on SSD: keep file for resumable block download
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "pending",
					ActionTaken: "SSD_BUFFER_PARTIAL_RETAINED_FOR_RESUME",
				})
			} else {
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "pending",
					ActionTaken: "SSD_BUFFER_NO_PART_RESET_TO_PENDING",
				})
			}

		default: // "none"
			partStat, partErr := os.Stat(tempPartPath)
			if partErr == nil && partStat.Size() > 0 {
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "pending",
					ActionTaken: "DIRECT_TARGET_PARTIAL_RETAINED_FOR_RESUME",
				})
			} else {
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "pending",
					ActionTaken: "DIRECT_TARGET_RESET_TO_PENDING",
				})
			}
		}
	}

	return results, nil
}

func (r *Reconciler) cleanStaleMovingFiles(dir string) {
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(path) == ".moving" {
			_ = os.Remove(path)
		}
		return nil
	})
}
