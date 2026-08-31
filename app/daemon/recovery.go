package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/mover"
	"github.com/Hittlert/TGX/core/targetwriter"
)

// TaskRecoveryResult encapsulates differential actions performed per task.
type TaskRecoveryResult struct {
	FileKey     string
	PrevState   string
	NextState   string
	ActionTaken string
}

// Reconciler performs differential crash recovery on non-terminal tasks at boot.
type Reconciler struct {
	db         *sql.DB
	outputDir  string
	tempDir    string
	bufferType string
	mover      *mover.Mover
	tw         *targetwriter.TargetWriter
	logger     *zap.Logger
}

func NewReconciler(db *sql.DB, outputDir, tempDir string, logger *zap.Logger) *Reconciler {
	return NewReconcilerWithBuffer(db, outputDir, tempDir, "memory", nil, logger)
}

func NewReconcilerWithBuffer(db *sql.DB, outputDir, tempDir, bufferType string, seqMover *mover.Mover, logger *zap.Logger) *Reconciler {
	return &Reconciler{
		db:         db,
		outputDir:  outputDir,
		tempDir:    tempDir,
		bufferType: bufferType,
		mover:      seqMover,
		logger:     logger,
	}
}

func (r *Reconciler) SetTargetWriter(tw *targetwriter.TargetWriter) {
	r.tw = tw
}

// ReconcileAll runs the differential crash recovery matrix on all non-terminal tasks.
func (r *Reconciler) ReconcileAll(ctx context.Context) ([]TaskRecoveryResult, error) {
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
		movingPath := finalPath + ".moving"
		metaPath := finalPath + ".moving.meta"

		// 1. Check if final file already exists with exact size in target directory
		stat, err := os.Stat(finalPath)
		if err == nil && stat.Size() == rec.FileSize && rec.FileSize > 0 {
			_ = os.Remove(movingPath)
			_ = os.Remove(metaPath)
			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'success', error = '' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "success",
				ActionTaken: "FINAL_FILE_EXISTS_PROMOTED_TO_SUCCESS",
			})
			continue
		}

		// 2. Medium-aware recovery logic
		switch r.bufferType {
		case "memory":
			// Volatile memory: clean stale .moving and sidecar meta, reset to pending
			_ = os.Remove(movingPath)
			_ = os.Remove(metaPath)
			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "pending",
				ActionTaken: "MEMORY_BUFFER_VOLATILE_RESET_TO_PENDING",
			})

		case "ssd":
			// Check if sidecar .moving.meta exists to resume target moving
			metaData, metaErr := os.ReadFile(metaPath)
			if metaErr == nil {
				var manifest targetwriter.TaskManifest
				if json.Unmarshal(metaData, &manifest) == nil && r.tw != nil {
					// Fix P0-3: Validate .moving file and sidecar ranges consistency
					valid := true
					reason := ""

					// 1. TaskID must match
					if manifest.TaskID != fileKey {
						valid = false
						reason = "taskID mismatch"
					}

					// 2. .moving file must exist
					movingStat, movingStatErr := os.Stat(movingPath)
					if movingStatErr != nil {
						valid = false
						reason = ".moving file missing"
					}

					// 3. Validate ranges
					if valid {
						var maxEnd int64
						for _, rng := range manifest.Ranges {
							if rng.Start < 0 || rng.End <= rng.Start {
								valid = false
								reason = "invalid range bounds"
								break
							}
							if manifest.ExpectedSize > 0 && rng.End > manifest.ExpectedSize {
								valid = false
								reason = "range exceeds expected size"
								break
							}
							if rng.End > maxEnd {
								maxEnd = rng.End
							}
						}
						// .moving must be at least as large as the highest durable range
						if valid && movingStat.Size() < maxEnd {
							valid = false
							reason = fmt.Sprintf(".moving size %d < max range end %d", movingStat.Size(), maxEnd)
						}
					}

					if valid {
						r.tw.RegisterTask(manifest)
						results = append(results, TaskRecoveryResult{
							FileKey:     fileKey,
							PrevState:   rec.Status,
							NextState:   "moving",
							ActionTaken: "SSD_BUFFER_RESUMED_IN_TARGET_WRITER",
						})
						continue
					}

					// Invalid sidecar: clean up and fall through to reset
					r.logger.Warn("invalid sidecar metadata, resetting task",
						zap.String("task", fileKey), zap.String("reason", reason))
					_ = os.Remove(movingPath)
					_ = os.Remove(metaPath)
				}
			}

			// Fallback: check legacy part file or reset to pending
			tempPartPath := CanonicalPartPath(r.tempDir, fileKey)
			partStat, partErr := os.Stat(tempPartPath)
			if partErr == nil && partStat.Size() == rec.FileSize && rec.FileSize > 0 {
				if r.mover != nil {
					chatID := rec.ChatID
					msgID := rec.MessageID
					_ = r.mover.Enqueue(&mover.MoveJob{
						ID:      fileKey,
						SrcPath: tempPartPath,
						DstPath: finalPath,
						Size:    rec.FileSize,
						OnDone: func(err error) {
							if err == nil {
								_, _ = r.db.Exec(`UPDATE download_records SET status = 'success', error = '' WHERE chat_id = ? AND message_id = ?`, chatID, msgID)
							} else {
								_, _ = r.db.Exec(`UPDATE download_records SET status = 'failed', error = ? WHERE chat_id = ? AND message_id = ?`, err.Error(), chatID, msgID)
							}
						},
					})
				}
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'moving' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "moving",
					ActionTaken: "SSD_BUFFER_COMPLETED_REQUEUED_IN_MOVER",
				})
				continue
			} else if partErr == nil && partStat.Size() > 0 {
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "pending",
					ActionTaken: "SSD_BUFFER_PARTIAL_RETAINED_FOR_RESUME",
				})
				continue
			}

			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "pending",
				ActionTaken: "SSD_BUFFER_RESET_TO_PENDING",
			})

		default:
			_ = os.Remove(movingPath)
			_ = os.Remove(metaPath)
			_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "pending",
				ActionTaken: "DIRECT_TARGET_RESET_TO_PENDING",
			})
		}
	}

	return results, nil
}
