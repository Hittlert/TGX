package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	atomicCommit "github.com/Hittlert/TGX/pkg/sbe/atomic"
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
	registry   *Registry
	logger     *zap.Logger
}

func NewReconciler(db *sql.DB, outputDir, tempDir string, logger *zap.Logger) *Reconciler {
	return NewReconcilerWithBuffer(db, outputDir, tempDir, "memory", nil, logger)
}

func NewReconcilerWithBuffer(db *sql.DB, outputDir, tempDir, bufferType string, _ any, logger *zap.Logger) *Reconciler {
	return &Reconciler{
		db:         db,
		outputDir:  outputDir,
		tempDir:    tempDir,
		bufferType: bufferType,
		logger:     logger,
	}
}

func (r *Reconciler) SetRegistry(reg *Registry) {
	r.registry = reg
}

// ReconcileAll runs the differential crash recovery matrix on all non-terminal tasks,
// as well as any tasks that were interrupted/failed due to shutdown cancellation.
func (r *Reconciler) ReconcileAll(ctx context.Context) ([]TaskRecoveryResult, error) {
	query := `SELECT chat_id, message_id, status, COALESCE(file_name, ''), COALESCE(save_path, ''), COALESCE(file_size, 0)
	          FROM download_records
	          WHERE (status IN ('downloading', 'moving'))
	             OR (status = 'failed' AND (error LIKE '%context canceled%' OR error LIKE '%context deadline exceeded%' OR error = 'canceled' OR error = 'task canceled' OR error LIKE '%forcibly closed%'))`
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

		movingMatches, _ := filepath.Glob(finalPath + ".*.moving")
		hasMoving := false
		if _, movingErr := os.Stat(movingPath); movingErr == nil {
			hasMoving = true
		}
		if len(movingMatches) > 0 {
			hasMoving = true
		}

		tempPartPath := CanonicalPartPath(r.tempDir, fileKey)
		_, partErr := os.Stat(tempPartPath)
		hasTempPart := (partErr == nil)

		stat, err := os.Stat(finalPath)
		if err == nil && stat.Size() == rec.FileSize && rec.FileSize > 0 && !hasMoving && !hasTempPart {
			// Query target_commits table for authoritative committed SHA
			var committedSHA string
			_ = r.db.QueryRowContext(ctx, `SELECT committed_sha256 FROM target_commits WHERE task_id = ? ORDER BY updated_at DESC LIMIT 1`, fileKey).Scan(&committedSHA)

			if committedSHA != "" {
				verifiedSHA, verifyErr := verifyFinalFileIdentity(finalPath, rec.FileSize, committedSHA, fileKey)
				if verifyErr == nil && verifiedSHA != "" {
					_ = os.Remove(metaPath)
					for _, m := range movingMatches {
						_ = os.Remove(m)
					}
					if execErr := r.updateRecordSuccess(ctx, rec.ChatID, rec.MessageID, verifiedSHA); execErr != nil {
						r.logger.Warn("failed to update record to success in recovery", zap.Error(execErr))
					} else {
						results = append(results, TaskRecoveryResult{
							FileKey:     fileKey,
							PrevState:   rec.Status,
							NextState:   "success",
							ActionTaken: "FINAL_FILE_COMMITTED_PROMOTED_TO_SUCCESS",
						})
						continue
					}
				}
			}
		}

		// 2. Check legacy part file
		if partErr == nil {
			if partStat, statErr := os.Stat(tempPartPath); statErr == nil && partStat.Size() == rec.FileSize && rec.FileSize > 0 {
				partSHA, shaErr := computeRecoverySHA256(tempPartPath)
				if err := atomicCommit.CommitFile(tempPartPath, finalPath); err == nil {
					_ = r.updateRecordSuccess(ctx, rec.ChatID, rec.MessageID, partSHA)
					results = append(results, TaskRecoveryResult{
						FileKey:     fileKey,
						PrevState:   rec.Status,
						NextState:   "success",
						ActionTaken: "LEGACY_PART_COMMITTED_TO_SUCCESS",
					})
					continue
				} else if errors.Is(err, atomicCommit.ErrTargetExists) {
					finalSHA, err2 := computeRecoverySHA256(finalPath)
					if shaErr == nil && err2 == nil && partSHA == finalSHA {
						_ = os.Remove(tempPartPath)
						_ = r.updateRecordSuccess(ctx, rec.ChatID, rec.MessageID, finalSHA)
						results = append(results, TaskRecoveryResult{
							FileKey:     fileKey,
							PrevState:   rec.Status,
							NextState:   "success",
							ActionTaken: "LEGACY_PART_TARGET_ALREADY_EXISTS_PROMOTED",
						})
						continue
					}
				}
			}
		}

		// 3. Reset incomplete/canceled tasks to pending so Spool engine re-downloads cleanly
		_ = os.Remove(movingPath)
		_ = os.Remove(metaPath)
		_ = os.Remove(tempPartPath)
		for _, m := range movingMatches {
			_ = os.Remove(m)
		}
		actionTaken := "BUFFER_RESET_TO_PENDING"
		if r.bufferType == "memory" {
			actionTaken = "MEMORY_BUFFER_VOLATILE_RESET_TO_PENDING"
		}
		if execErr := r.updateRecordPending(ctx, rec.ChatID, rec.MessageID); execErr != nil {
			r.logger.Warn("failed to reset record to pending in recovery", zap.Error(execErr))
		}
		results = append(results, TaskRecoveryResult{
			FileKey:     fileKey,
			PrevState:   rec.Status,
			NextState:   "pending",
			ActionTaken: actionTaken,
		})
	}

	return results, nil
}

func (r *Reconciler) updateRecordSuccess(ctx context.Context, chatID string, msgID int, sha string) error {
	res, err := r.db.ExecContext(ctx, `UPDATE download_records SET status = 'success', error = '' WHERE chat_id = ? AND message_id = ?`, chatID, msgID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("no record updated for %s:%d", chatID, msgID)
	}
	return nil
}

func (r *Reconciler) updateRecordPending(ctx context.Context, chatID string, msgID int) error {
	res, err := r.db.ExecContext(ctx, `UPDATE download_records SET status = 'pending', attempts = 0, next_retry_at = 0, error = '' WHERE chat_id = ? AND message_id = ?`, chatID, msgID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("no record updated for %s:%d", chatID, msgID)
	}
	return nil
}

func computeRecoverySHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hasher := sha256.New()
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(hasher, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
