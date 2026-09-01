package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/mover"
	"github.com/Hittlert/TGX/core/targetwriter"
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
	mover      *mover.Mover
	tw         *targetwriter.TargetWriter
	registry   *Registry
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

		// 1. Check if final file was already committed via our SHA-verified CommitFile.
		// Accept only if: final exists with exact size, .moving does NOT exist, and no temp part exists.
		stat, err := os.Stat(finalPath)
		_, movingErr := os.Stat(movingPath)
		tempPartPath := CanonicalPartPath(r.tempDir, fileKey)
		_, partErr := os.Stat(tempPartPath)
		hasTempPart := (partErr == nil)

		if err == nil && stat.Size() == rec.FileSize && rec.FileSize > 0 && errors.Is(movingErr, os.ErrNotExist) && !hasTempPart {
			var expectedSHA string
			if r.tw != nil {
				if _, _, finalSHA, ok := r.tw.TaskFinalInfo(fileKey); ok && finalSHA != "" {
					expectedSHA = finalSHA
				}
			}

			verifiedSHA, verifyErr := verifyFinalFileIdentity(finalPath, rec.FileSize, expectedSHA, fileKey)
			if verifyErr == nil && verifiedSHA != "" {
				_ = os.Remove(metaPath)
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

			// If read-only proof verification failed, check for valid crash recovery intent
			repairedSHA, repairErr := r.repairCommittedProof(finalPath, metaPath, rec.FileSize, fileKey, rec.SavePath)
			if repairErr == nil && repairedSHA != "" {
				if execErr := r.updateRecordSuccess(ctx, rec.ChatID, rec.MessageID, repairedSHA); execErr != nil {
					r.logger.Warn("failed to update record to success in recovery after proof repair", zap.Error(execErr))
				} else {
					results = append(results, TaskRecoveryResult{
						FileKey:     fileKey,
						PrevState:   rec.Status,
						NextState:   "success",
						ActionTaken: "FINAL_FILE_COMMITTED_PROMOTED_TO_SUCCESS",
					})
					continue
				}
			} else {
				r.logger.Warn("target file content unverified or mismatch during recovery", zap.Error(verifyErr), zap.NamedError("repair_err", repairErr))
			}
		}

		// 2. Check if durable target storage sidecar .moving.meta exists (valid across ALL buffer modes)
		metaData, metaErr := os.ReadFile(metaPath)
		if metaErr == nil {
			var manifest targetwriter.TaskManifest
			if json.Unmarshal(metaData, &manifest) == nil {
				// Validate manifest identity and physical consistency
				valid := true
				reason := ""

				// Version: must match sidecar version
				if manifest.Version <= 0 || manifest.Version != targetwriter.SidecarVersion {
					valid = false
					reason = fmt.Sprintf("unsupported sidecar version %d", manifest.Version)
				}
				// Generation: must be non-empty
				if valid && manifest.Gen == "" {
					valid = false
					reason = "empty sidecar generation"
				}

				// Identity: taskID, finalPath and expectedSize must match DB record
				if valid && manifest.TaskID != fileKey {
					valid = false
					reason = "taskID mismatch"
				}
				if valid && manifest.FinalPath != rec.SavePath {
					valid = false
					reason = fmt.Sprintf("finalPath mismatch: manifest=%q db=%q", manifest.FinalPath, rec.SavePath)
				}
				if valid && rec.FileSize > 0 && manifest.ExpectedSize != rec.FileSize {
					valid = false
					reason = fmt.Sprintf("expectedSize mismatch: manifest=%d db=%d", manifest.ExpectedSize, rec.FileSize)
				}

				// Physical: .moving file must exist and cover all durable ranges
				var movingStat os.FileInfo
				if valid {
					var movingStatErr error
					movingStat, movingStatErr = os.Stat(movingPath)
					if movingStatErr != nil {
						valid = false
						reason = ".moving file missing"
					}
				}

				// Ranges: bounds valid, within expected size, covered by .moving
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
					if valid && movingStat.Size() < maxEnd {
						valid = false
						reason = fmt.Sprintf(".moving size %d < max range end %d", movingStat.Size(), maxEnd)
					}
				}

				if valid {
					// Determine if bitmap is complete to choose recovery action
					bm := targetwriter.NewMovedBitmapWithRanges(manifest.ExpectedSize, manifest.Ranges)
					if bm.IsComplete() {
						// Complete bitmap: TargetWriter will finalize via pendingFinalize queue
						if r.tw != nil {
							res := r.tw.RegisterTask(manifest)
							switch res {
							case targetwriter.RegisterAccepted:
								if r.registry != nil {
									r.registry.RegisterRecoveredTask(manifest.TaskID, manifest.Gen, rec.SavePath, manifest.ExpectedSize)
								}
								results = append(results, TaskRecoveryResult{
									FileKey:     fileKey,
									PrevState:   rec.Status,
									NextState:   "moving",
									ActionTaken: "SSD_BUFFER_COMPLETE_FINALIZE_PENDING",
								})
							case targetwriter.RegisterAlreadyFinalized:
								var expectedSHA string
								if r.tw != nil {
									if _, _, finalSHA, ok := r.tw.TaskFinalInfo(manifest.TaskID); ok && finalSHA != "" {
										expectedSHA = finalSHA
									}
								}
								verifiedSHA, verifyErr := verifyFinalFileIdentity(finalPath, manifest.ExpectedSize, expectedSHA, manifest.TaskID)
								if verifyErr == nil && verifiedSHA != "" {
									_ = os.Remove(metaPath)
									_ = os.Remove(movingPath)
									if execErr := r.updateRecordSuccess(ctx, rec.ChatID, rec.MessageID, verifiedSHA); execErr != nil {
										r.logger.Warn("failed to update record to success in recovery", zap.Error(execErr))
									} else {
										results = append(results, TaskRecoveryResult{
											FileKey:     fileKey,
											PrevState:   rec.Status,
											NextState:   "success",
											ActionTaken: "ALREADY_FINALIZED_PROMOTED_TO_SUCCESS",
										})
									}
								} else {
									if execErr := r.updateRecordPending(ctx, rec.ChatID, rec.MessageID); execErr != nil {
										r.logger.Warn("failed to reset record to pending in recovery", zap.Error(execErr))
									} else {
										results = append(results, TaskRecoveryResult{
											FileKey:     fileKey,
											PrevState:   rec.Status,
											NextState:   "pending",
											ActionTaken: "ALREADY_FINALIZED_TARGET_MISSING_RESET",
										})
									}
								}
							case targetwriter.RegisterStale, targetwriter.RegisterConflict:
								_ = os.Remove(metaPath)
								_ = os.Remove(movingPath)
								if execErr := r.updateRecordPending(ctx, rec.ChatID, rec.MessageID); execErr != nil {
									r.logger.Warn("failed to reset record to pending in recovery", zap.Error(execErr))
								}
								results = append(results, TaskRecoveryResult{
									FileKey:     fileKey,
									PrevState:   rec.Status,
									NextState:   "pending",
									ActionTaken: "SSD_BUFFER_REJECTED_RESET_TO_PENDING",
								})
							}
						}
					} else {
						// Incomplete bitmap: reset DB to pending with 0 attempts / next_retry_at
						// so pending scanner re-dispatches the task cleanly.
						// Do NOT register a publishing task in Registry or TargetWriter.
						if execErr := r.updateRecordPending(ctx, rec.ChatID, rec.MessageID); execErr != nil {
							r.logger.Warn("failed to reset record to pending in recovery", zap.Error(execErr))
						}
						results = append(results, TaskRecoveryResult{
							FileKey:     fileKey,
							PrevState:   rec.Status,
							NextState:   "pending",
							ActionTaken: "SSD_BUFFER_PARTIAL_RESET_TO_PENDING",
						})
					}
					continue
				}

				// Invalid sidecar: clean up
				r.logger.Warn("invalid sidecar metadata, resetting task",
					zap.String("task", fileKey), zap.String("reason", reason))
				_ = os.Remove(movingPath)
				_ = os.Remove(metaPath)
			}
		}

		// 3. Fallback: check legacy part file or reset to pending
		tempPartPath = CanonicalPartPath(r.tempDir, fileKey)
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
				_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'moving' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
				results = append(results, TaskRecoveryResult{
					FileKey:     fileKey,
					PrevState:   rec.Status,
					NextState:   "moving",
					ActionTaken: "SSD_BUFFER_COMPLETED_REQUEUED_IN_MOVER",
				})
			} else {
				partSHA, shaErr := computeRecoverySHA256(tempPartPath)
				if err := atomicCommit.CommitFile(tempPartPath, finalPath); err == nil {
					if shaErr == nil {
						_ = persistDirectCommitProof(r.outputDir, rec.SavePath, fileKey, "1", rec.FileSize, partSHA)
					}
					_, _ = r.db.ExecContext(ctx, `UPDATE download_records SET status = 'success', error = '' WHERE chat_id = ? AND message_id = ?`, rec.ChatID, rec.MessageID)
					results = append(results, TaskRecoveryResult{
						FileKey:     fileKey,
						PrevState:   rec.Status,
						NextState:   "success",
						ActionTaken: "LEGACY_PART_COMMITTED_TO_SUCCESS",
					})
				} else {
					if errors.Is(err, atomicCommit.ErrTargetExists) {
						finalSHA, err2 := computeRecoverySHA256(finalPath)
						if shaErr == nil && err2 == nil && partSHA == finalSHA {
							_ = os.Remove(tempPartPath)
							_ = persistDirectCommitProof(r.outputDir, rec.SavePath, fileKey, "1", rec.FileSize, finalSHA)
							if execErr := r.updateRecordSuccess(ctx, rec.ChatID, rec.MessageID, finalSHA); execErr != nil {
								r.logger.Warn("failed to update record to success in recovery", zap.Error(execErr))
							}
							results = append(results, TaskRecoveryResult{
								FileKey:     fileKey,
								PrevState:   rec.Status,
								NextState:   "success",
								ActionTaken: "LEGACY_PART_TARGET_ALREADY_EXISTS_PROMOTED",
							})
							continue
						}
					}
					if execErr := r.updateRecordPending(ctx, rec.ChatID, rec.MessageID); execErr != nil {
						r.logger.Warn("failed to reset record to pending in recovery", zap.Error(execErr))
					}
					results = append(results, TaskRecoveryResult{
						FileKey:     fileKey,
						PrevState:   rec.Status,
						NextState:   "pending",
						ActionTaken: "LEGACY_PART_RESET_TO_PENDING",
					})
				}
			}
			continue
		} else if partErr == nil && partStat.Size() > 0 {
			if execErr := r.updateRecordPending(ctx, rec.ChatID, rec.MessageID); execErr != nil {
				r.logger.Warn("failed to reset record to pending in recovery", zap.Error(execErr))
			}
			results = append(results, TaskRecoveryResult{
				FileKey:     fileKey,
				PrevState:   rec.Status,
				NextState:   "pending",
				ActionTaken: "SSD_BUFFER_PARTIAL_RETAINED_FOR_RESUME",
			})
			continue
		}

		// 4. Default: reset to pending with attempts=0, next_retry_at=0, error=''
		_ = os.Remove(movingPath)
		_ = os.Remove(metaPath)
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

func (r *Reconciler) repairCommittedProof(finalPath, metaPath string, expectedSize int64, fileKey, savePath string) (string, error) {
	actualSHA, err := computeRecoverySHA256(finalPath)
	if err != nil {
		return "", fmt.Errorf("compute target sha256 for repair: %w", err)
	}

	proofPath := finalPath + ".tgx_commit"
	tmpProofPath := proofPath + ".tmp"

	// Source 1: Check .tgx_commit.tmp (crash during atomic proof rename)
	if data, err := os.ReadFile(tmpProofPath); err == nil {
		var proof targetwriter.CommitProof
		if json.Unmarshal(data, &proof) == nil && proof.SHA256 != "" && proof.Version == 1 {
			if proof.TaskID == fileKey && (expectedSize == 0 || proof.ExpectedSize == expectedSize) && proof.SHA256 == actualSHA {
				if err := os.Rename(tmpProofPath, proofPath); err == nil {
					if d, err := os.Open(filepath.Dir(proofPath)); err == nil {
						_ = d.Sync()
						_ = d.Close()
					}
					_ = os.Remove(metaPath)
					return actualSHA, nil
				}
			}
		}
	}

	// Source 2: Check .moving.meta (crash after CommitFile before proof write)
	if data, err := os.ReadFile(metaPath); err == nil {
		var manifest targetwriter.TaskManifest
		if json.Unmarshal(data, &manifest) == nil && manifest.Version == targetwriter.SidecarVersion {
			if manifest.TaskID == fileKey && (expectedSize == 0 || manifest.ExpectedSize == expectedSize) && manifest.ExpectedSHA != "" {
				// Anti-tamper verification: the file on disk must match the SHA computed before commit!
				if actualSHA != manifest.ExpectedSHA {
					return "", fmt.Errorf("tamper detected: actual sha %s != pre-commit manifest expected sha %s", actualSHA, manifest.ExpectedSHA)
				}

				// Write durable proof using full transaction
				if err := persistDirectCommitProof(r.outputDir, savePath, manifest.TaskID, manifest.Gen, expectedSize, actualSHA); err == nil {
					_ = os.Remove(metaPath)
					return actualSHA, nil
				} else {
					return "", fmt.Errorf("persist repaired commit proof: %w", err)
				}
			}
		}
	}

	return "", fmt.Errorf("no verifiable commit proof or pre-commit intent found for %s", finalPath)
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
