package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/Hittlert/TGX/internal/fscommit"
)

// ReconcileOnStartup executes the Section 10 crash recovery matrix before new admissions begin.
func ReconcileOnStartup(ctx context.Context, db *Database, ssdDir, archiveDir string, logger *zap.Logger, meters ...*StorageIOMeter) error {
	var errs []error
	archiveEnabled := archiveDir != ""

	var meter *StorageIOMeter
	if len(meters) > 0 {
		meter = meters[0]
	}

	// 1. Reconcile 'committing' download_records: only promote if matching proof exists
	committingRecs, err := db.GetPendingCommittingDownloads()
	if err != nil {
		logger.Error("failed to get pending committing downloads during recovery", zap.Error(err))
		errs = append(errs, fmt.Errorf("get pending committing: %w", err))
	} else {
		for _, rec := range committingRecs {
			if recErr := ReconcileCommittingRecord(ctx, db, ssdDir, archiveEnabled, rec, nil, logger, meter); recErr != nil {
				logger.Error("failed to reconcile committing download during startup recovery",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
					zap.Error(recErr),
				)
				errs = append(errs, fmt.Errorf("reconcile committing (%s:%d): %w", rec.ChatID, rec.MessageID, recErr))
			}
		}
	}

	// 2. Reconcile 'downloading' records interrupted by crash:
	// A task that was only downloading never reached commit intent; clean .part and reset to pending!
	downloadingRecs, err := db.GetStaleDownloadingRecords()
	if err != nil {
		logger.Error("failed to get stale downloading records during recovery", zap.Error(err))
		errs = append(errs, fmt.Errorf("get stale downloading: %w", err))
	} else {
		for _, rec := range downloadingRecs {
			finalAbsPath := filepath.Join(ssdDir, filepath.FromSlash(rec.SavePath))
			partAbsPath := finalAbsPath + ".part"

			// Clean up uncommitted .part residue and reset to pending
			_ = os.Remove(partAbsPath)
			if updateErr := db.UpdateDownloadStatus(rec.ChatID, rec.MessageID, "pending", rec.FileName, rec.SavePath, rec.MediaType, rec.FileSize, ""); updateErr != nil {
				logger.Error("failed to reset stale downloading record to pending", zap.String("chat_id", rec.ChatID), zap.Int("message_id", rec.MessageID), zap.Error(updateErr))
				errs = append(errs, fmt.Errorf("reset stale downloading (%s:%d): %w", rec.ChatID, rec.MessageID, updateErr))
			} else {
				logger.Info("reset stale downloading record to pending",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
				)
			}
		}
	}

	// 3. Reconcile 'resolving' and legacy 'moving' records: reset to pending
	if _, execErr := db.Execute(`
		UPDATE download_records
		SET status = 'pending', error = ''
		WHERE status IN ('resolving', 'moving')
	`); execErr != nil {
		logger.Error("failed to reconcile resolving/moving records", zap.Error(execErr))
		errs = append(errs, fmt.Errorf("reconcile resolving/moving: %w", execErr))
	}

	// 4. Reconcile shutdown cancellation failures to pending
	if _, execErr := db.Execute(`
		UPDATE download_records
		SET status = 'pending', error = ''
		WHERE status = 'failed' AND (
			error LIKE '%context canceled%' OR
			error LIKE '%context deadline exceeded%' OR
			error = 'canceled' OR
			error = 'task canceled' OR
			error LIKE '%forcibly closed%'
		)
	`); execErr != nil {
		logger.Error("failed to reconcile cancellation failures", zap.Error(execErr))
		errs = append(errs, fmt.Errorf("reconcile cancellation failures: %w", execErr))
	}

	// 5. Reconcile archive jobs if archive is enabled
	if archiveEnabled {
		// Backlog fill: enqueue archive jobs ONLY for successful downloads with a valid, verified SHA256!
		if _, execErr := db.Execute(`
			INSERT OR IGNORE INTO archive_jobs (chat_id, message_id, relative_path, expected_size, sha256, state, attempts, next_retry_at, created_at, updated_at)
			SELECT chat_id, message_id, save_path, file_size, sha256, 'pending', 0, 0, created_at, updated_at
			FROM download_records
			WHERE status = 'success' AND save_path != '' AND sha256 IS NOT NULL AND sha256 != ''
			  AND (chat_id, message_id) NOT IN (SELECT chat_id, message_id FROM archive_jobs)
		`); execErr != nil {
			logger.Error("failed to backlog fill archive jobs", zap.Error(execErr))
			errs = append(errs, fmt.Errorf("backlog fill archive jobs: %w", execErr))
		}

		// Reconcile 'copying' / 'moving' archive jobs interrupted during transfer
		copyingJobs, err := db.GetStaleCopyingArchiveJobs()
		if err != nil {
			logger.Error("failed to get stale copying archive jobs during recovery", zap.Error(err))
			errs = append(errs, fmt.Errorf("get stale copying archive jobs: %w", err))
		} else {
			for _, job := range copyingJobs {
				dstFinal := filepath.Join(archiveDir, filepath.FromSlash(job.RelativePath))
				dstMoving := dstFinal + ".moving"
				srcPath := filepath.Join(ssdDir, filepath.FromSlash(job.RelativePath))

				// Check if archive final file exists and is verified
				if finInfo, statErr := os.Stat(dstFinal); statErr == nil && finInfo.Size() == job.ExpectedSize {
					sha, shaErr := meter.ComputeArchiveFileSHA256(dstFinal)
					if shaErr == nil && job.SHA256 != "" && sha == job.SHA256 {
						_ = os.Remove(dstMoving)
						if completeErr := db.RecoverArchiveJobComplete(job.ChatID, job.MessageID, job.ClaimID, job.SHA256); completeErr != nil {
							logger.Error("failed to complete recovered copying archive job", zap.String("chat_id", job.ChatID), zap.Int("message_id", job.MessageID), zap.Error(completeErr))
							errs = append(errs, fmt.Errorf("complete recovered copying archive (%s:%d): %w", job.ChatID, job.MessageID, completeErr))
						} else {
							_ = os.Remove(srcPath) // Clean up SSD duplicate
							logger.Info("recovered copying archive job to archived",
								zap.String("chat_id", job.ChatID),
								zap.Int("message_id", job.MessageID),
							)
						}
						continue
					}
				}

				// Otherwise remove .moving residue and reset state to pending (strictly conditional on copying state)
				_ = os.Remove(dstMoving)
				if execErr := db.RecoverStaleArchiveJob(job.ChatID, job.MessageID, job.ClaimID); execErr != nil {
					logger.Error("failed to reset incomplete copying archive job to pending", zap.String("chat_id", job.ChatID), zap.Int("message_id", job.MessageID), zap.Error(execErr))
					errs = append(errs, fmt.Errorf("reset incomplete copying archive (%s:%d): %w", job.ChatID, job.MessageID, execErr))
				} else {
					logger.Info("reset incomplete copying archive job to pending",
						zap.String("chat_id", job.ChatID),
						zap.Int("message_id", job.MessageID),
					)
				}
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// RecoveryTestHooks provides deterministic interception points for crash recovery testing.
type RecoveryTestHooks struct {
	BeforeCommitSiblingPart func(partPath, finalPath string)
}

var recoveryTestHooks RecoveryTestHooks

// SetRecoveryTestHooks sets global test hooks for recovery package.
func SetRecoveryTestHooks(hooks RecoveryTestHooks) {
	recoveryTestHooks = hooks
}

// CommitDecisionType classifies the authoritative outcome of an atomic sibling commit.
type CommitDecisionType int

const (
	CommitDecisionSuccess CommitDecisionType = iota
	CommitDecisionTargetConflict
	CommitDecisionIOError
)

// CommitDecision captures the evaluated outcome of fscommit.CommitSiblingPart.
type CommitDecision struct {
	Type        CommitDecisionType
	FinalSHA    string
	Disposition FailureDisposition
	Err         error
}

// EvaluateCommitSiblingPart evaluates the result of fscommit.CommitSiblingPart across worker and recovery paths.
// Invariants enforced:
// 1. Successful commit -> CommitDecisionSuccess with final SHA.
// 2. ErrTargetExists with matching size & SHA-256 -> CommitDecisionSuccess with part cleanup.
// 3. ErrTargetExists with conflicting size/SHA -> CommitDecisionTargetConflict preserving both proofs.
// 4. Other commit failures (EIO, EPERM, IsDir) -> CommitDecisionIOError preserving valid part.
func EvaluateCommitSiblingPart(partAbsPath, finalAbsPath string, authoritativeSize int64, expectedSHA string, commitErr error, meters ...*StorageIOMeter) CommitDecision {
	if commitErr == nil {
		return CommitDecision{
			Type:     CommitDecisionSuccess,
			FinalSHA: expectedSHA,
		}
	}

	var meter *StorageIOMeter
	if len(meters) > 0 {
		meter = meters[0]
	}

	if errors.Is(commitErr, fscommit.ErrTargetExists) || errors.Is(commitErr, os.ErrExist) {
		if finInfo, statErr := os.Stat(finalAbsPath); statErr == nil && !finInfo.IsDir() {
			if finInfo.Size() == authoritativeSize {
				finSHA, finSHAErr := meter.ComputeSSDFileSHA256(finalAbsPath)
				if finSHAErr == nil && finSHA == expectedSHA {
					_ = os.Remove(partAbsPath)
					return CommitDecision{
						Type:     CommitDecisionSuccess,
						FinalSHA: finSHA,
					}
				}
			}

			// Target exists with conflicting proof against valid part: preserve both!
			disp := FailureDisposition{
				Stage:       "commit",
				Op:          "target_exists",
				Class:       "target_conflict",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Message:     fmt.Sprintf("target exists with conflicting proof against valid part (%s)", finalAbsPath),
				Cause:       commitErr,
			}
			return CommitDecision{
				Type:        CommitDecisionTargetConflict,
				Disposition: disp,
				Err:         commitErr,
			}
		}
	}

	// Other commit failure (EIO, EPERM, IsDir): preserve valid part!
	disp := FailureDisposition{
		Stage:       "commit",
		Op:          "rename",
		Class:       "io",
		Unavailable: false,
		Retryable:   false,
		RetryOwner:  "none",
		Message:     commitErr.Error(),
		Cause:       commitErr,
	}
	return CommitDecision{
		Type:        CommitDecisionIOError,
		Disposition: disp,
		Err:         commitErr,
	}
}

// ReconcileCommittingRecord executes the authoritative recovery state machine for a single committing record.
// Both startup crash recovery and the online finalizer loop MUST share this exact primitive.
func ReconcileCommittingRecord(
	ctx context.Context,
	db *Database,
	ssdDir string,
	archiveEnabled bool,
	rec DownloadRecord,
	registry *Registry,
	logger *zap.Logger,
	meters ...*StorageIOMeter,
) error {
	if db == nil {
		return nil
	}
	var meter *StorageIOMeter
	if len(meters) > 0 {
		meter = meters[0]
	}
	finalAbsPath := filepath.Join(ssdDir, filepath.FromSlash(rec.SavePath))
	partAbsPath := finalAbsPath + ".part"

	// 1. Check if final SSD file already exists
	if finInfo, statErr := os.Stat(finalAbsPath); statErr == nil && !finInfo.IsDir() {
		finSHA, shaErr := meter.ComputeSSDFileSHA256(finalAbsPath)
		if shaErr == nil && rec.SHA256 != "" && finInfo.Size() == rec.FileSize && finSHA == rec.SHA256 {
			// Final SSD file already exists and matches committed size and SHA proof
			_ = os.Remove(partAbsPath)
			completeErr := db.CompleteDownloadAndQueueArchive(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.SavePath, rec.FileSize, finSHA, archiveEnabled)
			if completeErr == nil || errors.Is(completeErr, ErrArchiveConflict) {
				logger.Info("reconciled committing record to success from final file",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
				)
				if registry != nil {
					registry.FinishTaskByMessage(rec.ChatID, rec.MessageID, rec.AttemptGeneration, StateSuccess, "", "", rec.SavePath, false, finSHA)
				}
				return nil
			}
			logger.Warn("reconciler failed to complete download in DB from final file",
				zap.String("chat_id", rec.ChatID),
				zap.Int("message_id", rec.MessageID),
				zap.Error(completeErr),
			)
			return completeErr
		}

		// Final SSD file exists but does NOT match!
		// Check if .part file exists and is valid
		if partInfo, statPartErr := os.Stat(partAbsPath); statPartErr == nil && partInfo.Size() == rec.FileSize {
			partSHA, shaPartErr := meter.ComputeSSDFileSHA256(partAbsPath)
			if shaPartErr == nil && rec.SHA256 != "" && partSHA == rec.SHA256 {
				// Valid part + conflicting final!
				// Preserve both proofs, do not delete part, record durable conflict!
				logger.Error("target exists with conflicting proof against valid part: preserving both",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
					zap.String("part_path", partAbsPath),
					zap.String("final_path", finalAbsPath),
				)
				disp := FailureDisposition{
					Stage:       "commit",
					Op:          "target_exists",
					Class:       "target_conflict",
					Unavailable: false,
					Retryable:   false,
					RetryOwner:  "none",
					Message:     fmt.Sprintf("target exists with conflicting proof against valid part (%s)", finalAbsPath),
				}
				if failErr := db.RecordTargetConflict(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.FileName, rec.SavePath, rec.MediaType, rec.FileSize, disp); failErr != nil {
					logger.Error("failed to record target conflict disposition in DB", zap.Error(failErr))
					return failErr
				}
				if registry != nil {
					registry.FinishTaskByMessage(rec.ChatID, rec.MessageID, rec.AttemptGeneration, StateFailed, disp.Class, disp.Error(), rec.SavePath, false, "")
				}
				return fmt.Errorf("target exists with conflicting proof against valid part: %s", finalAbsPath)
			}
		}
	}

	// 2. .part file exists with matching SHA -> commit sibling part and evaluate canonical outcome
	if partInfo, statErr := os.Stat(partAbsPath); statErr == nil && partInfo.Size() == rec.FileSize {
		sha, shaErr := meter.ComputeSSDFileSHA256(partAbsPath)
		if shaErr == nil && rec.SHA256 != "" && sha == rec.SHA256 {
			if recoveryTestHooks.BeforeCommitSiblingPart != nil {
				recoveryTestHooks.BeforeCommitSiblingPart(partAbsPath, finalAbsPath)
			}
			commitErr := fscommit.CommitSiblingPart(partAbsPath, finalAbsPath)
			decision := EvaluateCommitSiblingPart(partAbsPath, finalAbsPath, rec.FileSize, sha, commitErr, meter)
			switch decision.Type {
			case CommitDecisionSuccess:
				completeErr := db.CompleteDownloadAndQueueArchive(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.SavePath, rec.FileSize, decision.FinalSHA, archiveEnabled)
				if completeErr == nil || errors.Is(completeErr, ErrArchiveConflict) {
					logger.Info("reconciled committing record to success via atomic part commit",
						zap.String("chat_id", rec.ChatID),
						zap.Int("message_id", rec.MessageID),
					)
					if registry != nil {
						registry.FinishTaskByMessage(rec.ChatID, rec.MessageID, rec.AttemptGeneration, StateSuccess, "", "", rec.SavePath, false, decision.FinalSHA)
					}
					return nil
				}
				logger.Warn("reconciler failed to complete download in DB after commit part",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
					zap.Error(completeErr),
				)
				return completeErr

			case CommitDecisionTargetConflict:
				logger.Error("target exists with conflicting proof during recovery: preserving both part and final",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
					zap.String("part_path", partAbsPath),
					zap.String("final_path", finalAbsPath),
				)
				if failErr := db.RecordTargetConflict(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.FileName, rec.SavePath, rec.MediaType, rec.FileSize, decision.Disposition); failErr != nil {
					logger.Error("failed to record target conflict disposition in DB", zap.Error(failErr))
					return failErr
				}
				if registry != nil {
					registry.FinishTaskByMessage(rec.ChatID, rec.MessageID, rec.AttemptGeneration, StateFailed, decision.Disposition.Class, decision.Disposition.Error(), rec.SavePath, false, "")
				}
				return fmt.Errorf("target exists with conflicting proof against valid part: %s", finalAbsPath)

			case CommitDecisionIOError:
				logger.Error("failed to commit valid sibling part: preserving valid part file",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
					zap.String("part_path", partAbsPath),
					zap.Error(decision.Err),
				)
				return fmt.Errorf("failed to commit valid sibling part: %w", decision.Err)
			}
		}
	}

	// 3. Neither valid: reset to pending so it can be re-downloaded
	_ = os.Remove(partAbsPath)
	if updateErr := db.UpdateDownloadStatus(rec.ChatID, rec.MessageID, "pending", rec.FileName, rec.SavePath, rec.MediaType, rec.FileSize, ""); updateErr != nil {
		logger.Error("reconciler failed to reset invalid committing record to pending",
			zap.String("chat_id", rec.ChatID),
			zap.Int("message_id", rec.MessageID),
			zap.Error(updateErr),
		)
		return updateErr
	}
	logger.Warn("reconciler reset incomplete committing record to pending",
		zap.String("chat_id", rec.ChatID),
		zap.Int("message_id", rec.MessageID),
	)
	return nil
}
