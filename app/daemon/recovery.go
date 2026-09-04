package daemon

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/Hittlert/TGX/internal/fscommit"
)

// ReconcileOnStartup executes the Section 10 crash recovery matrix before new admissions begin.
func ReconcileOnStartup(ctx context.Context, db *Database, ssdDir, archiveDir string, logger *zap.Logger) error {
	archiveEnabled := archiveDir != ""

	// 1. Reconcile 'committing' download_records: only promote if matching proof exists
	committingRecs, err := db.GetPendingCommittingDownloads()
	if err != nil {
		logger.Error("failed to get pending committing downloads during recovery", zap.Error(err))
	} else {
		for _, rec := range committingRecs {
			finalAbsPath := filepath.Join(ssdDir, filepath.FromSlash(rec.SavePath))
			partAbsPath := finalAbsPath + ".part"

			// Check if final SSD file already exists and matches committed SHA proof
			if finInfo, statErr := os.Stat(finalAbsPath); statErr == nil && finInfo.Size() == rec.FileSize {
				sha, shaErr := computeFileSHA256(finalAbsPath)
				if shaErr == nil && rec.SHA256 != "" && sha == rec.SHA256 {
					_ = os.Remove(partAbsPath)
					if completeErr := db.CompleteDownloadAndQueueArchive(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.SavePath, rec.FileSize, sha, archiveEnabled); completeErr != nil {
						logger.Error("failed to complete recovered committing download", zap.String("chat_id", rec.ChatID), zap.Int("message_id", rec.MessageID), zap.Error(completeErr))
					} else {
						logger.Info("recovered committing record to success from final file",
							zap.String("chat_id", rec.ChatID),
							zap.Int("message_id", rec.MessageID),
						)
					}
					continue
				}
			}

			// Check if .part file exists with matching SHA
			if partInfo, statErr := os.Stat(partAbsPath); statErr == nil && partInfo.Size() == rec.FileSize {
				sha, shaErr := computeFileSHA256(partAbsPath)
				if shaErr == nil && rec.SHA256 != "" && sha == rec.SHA256 {
					if commitErr := fscommit.CommitSiblingPart(partAbsPath, finalAbsPath); commitErr == nil {
						if completeErr := db.CompleteDownloadAndQueueArchive(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.SavePath, rec.FileSize, sha, archiveEnabled); completeErr != nil {
							logger.Error("failed to complete atomic part commit during recovery", zap.String("chat_id", rec.ChatID), zap.Int("message_id", rec.MessageID), zap.Error(completeErr))
						} else {
							logger.Info("recovered committing record to success via atomic part commit",
								zap.String("chat_id", rec.ChatID),
								zap.Int("message_id", rec.MessageID),
							)
						}
						continue
					}
				}
			}

			// Neither valid: reset to pending
			_ = os.Remove(partAbsPath)
			if updateErr := db.UpdateDownloadStatus(rec.ChatID, rec.MessageID, "pending", rec.FileName, rec.SavePath, rec.MediaType, rec.FileSize, ""); updateErr != nil {
				logger.Error("failed to reset incomplete committing record to pending", zap.String("chat_id", rec.ChatID), zap.Int("message_id", rec.MessageID), zap.Error(updateErr))
			} else {
				logger.Warn("reset incomplete committing record to pending",
					zap.String("chat_id", rec.ChatID),
					zap.Int("message_id", rec.MessageID),
				)
			}
		}
	}

	// 2. Reconcile 'downloading' records interrupted by crash:
	// A task that was only downloading never reached commit intent; clean .part and reset to pending!
	downloadingRecs, err := db.GetStaleDownloadingRecords()
	if err != nil {
		logger.Error("failed to get stale downloading records during recovery", zap.Error(err))
	} else {
		for _, rec := range downloadingRecs {
			finalAbsPath := filepath.Join(ssdDir, filepath.FromSlash(rec.SavePath))
			partAbsPath := finalAbsPath + ".part"

			// Clean up uncommitted .part residue and reset to pending
			_ = os.Remove(partAbsPath)
			if updateErr := db.UpdateDownloadStatus(rec.ChatID, rec.MessageID, "pending", rec.FileName, rec.SavePath, rec.MediaType, rec.FileSize, ""); updateErr != nil {
				logger.Error("failed to reset stale downloading record to pending", zap.String("chat_id", rec.ChatID), zap.Int("message_id", rec.MessageID), zap.Error(updateErr))
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
		}

		// Reconcile 'copying' / 'moving' archive jobs interrupted during transfer
		copyingJobs, err := db.GetStaleCopyingArchiveJobs()
		if err != nil {
			logger.Error("failed to get stale copying archive jobs during recovery", zap.Error(err))
		} else {
			for _, job := range copyingJobs {
				dstFinal := filepath.Join(archiveDir, filepath.FromSlash(job.RelativePath))
				dstMoving := dstFinal + ".moving"
				srcPath := filepath.Join(ssdDir, filepath.FromSlash(job.RelativePath))

				// Check if archive final file exists and is verified
				if finInfo, statErr := os.Stat(dstFinal); statErr == nil && finInfo.Size() == job.ExpectedSize {
					sha, shaErr := computeFileSHA256(dstFinal)
					if shaErr == nil && job.SHA256 != "" && sha == job.SHA256 {
						_ = os.Remove(dstMoving)
						if completeErr := db.CompleteArchiveJob(job.ChatID, job.MessageID); completeErr != nil {
							logger.Error("failed to complete recovered copying archive job", zap.String("chat_id", job.ChatID), zap.Int("message_id", job.MessageID), zap.Error(completeErr))
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

				// Otherwise remove .moving residue and reset state to pending
				_ = os.Remove(dstMoving)
				if _, execErr := db.Execute(`UPDATE archive_jobs SET state = 'pending', updated_at = ? WHERE chat_id = ? AND message_id = ?`,
					time.Now().Unix(), job.ChatID, job.MessageID); execErr != nil {
					logger.Error("failed to reset incomplete copying archive job to pending", zap.String("chat_id", job.ChatID), zap.Int("message_id", job.MessageID), zap.Error(execErr))
				} else {
					logger.Info("reset incomplete copying archive job to pending",
						zap.String("chat_id", job.ChatID),
						zap.Int("message_id", job.MessageID),
					)
				}
			}
		}
	}

	return nil
}

