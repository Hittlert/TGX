package daemon

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/pkg/sbe/meta"
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
	query := `SELECT file_key, attempt_id, state, file_name, total_size, block_size, total_blocks FROM tasks WHERE state IN ('RUNNING', 'FINALIZING', 'COMMITTING', 'QUEUED')`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks for recovery: %w", err)
	}

	var records []taskRecord
	for rows.Next() {
		var rec taskRecord
		if err := rows.Scan(&rec.fileKey, &rec.attemptID, &rec.state, &rec.fileName, &rec.totalSize, &rec.blockSize, &rec.totalBlocks); err != nil {
			continue
		}
		records = append(records, rec)
	}
	_ = rows.Close()

	var results []TaskRecoveryResult
	for _, rec := range records {
		res := r.reconcileTask(ctx, rec.fileKey, rec.attemptID, rec.state, rec.fileName, rec.totalSize, rec.blockSize, rec.totalBlocks)
		results = append(results, res)
	}

	// Clean historical orphan part/meta files in temp directory
	r.cleanHistoricalOrphans(ctx)

	return results, nil
}

func (r *Reconciler) reconcileTask(ctx context.Context, fileKey, attemptID, state, fileName string, totalSize int64, blockSize, totalBlocks uint32) TaskRecoveryResult {
	finalPath := filepath.Join(r.outputDir, fileName)
	partName := fmt.Sprintf("%s.part.%s", fileName, attemptID)
	partPath := filepath.Join(r.tempDir, partName)
	metaName := fmt.Sprintf("%s.meta.%s", fileName, attemptID)
	metaPath := filepath.Join(r.tempDir, metaName)

	finalExists := fileExists(finalPath)
	partExists := fileExists(partPath)
	metaExists := fileExists(metaPath)

	res := TaskRecoveryResult{
		FileKey:   fileKey,
		AttemptID: attemptID,
		PrevState: state,
	}

	switch state {
	case "COMMITTING":
		if finalExists && !partExists {
			// Succeeded rename before crash -> promote to SUCCESS
			if err := r.updateStateCAS(ctx, fileKey, attemptID, "COMMITTING", "SUCCESS"); err != nil {
				res.Error = err
				return res
			}
			_ = os.Remove(metaPath)
			_ = atomic.SyncDir(r.outputDir)
			res.NextState = "SUCCESS"
			res.ActionTaken = "COMMITTING_RENAME_ALREADY_DONE_PROMOTED"
			return res
		}

		if finalExists && partExists {
			// Check inodes
			sameInode, err := compareInodes(finalPath, partPath)
			if err == nil && sameInode {
				// linkat succeeded but unlink crashed -> unlink part and promote
				_ = os.Remove(partPath)
				_ = atomic.SyncDir(r.outputDir)
				if err := r.updateStateCAS(ctx, fileKey, attemptID, "COMMITTING", "SUCCESS"); err != nil {
					res.Error = err
					return res
				}
				_ = os.Remove(metaPath)
				res.NextState = "SUCCESS"
				res.ActionTaken = "COMMITTING_UNLINK_CRASH_REPAIRED"
				return res
			} else {
				// Different inodes -> real path conflict
				res.NextState = "ERROR_PATH_CONFLICT"
				res.ActionTaken = "PATH_CONFLICT_BLOCKED"
				res.Error = errors.New("path conflict: final file exists with different inode")
				return res
			}
		}

		if !finalExists && partExists && metaExists {
			// Crash before rename -> re-execute atomic commit
			if err := atomic.CommitFile(partPath, finalPath); err != nil {
				res.Error = fmt.Errorf("re-commit failed: %w", err)
				return res
			}
			if err := r.updateStateCAS(ctx, fileKey, attemptID, "COMMITTING", "SUCCESS"); err != nil {
				res.Error = err
				return res
			}
			_ = os.Remove(metaPath)
			_ = atomic.SyncDir(r.outputDir)
			res.NextState = "SUCCESS"
			res.ActionTaken = "COMMITTING_RE_EXECUTED_SUCCESS"
			return res
		}

		// Missing both final and part -> corrupted commit
		_ = r.updateStateCAS(ctx, fileKey, attemptID, "COMMITTING", "QUEUED")
		res.NextState = "QUEUED"
		res.ActionTaken = "COMMITTING_MISSING_PARTS_RESET_TO_QUEUED"
		return res

	case "RUNNING", "FINALIZING":
		if finalExists {
			res.NextState = "ERROR_FILE_EXISTS"
			res.ActionTaken = "TARGET_FILE_ALREADY_EXISTS_HALTED"
			res.Error = os.ErrExist
			return res
		}

		if partExists && metaExists {
			// Try recovering meta
			att := parseAttemptID(attemptID)
			metaH := &meta.MetaHeader{
				Magic:       meta.MetaMagic,
				Version:     meta.MetaVersion,
				AttemptID:   att,
				TotalSize:   totalSize,
				BlockSize:   blockSize,
				TotalBlocks: totalBlocks,
			}
			copy(metaH.FileKeyHash[:], []byte(fileKey))

			mf, rec, err := meta.CreateOrOpenMetaFile(r.tempDir, fileName, metaH)
			if err == nil && mf != nil {
				defer mf.Close()

				if rec.IsComplete && rec.DurableBitmap.Count() == uint(totalBlocks) {
					// Power-cut before CAS -> directly finalize and commit!
					_ = mf.Close()
					if err := atomic.CommitFile(partPath, finalPath); err == nil {
						_ = r.updateStateCAS(ctx, fileKey, attemptID, state, "SUCCESS")
						_ = os.Remove(metaPath)
						_ = atomic.SyncDir(r.outputDir)
						res.NextState = "SUCCESS"
						res.ActionTaken = "COMPLETE_META_PROMOTED_TO_SUCCESS"
						return res
					}
				}

				if rec.ValidSlot != "NONE" {
					// Resumable state!
					res.NextState = "RUNNING"
					res.ActionTaken = fmt.Sprintf("RESUMABLE_FROM_SLOT_%s_BLOCKS_%d/%d", rec.ValidSlot, rec.DurableBitmap.Count(), totalBlocks)
					return res
				}
			}

			// Corrupted meta -> reset part and meta
			_ = os.Remove(partPath)
			_ = os.Remove(metaPath)
			_ = atomic.SyncDir(r.tempDir)
			res.NextState = "RUNNING"
			res.ActionTaken = "CORRUPTED_META_RESET_TO_BLOCK_ZERO"
			return res
		}

		res.NextState = "RUNNING"
		res.ActionTaken = "FRESH_START"
		return res

	default:
		res.NextState = state
		res.ActionTaken = "NO_ACTION"
		return res
	}
}

func (r *Reconciler) updateStateCAS(ctx context.Context, fileKey, attemptID, fromState, toState string) error {
	query := `UPDATE tasks SET state = ? WHERE file_key = ? AND attempt_id = ? AND state = ?`
	res, err := r.db.ExecContext(ctx, query, toState, fileKey, attemptID, fromState)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("CAS update failed: expected 1 row affected, got %d", rows)
	}
	return nil
}

func (r *Reconciler) cleanHistoricalOrphans(ctx context.Context) {
	entries, err := os.ReadDir(r.tempDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		name := entry.Name()
		if strings.Contains(name, ".part.") || strings.Contains(name, ".meta.") {
			parts := strings.Split(name, ".")
			if len(parts) >= 3 {
				attemptID := parts[len(parts)-1]
				// Check if this attempt exists and is active in DB
				var count int
				err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tasks WHERE attempt_id = ? AND state IN ('RUNNING', 'COMMITTING', 'FINALIZING')`, attemptID).Scan(&count)
				if err == nil && count == 0 {
					// Orphan from previous attempt -> remove
					_ = os.Remove(filepath.Join(r.tempDir, name))
				}
			}
		}
	}
	_ = atomic.SyncDir(r.tempDir)
}

func parseAttemptID(attemptID string) [16]byte {
	var att [16]byte
	if len(attemptID) == 32 {
		if b, err := hex.DecodeString(attemptID); err == nil && len(b) == 16 {
			copy(att[:], b)
			return att
		}
	}
	copy(att[:], []byte(attemptID))
	return att
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func compareInodes(path1, path2 string) (bool, error) {
	fi1, err := os.Stat(path1)
	if err != nil {
		return false, err
	}
	fi2, err := os.Stat(path2)
	if err != nil {
		return false, err
	}

	stat1, ok1 := fi1.Sys().(*syscall.Stat_t)
	stat2, ok2 := fi2.Sys().(*syscall.Stat_t)

	if ok1 && ok2 {
		return stat1.Ino == stat2.Ino && stat1.Dev == stat2.Dev, nil
	}

	return false, errors.New("platform does not support inode comparison")
}
