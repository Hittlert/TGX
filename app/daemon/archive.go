package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hittlert/TGX/internal/fscommit"
	"go.uber.org/zap"
)

// ArchiveWorker processes whole-file archive jobs from SQLite sequentially (concurrency = 1).
type ArchiveWorker struct {
	db          *Database
	downloadDir string // SSD root
	archiveDir  string // HDD root
	logger      *zap.Logger

	wakeCh    chan struct{}
	runningMu sync.Mutex
	running   bool

	writeBytes   int64 // atomic: cumulative physical bytes written to HDD archive
	readBytes    int64 // atomic: cumulative physical bytes read from SSD
	activeWriter int64 // atomic: 1 while actively copying, 0 otherwise
}

// NewArchiveWorker creates the single archive worker.
func NewArchiveWorker(db *Database, downloadDir, archiveDir string, logger *zap.Logger) (*ArchiveWorker, error) {
	cleanDownload, err := filepath.Abs(downloadDir)
	if err != nil {
		return nil, fmt.Errorf("canonicalize downloadDir: %w", err)
	}

	var cleanArchive string
	if archiveDir != "" {
		cleanArchive, err = filepath.Abs(archiveDir)
		if err != nil {
			return nil, fmt.Errorf("canonicalize archiveDir: %w", err)
		}

		// Validation: downloadDir and archiveDir must not be equal, or nested inside each other
		if cleanDownload == cleanArchive {
			return nil, errors.New("downloadDir and archiveDir cannot be the same directory")
		}
		rel1, err1 := filepath.Rel(cleanDownload, cleanArchive)
		if err1 == nil && !strings.HasPrefix(rel1, "..") {
			return nil, errors.New("archiveDir cannot be inside downloadDir")
		}
		rel2, err2 := filepath.Rel(cleanArchive, cleanDownload)
		if err2 == nil && !strings.HasPrefix(rel2, "..") {
			return nil, errors.New("downloadDir cannot be inside archiveDir")
		}
	}

	return &ArchiveWorker{
		db:          db,
		downloadDir: cleanDownload,
		archiveDir:  cleanArchive,
		logger:      logger,
		wakeCh:      make(chan struct{}, 1),
	}, nil
}

// IsEnabled returns true if an archive directory is configured.
func (w *ArchiveWorker) IsEnabled() bool {
	return w != nil && w.archiveDir != ""
}

// Wake signals the worker that a new archive job is ready.
func (w *ArchiveWorker) Wake() {
	if w == nil || !w.IsEnabled() {
		return
	}
	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
}

// Start runs the single archive worker loop.
func (w *ArchiveWorker) Start(ctx context.Context) {
	if !w.IsEnabled() {
		return
	}

	w.runningMu.Lock()
	w.running = true
	w.runningMu.Unlock()

	go w.runLoop(ctx)
}

func (w *ArchiveWorker) runLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wakeCh:
		case <-ticker.C:
		}

		for {
			if ctx.Err() != nil {
				return
			}

			// Claim exactly 1 due job at a time (sequential whole-file worker)
			jobs, err := w.db.GetDueArchiveJobs(1)
			if err != nil || len(jobs) == 0 {
				break
			}

			job := jobs[0]
			claimID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
			claimed, err := w.db.ClaimArchiveJob(job.ChatID, job.MessageID, claimID)
			if err != nil {
				w.logger.Error("failed to claim archive job", zap.Error(err), zap.String("chat_id", job.ChatID), zap.Int("message_id", job.MessageID))
				break
			}
			if !claimed {
				break
			}
			job.ClaimID = claimID

			w.processJob(ctx, job)
		}
	}
}

// ProcessNext claims and processes a single due archive job.
// Returns true if a job was found and processed, false otherwise.
func (w *ArchiveWorker) ProcessNext(ctx context.Context) bool {
	if !w.IsEnabled() {
		return false
	}
	jobs, err := w.db.GetDueArchiveJobs(1)
	if err != nil || len(jobs) == 0 {
		return false
	}

	job := jobs[0]
	claimID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	claimed, err := w.db.ClaimArchiveJob(job.ChatID, job.MessageID, claimID)
	if err != nil || !claimed {
		return false
	}
	job.ClaimID = claimID

	w.processJob(ctx, job)
	return true
}

func (w *ArchiveWorker) processJob(ctx context.Context, job ArchiveJob) {
	// 0. Strict validation of invariant: job must have valid non-empty SHA256 proof
	if strings.TrimSpace(job.SHA256) == "" {
		w.logger.Error("archive job rejected: missing or empty SHA256 proof",
			zap.String("chat_id", job.ChatID),
			zap.Int("message_id", job.MessageID),
		)
		if err := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, "archive job rejected: empty SHA256 proof"); err != nil {
			w.logger.Error("failed to fail archive job with empty SHA", zap.Error(err))
		}
		return
	}

	// 0b. Path containment and security validation
	cleanRel := filepath.Clean(filepath.FromSlash(job.RelativePath))
	if cleanRel == "." || cleanRel == "/" || strings.HasPrefix(cleanRel, "..") || filepath.IsAbs(cleanRel) {
		w.logger.Error("archive job rejected: invalid or escaping relative path",
			zap.String("chat_id", job.ChatID),
			zap.Int("message_id", job.MessageID),
			zap.String("rel_path", job.RelativePath),
		)
		if err := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, "archive job rejected: invalid relative path"); err != nil {
			w.logger.Error("failed to fail archive job with invalid path", zap.Error(err))
		}
		return
	}

	srcPath := filepath.Join(w.downloadDir, cleanRel)
	dstPath := filepath.Join(w.archiveDir, cleanRel)

	relSrc, errSrc := filepath.Rel(w.downloadDir, srcPath)
	if errSrc != nil || strings.HasPrefix(relSrc, "..") {
		w.logger.Error("archive job rejected: srcPath escapes downloadDir", zap.String("src_path", srcPath))
		_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, "srcPath escapes downloadDir")
		return
	}
	relDst, errDst := filepath.Rel(w.archiveDir, dstPath)
	if errDst != nil || strings.HasPrefix(relDst, "..") {
		w.logger.Error("archive job rejected: dstPath escapes archiveDir", zap.String("dst_path", dstPath))
		_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, "dstPath escapes archiveDir")
		return
	}

	w.logger.Debug("starting whole-file archive",
		zap.String("chat_id", job.ChatID),
		zap.Int("message_id", job.MessageID),
		zap.String("rel_path", job.RelativePath),
		zap.Int64("size", job.ExpectedSize),
	)

	EmitLifecycle(w.logger, LifecycleEvent{
		Event:     EventArchiveStarted,
		TaskID:    fmt.Sprintf("%s:%d", job.ChatID, job.MessageID),
		AttemptID: job.ClaimID,
		ChatID:    job.ChatID,
		MessageID: job.MessageID,
		Path:      job.RelativePath,
		Size:      job.ExpectedSize,
		SHA256:    job.SHA256,
	})

	// 1. Check if archive target already exists
	if dstInfo, err := os.Stat(dstPath); err == nil {
		// Existing target check: verify size and SHA
		if dstInfo.Size() == job.ExpectedSize {
			actualDstSHA, shaErr := computeFileSHA256(dstPath)
			if shaErr == nil && actualDstSHA == job.SHA256 {
				// Idempotent archive success
				w.logger.Info("archive target already exists and verified, completing job",
					zap.String("chat_id", job.ChatID),
					zap.Int("message_id", job.MessageID),
				)
				if dbErr := w.db.CompleteArchiveJob(job.ChatID, job.MessageID, job.ClaimID, job.SHA256); dbErr != nil {
					w.logger.Error("failed to mark archive complete in DB for existing verified target", zap.Error(dbErr))
					return
				}
				if rmErr := os.Remove(srcPath); rmErr != nil && !os.IsNotExist(rmErr) {
					w.logger.Warn("failed to remove SSD source file after verified existing target", zap.Error(rmErr))
				} else {
					_ = fscommit.FsyncDir(filepath.Dir(srcPath))
				}
				return
			}
		}

		// Content mismatch -> mark conflict, preserve both files
		w.logger.Warn("archive destination exists with conflicting content, marking conflict",
			zap.String("chat_id", job.ChatID),
			zap.Int("message_id", job.MessageID),
			zap.String("dst_path", dstPath),
		)
		if cErr := w.db.SetArchiveJobConflict(job.ChatID, job.MessageID, job.ClaimID, "destination file exists with conflicting content"); cErr != nil {
			w.logger.Error("failed to set archive job conflict", zap.Error(cErr))
		}
		return
	}

	// 2. Verify SSD source file exists
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			w.logger.Error("SSD source file missing for archive",
				zap.String("chat_id", job.ChatID),
				zap.Int("message_id", job.MessageID),
				zap.String("src_path", srcPath),
			)
			if fErr := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, fmt.Sprintf("SSD source missing: %v", err)); fErr != nil {
				w.logger.Error("failed to fail archive job for missing source", zap.Error(fErr))
			}
			return
		}
		if fErr := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, fmt.Sprintf("stat SSD source: %v", err)); fErr != nil {
			w.logger.Error("failed to fail archive job for stat source error", zap.Error(fErr))
		}
		return
	}

	if srcInfo.Size() != job.ExpectedSize {
		w.logger.Error("SSD source file size mismatch",
			zap.String("chat_id", job.ChatID),
			zap.Int("message_id", job.MessageID),
			zap.Int64("actual", srcInfo.Size()),
			zap.Int64("expected", job.ExpectedSize),
		)
		if cErr := w.db.SetArchiveJobConflict(job.ChatID, job.MessageID, job.ClaimID, fmt.Sprintf("SSD source size mismatch: got %d want %d", srcInfo.Size(), job.ExpectedSize)); cErr != nil {
			w.logger.Error("failed to set archive job conflict for size mismatch", zap.Error(cErr))
		}
		return
	}

	// 3. Ensure archive parent directory exists
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		w.logger.Warn("failed to create archive directory", zap.Error(err))
		if fErr := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, fmt.Sprintf("mkdir archive dir: %v", err)); fErr != nil {
			w.logger.Error("failed to fail archive job for mkdir error", zap.Error(fErr))
		}
		return
	}

	// 4. Perform whole-file archive copy via .moving sibling on destination filesystem
	movingPath := dstPath + ".moving"
	atomic.StoreInt64(&w.activeWriter, 1)
	written, actualSHA, copyErr := fscommit.CopyFileSequential(srcPath, movingPath)
	atomic.StoreInt64(&w.activeWriter, 0)
	if written > 0 {
		atomic.AddInt64(&w.writeBytes, written)
		atomic.AddInt64(&w.readBytes, written)
	}
	if copyErr != nil {
		w.logger.Warn("archive copy failed", zap.Error(copyErr))
		_ = os.Remove(movingPath)
		if fErr := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, fmt.Sprintf("copy file: %v", copyErr)); fErr != nil {
			w.logger.Error("failed to fail archive job for copy error", zap.Error(fErr))
		}
		return
	}

	// 5. Verify size and SHA of copied file
	if written != job.ExpectedSize || actualSHA != job.SHA256 {
		w.logger.Error("archive verification failed on copied file",
			zap.Int64("written", written),
			zap.Int64("expected_size", job.ExpectedSize),
			zap.String("actual_sha", actualSHA),
			zap.String("expected_sha", job.SHA256),
		)
		_ = os.Remove(movingPath)
		if fErr := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, "copied file size or sha mismatch"); fErr != nil {
			w.logger.Error("failed to fail archive job for verification mismatch", zap.Error(fErr))
		}
		return
	}

	// 6. Non-replacing atomic rename from .moving to archive final
	if err := fscommit.CommitSiblingPart(movingPath, dstPath); err != nil {
		w.logger.Error("archive final rename failed", zap.Error(err))
		if errors.Is(err, fscommit.ErrTargetExists) {
			if cErr := w.db.SetArchiveJobConflict(job.ChatID, job.MessageID, job.ClaimID, "archive destination already exists during final commit"); cErr != nil {
				w.logger.Error("failed to set archive conflict for existing target", zap.Error(cErr))
			}
		} else {
			if fErr := w.db.FailArchiveJob(job.ChatID, job.MessageID, job.ClaimID, fmt.Sprintf("archive final commit: %v", err)); fErr != nil {
				w.logger.Error("failed to fail archive job for commit error", zap.Error(fErr))
			}
		}
		return
	}

	// 7. Mark archived in DB
	if err := w.db.CompleteArchiveJob(job.ChatID, job.MessageID, job.ClaimID, job.SHA256); err != nil {
		w.logger.Error("failed to mark archive complete in DB", zap.Error(err))
		return
	}
	EmitLifecycle(w.logger, LifecycleEvent{
		Event:     EventArchiveCommitted,
		TaskID:    fmt.Sprintf("%s:%d", job.ChatID, job.MessageID),
		AttemptID: job.ClaimID,
		ChatID:    job.ChatID,
		MessageID: job.MessageID,
		Path:      job.RelativePath,
		Size:      job.ExpectedSize,
		SHA256:    job.SHA256,
	})

	// 8. Delete SSD source only after archive final is durable and DB committed
	if err := os.Remove(srcPath); err != nil {
		w.logger.Warn("failed to delete SSD source file after archive success", zap.Error(err))
	} else {
		_ = fscommit.FsyncDir(filepath.Dir(srcPath))
		EmitLifecycle(w.logger, LifecycleEvent{
			Event:     EventArchiveSourceRemoved,
			TaskID:    fmt.Sprintf("%s:%d", job.ChatID, job.MessageID),
			AttemptID: job.ClaimID,
			ChatID:    job.ChatID,
			MessageID: job.MessageID,
			Path:      job.RelativePath,
		})
	}

	w.logger.Info("archive completed successfully",
		zap.String("chat_id", job.ChatID),
		zap.Int("message_id", job.MessageID),
		zap.String("rel_path", job.RelativePath),
	)
}

func computeFileSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (w *ArchiveWorker) PhysicalWriteBytes() int64 {
	if w == nil {
		return 0
	}
	return atomic.LoadInt64(&w.writeBytes)
}

func (w *ArchiveWorker) PhysicalReadBytes() int64 {
	if w == nil {
		return 0
	}
	return atomic.LoadInt64(&w.readBytes)
}

func (w *ArchiveWorker) ActiveWorkers() int {
	if w == nil {
		return 0
	}
	return int(atomic.LoadInt64(&w.activeWriter))
}
