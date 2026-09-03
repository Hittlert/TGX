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
			claimed, err := w.db.ClaimArchiveJob(job.ChatID, job.MessageID)
			if err != nil || !claimed {
				break
			}

			w.processJob(ctx, job)
		}
	}
}

func (w *ArchiveWorker) processJob(ctx context.Context, job ArchiveJob) {
	srcPath := filepath.Join(w.downloadDir, job.RelativePath)
	dstPath := filepath.Join(w.archiveDir, job.RelativePath)

	w.logger.Debug("starting whole-file archive",
		zap.String("chat_id", job.ChatID),
		zap.Int("message_id", job.MessageID),
		zap.String("rel_path", job.RelativePath),
		zap.Int64("size", job.ExpectedSize),
	)

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
				_ = w.db.CompleteArchiveJob(job.ChatID, job.MessageID)
				_ = os.Remove(srcPath) // Remove verified SSD duplicate
				return
			}
		}

		// Content mismatch -> mark conflict, preserve both files
		w.logger.Warn("archive destination exists with conflicting content, marking conflict",
			zap.String("chat_id", job.ChatID),
			zap.Int("message_id", job.MessageID),
			zap.String("dst_path", dstPath),
		)
		_ = w.db.SetArchiveJobConflict(job.ChatID, job.MessageID, "destination file exists with conflicting content")
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
			_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, fmt.Sprintf("SSD source missing: %v", err))
			return
		}
		_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, fmt.Sprintf("stat SSD source: %v", err))
		return
	}

	if srcInfo.Size() != job.ExpectedSize {
		w.logger.Error("SSD source file size mismatch",
			zap.String("chat_id", job.ChatID),
			zap.Int("message_id", job.MessageID),
			zap.Int64("actual", srcInfo.Size()),
			zap.Int64("expected", job.ExpectedSize),
		)
		_ = w.db.SetArchiveJobConflict(job.ChatID, job.MessageID, fmt.Sprintf("SSD source size mismatch: got %d want %d", srcInfo.Size(), job.ExpectedSize))
		return
	}

	// 3. Ensure archive parent directory exists
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		w.logger.Warn("failed to create archive directory", zap.Error(err))
		_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, fmt.Sprintf("mkdir archive dir: %v", err))
		return
	}

	// 4. Perform whole-file archive copy via .moving sibling on destination filesystem
	movingPath := dstPath + ".moving"
	written, actualSHA, copyErr := fscommit.CopyFileSequential(srcPath, movingPath)
	if copyErr != nil {
		w.logger.Warn("archive copy failed", zap.Error(copyErr))
		_ = os.Remove(movingPath)
		_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, fmt.Sprintf("copy file: %v", copyErr))
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
		_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, "copied file size or sha mismatch")
		return
	}

	// 6. Non-replacing atomic rename from .moving to archive final
	if err := fscommit.CommitSiblingPart(movingPath, dstPath); err != nil {
		w.logger.Error("archive final rename failed", zap.Error(err))
		if errors.Is(err, fscommit.ErrTargetExists) {
			_ = w.db.SetArchiveJobConflict(job.ChatID, job.MessageID, "archive destination already exists during final commit")
		} else {
			_ = w.db.FailArchiveJob(job.ChatID, job.MessageID, fmt.Sprintf("archive final commit: %v", err))
		}
		return
	}

	// 7. Mark archived in DB
	if err := w.db.CompleteArchiveJob(job.ChatID, job.MessageID); err != nil {
		w.logger.Error("failed to mark archive complete in DB", zap.Error(err))
		return
	}

	// 8. Delete SSD source only after archive final is durable and DB committed
	if err := os.Remove(srcPath); err != nil {
		w.logger.Warn("failed to delete SSD source file after archive success", zap.Error(err))
	} else {
		_ = fscommit.FsyncDir(filepath.Dir(srcPath))
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
