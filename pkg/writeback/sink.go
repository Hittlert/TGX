package writeback

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/pkg/spool"
	"go.uber.org/zap"
)

const targetLockShards = 256

// TargetSink coordinates streaming write-back workers from Spool to destination storage.
type TargetSink struct {
	cfg       Config
	store     spool.Store
	queue     *Queue
	callbacks Callbacks
	logger    *zap.Logger

	mu        sync.RWMutex
	tasks     map[string]*TaskWriteContext // taskID:gen -> TaskWriteContext
	pathLocks [targetLockShards]sync.Mutex // Fixed sharded mutexes keyed by final target path
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closed    bool
}

// NewTargetSink creates and initializes a TargetSink.
func NewTargetSink(cfg Config, store spool.Store, queue *Queue, cb Callbacks, logger *zap.Logger) *TargetSink {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())
	sink := &TargetSink{
		cfg:       cfg,
		store:     store,
		queue:     queue,
		callbacks: cb,
		logger:    logger,
		tasks:     make(map[string]*TaskWriteContext),
		ctx:       ctx,
		cancel:    cancel,
	}

	sink.startWorkers()
	return sink
}

func sanitizeTaskID(taskID string) string {
	r := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return r.Replace(taskID)
}

func (s *TargetSink) getPathLock(finalRelPath string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(filepath.Clean(finalRelPath)))
	idx := h.Sum32() % targetLockShards
	return &s.pathLocks[idx]
}

func (s *TargetSink) getOrCreateTaskContext(item *Item) (*TaskWriteContext, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	taskKey := fmt.Sprintf("%s:%s", item.Key.TaskID, item.Key.Gen)
	taskCtx, exists := s.tasks[taskKey]
	if exists {
		return taskCtx, nil
	}

	finalPath := filepath.Join(s.cfg.OutputDir, filepath.FromSlash(item.FinalRelPath))
	// Generation-scoped and TaskID-scoped moving path to prevent cross-task and cross-generation collisions
	movingPath := fmt.Sprintf("%s.%s.%s.moving", finalPath, sanitizeTaskID(item.Key.TaskID), item.Key.Gen)

	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return nil, fmt.Errorf("create destination directory: %w", err)
	}

	taskCtx = NewTaskWriteContext(
		item.Key.TaskID,
		item.Key.Gen,
		item.FinalRelPath,
		movingPath,
		finalPath,
		item.ExpectedFileSize,
		item.FileDate,
	)

	s.tasks[taskKey] = taskCtx
	return taskCtx, nil
}

func (s *TargetSink) startWorkers() {
	for i := 0; i < s.cfg.Concurrency; i++ {
		s.wg.Add(1)
		go s.workerLoop(i)
	}
}

func (s *TargetSink) workerLoop(workerID int) {
	defer s.wg.Done()

	buf := make([]byte, 512*1024) // 512 KiB read/write chunk buffer

	for {
		item, err := s.queue.Dequeue(s.ctx)
		if err != nil {
			return // Context cancelled or queue closed
		}

		if processErr := s.processItem(item, buf); processErr != nil {
			if errors.Is(processErr, ErrStaleGeneration) {
				continue // Stale generation silently ignored
			}
			s.logger.Error("failed to write-back segment to target",
				zap.Int("worker", workerID),
				zap.String("task_id", item.Key.TaskID),
				zap.String("generation", item.Key.Gen),
				zap.Int("segment_index", item.Key.SegmentIndex),
				zap.Error(processErr),
			)
			// Requeue if retryable and not terminal conflict
			if !errors.Is(processErr, ErrTargetConflict) && item.Attempts < 5 {
				s.queue.Requeue(item, time.Duration(item.Attempts+1)*500*time.Millisecond)
			} else if s.callbacks.OnTaskFinalized != nil {
				s.callbacks.OnTaskFinalized(item.Key.TaskID, item.Key.Gen, item.FinalRelPath, "", 0, processErr)
			}
		}
	}
}

func (s *TargetSink) processItem(item *Item, readBuf []byte) error {
	pathLock := s.getPathLock(item.FinalRelPath)
	pathLock.Lock()
	defer pathLock.Unlock()

	taskCtx, err := s.getOrCreateTaskContext(item)
	if err != nil {
		return fmt.Errorf("get or create task context: %w", err)
	}

	taskCtx.mu.Lock()
	defer taskCtx.mu.Unlock()

	if taskCtx.Closed {
		return fmt.Errorf("task context already closed for %s", item.Key.TaskID)
	}

	if item.Key.Gen != taskCtx.Gen {
		return ErrStaleGeneration
	}

	// Open target .moving file for writing at segment offset
	targetFile, err := os.OpenFile(taskCtx.MovingPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open target moving file: %w", err)
	}
	defer targetFile.Close()

	// Stream segment bytes from Spool into target .moving file
	var offset int64 = 0
	for offset < item.Key.Length {
		toRead := int64(len(readBuf))
		if offset+toRead > item.Key.Length {
			toRead = item.Key.Length - offset
		}

		n, readErr := s.store.ReadAt(item.Key, offset, readBuf[:toRead])
		if readErr != nil && n == 0 {
			return fmt.Errorf("read from spool segment %s at offset %d: %w", item.Key.String(), offset, readErr)
		}

		targetOffset := item.Key.StartOffset + offset
		if _, writeErr := targetFile.WriteAt(readBuf[:n], targetOffset); writeErr != nil {
			return fmt.Errorf("write to target moving file at %d: %w", targetOffset, writeErr)
		}

		offset += int64(n)
	}

	// Sync target data to disk
	if err := targetFile.Sync(); err != nil {
		return fmt.Errorf("sync target file %s: %w", taskCtx.MovingPath, err)
	}

	// Update durable range set and written accounting
	taskCtx.DurableRanges.Add(item.Key.StartOffset, item.Key.StartOffset+item.Key.Length)
	taskCtx.WrittenBytes = taskCtx.DurableRanges.TotalCovered()

	// Reclaim source segment from Spool immediately to free SSD/RAM capacity
	if err := s.store.Reclaim(item.Key); err != nil {
		s.logger.Warn("failed to reclaim segment from spool after target sync",
			zap.String("segment", item.Key.String()),
			zap.Error(err),
		)
	}

	if s.callbacks.OnSegmentDurable != nil {
		s.callbacks.OnSegmentDurable(item.Key, item.Key.Length)
	}

	// Check if ALL segments of the file are durable:
	if taskCtx.DurableRanges.IsComplete(taskCtx.ExpectedSize) && taskCtx.WrittenBytes == taskCtx.ExpectedSize {
		return s.finalizeTask(taskCtx, targetFile)
	}

	return nil
}

func (s *TargetSink) finalizeTask(taskCtx *TaskWriteContext, targetFile *os.File) error {
	if err := targetFile.Sync(); err != nil {
		return fmt.Errorf("sync final target file: %w", err)
	}
	_ = targetFile.Close()

	// Verify exact size of moving file before commit
	movingStat, statErr := os.Stat(taskCtx.MovingPath)
	if statErr != nil {
		return fmt.Errorf("stat moving file before commit: %w", statErr)
	}
	if movingStat.Size() != taskCtx.ExpectedSize {
		return fmt.Errorf("moving file size mismatch (got %d, expected %d): %w", movingStat.Size(), taskCtx.ExpectedSize, ErrIncompleteFile)
	}

	// Compute verified sequential SHA256 over finalized moving file
	shaHex, err := computeTargetSHA256(taskCtx.MovingPath)
	if err != nil {
		return fmt.Errorf("compute finalized moving file sha256: %w", err)
	}

	if taskCtx.FileDate > 0 {
		when := time.Unix(taskCtx.FileDate, 0)
		_ = os.Chtimes(taskCtx.MovingPath, when, when)
	}

	// Atomic non-replacing commit: moving -> final
	if err := atomic.CommitFile(taskCtx.MovingPath, taskCtx.FinalPath); err != nil {
		if errors.Is(err, atomic.ErrTargetExists) {
			// Verify existing file size and SHA256
			existingStat, statErr := os.Stat(taskCtx.FinalPath)
			if statErr != nil {
				return fmt.Errorf("stat existing target file: %w", statErr)
			}
			if existingStat.Size() != taskCtx.ExpectedSize {
				return fmt.Errorf("target exists with size mismatch (got %d, expected %d): %w", existingStat.Size(), taskCtx.ExpectedSize, ErrTargetConflict)
			}
			existingSHA, shaErr := computeTargetSHA256(taskCtx.FinalPath)
			if shaErr != nil {
				return fmt.Errorf("compute existing target sha256: %w", shaErr)
			}
			if existingSHA != shaHex {
				return fmt.Errorf("target exists with SHA mismatch (got %s, expected %s): %w", existingSHA, shaHex, ErrTargetConflict)
			}
			// Verified identical content! Clean up temporary moving file
			_ = os.Remove(taskCtx.MovingPath)
		} else {
			return fmt.Errorf("atomic commit final file: %w", err)
		}
	}

	taskCtx.Closed = true

	// Cleanup task context from active map
	s.mu.Lock()
	delete(s.tasks, fmt.Sprintf("%s:%s", taskCtx.TaskID, taskCtx.Gen))
	s.mu.Unlock()

	s.logger.Info("target file successfully committed",
		zap.String("task_id", taskCtx.TaskID),
		zap.String("generation", taskCtx.Gen),
		zap.String("final_path", taskCtx.FinalRelPath),
		zap.Int64("size", taskCtx.ExpectedSize),
		zap.String("sha256", shaHex),
	)

	if s.callbacks.OnTaskFinalized != nil {
		s.callbacks.OnTaskFinalized(taskCtx.TaskID, taskCtx.Gen, taskCtx.FinalRelPath, shaHex, taskCtx.ExpectedSize, nil)
	}

	return nil
}

func computeTargetSHA256(path string) (string, error) {
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

func (s *TargetSink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}
