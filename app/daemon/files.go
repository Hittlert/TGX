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

	"github.com/Hittlert/TGX/core/downloader"
	atomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/pkg/spool"
	"github.com/Hittlert/TGX/pkg/writeback"
)

// spoolWriterAt writes chunks directly to spool.Store segments with automatic Ready triggering.
type spoolWriterAt struct {
	store        spool.Store
	queue        *writeback.Queue
	task         *Task
	taskID       string
	gen          string
	finalRelPath string
	fileSize     int64
	fileDate     int64
}

func (w *spoolWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	ctx := context.Background()
	if w.task != nil {
		ctx = w.task.Context()
	}

	segIdx := int(offset / spool.DefaultSegmentSize)
	segStart := int64(segIdx) * spool.DefaultSegmentSize
	segLen := spool.DefaultSegmentSize
	if segStart+segLen > w.fileSize {
		segLen = w.fileSize - segStart
	}
	relOffset := offset - segStart

	segKey := spool.SegmentKey{
		TaskID:       w.taskID,
		Gen:          w.gen,
		SegmentIndex: segIdx,
		StartOffset:  segStart,
		Length:       segLen,
	}

	if w.store != nil {
		if err := w.store.Reserve(ctx, int64(len(p))); err != nil {
			return 0, err
		}
		item, err := w.store.CreateSegment(segKey)
		if err != nil {
			w.store.ReleaseReservation(int64(len(p)))
			return 0, err
		}
		n, err := w.store.WriteAt(segKey, relOffset, p)
		if err != nil {
			return n, err
		}
		if item.Ranges.IsComplete(segLen) {
			_ = w.store.MarkReady(segKey)
			if w.queue != nil {
				isLast := (segStart+segLen >= w.fileSize)
				w.queue.Enqueue(&writeback.Item{
					Key:              segKey,
					FinalRelPath:     w.finalRelPath,
					ExpectedFileSize: w.fileSize,
					IsLastSegment:    isLast,
					FileDate:         w.fileDate,
					Item:             item,
					AddedAt:          time.Now(),
				})
			}
		}
	}

	if w.task != nil {
		w.task.RecordWrite(offset, len(p))
	}
	return len(p), nil
}

type spoolFileElement struct {
	task       *Task
	file       downloader.File
	store      spool.Store
	queue      *writeback.Queue
	writer     *spoolWriterAt
	outputRoot string
	finalPath  string
	date       int64
}

func newSpoolFileElement(
	task *Task,
	file downloader.File,
	outputRoot string,
	date int64,
	store spool.Store,
	queue *writeback.Queue,
) (taskElement, error) {
	if task.IsTerminal() || (task.Context() != nil && task.Context().Err() != nil) {
		return nil, errors.New("task attempt is no longer active")
	}

	gen := task.AttemptGen()
	elem := &spoolFileElement{
		task:       task,
		file:       file,
		store:      store,
		queue:      queue,
		outputRoot: outputRoot,
		finalPath:  task.Request().FinalPath,
		date:       date,
	}
	elem.writer = &spoolWriterAt{
		store:        store,
		queue:        queue,
		task:         task,
		taskID:       task.Request().ID,
		gen:          gen,
		finalRelPath: task.Request().FinalPath,
		fileSize:     file.Size(),
		fileDate:     date,
	}
	return elem, nil
}

func (e *spoolFileElement) File() downloader.File { return e.file }
func (e *spoolFileElement) To() io.WriterAt       { return e.writer }
func (e *spoolFileElement) AsTakeout() bool       { return false }
func (e *spoolFileElement) Task() *Task           { return e.task }
func (e *spoolFileElement) Context() context.Context {
	if e.task != nil {
		return e.task.Context()
	}
	return context.Background()
}
func (e *spoolFileElement) IsCanceled() bool {
	return e.task != nil && e.task.IsTerminal()
}
func (e *spoolFileElement) AlreadyComplete() (string, bool) {
	return "", false
}
func (e *spoolFileElement) Abort() error {
	if e.queue != nil {
		e.queue.Cancel(e.task.Request().ID, e.task.AttemptGen())
	}
	return nil
}

func (e *spoolFileElement) Publish() (PublishResult, error) {
	if e.task != nil && e.task.IsTerminal() {
		return PublishResult{}, errors.New("task is terminal, aborting publish")
	}
	return PublishResult{
		Path:        e.finalPath,
		AsyncMoving: true,
	}, nil
}

type trackedWriterAt struct {
	file *os.File
	task *Task
}

func (w *trackedWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	written, err := w.file.WriteAt(p, offset)
	if written > 0 {
		w.task.RecordWrite(offset, written)
	}
	return written, err
}

type fileElement struct {
	task       *Task
	file       downloader.File
	writer     *os.File
	tracked    *trackedWriterAt
	tempPath   string
	tempRoot   string
	outputRoot string
	finalPath  string
	date       int64
	closeOnce  sync.Once
	closeErr   error
}

func newFileElement(task *Task, file downloader.File, tempRoot, outputRoot string, date int64) (*fileElement, error) {
	absolute, err := safeOutputPath(outputRoot, task.Request().FinalPath)
	if err != nil {
		return nil, NewTaskError("path", false, err)
	}
	dir := filepath.Dir(absolute)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tempDir := filepath.Join(tempRoot, "downloading")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, err
	}
	tempPath := filepath.Join(tempDir, task.Request().ID+".part")
	writer, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	elem := &fileElement{
		task:       task,
		file:       file,
		writer:     writer,
		tempPath:   tempPath,
		tempRoot:   tempRoot,
		outputRoot: outputRoot,
		finalPath:  task.Request().FinalPath,
		date:       date,
	}
	elem.tracked = &trackedWriterAt{file: writer, task: task}
	return elem, nil
}

func (e *fileElement) File() downloader.File       { return e.file }
func (e *fileElement) To() io.WriterAt             { return e.tracked }
func (e *fileElement) AsTakeout() bool             { return false }
func (e *fileElement) Task() *Task                 { return e.task }
func (e *fileElement) Context() context.Context    { return e.task.Context() }
func (e *fileElement) IsCanceled() bool            { return e.task.IsTerminal() }
func (e *fileElement) AlreadyComplete() (string, bool) { return "", false }

func (e *fileElement) Abort() error {
	_ = e.closeTemp()
	if e.tempPath != "" {
		_ = os.Remove(e.tempPath)
	}
	return nil
}

func (e *fileElement) Publish() (PublishResult, error) {
	result := PublishResult{Path: e.finalPath}
	if e.task != nil && e.task.IsTerminal() {
		_ = e.Abort()
		return result, errors.New("task is terminal, aborting publish")
	}
	if err := e.closeTemp(); err != nil {
		_ = os.Remove(e.tempPath)
		return result, fmt.Errorf("close temp file: %w", err)
	}

	stat, err := os.Stat(e.tempPath)
	if err != nil {
		return result, fmt.Errorf("stat temp file: %w", err)
	}
	if stat.Size() != e.file.Size() {
		_ = os.Remove(e.tempPath)
		return result, fmt.Errorf("short write: got %d bytes, want %d", stat.Size(), e.file.Size())
	}

	shaHash, err := computeSHA256(e.tempPath)
	if err != nil {
		_ = os.Remove(e.tempPath)
		return result, fmt.Errorf("compute sha256: %w", err)
	}

	absolute, err := safeOutputPath(e.outputRoot, e.finalPath)
	if err != nil {
		_ = os.Remove(e.tempPath)
		return result, err
	}
	if exists, err := existingFile(absolute, e.file.Size()); err != nil {
		_ = os.Remove(e.tempPath)
		return result, err
	} else if exists {
		_ = os.Remove(e.tempPath)
		return PublishResult{Path: e.finalPath, SHA256: shaHash, AlreadyExists: true, absolutePath: absolute}, nil
	}

	dir := filepath.Dir(absolute)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		_ = os.Remove(e.tempPath)
		return result, fmt.Errorf("create destination directory: %w", err)
	}

	if e.date > 0 {
		when := time.Unix(e.date, 0)
		_ = os.Chtimes(e.tempPath, when, when)
	}

	if err := atomic.CommitFile(e.tempPath, absolute); err != nil {
		_ = os.Remove(e.tempPath)
		if errors.Is(err, atomic.ErrTargetExists) {
			if exists, checkErr := existingFile(absolute, e.file.Size()); checkErr == nil && exists {
				return PublishResult{Path: e.finalPath, SHA256: shaHash, AlreadyExists: true, absolutePath: absolute}, nil
			}
			return result, fmt.Errorf("publish destination without overwrite: %w", err)
		}
		return result, fmt.Errorf("publish destination atomic rename: %w", err)
	}

	if err := syncDirectory(dir); err != nil {
		return result, err
	}
	return PublishResult{
		Path: e.finalPath, SHA256: shaHash, absolutePath: absolute,
	}, nil
}

func (e *fileElement) closeTemp() error {
	e.closeOnce.Do(func() {
		if e.writer != nil {
			if err := e.writer.Sync(); err != nil && e.closeErr == nil {
				e.closeErr = err
			}
			if err := e.writer.Close(); err != nil && e.closeErr == nil {
				e.closeErr = err
			}
		}
	})
	return e.closeErr
}

type existingElement struct {
	task *Task
	file downloader.File
	path string
	sha  string
}

func (e *existingElement) File() downloader.File { return e.file }
func (e *existingElement) To() io.WriterAt       { return discardWriterAt{} }
func (e *existingElement) AsTakeout() bool       { return false }
func (e *existingElement) Task() *Task           { return e.task }
func (e *existingElement) Context() context.Context {
	if e.task != nil {
		return e.task.Context()
	}
	return context.Background()
}
func (e *existingElement) IsCanceled() bool {
	return e.task != nil && e.task.IsTerminal()
}
func (e *existingElement) AlreadyComplete() (string, bool) {
	return e.path, true
}
func (e *existingElement) Abort() error { return nil }
func (e *existingElement) Publish() (PublishResult, error) {
	return PublishResult{Path: e.path, SHA256: e.sha, AlreadyExists: true}, nil
}

type discardWriterAt struct{}

func (discardWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}

func safeOutputPath(root, relative string) (string, error) {
	cleaned := filepath.Clean(relative)
	if strings.HasPrefix(cleaned, "..") || filepath.IsAbs(cleaned) {
		return "", errors.New("unsafe relative path")
	}
	combined := filepath.Join(root, cleaned)
	return combined, nil
}

func existingFile(path string, expectedSize int64) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if stat.IsDir() {
		return false, errors.New("destination exists as a directory")
	}
	if stat.Size() != expectedSize {
		return false, fmt.Errorf("destination exists with conflicting size %d (expected %d)", stat.Size(), expectedSize)
	}
	return true, nil
}

func computeSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(hasher, file, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func verifyFinalFileIdentity(finalPath string, expectedSize int64, expectedSHA string, expectedTaskID string) (string, error) {
	stat, err := os.Stat(finalPath)
	if err != nil {
		return "", fmt.Errorf("target file missing: %w", err)
	}
	if expectedSize > 0 && stat.Size() != expectedSize {
		return "", fmt.Errorf("size mismatch: expected %d, got %d", expectedSize, stat.Size())
	}
	actualSHA, err := computeSHA256(finalPath)
	if err != nil {
		return "", fmt.Errorf("compute target sha256: %w", err)
	}
	if expectedSHA == "" {
		return "", errors.New("expected SHA is required for trusted identity verification")
	}
	if actualSHA != expectedSHA {
		return "", fmt.Errorf("content conflict: expected sha %s, got %s", expectedSHA, actualSHA)
	}
	return actualSHA, nil
}
