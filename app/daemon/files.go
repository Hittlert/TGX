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
	"github.com/Hittlert/TGX/core/mover"
	atomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
)

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
	mover      *mover.Mover
	closeOnce  sync.Once
	closeErr   error
}

func newFileElement(task *Task, file downloader.File, tempRoot, outputRoot string, date int64, optMover ...*mover.Mover) (*fileElement, error) {
	absolute, err := safeOutputPath(outputRoot, task.Request().FinalPath)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(absolute)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create destination directory: %w", err)
	}

	tempDir := dir
	if tempRoot != "" {
		tempDir = tempRoot
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			return nil, fmt.Errorf("create temp directory: %w", err)
		}
	}

	tempPath := CanonicalPartPath(tempDir, task.Request().ID)
	writer, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	var m *mover.Mover
	if len(optMover) > 0 {
		m = optMover[0]
	}

	element := &fileElement{
		task: task, file: file, writer: writer, tempPath: tempPath,
		tempRoot: tempRoot, outputRoot: outputRoot, finalPath: task.Request().FinalPath,
		date: date, mover: m,
	}
	element.tracked = &trackedWriterAt{file: writer, task: task}
	return element, nil
}

func (e *fileElement) File() downloader.File { return e.file }
func (e *fileElement) To() io.WriterAt       { return e.tracked }
func (e *fileElement) AsTakeout() bool       { return false }
func (e *fileElement) Task() *Task           { return e.task }
func (e *fileElement) Context() context.Context {
	if e.task != nil {
		return e.task.Context()
	}
	return context.Background()
}
func (e *fileElement) IsCanceled() bool {
	return e.task != nil && e.task.IsTerminal()
}
func (e *fileElement) AlreadyComplete() (string, bool) {
	return "", false
}

func (e *fileElement) Abort() error {
	_ = e.closeTemp()
	_ = os.Remove(e.tempPath)
	return nil
}

func (e *fileElement) Publish() (result PublishResult, resultErr error) {
	if e.task != nil && e.task.IsTerminal() {
		_ = e.Abort()
		return result, errors.New("task is terminal, aborting publish")
	}
	if err := e.closeTemp(); err != nil {
		return result, fmt.Errorf("sync temp file: %w", err)
	}
	stat, err := os.Stat(e.tempPath)
	if err != nil {
		return result, fmt.Errorf("stat temp file: %w", err)
	}
	if stat.Size() != e.file.Size() {
		return result, fmt.Errorf("temporary file size %d does not match expected %d", stat.Size(), e.file.Size())
	}
	absolute, err := safeOutputPath(e.outputRoot, e.finalPath)
	if err != nil {
		return result, err
	}
	if exists, err := existingFile(absolute, e.file.Size()); err != nil {
		return result, err
	} else if exists {
		_ = os.Remove(e.tempPath)
		return PublishResult{Path: e.finalPath, AlreadyExists: true, absolutePath: absolute}, nil
	}
	dir := filepath.Dir(absolute)

	if e.date > 0 {
		when := time.Unix(e.date, 0)
		_ = os.Chtimes(e.tempPath, when, when)
	}

	shaHash, err := computeSHA256(e.tempPath)
	if err != nil {
		return result, fmt.Errorf("hash temp file: %w", err)
	}

	if e.task != nil && e.task.IsTerminal() {
		_ = e.Abort()
		return result, errors.New("task became terminal during hash calculation, aborting publish")
	}

	if e.mover != nil && e.tempRoot != "" && e.tempRoot != e.outputRoot {
		doneChan := make(chan error, 1)
		job := &mover.MoveJob{
			ID:      e.task.Request().ID,
			SrcPath: e.tempPath,
			DstPath: absolute,
			Size:    e.file.Size(),
			OnDone: func(err error) {
				doneChan <- err
			},
		}
		if err := e.mover.Enqueue(job); err != nil {
			return result, fmt.Errorf("enqueue mover: %w", err)
		}
		select {
		case <-e.task.Context().Done():
			return result, e.task.Context().Err()
		case err := <-doneChan:
			if err != nil {
				if errors.Is(err, atomic.ErrTargetExists) {
					if exists, checkErr := existingFile(absolute, e.file.Size()); checkErr == nil && exists {
						_ = os.Remove(e.tempPath)
						return PublishResult{Path: e.finalPath, SHA256: shaHash, AlreadyExists: true, absolutePath: absolute}, nil
					}
					return result, fmt.Errorf("publish destination without overwrite: %w", err)
				}
				return result, fmt.Errorf("mover copy: %w", err)
			}
		}
	} else {
		// Zero-copy direct atomic rename in the exact same directory / volume!
		if err := atomic.CommitFile(e.tempPath, absolute); err != nil {
			if errors.Is(err, atomic.ErrTargetExists) {
				if exists, checkErr := existingFile(absolute, e.file.Size()); checkErr == nil && exists {
					_ = os.Remove(e.tempPath)
					return PublishResult{Path: e.finalPath, SHA256: shaHash, AlreadyExists: true, absolutePath: absolute}, nil
				}
				return result, fmt.Errorf("publish destination without overwrite: %w", err)
			}
			return result, fmt.Errorf("publish destination atomic rename: %w", err)
		}
	}

	if err := syncDirectory(dir); err != nil {
		return result, err
	}
	return PublishResult{
		Path: e.finalPath, SHA256: shaHash, absolutePath: absolute,
	}, nil
}

type memBufferWriterAt struct {
	mu   sync.Mutex
	data []byte
	task *Task
}

func newMemBufferWriterAt(task *Task) *memBufferWriterAt {
	return &memBufferWriterAt{task: task}
}

func (w *memBufferWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	required := int(offset) + len(p)
	if required > len(w.data) {
		newData := make([]byte, required)
		copy(newData, w.data)
		w.data = newData
	}

	copy(w.data[offset:], p)
	if w.task != nil {
		w.task.RecordWrite(offset, len(p))
	}
	return len(p), nil
}

func (w *memBufferWriterAt) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	res := make([]byte, len(w.data))
	copy(res, w.data)
	return res
}

type lazySmallFileElement struct {
	task       *Task
	file       downloader.File
	outputRoot string
	finalPath  string
	date       int64
	buf        *memBufferWriterAt
	mover      *mover.Mover
}

func newLazySmallFileElement(task *Task, file downloader.File, outputRoot string, date int64, optMover ...*mover.Mover) (*lazySmallFileElement, error) {
	var m *mover.Mover
	if len(optMover) > 0 {
		m = optMover[0]
	}
	return &lazySmallFileElement{
		task:       task,
		file:       file,
		outputRoot: outputRoot,
		finalPath:  task.Request().FinalPath,
		date:       date,
		buf:        newMemBufferWriterAt(task),
		mover:      m,
	}, nil
}

func (e *lazySmallFileElement) File() downloader.File { return e.file }
func (e *lazySmallFileElement) To() io.WriterAt       { return e.buf }
func (e *lazySmallFileElement) AsTakeout() bool       { return false }
func (e *lazySmallFileElement) Task() *Task           { return e.task }
func (e *lazySmallFileElement) Context() context.Context {
	if e.task != nil {
		return e.task.Context()
	}
	return context.Background()
}
func (e *lazySmallFileElement) IsCanceled() bool {
	return e.task != nil && e.task.IsTerminal()
}
func (e *lazySmallFileElement) AlreadyComplete() (string, bool) {
	return "", false
}
func (e *lazySmallFileElement) Abort() error { return nil }

func (e *lazySmallFileElement) Publish() (result PublishResult, resultErr error) {
	if e.task != nil && e.task.IsTerminal() {
		return result, errors.New("task is terminal, aborting publish")
	}

	data := e.buf.Bytes()
	if int64(len(data)) != e.file.Size() {
		return result, fmt.Errorf("in-memory file size %d does not match expected %d", len(data), e.file.Size())
	}

	// Compute SHA256 directly in memory - 0 disk reads!
	hashBytes := sha256.Sum256(data)
	shaHash := hex.EncodeToString(hashBytes[:])

	absolute, err := safeOutputPath(e.outputRoot, e.finalPath)
	if err != nil {
		return result, err
	}
	if exists, err := existingFile(absolute, e.file.Size()); err != nil {
		return result, err
	} else if exists {
		return PublishResult{Path: e.finalPath, SHA256: shaHash, AlreadyExists: true, absolutePath: absolute}, nil
	}

	dir := filepath.Dir(absolute)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return result, fmt.Errorf("create destination directory: %w", err)
	}

	if e.mover != nil {
		doneChan := make(chan error, 1)
		job := &mover.MoveJob{
			ID:      e.task.Request().ID,
			SrcData: data,
			DstPath: absolute,
			Size:    e.file.Size(),
			OnDone: func(err error) {
				doneChan <- err
			},
		}
		if err := e.mover.Enqueue(job); err != nil {
			return result, fmt.Errorf("enqueue mover: %w", err)
		}
		select {
		case <-e.task.Context().Done():
			return result, e.task.Context().Err()
		case err := <-doneChan:
			if err != nil {
				if errors.Is(err, atomic.ErrTargetExists) {
					if exists, checkErr := existingFile(absolute, e.file.Size()); checkErr == nil && exists {
						return PublishResult{Path: e.finalPath, SHA256: shaHash, AlreadyExists: true, absolutePath: absolute}, nil
					}
					return result, fmt.Errorf("publish destination without overwrite: %w", err)
				}
				return result, fmt.Errorf("mover write: %w", err)
			}
		}
	} else {
		tempPath := CanonicalPartPath(dir, e.task.Request().ID)
		f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return result, fmt.Errorf("create part file: %w", err)
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			_ = os.Remove(tempPath)
			return result, fmt.Errorf("write part file: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(tempPath)
			return result, fmt.Errorf("sync part file: %w", err)
		}
		if err := f.Close(); err != nil {
			_ = os.Remove(tempPath)
			return result, fmt.Errorf("close part file: %w", err)
		}

		if e.date > 0 {
			when := time.Unix(e.date, 0)
			_ = os.Chtimes(tempPath, when, when)
		}

		if err := atomic.CommitFile(tempPath, absolute); err != nil {
			if errors.Is(err, atomic.ErrTargetExists) {
				if exists, checkErr := existingFile(absolute, e.file.Size()); checkErr == nil && exists {
					_ = os.Remove(tempPath)
					return PublishResult{Path: e.finalPath, SHA256: shaHash, AlreadyExists: true, absolutePath: absolute}, nil
				}
				return result, fmt.Errorf("publish destination without overwrite: %w", err)
			}
			return result, fmt.Errorf("publish destination atomic rename: %w", err)
		}
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
	return PublishResult{Path: e.path, AlreadyExists: true}, nil
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
