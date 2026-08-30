package daemon

import (
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
	outputRoot string
	finalPath  string
	date       int64
	closeOnce  sync.Once
	closeErr   error
}

func newFileElement(task *Task, file downloader.File, tempRoot, outputRoot string, date int64) (*fileElement, error) {
	if err := os.MkdirAll(tempRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create temp root: %w", err)
	}
	hash := sha256.Sum256([]byte(task.Request().ID))
	tempPath := filepath.Join(tempRoot, hex.EncodeToString(hash[:])+".part")
	writer, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}
	element := &fileElement{
		task: task, file: file, writer: writer, tempPath: tempPath,
		outputRoot: outputRoot, finalPath: task.Request().FinalPath, date: date,
	}
	element.tracked = &trackedWriterAt{file: writer, task: task}
	return element, nil
}

func (e *fileElement) File() downloader.File { return e.file }
func (e *fileElement) To() io.WriterAt       { return e.tracked }
func (e *fileElement) AsTakeout() bool       { return false }
func (e *fileElement) Task() *Task           { return e.task }
func (e *fileElement) AlreadyComplete() (string, bool) {
	return "", false
}

func (e *fileElement) Abort() error {
	return e.closeTemp()
}

func (e *fileElement) Publish() (result PublishResult, resultErr error) {
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return result, fmt.Errorf("create destination directory: %w", err)
	}

	source, err := os.Open(e.tempPath)
	if err != nil {
		return result, fmt.Errorf("open temp file: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, source.Close()) }()
	stage, err := os.CreateTemp(dir, ".tdl-stage-*")
	if err != nil {
		return result, fmt.Errorf("create destination stage: %w", err)
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(stage, hasher), source)
	if copyErr != nil {
		_ = stage.Close()
		return result, fmt.Errorf("copy to destination stage: %w", copyErr)
	}
	if written != e.file.Size() {
		_ = stage.Close()
		return result, fmt.Errorf("staged file size %d does not match expected %d", written, e.file.Size())
	}
	if err := stage.Chmod(0o644); err != nil {
		_ = stage.Close()
		return result, fmt.Errorf("chmod destination stage: %w", err)
	}
	if err := stage.Sync(); err != nil {
		_ = stage.Close()
		return result, fmt.Errorf("sync destination stage: %w", err)
	}
	if err := stage.Close(); err != nil {
		return result, fmt.Errorf("close destination stage: %w", err)
	}
	if e.date > 0 {
		when := time.Unix(e.date, 0)
		if err := os.Chtimes(stagePath, when, when); err != nil {
			return result, fmt.Errorf("set destination time: %w", err)
		}
	}
	if err := atomic.CommitFile(stagePath, absolute); err != nil {
		if errors.Is(err, atomic.ErrTargetExists) {
			if exists, checkErr := existingFile(absolute, e.file.Size()); checkErr == nil && exists {
				_ = os.Remove(e.tempPath)
				return PublishResult{Path: e.finalPath, AlreadyExists: true, absolutePath: absolute}, nil
			}
			return result, fmt.Errorf("publish destination without overwrite: %w", err)
		}
		return result, fmt.Errorf("publish destination atomic rename: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return result, err
	}
	if err := os.Remove(e.tempPath); err != nil {
		return result, fmt.Errorf("remove temp file: %w", err)
	}
	return PublishResult{
		Path: e.finalPath, SHA256: hex.EncodeToString(hasher.Sum(nil)), absolutePath: absolute,
	}, nil
}

func (e *fileElement) closeTemp() error {
	e.closeOnce.Do(func() {
		e.closeErr = errors.Join(e.writer.Sync(), e.writer.Close())
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
func (e *existingElement) AlreadyComplete() (string, bool) {
	return e.path, true
}
func (e *existingElement) Publish() (PublishResult, error) {
	return PublishResult{Path: e.path, AlreadyExists: true}, nil
}
func (e *existingElement) Abort() error { return nil }

type discardWriterAt struct{}

func (discardWriterAt) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

func existingFile(path string, expectedSize int64) (bool, error) {
	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat destination: %w", err)
	}
	if !stat.Mode().IsRegular() {
		return false, fmt.Errorf("destination is not a regular file: %s", path)
	}
	if stat.Size() != expectedSize {
		if stat.Size() == 0 {
			_ = os.Remove(path)
			return false, nil
		}
		return false, fmt.Errorf("destination collision: size %d does not match expected %d", stat.Size(), expectedSize)
	}
	return true, nil
}

func safeOutputPath(root, relative string) (string, error) {
	if root == "" {
		return "", errors.New("output root is required")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve output root: %w", err)
	}
	joined := filepath.Join(absoluteRoot, filepath.FromSlash(relative))
	rel, err := filepath.Rel(absoluteRoot, joined)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("destination escapes output root: %q", relative)
	}
	return joined, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open destination directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync destination directory: %w", err)
	}
	return nil
}
