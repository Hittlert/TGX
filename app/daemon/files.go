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
	"github.com/Hittlert/TGX/internal/fscommit"
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
	tracker    *trackedWriterAt
	tempPath   string
	outputRoot string
	finalPath  string
	date       int64
	closeOnce  sync.Once
	closeErr   error
}

func newFileElement(task *Task, file downloader.File, tempRoot, outputRoot string, date int64) (taskElement, error) {
	if task.IsTerminal() || (task.Context() != nil && task.Context().Err() != nil) {
		return nil, errors.New("task attempt is no longer active")
	}

	absolute, err := safeOutputPath(outputRoot, task.Request().FinalPath)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(absolute)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create destination directory: %w", err)
	}

	tempPath := absolute + ".part"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	return &fileElement{
		task:       task,
		file:       file,
		writer:     tempFile,
		tracker:    &trackedWriterAt{file: tempFile, task: task},
		tempPath:   tempPath,
		outputRoot: outputRoot,
		finalPath:  task.Request().FinalPath,
		date:       date,
	}, nil
}

func (e *fileElement) File() downloader.File { return e.file }
func (e *fileElement) To() io.WriterAt       { return e.tracker }
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
	closeErr := e.closeTemp()
	removeErr := os.Remove(e.tempPath)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
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
		_ = os.Remove(e.tempPath)
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

	if err := fscommit.CommitSiblingPart(e.tempPath, absolute); err != nil {
		_ = os.Remove(e.tempPath)
		if errors.Is(err, fscommit.ErrTargetExists) {
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
			e.closeErr = e.writer.Close()
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

func (e *existingElement) File() downloader.File          { return e.file }
func (e *existingElement) To() io.WriterAt                { return devNullWriterAt{} }
func (e *existingElement) AsTakeout() bool                { return false }
func (e *existingElement) Task() *Task                    { return e.task }
func (e *existingElement) Context() context.Context       { return e.task.Context() }
func (e *existingElement) IsCanceled() bool               { return e.task.IsTerminal() }
func (e *existingElement) AlreadyComplete() (string, bool) { return e.path, true }
func (e *existingElement) Abort() error                   { return nil }
func (e *existingElement) Publish() (PublishResult, error) {
	return PublishResult{Path: e.path, SHA256: e.sha, AlreadyExists: true}, nil
}

type devNullWriterAt struct{}

func (devNullWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}

func safeOutputPath(root, relative string) (string, error) {
	cleaned := filepath.Clean(filepath.FromSlash(relative))
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("relative path cannot be absolute: %s", relative)
	}
	if cleaned == "." || strings.HasPrefix(cleaned, "..") {
		return "", fmt.Errorf("path escapes output root: %s", relative)
	}
	rootClean := filepath.Clean(root)
	joined := filepath.Join(rootClean, cleaned)
	rel, err := filepath.Rel(rootClean, joined)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes output root: %s", relative)
	}
	return joined, nil
}

func existingFile(path string, expectedSize ...int64) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("destination path is directory: %s", path)
	}
	if len(expectedSize) > 0 && expectedSize[0] > 0 && info.Size() != expectedSize[0] {
		return false, fmt.Errorf("file size mismatch: expected %d, got %d", expectedSize[0], info.Size())
	}
	return true, nil
}

func verifyFinalFileIdentity(finalPath string, expectedSize int64, expectedSHA string, taskID string) (string, error) {
	finInfo, err := os.Stat(finalPath)
	if err != nil {
		return "", err
	}
	if finInfo.Size() != expectedSize {
		return "", fmt.Errorf("identity mismatch for %s: expected size %d, got %d", taskID, expectedSize, finInfo.Size())
	}
	actualSHA, err := computeSHA256(finalPath)
	if err != nil {
		return "", fmt.Errorf("failed to compute sha256 for existing file: %w", err)
	}
	if expectedSHA != "" && !strings.EqualFold(actualSHA, expectedSHA) {
		return "", fmt.Errorf("identity mismatch for %s: expected sha256 %s, got %s", taskID, expectedSHA, actualSHA)
	}
	return actualSHA, nil
}

func computeSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
