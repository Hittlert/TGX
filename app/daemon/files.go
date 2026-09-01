package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Hittlert/TGX/core/bucket"
	"github.com/Hittlert/TGX/core/downloader"
	"github.com/Hittlert/TGX/core/targetwriter"
	atomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
)

// bucketWriterAt writes immutable chunk objects directly to bucket.Bucket with object-level reservation.
type bucketWriterAt struct {
	bkt      bucket.Bucket
	task     *Task
	taskID   string
	gen      string
	fileSize int64
}

func (w *bucketWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	key := bucket.ObjectKey{
		TaskID:           w.taskID,
		Gen:              w.gen,
		Offset:           offset,
		Length:           int64(len(p)),
		ExpectedFileSize: w.fileSize,
	}
	if w.bkt != nil {
		ctx := context.Background()
		if w.task != nil {
			ctx = w.task.Context()
		}
		if err := w.bkt.Reserve(ctx, int64(len(p))); err != nil {
			return 0, err
		}
	}
	if err := w.bkt.PutObject(key, p); err != nil {
		if w.bkt != nil {
			w.bkt.ReleaseReservation(int64(len(p)))
		}
		return 0, err
	}
	if w.task != nil {
		w.task.RecordWrite(offset, len(p))
	}
	return len(p), nil
}

type bucketFileElement struct {
	task       *Task
	file       downloader.File
	bkt        bucket.Bucket
	tw         *targetwriter.TargetWriter
	writer     *bucketWriterAt
	outputRoot string
	finalPath  string
	date       int64
}

func newBucketFileElement(
	task *Task,
	file downloader.File,
	outputRoot string,
	date int64,
	bkt bucket.Bucket,
	tw *targetwriter.TargetWriter,
) (taskElement, error) {
	// Guard against stale/canceled Task attempt after slow resolve
	if task.IsTerminal() || (task.Context() != nil && task.Context().Err() != nil) {
		return nil, errors.New("task attempt is no longer active")
	}

	// Generation determined once at task submission, not derived at download time
	gen := task.AttemptGen()
	manifest := targetwriter.TaskManifest{
		TaskID:       task.Request().ID,
		FinalPath:    task.Request().FinalPath,
		ExpectedSize: file.Size(),
		Date:         date,
		Gen:          gen,
	}
	if bkt != nil {
		bkt.SetTaskGeneration(task.Request().ID, gen)
	}
	if tw != nil {
		res := tw.RegisterTask(manifest)
		switch res {
		case targetwriter.RegisterAccepted:
			// Normal download path
		case targetwriter.RegisterAlreadyFinalized:
			finalFile := filepath.Join(outputRoot, manifest.FinalPath)
			var expectedSHA string
			if _, _, sha, ok := tw.TaskFinalInfo(manifest.TaskID); ok && sha != "" {
				expectedSHA = sha
			}
			verifiedSHA, err := verifyFinalFileIdentity(finalFile, file.Size(), expectedSHA, manifest.TaskID)
			if err != nil {
				return nil, fmt.Errorf("already finalized verification failed: %w", err)
			}
			return &existingElement{task: task, file: file, path: manifest.FinalPath, sha: verifiedSHA}, nil
		case targetwriter.RegisterStale:
			return nil, fmt.Errorf("target writer rejected attempt: STALE")
		case targetwriter.RegisterConflict:
			return nil, fmt.Errorf("target writer rejected attempt: CONFLICT")
		}
	}

	elem := &bucketFileElement{
		task:       task,
		file:       file,
		bkt:        bkt,
		tw:         tw,
		outputRoot: outputRoot,
		finalPath:  task.Request().FinalPath,
		date:       date,
	}
	elem.writer = &bucketWriterAt{
		bkt:      bkt,
		task:     task,
		taskID:   task.Request().ID,
		gen:      gen,
		fileSize: file.Size(),
	}
	return elem, nil
}

func (e *bucketFileElement) File() downloader.File { return e.file }
func (e *bucketFileElement) To() io.WriterAt       { return e.writer }
func (e *bucketFileElement) AsTakeout() bool       { return false }
func (e *bucketFileElement) Task() *Task           { return e.task }
func (e *bucketFileElement) Context() context.Context {
	if e.task != nil {
		return e.task.Context()
	}
	return context.Background()
}
func (e *bucketFileElement) IsCanceled() bool {
	return e.task != nil && e.task.IsTerminal()
}
func (e *bucketFileElement) AlreadyComplete() (string, bool) {
	return "", false
}
func (e *bucketFileElement) Abort() error {
	return nil
}

func (e *bucketFileElement) Publish() (PublishResult, error) {
	if e.task != nil && e.task.IsTerminal() {
		return PublishResult{}, errors.New("task is terminal, aborting publish")
	}
	// Network chunk downloading is 100% complete!
	// TargetWriter handles async background target streaming and final atomic non-replacing commit.
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

	element := &fileElement{
		task: task, file: file, writer: writer, tempPath: tempPath,
		tempRoot: tempRoot, outputRoot: outputRoot, finalPath: task.Request().FinalPath,
		date: date,
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
	bkt        bucket.Bucket
	tw         *targetwriter.TargetWriter
}

func newLazySmallFileElement(
	task *Task,
	file downloader.File,
	outputRoot string,
	date int64,
	bkt bucket.Bucket,
	tw *targetwriter.TargetWriter,
) (taskElement, error) {
	// Guard against stale/canceled Task attempt after slow resolve
	if task.IsTerminal() || (task.Context() != nil && task.Context().Err() != nil) {
		return nil, errors.New("task attempt is no longer active")
	}

	// Generation determined once at task submission, not derived at download time
	gen := task.AttemptGen()
	if bkt != nil {
		bkt.SetTaskGeneration(task.Request().ID, gen)
	}
	if tw != nil {
		manifest := targetwriter.TaskManifest{
			TaskID:       task.Request().ID,
			FinalPath:    task.Request().FinalPath,
			ExpectedSize: file.Size(),
			Date:         date,
			Gen:          gen,
		}
		res := tw.RegisterTask(manifest)
		switch res {
		case targetwriter.RegisterAccepted:
			// Normal download path
		case targetwriter.RegisterAlreadyFinalized:
			finalFile := filepath.Join(outputRoot, manifest.FinalPath)
			var expectedSHA string
			if _, _, sha, ok := tw.TaskFinalInfo(manifest.TaskID); ok && sha != "" {
				expectedSHA = sha
			}
			verifiedSHA, err := verifyFinalFileIdentity(finalFile, file.Size(), expectedSHA, manifest.TaskID)
			if err != nil {
				return nil, fmt.Errorf("already finalized verification failed: %w", err)
			}
			return &existingElement{task: task, file: file, path: manifest.FinalPath, sha: verifiedSHA}, nil
		case targetwriter.RegisterStale:
			return nil, fmt.Errorf("target writer rejected small file attempt: STALE")
		case targetwriter.RegisterConflict:
			return nil, fmt.Errorf("target writer rejected small file attempt: CONFLICT")
		}
	}
	return &lazySmallFileElement{
		task:       task,
		file:       file,
		outputRoot: outputRoot,
		finalPath:  task.Request().FinalPath,
		date:       date,
		buf:        newMemBufferWriterAt(task),
		bkt:        bkt,
		tw:         tw,
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

	hashBytes := sha256.Sum256(data)
	shaHash := hex.EncodeToString(hashBytes[:])

	if e.bkt != nil && e.tw != nil {
		// Generation from task state, consistent with element construction
		gen := e.task.AttemptGen()
		key := bucket.ObjectKey{
			TaskID:           e.task.Request().ID,
			Gen:              gen,
			Offset:           0,
			Length:           int64(len(data)),
			ExpectedFileSize: e.file.Size(),
		}
		if err := e.bkt.Reserve(e.Context(), int64(len(data))); err != nil {
			return result, fmt.Errorf("reserve small file in bucket: %w", err)
		}
		if err := e.bkt.PutObject(key, data); err != nil {
			e.bkt.ReleaseReservation(int64(len(data)))
			return result, fmt.Errorf("put small file to bucket: %w", err)
		}
		return PublishResult{
			Path:        e.finalPath,
			SHA256:      shaHash,
			AsyncMoving: true,
		}, nil
	}

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
	if expectedSHA != "" {
		if actualSHA != expectedSHA {
			return "", fmt.Errorf("content conflict: expected sha %s, got %s", expectedSHA, actualSHA)
		}
		return actualSHA, nil
	}
	// Check .tgx_commit sidecar proof if expectedSHA was not provided in memory
	proofPath := finalPath + ".tgx_commit"
	if data, err := os.ReadFile(proofPath); err == nil {
		var proof targetwriter.CommitProof
		if json.Unmarshal(data, &proof) == nil && proof.SHA256 != "" {
			if expectedTaskID != "" && proof.TaskID != "" && proof.TaskID != expectedTaskID {
				return "", fmt.Errorf("task ID mismatch in commit proof: expected %s, got %s", expectedTaskID, proof.TaskID)
			}
			if expectedSize > 0 && proof.ExpectedSize > 0 && proof.ExpectedSize != expectedSize {
				return "", fmt.Errorf("size mismatch in commit proof: expected %d, got %d", expectedSize, proof.ExpectedSize)
			}
			if actualSHA != proof.SHA256 {
				return "", fmt.Errorf("content conflict with commit proof: expected %s, got %s", proof.SHA256, actualSHA)
			}
			return actualSHA, nil
		}
	}
	return actualSHA, nil
}
