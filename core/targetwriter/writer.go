package targetwriter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hittlert/TGX/core/bucket"
	atomicCommit "github.com/Hittlert/TGX/pkg/sbe/atomic"
)

type TaskManifest struct {
	TaskID       string
	FinalPath    string
	ExpectedSize int64
	Date         int64
}

type Metrics struct {
	Active                bool    `json:"active"`
	BytesPerSecond        float64 `json:"bytes_per_second"`
	ContiguousWriteRatio  float64 `json:"contiguous_write_ratio"`
	ActiveFilesCount      int     `json:"active_files_count"`
	TotalBytesWritten     int64   `json:"total_bytes_written"`
	LastError             string  `json:"last_error"`
}

type TargetWriter struct {
	bkt         bucket.Bucket
	outputDir   string
	manifests   sync.Map // taskID -> TaskManifest
	bitmaps     sync.Map // taskID -> *MovedBitmap
	openFiles   sync.Map // taskID -> *os.File
	onComplete  func(taskID, finalPath string, shaHash string)
	onProgress  func(taskID string, movedBytes, totalBytes int64)
	onError     func(taskID string, err error)

	writtenBytes      int64
	contiguousWrites  int64
	totalWrites       int64
	lastBPS           float64
	lastError         string
	errMu             sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed int32
}

func New(bkt bucket.Bucket, outputDir string) *TargetWriter {
	return &TargetWriter{
		bkt:       bkt,
		outputDir: outputDir,
	}
}

func (w *TargetWriter) SetCallbacks(
	onComplete func(taskID, finalPath string, shaHash string),
	onProgress func(taskID string, movedBytes, totalBytes int64),
	onError func(taskID string, err error),
) {
	w.onComplete = onComplete
	w.onProgress = onProgress
	w.onError = onError
}

func (w *TargetWriter) RegisterTask(manifest TaskManifest) {
	w.manifests.Store(manifest.TaskID, manifest)
	if _, ok := w.bitmaps.Load(manifest.TaskID); !ok {
		w.bitmaps.Store(manifest.TaskID, NewMovedBitmap(manifest.ExpectedSize))
	}
}

func (w *TargetWriter) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.writerLoop()
	go w.metricsLoop()
}

func (w *TargetWriter) metricsLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var prevBytes int64
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			current := atomic.LoadInt64(&w.writtenBytes)
			diff := current - prevBytes
			prevBytes = current
			w.lastBPS = float64(diff)
		}
	}
}

func (w *TargetWriter) Metrics() Metrics {
	w.errMu.RLock()
	lastErr := w.lastError
	w.errMu.RUnlock()

	tw := atomic.LoadInt64(&w.totalWrites)
	cw := atomic.LoadInt64(&w.contiguousWrites)
	ratio := 0.0
	if tw > 0 {
		ratio = float64(cw) / float64(tw) * 100.0
	}

	activeFiles := 0
	w.openFiles.Range(func(_, _ any) bool {
		activeFiles++
		return true
	})

	return Metrics{
		Active:               atomic.LoadInt32(&w.closed) == 0,
		BytesPerSecond:       w.lastBPS,
		ContiguousWriteRatio: ratio,
		ActiveFilesCount:     activeFiles,
		TotalBytesWritten:    atomic.LoadInt64(&w.writtenBytes),
		LastError:            lastErr,
	}
}

func (w *TargetWriter) writerLoop() {
	defer w.wg.Done()

	var currentTaskID string
	var nextOffset int64 = 0

	for {
		if atomic.LoadInt32(&w.closed) == 1 {
			return
		}
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		var obj *bucket.BufferObject
		var isContiguous bool

		// 1. Try to continue current file sequentially
		if currentTaskID != "" {
			if nextObj, ok := w.bkt.TryTakeNext(currentTaskID, nextOffset); ok {
				obj = nextObj
				isContiguous = true
			}
		}

		// 2. If no contiguous object for current task, take any ready object
		if obj == nil {
			if readyObj, ok := w.bkt.TakeReady(); ok {
				obj = readyObj
				isContiguous = false
				currentTaskID = obj.Key.TaskID
				nextOffset = obj.Key.Offset
			}
		}

		// 3. If bucket is empty, sleep briefly
		if obj == nil {
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}

		// Process and write object to target
		err := w.processObject(obj, isContiguous)
		if err != nil {
			w.errMu.Lock()
			w.lastError = err.Error()
			w.errMu.Unlock()
			if w.onError != nil {
				w.onError(obj.Key.TaskID, err)
			}
			// Reset current task tracker
			currentTaskID = ""
			nextOffset = 0
		} else {
			currentTaskID = obj.Key.TaskID
			nextOffset = obj.Key.Offset + obj.Key.Length
		}
	}
}

func (w *TargetWriter) processObject(obj *bucket.BufferObject, isContiguous bool) error {
	taskID := obj.Key.TaskID

	// 1. Fetch data from Buffer
	var data []byte
	if len(obj.Data) > 0 {
		data = obj.Data
	} else {
		readData, err := w.bkt.ReadObject(obj.Key)
		if err != nil {
			return fmt.Errorf("read buffer object %s: %w", obj.Key, err)
		}
		data = readData
	}

	// 2. Look up task manifest
	val, ok := w.manifests.Load(taskID)
	if !ok {
		// Task not registered, discard buffer object
		_ = w.bkt.AckDurable([]bucket.ObjectKey{obj.Key})
		return nil
	}
	manifest := val.(TaskManifest)

	// 3. Get or open target.moving file descriptor
	f, err := w.getOrOpenFile(manifest)
	if err != nil {
		return fmt.Errorf("open target moving file: %w", err)
	}

	// 4. Write data at exact offset
	nw, err := f.WriteAt(data, obj.Key.Offset)
	if err != nil {
		return fmt.Errorf("writeAt target moving file: %w", err)
	}
	if int64(nw) != obj.Key.Length {
		return fmt.Errorf("short write to target: %d of %d bytes", nw, obj.Key.Length)
	}

	// 5. fdatasync target file
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync target file: %w", err)
	}

	// 6. Update durable moved bitmap
	bmVal, _ := w.bitmaps.Load(taskID)
	bm := bmVal.(*MovedBitmap)
	bm.AddMark(obj.Key.Offset, obj.Key.Length)

	// 7. Ack durable in bucket (deletes source object and frees capacity!)
	if err := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); err != nil {
		return fmt.Errorf("ack durable object: %w", err)
	}

	// Track metrics
	atomic.AddInt64(&w.writtenBytes, obj.Key.Length)
	atomic.AddInt64(&w.totalWrites, 1)
	if isContiguous {
		atomic.AddInt64(&w.contiguousWrites, 1)
	}

	if w.onProgress != nil {
		w.onProgress(taskID, bm.DurableBytes(), manifest.ExpectedSize)
	}

	// 8. Check if file is 100% complete
	if bm.IsComplete() {
		return w.finalizeTask(manifest)
	}

	return nil
}

func (w *TargetWriter) getOrOpenFile(manifest TaskManifest) (*os.File, error) {
	if val, ok := w.openFiles.Load(manifest.TaskID); ok {
		return val.(*os.File), nil
	}

	finalPath := filepath.Join(w.outputDir, manifest.FinalPath)
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir %s: %w", dir, err)
	}

	movingPath := finalPath + ".moving"
	f, err := os.OpenFile(movingPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w.openFiles.Store(manifest.TaskID, f)
	return f, nil
}

func (w *TargetWriter) finalizeTask(manifest TaskManifest) error {
	taskID := manifest.TaskID
	finalPath := filepath.Join(w.outputDir, manifest.FinalPath)
	movingPath := finalPath + ".moving"

	// Close open FD
	if val, ok := w.openFiles.LoadAndDelete(taskID); ok {
		f := val.(*os.File)
		_ = f.Sync()
		_ = f.Close()
	}

	// Verify size
	stat, err := os.Stat(movingPath)
	if err != nil {
		return fmt.Errorf("stat completed moving file: %w", err)
	}
	if manifest.ExpectedSize > 0 && stat.Size() != manifest.ExpectedSize {
		return fmt.Errorf("final size %d does not match expected %d", stat.Size(), manifest.ExpectedSize)
	}

	// Set modification time if present
	if manifest.Date > 0 {
		when := time.Unix(manifest.Date, 0)
		_ = os.Chtimes(movingPath, when, when)
	}

	// Compute SHA256
	shaHash, _ := computeFileSHA256(movingPath)

	// Atomic non-replacing commit to final destination
	if err := atomicCommit.CommitFile(movingPath, finalPath); err != nil {
		if errors.Is(err, atomicCommit.ErrTargetExists) {
			_ = os.Remove(movingPath)
		} else {
			return fmt.Errorf("commit target file: %w", err)
		}
	}

	// Clean tracking state
	w.manifests.Delete(taskID)
	w.bitmaps.Delete(taskID)

	if w.onComplete != nil {
		w.onComplete(taskID, manifest.FinalPath, shaHash)
	}
	return nil
}

func computeFileSHA256(path string) (string, error) {
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

func (w *TargetWriter) Close() error {
	atomic.StoreInt32(&w.closed, 1)
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()

	// Close any open file descriptors
	w.openFiles.Range(func(key, val any) bool {
		if f, ok := val.(*os.File); ok {
			_ = f.Sync()
			_ = f.Close()
		}
		w.openFiles.Delete(key)
		return true
	})
	return nil
}
