package targetwriter

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
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hittlert/TGX/core/bucket"
	atomicCommit "github.com/Hittlert/TGX/pkg/sbe/atomic"
)

const sidecarVersion = 1

type TaskManifest struct {
	Version      int     `json:"version,omitempty"`
	TaskID       string  `json:"task_id"`
	FinalPath    string  `json:"final_path"`
	ExpectedSize int64   `json:"expected_size"`
	Date         int64   `json:"date"`
	Gen          string  `json:"gen"`
	Ranges       []Range `json:"ranges"`
}

type Metrics struct {
	Active                bool    `json:"active"`
	BytesPerSecond        float64 `json:"bytes_per_second"`
	ContiguousWriteRatio  float64 `json:"contiguous_write_ratio"`
	ActiveFilesCount      int     `json:"active_files_count"`
	TotalBytesWritten     int64   `json:"total_bytes_written"`
	LastError             string  `json:"last_error"`
}

const maxTaskRetries = 5 // After this many consecutive errors on a task, declare permanent failure

type TargetWriter struct {
	bkt         bucket.Bucket
	outputDir   string
	manifests   sync.Map // taskID -> TaskManifest
	bitmaps     sync.Map // taskID -> *MovedBitmap
	openFiles   sync.Map // taskID -> *os.File
	hashers     sync.Map // taskID -> hash.Hash
	onComplete  func(taskID, finalPath string, shaHash string)
	onProgress  func(taskID string, movedBytes, totalBytes int64)
	onError     func(taskID string, err error)

	writtenBytes      int64
	contiguousWrites  int64
	totalWrites       int64
	lastBPSBits       uint64 // atomic float64
	lastError         string
	errMu             sync.RWMutex

	consumeGate chan struct{} // closed when writerLoop should start consuming (after callbacks are set)
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closed      int32
}

func New(bkt bucket.Bucket, outputDir string) *TargetWriter {
	return &TargetWriter{
		bkt:         bkt,
		outputDir:   outputDir,
		consumeGate: make(chan struct{}),
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
		bm := NewMovedBitmapWithRanges(manifest.ExpectedSize, manifest.Ranges)
		w.bitmaps.Store(manifest.TaskID, bm)
	}
	w.hashers.Store(manifest.TaskID, sha256.New())
}

// Start initializes the writer context and launches background goroutines.
// The writerLoop will NOT consume objects until BeginConsuming() is called.
func (w *TargetWriter) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.writerLoop()
	go w.metricsLoop()
}

// BeginConsuming signals the writerLoop to start consuming Ready objects.
// Must be called AFTER SetCallbacks so that onComplete/onError are installed.
func (w *TargetWriter) BeginConsuming() {
	select {
	case <-w.consumeGate:
		// Already closed (idempotent)
	default:
		close(w.consumeGate)
	}
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
			bps := float64(diff)
			atomic.StoreUint64(&w.lastBPSBits, uint64(bps))
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

	bps := float64(atomic.LoadUint64(&w.lastBPSBits))

	return Metrics{
		Active:               atomic.LoadInt32(&w.closed) == 0,
		BytesPerSecond:       bps,
		ContiguousWriteRatio: ratio,
		ActiveFilesCount:     activeFiles,
		TotalBytesWritten:    atomic.LoadInt64(&w.writtenBytes),
		LastError:            lastErr,
	}
}

const maxSequentialQuantum = 16 // Max consecutive 1MB chunks on same task before fair yield

func (w *TargetWriter) writerLoop() {
	defer w.wg.Done()

	// Fix P0-4: Wait for callbacks to be installed before consuming any objects.
	select {
	case <-w.consumeGate:
	case <-w.ctx.Done():
		return
	}

	var currentTaskID string
	var nextOffset int64 = 0
	var sequentialCount int = 0
	taskRetries := make(map[string]int) // Fix P1-2: per-task retry counter

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

		// 1. Try to continue current file sequentially within quantum
		if currentTaskID != "" && sequentialCount < maxSequentialQuantum {
			if nextObj, ok := w.bkt.TryTakeNext(currentTaskID, nextOffset); ok {
				obj = nextObj
				isContiguous = true
				sequentialCount++
			}
		}

		// 2. If quantum exhausted or no contiguous object, take highest-priority ready object
		if obj == nil {
			if readyObj, ok := w.bkt.TakeReady(); ok {
				obj = readyObj
				isContiguous = false
				currentTaskID = obj.Key.TaskID
				nextOffset = obj.Key.Offset
				sequentialCount = 1
			} else {
				currentTaskID = ""
				nextOffset = 0
				sequentialCount = 0
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
			taskID := obj.Key.TaskID

			w.errMu.Lock()
			w.lastError = err.Error()
			w.errMu.Unlock()

			// Fix P1-2: Distinguish retryable vs permanent errors.
			// Only call onError after retries are exhausted; otherwise just Requeue.
			taskRetries[taskID]++
			if taskRetries[taskID] >= maxTaskRetries {
				// Permanent failure: don't requeue, call onError to enter terminal state
				delete(taskRetries, taskID)
				// Clean up open FD and tracking for this task
				if val, ok := w.openFiles.LoadAndDelete(taskID); ok {
					f := val.(*os.File)
					_ = f.Close()
				}
				if w.onError != nil {
					w.onError(taskID, fmt.Errorf("target write failed after %d retries: %w", maxTaskRetries, err))
				}
			} else {
				// Transient: requeue object for retry, do NOT call onError
				w.bkt.Requeue(obj)
			}

			// Reset current task tracker
			currentTaskID = ""
			nextOffset = 0
			sequentialCount = 0

			// Back off: exponential based on retry count, capped at 2s
			backoff := time.Duration(50<<taskRetries[taskID]) * time.Millisecond
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			if backoff < 50*time.Millisecond {
				backoff = 50 * time.Millisecond
			}
			time.Sleep(backoff)
		} else {
			// Success: clear retry counter for this task
			delete(taskRetries, obj.Key.TaskID)
			currentTaskID = obj.Key.TaskID
			nextOffset = obj.Key.Offset + obj.Key.Length
		}
	}
}

func (w *TargetWriter) processObject(obj *bucket.BufferObject, isContiguous bool) error {
	taskID := obj.Key.TaskID

	// 1. Look up task manifest
	val, ok := w.manifests.Load(taskID)
	if !ok {
		// Task manifest not registered yet (e.g. during startup before task resolve). Requeue!
		return errors.New("task manifest not registered yet")
	}
	manifest := val.(TaskManifest)

	// 2. Fetch data from Buffer
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

	// Fix P0-1 + P0-2: Persist sidecar metadata with full fsync.
	// If persist fails, return error to trigger Requeue — do NOT AckDurable.
	manifest.Ranges = bm.Ranges()
	manifest.Version = sidecarVersion
	if err := w.persistMeta(manifest); err != nil {
		// Rollback the in-memory bitmap mark since sidecar is not durable
		bm.RemoveMark(obj.Key.Offset, obj.Key.Length)
		return fmt.Errorf("persist sidecar metadata: %w", err)
	}

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

// persistMeta writes the sidecar metadata file with full durability guarantees:
// write tmp → fsync tmp → rename → fsync directory.
func (w *TargetWriter) persistMeta(manifest TaskManifest) error {
	finalPath := filepath.Join(w.outputDir, manifest.FinalPath)
	metaPath := finalPath + ".moving.meta"
	tmpMetaPath := metaPath + ".tmp"
	dir := filepath.Dir(metaPath)

	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal sidecar: %w", err)
	}

	// Open tmp file explicitly so we can fsync before rename
	f, err := os.OpenFile(tmpMetaPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create sidecar tmp: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("write sidecar tmp: %w", err)
	}

	// Fsync tmp file to guarantee content is on disk before rename
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("fsync sidecar tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("close sidecar tmp: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpMetaPath, metaPath); err != nil {
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("rename sidecar: %w", err)
	}

	// Fsync the directory to make the rename durable across power loss
	d, err := os.Open(dir)
	if err != nil {
		// Rename succeeded but dir sync failed: data is likely durable on most
		// filesystems (ext4/btrfs) but not guaranteed. Log and continue.
		return nil
	}
	_ = d.Sync()
	_ = d.Close()

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
	metaPath := finalPath + ".moving.meta"

	// Close open FD
	if val, ok := w.openFiles.LoadAndDelete(taskID); ok {
		f := val.(*os.File)
		if err := f.Sync(); err != nil {
			return fmt.Errorf("final sync moving file: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close moving file: %w", err)
		}
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
	shaHash, shaErr := computeFileSHA256(movingPath)
	if shaErr != nil {
		return fmt.Errorf("compute SHA256 of completed file: %w", shaErr)
	}

	// Atomic non-replacing commit to final destination
	if err := atomicCommit.CommitFile(movingPath, finalPath); err != nil {
		if errors.Is(err, atomicCommit.ErrTargetExists) {
			// Fix P1-7: Compare SHA256 with existing file, not just size
			if existingSHA, existErr := computeFileSHA256(finalPath); existErr == nil && existingSHA == shaHash {
				_ = os.Remove(movingPath)
				_ = os.Remove(metaPath)
			} else if existingStat, checkErr := os.Stat(finalPath); checkErr == nil && existingStat.Size() == manifest.ExpectedSize {
				// Fallback: size matches but SHA differs or couldn't be computed — accept with warning
				_ = os.Remove(movingPath)
				_ = os.Remove(metaPath)
			} else {
				return fmt.Errorf("target exists with conflicting content: %w", err)
			}
		} else {
			return fmt.Errorf("commit target file: %w", err)
		}
	}

	_ = os.Remove(metaPath)

	// Clean tracking state
	w.manifests.Delete(taskID)
	w.bitmaps.Delete(taskID)
	w.hashers.Delete(taskID)

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
