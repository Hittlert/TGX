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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hittlert/TGX/core/bucket"
	atomicCommit "github.com/Hittlert/TGX/pkg/sbe/atomic"
)

const SidecarVersion = 1

// Permanent finalize errors — these will never succeed on retry.
var (
	// ErrContentConflict indicates target exists with different SHA256.
	ErrContentConflict = errors.New("target content conflict")
	// ErrSizeMismatch indicates .moving file size does not match expected.
	ErrSizeMismatch = errors.New("moving file size mismatch")
)

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
	Active               bool    `json:"active"`
	BytesPerSecond       float64 `json:"bytes_per_second"`
	ContiguousWriteRatio float64 `json:"contiguous_write_ratio"`
	ActiveFilesCount     int     `json:"active_files_count"`
	TotalBytesWritten    int64   `json:"total_bytes_written"`
	LastError            string  `json:"last_error"`
}

// processPhase represents the outcome of processObject, determined by which
// durability boundary the operation crossed before encountering an error.
//
// The critical invariant: once AckDurable succeeds, the source buffer object
// no longer exists and must NEVER be Requeued.
type processPhase int

const (
	phaseDurableOK         processPhase = iota // Object durable, source deleted, all good
	phaseObjectRetryable                       // Error before AckDurable: source exists, safe to Requeue
	phaseFinalizeRetryable                     // Error after AckDurable (in finalize): must not Requeue source
	phaseStaleDiscarded                        // Object from old generation or completed task: silently consumed
)

type processResult struct {
	phase processPhase
	err   error
}

// AttemptWriteState tracks all state for a single active or completed task attempt in TargetWriter.
type AttemptWriteState struct {
	manifest        TaskManifest
	bitmap          *MovedBitmap
	file            *os.File
	pendingFinalize bool
	pendingNext     time.Time
	finalized       bool
	finalGen        string
}

type TargetWriter struct {
	bkt       bucket.Bucket
	outputDir string

	stateMu sync.RWMutex
	tasks   map[string]*AttemptWriteState // taskID → AttemptWriteState

	onComplete func(taskID, gen, finalPath string, shaHash string)
	onProgress func(taskID string, movedBytes, totalBytes int64)
	onError    func(taskID, gen string, err error)

	writtenBytes     int64
	contiguousWrites int64
	totalWrites      int64
	lastBPSBits      uint64 // atomic float64
	lastError        string
	errMu            sync.RWMutex

	consumeOnce sync.Once
	consumeGate chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closed      int32
}

func New(bkt bucket.Bucket, outputDir string) *TargetWriter {
	return &TargetWriter{
		bkt:         bkt,
		outputDir:   outputDir,
		tasks:       make(map[string]*AttemptWriteState),
		consumeGate: make(chan struct{}),
	}
}

func (w *TargetWriter) SetCallbacks(
	onComplete func(taskID, gen, finalPath string, shaHash string),
	onProgress func(taskID string, movedBytes, totalBytes int64),
	onError func(taskID, gen string, err error),
) {
	w.onComplete = onComplete
	w.onProgress = onProgress
	w.onError = onError
}

// TaskBitmap returns a snapshot of the bitmap for a task, if registered.
func (w *TargetWriter) TaskBitmap(taskID string) (*MovedBitmap, bool) {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	state, ok := w.tasks[taskID]
	if !ok || state.bitmap == nil {
		return nil, false
	}
	return state.bitmap, true
}

// TaskCompleted returns the finalized generation if completed.
func (w *TargetWriter) TaskCompleted(taskID string) (string, bool) {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	state, ok := w.tasks[taskID]
	if !ok || !state.finalized {
		return "", false
	}
	return state.finalGen, true
}

// MarkTaskCompleted marks a task as finalized so any leftover buffer objects
// for this task can be safely acknowledged and discarded without infinite requeue.
func (w *TargetWriter) MarkTaskCompleted(taskID, gen string) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if state, ok := w.tasks[taskID]; ok {
		state.finalized = true
		state.finalGen = gen
		state.pendingFinalize = false
		if state.file != nil {
			_ = state.file.Sync()
			_ = state.file.Close()
			state.file = nil
		}
	} else {
		w.tasks[taskID] = &AttemptWriteState{
			manifest:  TaskManifest{TaskID: taskID, Gen: gen},
			finalized: true,
			finalGen:  gen,
		}
	}
}

// RegisterTask registers or re-registers a task manifest for target writing.
//
// Generation isolation: if a manifest already exists with a different generation,
// all prior state (bitmap, FD, pending finalize) is cleared. A new generation
// must not inherit durable ranges or file handles from a previous attempt.
//
// isOlderGeneration checks if newGen is strictly older than currentGen.
func isOlderGeneration(newGen, currentGen string) bool {
	if currentGen == "" || newGen == currentGen {
		return false
	}
	if currentGen != "1" && newGen == "1" {
		return true // "1" is older than any retry generation
	}
	if strings.HasPrefix(currentGen, "retry_") && strings.HasPrefix(newGen, "retry_") {
		var curNano, newNano int64
		if _, err := fmt.Sscanf(currentGen, "retry_%d", &curNano); err == nil {
			if _, err2 := fmt.Sscanf(newGen, "retry_%d", &newNano); err2 == nil {
				return newNano < curNano
			}
		}
	}
	return false
}

// RegisterTask registers or re-registers a task manifest for target writing.
//
// Generation isolation: if a manifest already exists with a different generation,
// all prior state (bitmap, FD, pending finalize) is cleared. A new generation
// must not inherit durable ranges or file handles from a previous attempt.
// If an older generation is passed (e.g. from a slow resolver), it is safely rejected.
//
// Complete bitmap recovery: if the registered bitmap is already complete (all
// ranges cover [0, expectedSize)), the task is queued for immediate finalize.
// This handles the crash-after-last-Ack-before-finalize recovery case where
// no more Ready objects will arrive to trigger finalize.
func (w *TargetWriter) RegisterTask(manifest TaskManifest) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	// 1. If task was already completed and finalized:
	if existing, ok := w.tasks[manifest.TaskID]; ok {
		if existing.finalized {
			if isOlderGeneration(manifest.Gen, existing.finalGen) || manifest.Gen == existing.finalGen {
				// Stale or duplicate attempt trying to re-register an already finalized task: reject!
				return
			}
		} else if existing.manifest.Gen != manifest.Gen {
			if isOlderGeneration(manifest.Gen, existing.manifest.Gen) {
				// Reject rollback to older generation by slow/stale resolver
				return
			}
			// New generation: clean up old file handle
			if existing.file != nil {
				_ = existing.file.Close()
			}
		} else {
			// Same generation re-register:
			// If FinalPath or ExpectedSize changed, reject inconsistent identity for same generation
			if existing.manifest.FinalPath != manifest.FinalPath || existing.manifest.ExpectedSize != manifest.ExpectedSize {
				return
			}
			// Same identity: update manifest but do NOT rebuild bitmap.
			// This preserves durable ranges recovered from sidecar or written since first register.
			existing.manifest = manifest
			return
		}
	}

	bm := NewMovedBitmapWithRanges(manifest.ExpectedSize, manifest.Ranges)
	w.tasks[manifest.TaskID] = &AttemptWriteState{
		manifest:        manifest,
		bitmap:          bm,
		pendingFinalize: bm.IsComplete(),
	}
}

// Start initializes context and launches background goroutines.
// The writerLoop blocks on consumeGate until BeginConsuming is called.
func (w *TargetWriter) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.writerLoop()
	go w.metricsLoop()
}

// BeginConsuming unblocks the writerLoop. Must be called after SetCallbacks
// to guarantee callbacks are installed before any object is processed.
// Concurrent-safe and idempotent (uses sync.Once internally).
func (w *TargetWriter) BeginConsuming() {
	w.consumeOnce.Do(func() { close(w.consumeGate) })
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
			atomic.StoreUint64(&w.lastBPSBits, uint64(float64(diff)))
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
	w.stateMu.RLock()
	for _, state := range w.tasks {
		if !state.finalized && state.file != nil {
			activeFiles++
		}
	}
	w.stateMu.RUnlock()

	return Metrics{
		Active:               atomic.LoadInt32(&w.closed) == 0,
		BytesPerSecond:       float64(atomic.LoadUint64(&w.lastBPSBits)),
		ContiguousWriteRatio: ratio,
		ActiveFilesCount:     activeFiles,
		TotalBytesWritten:    atomic.LoadInt64(&w.writtenBytes),
		LastError:            lastErr,
	}
}

func (w *TargetWriter) setLastError(err error) {
	if err == nil {
		return
	}
	w.errMu.Lock()
	w.lastError = err.Error()
	w.errMu.Unlock()
}

const maxSequentialQuantum = 16

func (w *TargetWriter) writerLoop() {
	defer w.wg.Done()

	// Block until callbacks are installed
	select {
	case <-w.consumeGate:
	case <-w.ctx.Done():
		return
	}

	var currentTaskID string
	var nextOffset int64
	var sequentialCount int

	for {
		if atomic.LoadInt32(&w.closed) == 1 {
			return
		}
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		// Drain any tasks with complete bitmaps awaiting finalize (recovery case)
		w.drainPendingFinalizes()

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

		// 2. Take highest-priority ready object
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

		// Phase state machine: action depends on which durability boundary was crossed
		result := w.processObject(obj, isContiguous)
		switch result.phase {
		case phaseDurableOK, phaseStaleDiscarded:
			if result.phase == phaseDurableOK {
				currentTaskID = obj.Key.TaskID
				nextOffset = obj.Key.Offset + obj.Key.Length
			}

		case phaseObjectRetryable:
			// Error before AckDurable: source object still exists → safe to Requeue
			w.bkt.Requeue(obj)
			w.setLastError(result.err)
			currentTaskID = ""
			nextOffset = 0
			sequentialCount = 0
			time.Sleep(100 * time.Millisecond)

		case phaseFinalizeRetryable:
			// Error after AckDurable (during finalize): source is deleted.
			// Enqueue task for finalize retry. NEVER Requeue the source object.
			taskID := obj.Key.TaskID
			w.stateMu.Lock()
			if state, ok := w.tasks[taskID]; ok && state.manifest.Gen == obj.Key.Gen {
				state.pendingFinalize = true
			}
			w.stateMu.Unlock()
			w.setLastError(result.err)
			currentTaskID = ""
			nextOffset = 0
			sequentialCount = 0
		}
	}
}

// drainPendingFinalizes attempts to finalize all tasks that have complete
// bitmaps but haven't been finalized yet.
// Permanent errors (content conflict, size mismatch) trigger onError and removal.
// Transient errors use per-task backoff to avoid tight-loop SHA recomputation.
func (w *TargetWriter) drainPendingFinalizes() {
	w.stateMu.RLock()
	var toFinalize []TaskManifest
	now := time.Now()
	for _, state := range w.tasks {
		if !state.finalized && state.pendingFinalize && now.After(state.pendingNext) {
			toFinalize = append(toFinalize, state.manifest)
		}
	}
	w.stateMu.RUnlock()

	for _, manifest := range toFinalize {
		taskID := manifest.TaskID
		if err := w.finalizeTask(manifest); err != nil {
			w.setLastError(err)
			if isFinalizePermError(err) {
				// Permanent: content conflict or irrecoverable. Enter terminal state.
				w.stateMu.Lock()
				if curState, curOk := w.tasks[taskID]; curOk && curState.manifest.Gen == manifest.Gen {
					delete(w.tasks, taskID)
				}
				w.stateMu.Unlock()
				if w.onError != nil {
					w.onError(taskID, manifest.Gen, fmt.Errorf("permanent finalize error: %w", err))
				}
			} else {
				// Transient: backoff 5s before next retry to avoid tight-loop SHA
				w.stateMu.Lock()
				if curState, curOk := w.tasks[taskID]; curOk && curState.manifest.Gen == manifest.Gen {
					curState.pendingNext = now.Add(5 * time.Second)
				}
				w.stateMu.Unlock()
			}
		}
	}
}

// isFinalizePermError returns true for errors that will never succeed on retry.
// Uses typed sentinel errors instead of string matching for reliable classification.
func isFinalizePermError(err error) bool {
	if errors.Is(err, ErrContentConflict) {
		return true
	}
	if errors.Is(err, ErrSizeMismatch) {
		return true
	}
	if errors.Is(err, os.ErrPermission) {
		return true
	}
	return false
}

// processObject writes a single buffer object to the target .moving file.
//
// The function is organized around the AckDurable boundary:
//   - BEFORE AckDurable: errors are ObjectRetryable (source can be Requeued)
//   - AFTER AckDurable: errors are FinalizeRetryable (source is gone, must not Requeue)
func (w *TargetWriter) processObject(obj *bucket.BufferObject, isContiguous bool) processResult {
	taskID := obj.Key.TaskID

	// Phase 1: Look up task attempt state
	w.stateMu.Lock()
	state, ok := w.tasks[taskID]
	if !ok {
		w.stateMu.Unlock()
		return processResult{phaseObjectRetryable, errors.New("task manifest not registered yet")}
	}

	if state.finalized {
		w.stateMu.Unlock()
		// If task was already completed and finalized, safely consume and delete leftover object
		if err := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); err != nil {
			return processResult{phaseObjectRetryable, fmt.Errorf("ack completed task leftover object: %w", err)}
		}
		return processResult{phase: phaseStaleDiscarded}
	}

	// Generation guard: reject objects from stale attempts.
	if obj.Key.Gen != state.manifest.Gen {
		w.stateMu.Unlock()
		if err := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); err != nil {
			return processResult{phaseObjectRetryable, fmt.Errorf("ack stale object: %w", err)}
		}
		return processResult{phase: phaseStaleDiscarded}
	}

	manifest := state.manifest
	bm := state.bitmap
	f, err := w.getOrOpenFileLocked(state)
	w.stateMu.Unlock()

	if err != nil {
		return processResult{phaseObjectRetryable, fmt.Errorf("open target moving file: %w", err)}
	}

	// Phase 2: Fetch data from buffer
	var data []byte
	if len(obj.Data) > 0 {
		data = obj.Data
	} else {
		readData, err := w.bkt.ReadObject(obj.Key)
		if err != nil {
			return processResult{phaseObjectRetryable, fmt.Errorf("read buffer object %s: %w", obj.Key, err)}
		}
		data = readData
	}

	// Phase 3: Write data to target .moving file
	nw, err := f.WriteAt(data, obj.Key.Offset)
	if err != nil {
		return processResult{phaseObjectRetryable, fmt.Errorf("writeAt target moving file: %w", err)}
	}
	if int64(nw) != obj.Key.Length {
		return processResult{phaseObjectRetryable, fmt.Errorf("short write to target: %d of %d bytes", nw, obj.Key.Length)}
	}

	if err := f.Sync(); err != nil {
		return processResult{phaseObjectRetryable, fmt.Errorf("sync target file: %w", err)}
	}

	// Phase 4: Bitmap + sidecar durable commit (transactional via snapshot/restore)
	snapshot := bm.Snapshot()
	bm.AddMark(obj.Key.Offset, obj.Key.Length)

	manifest.Ranges = bm.Ranges()
	manifest.Version = SidecarVersion
	if err := w.persistMeta(manifest); err != nil {
		// Sidecar not durable: restore bitmap to match the persisted state
		bm.Restore(snapshot)
		return processResult{phaseObjectRetryable, fmt.Errorf("persist sidecar metadata: %w", err)}
	}

	// ──── DURABILITY BOUNDARY ────
	// After this point, the source buffer object is deleted.
	// Errors below must NEVER result in Requeue of the source object.

	if err := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); err != nil {
		// AckDurable failed: source might still exist. Safe to retry via Requeue.
		return processResult{phaseObjectRetryable, fmt.Errorf("ack durable object: %w", err)}
	}

	// Track write metrics
	atomic.AddInt64(&w.writtenBytes, obj.Key.Length)
	atomic.AddInt64(&w.totalWrites, 1)
	if isContiguous {
		atomic.AddInt64(&w.contiguousWrites, 1)
	}

	if w.onProgress != nil {
		w.onProgress(taskID, bm.DurableBytes(), manifest.ExpectedSize)
	}

	// Phase 5: Finalize if all ranges are complete
	if bm.IsComplete() {
		if err := w.finalizeTask(manifest); err != nil {
			return processResult{phaseFinalizeRetryable, err}
		}
	}

	return processResult{phase: phaseDurableOK}
}

// persistMeta writes the sidecar metadata file with full durability guarantee:
// write tmp → fsync tmp → close → rename → fsync directory.
//
// The contract: if this function returns nil, the sidecar content survives
// power loss. All errors (including directory fsync) are returned.
func (w *TargetWriter) persistMeta(manifest TaskManifest) error {
	finalPath := filepath.Join(w.outputDir, manifest.FinalPath)
	metaPath := finalPath + ".moving.meta"
	tmpMetaPath := metaPath + ".tmp"
	dir := filepath.Dir(metaPath)

	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal sidecar: %w", err)
	}

	f, err := os.OpenFile(tmpMetaPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create sidecar tmp: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("write sidecar tmp: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("fsync sidecar tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("close sidecar tmp: %w", err)
	}

	if err := os.Rename(tmpMetaPath, metaPath); err != nil {
		_ = os.Remove(tmpMetaPath)
		return fmt.Errorf("rename sidecar: %w", err)
	}

	// Directory fsync: ensures the rename is durable across power loss.
	// This is required — returning nil without dir fsync violates the durability contract.
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory for fsync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("fsync directory: %w", err)
	}
	_ = d.Close()

	return nil
}

func (w *TargetWriter) getOrOpenFileLocked(state *AttemptWriteState) (*os.File, error) {
	if state.file != nil {
		return state.file, nil
	}

	finalPath := filepath.Join(w.outputDir, state.manifest.FinalPath)
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir %s: %w", dir, err)
	}

	movingPath := finalPath + ".moving"
	f, err := os.OpenFile(movingPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	state.file = f
	return f, nil
}

func (w *TargetWriter) finalizeTask(manifest TaskManifest) error {
	taskID := manifest.TaskID
	finalPath := filepath.Join(w.outputDir, manifest.FinalPath)
	movingPath := finalPath + ".moving"
	metaPath := finalPath + ".moving.meta"

	// Close open FD under lock — must Close even if Sync fails to avoid FD leak
	w.stateMu.Lock()
	if state, ok := w.tasks[taskID]; ok && state.file != nil {
		f := state.file
		state.file = nil
		_ = f.Sync()
		_ = f.Close()
	}
	w.stateMu.Unlock()

	// Verify size
	stat, err := os.Stat(movingPath)
	if err != nil {
		return fmt.Errorf("stat completed moving file: %w", err)
	}
	if manifest.ExpectedSize > 0 && stat.Size() != manifest.ExpectedSize {
		return fmt.Errorf("final size %d does not match expected %d: %w", stat.Size(), manifest.ExpectedSize, ErrSizeMismatch)
	}

	// Set modification time
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
			// Content identity check: accept only if SHA256 matches exactly.
			// No size-only fallback — different content must not be silently accepted.
			existingSHA, existErr := computeFileSHA256(finalPath)
			if existErr == nil && existingSHA == shaHash {
				_ = os.Remove(movingPath)
				_ = os.Remove(metaPath)
			} else if existErr != nil {
				// Transient IO error reading existing target file
				return fmt.Errorf("read existing target file for sha verification: %w", existErr)
			} else {
				return fmt.Errorf("existing_sha=%s, new_sha=%s: %w",
					existingSHA, shaHash, ErrContentConflict)
			}
		} else {
			return fmt.Errorf("commit target file: %w", err)
		}
	}

	_ = os.Remove(metaPath)

	// Clean tracking state — atomically mark finalized if generation still matches
	w.stateMu.Lock()
	if state, ok := w.tasks[taskID]; ok && state.manifest.Gen == manifest.Gen {
		state.finalized = true
		state.finalGen = manifest.Gen
		state.pendingFinalize = false
		if state.file != nil {
			_ = state.file.Close()
			state.file = nil
		}
	}
	w.stateMu.Unlock()

	if w.onComplete != nil {
		w.onComplete(taskID, manifest.Gen, manifest.FinalPath, shaHash)
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

	w.stateMu.Lock()
	for _, state := range w.tasks {
		if state.file != nil {
			_ = state.file.Sync()
			_ = state.file.Close()
			state.file = nil
		}
	}
	w.stateMu.Unlock()
	return nil
}
