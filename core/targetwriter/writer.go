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
	"syscall"
	"time"

	"github.com/Hittlert/TGX/core/bucket"
	atomicCommit "github.com/Hittlert/TGX/pkg/sbe/atomic"
)

var (
	ErrTaskNotRegistered = errors.New("task manifest not registered yet")
	ErrStaleObject       = errors.New("stale object from previous attempt")
	ErrSizeMismatch      = errors.New("final size mismatch")
	ErrContentConflict   = errors.New("target exists with different content")
)

type TaskManifest struct {
	TaskID       string  `json:"task_id"`
	FinalPath    string  `json:"final_path"`
	ExpectedSize int64   `json:"expected_size"`
	Date         int64   `json:"date"`
	Gen          string  `json:"gen"`
	Ranges       []Range `json:"ranges"`
	Version      int     `json:"version"`
}

const SidecarVersion = 1

type Metrics struct {
	Active               bool    `json:"active"`
	BytesPerSecond       float64 `json:"bytes_per_second"`
	ContiguousWriteRatio float64 `json:"contiguous_write_ratio"`
	ActiveFilesCount     int     `json:"active_files_count"`
	TotalBytesWritten    int64   `json:"total_bytes_written"`
	LastError            string  `json:"last_error,omitempty"`
}

type processPhase int

const (
	phaseDurableOK         processPhase = iota // Write + sidecar + AckDurable succeeded
	phaseObjectRetryable                       // Error before AckDurable / unactivated newer gen: source exists, safe to Requeue
	phaseFinalizeRetryable                     // Error after AckDurable (in finalize): must not Requeue source
	phaseStaleDiscarded                        // Object from old generation or completed task: silently consumed
)

type processResult struct {
	phase processPhase
	err   error
}

type AttemptPhase int

const (
	PhaseActive AttemptPhase = iota
	PhaseClosing
	PhaseFinalizing
	PhaseFinalized
	PhaseFailed
)

// AttemptWriteState tracks all state for a single active or completed task attempt in TargetWriter.
type AttemptWriteState struct {
	mu              sync.Mutex
	drainedCh       chan struct{}
	manifest        TaskManifest
	bitmap          *MovedBitmap
	file            *os.File
	dataSynced      bool
	pendingFinalize bool
	pendingNext     time.Time
	finalized       bool
	finalGen        string
	finalSHA        string
	finalPath       string
	activeOps       int
	phase           AttemptPhase
	failedOnce      bool
}

func newAttemptWriteState(manifest TaskManifest, bm *MovedBitmap) *AttemptWriteState {
	s := &AttemptWriteState{
		manifest:        manifest,
		bitmap:          bm,
		pendingFinalize: bm != nil && bm.IsComplete(),
		phase:           PhaseActive,
		drainedCh:       make(chan struct{}),
	}
	close(s.drainedCh) // 0 active ops, initially drained
	return s
}

func (s *AttemptWriteState) AcquireOp() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != PhaseActive {
		return false
	}
	if s.activeOps == 0 {
		s.drainedCh = make(chan struct{})
	}
	s.activeOps++
	return true
}

func (s *AttemptWriteState) AcquireFinalizeOp(expectedGen string) (fToClose *os.File, dataSynced bool, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized || s.manifest.Gen != expectedGen || s.phase != PhaseActive {
		return nil, false, false
	}
	s.phase = PhaseFinalizing
	if s.activeOps == 0 {
		s.drainedCh = make(chan struct{})
	}
	s.activeOps++
	if s.file != nil {
		fToClose = s.file
		s.file = nil
	}
	dataSynced = s.dataSynced
	return fToClose, dataSynced, true
}

func (s *AttemptWriteState) ReleaseOp() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeOps--
	if s.activeOps <= 0 {
		s.activeOps = 0
		select {
		case <-s.drainedCh:
		default:
			close(s.drainedCh)
		}
	}
}

func (s *AttemptWriteState) WaitForDraining(ctx context.Context) error {
	s.mu.Lock()
	if s.activeOps <= 0 {
		s.mu.Unlock()
		return nil
	}
	ch := s.drainedCh
	s.mu.Unlock()

	if ctx == nil {
		<-ch
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

type RegisterResult int

const (
	RegisterAccepted RegisterResult = iota
	RegisterStale
	RegisterAlreadyFinalized
	RegisterConflict
)

func (r RegisterResult) String() string {
	switch r {
	case RegisterAccepted:
		return "ACCEPTED"
	case RegisterStale:
		return "STALE"
	case RegisterAlreadyFinalized:
		return "ALREADY_FINALIZED"
	case RegisterConflict:
		return "CONFLICT"
	default:
		return "UNKNOWN"
	}
}

const maxTombstoneEntries = 1000

type TargetWriter struct {
	bkt       bucket.Bucket
	outputDir string

	stateMu        sync.RWMutex
	tasks          map[string]*AttemptWriteState // taskID → AttemptWriteState
	tombstoneOrder []string

	onComplete func(taskID, gen, finalPath string, shaHash string)
	onProgress func(taskID string, movedBytes, totalBytes int64)
	onError    func(taskID, gen string, err error)

	writtenBytes     int64
	contiguousWrites int64
	totalWrites      int64
	openFilesCount   int64
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

// TaskBitmap returns a snapshot copy of the bitmap for a task, if registered.
func (w *TargetWriter) TaskBitmap(taskID string) (*MovedBitmap, bool) {
	w.stateMu.RLock()
	state, ok := w.tasks[taskID]
	w.stateMu.RUnlock()
	if !ok {
		return nil, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.bitmap == nil {
		return nil, false
	}
	return state.bitmap.Clone(), true
}

// TaskCompleted returns the finalized generation if completed.
func (w *TargetWriter) TaskCompleted(taskID string) (string, bool) {
	w.stateMu.RLock()
	state, ok := w.tasks[taskID]
	w.stateMu.RUnlock()
	if !ok {
		return "", false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.finalized {
		return "", false
	}
	return state.finalGen, true
}

// TaskFinalInfo returns the finalized generation, final relative path, and SHA256 if completed.
func (w *TargetWriter) TaskFinalInfo(taskID string) (gen, finalPath, sha string, ok bool) {
	w.stateMu.RLock()
	state, exists := w.tasks[taskID]
	w.stateMu.RUnlock()
	if !exists {
		return "", "", "", false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.finalized {
		return "", "", "", false
	}
	return state.finalGen, state.finalPath, state.finalSHA, true
}

func (w *TargetWriter) recordTombstoneLocked(taskID string) {
	// Deduplicate taskID in tombstoneOrder so each taskID appears at most once
	filtered := make([]string, 0, len(w.tombstoneOrder))
	for _, id := range w.tombstoneOrder {
		if id != taskID {
			filtered = append(filtered, id)
		}
	}
	w.tombstoneOrder = append(filtered, taskID)
	for len(w.tombstoneOrder) > maxTombstoneEntries {
		oldest := w.tombstoneOrder[0]
		w.tombstoneOrder = w.tombstoneOrder[1:]
		if state, ok := w.tasks[oldest]; ok && state.finalized {
			delete(w.tasks, oldest)
		}
	}
}

// MarkTaskCompleted marks a task as finalized so any leftover buffer objects
// for this task can be safely acknowledged and discarded without infinite requeue.
func (w *TargetWriter) MarkTaskCompleted(taskID, gen string) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if state, ok := w.tasks[taskID]; ok {
		state.mu.Lock()
		state.finalized = true
		state.finalGen = gen
		state.pendingFinalize = false
		state.phase = PhaseFinalized
		state.bitmap = nil
		state.manifest.Ranges = nil
		if state.file != nil {
			_ = state.file.Sync()
			_ = state.file.Close()
			state.file = nil
			atomic.AddInt64(&w.openFilesCount, -1)
		}
		state.mu.Unlock()
	} else {
		newState := newAttemptWriteState(TaskManifest{TaskID: taskID, Gen: gen}, nil)
		newState.finalized = true
		newState.finalGen = gen
		newState.phase = PhaseFinalized
		w.tasks[taskID] = newState
	}
	w.recordTombstoneLocked(taskID)
}

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
func (w *TargetWriter) RegisterTask(manifest TaskManifest) RegisterResult {
	w.stateMu.Lock()

	existing, ok := w.tasks[manifest.TaskID]
	if ok {
		existing.mu.Lock()
		if existing.finalized {
			if isOlderGeneration(manifest.Gen, existing.finalGen) || manifest.Gen == existing.finalGen {
				existing.mu.Unlock()
				w.stateMu.Unlock()
				return RegisterAlreadyFinalized
			}
			// Newer generation after finalized: transition to new attempt
			existing.mu.Unlock()
		} else if existing.manifest.Gen != manifest.Gen {
			if isOlderGeneration(manifest.Gen, existing.manifest.Gen) {
				existing.mu.Unlock()
				w.stateMu.Unlock()
				return RegisterStale
			}

			// Cutover to newer generation:
			// 1. Mark existing attempt as PhaseClosing under existing.mu
			existing.phase = PhaseClosing
			existing.mu.Unlock()

			// 2. Unlock global stateMu so we NEVER block other tasks during draining or I/O!
			w.stateMu.Unlock()

			// 3. Wait for in-flight operations of the old attempt to drain
			if err := existing.WaitForDraining(w.ctx); err != nil {
				return RegisterConflict
			}

			// 4. Safely sync and close old file handle
			existing.mu.Lock()
			if existing.file != nil {
				_ = existing.file.Sync()
				_ = existing.file.Close()
				existing.file = nil
				atomic.AddInt64(&w.openFilesCount, -1)
			}
			existing.mu.Unlock()

			// 5. Re-acquire global stateMu to install new Attempt
			w.stateMu.Lock()
			current, stillThere := w.tasks[manifest.TaskID]
			if stillThere && current != existing {
				current.mu.Lock()
				if current.manifest.Gen == manifest.Gen {
					current.mu.Unlock()
					w.stateMu.Unlock()
					return RegisterAccepted
				}
				if isOlderGeneration(manifest.Gen, current.manifest.Gen) {
					current.mu.Unlock()
					w.stateMu.Unlock()
					return RegisterStale
				}
				current.mu.Unlock()
			}
		} else {
			// Same generation re-register:
			if existing.manifest.FinalPath != manifest.FinalPath || existing.manifest.ExpectedSize != manifest.ExpectedSize {
				existing.mu.Unlock()
				w.stateMu.Unlock()
				return RegisterConflict
			}
			if existing.phase == PhaseClosing {
				existing.phase = PhaseActive
				existing.pendingFinalize = existing.bitmap != nil && existing.bitmap.IsComplete()
			}
			existing.manifest = manifest
			existing.mu.Unlock()
			w.stateMu.Unlock()
			return RegisterAccepted
		}
	}

	bm := NewMovedBitmapWithRanges(manifest.ExpectedSize, manifest.Ranges)
	newState := newAttemptWriteState(manifest, bm)
	w.tasks[manifest.TaskID] = newState
	w.stateMu.Unlock()
	return RegisterAccepted
}

// Start initializes context and launches background goroutines.
func (w *TargetWriter) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.writerLoop()
	go w.metricsLoop()
}

// BeginConsuming unblocks the writerLoop. Must be called after SetCallbacks
// to guarantee callbacks are installed before any object is processed.
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

	return Metrics{
		Active:               atomic.LoadInt32(&w.closed) == 0,
		BytesPerSecond:       float64(atomic.LoadUint64(&w.lastBPSBits)),
		ContiguousWriteRatio: ratio,
		ActiveFilesCount:     int(atomic.LoadInt64(&w.openFilesCount)),
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
			// Error before AckDurable or unactivated newer gen: source object still exists → safe to Requeue
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
			w.stateMu.RLock()
			if state, ok := w.tasks[taskID]; ok {
				state.mu.Lock()
				if state.manifest.Gen == obj.Key.Gen {
					state.pendingFinalize = true
				}
				state.mu.Unlock()
			}
			w.stateMu.RUnlock()
			w.setLastError(result.err)
			currentTaskID = ""
			nextOffset = 0
			sequentialCount = 0
		}
	}
}

// drainPendingFinalizes attempts to finalize all tasks that have complete
// bitmaps but haven't been finalized yet.
func (w *TargetWriter) drainPendingFinalizes() {
	w.stateMu.RLock()
	var toFinalize []TaskManifest
	now := time.Now()
	for _, state := range w.tasks {
		state.mu.Lock()
		if !state.finalized && state.pendingFinalize && now.After(state.pendingNext) {
			toFinalize = append(toFinalize, state.manifest)
		}
		state.mu.Unlock()
	}
	w.stateMu.RUnlock()

	for _, manifest := range toFinalize {
		taskID := manifest.TaskID
		if err := w.finalizeTask(manifest); err != nil {
			w.setLastError(err)
			w.stateMu.RLock()
			state, ok := w.tasks[taskID]
			w.stateMu.RUnlock()

			if ok {
				state.mu.Lock()
				if state.manifest.Gen == manifest.Gen {
					if isFinalizePermError(err) {
						state.phase = PhaseClosing
						state.pendingFinalize = false
						if state.file != nil {
							_ = state.file.Close()
							state.file = nil
							atomic.AddInt64(&w.openFilesCount, -1)
						}
					} else {
						state.pendingNext = now.Add(5 * time.Second)
					}
				}
				state.mu.Unlock()
			}

			if isFinalizePermError(err) && w.onError != nil {
				w.onError(taskID, manifest.Gen, fmt.Errorf("permanent finalize error: %w", err))
			}
		}
	}
}

// isFinalizePermError returns true for errors that will never succeed on retry.
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
func (w *TargetWriter) processObject(obj *bucket.BufferObject, isContiguous bool) processResult {
	taskID := obj.Key.TaskID

	// Phase 1: Look up task attempt state
	w.stateMu.RLock()
	state, ok := w.tasks[taskID]
	w.stateMu.RUnlock()

	if !ok {
		// Task manifest not registered yet: requeue so source object is NOT lost in pending-delete
		return processResult{phase: phaseObjectRetryable, err: ErrTaskNotRegistered}
	}

	state.mu.Lock()
	if state.finalized {
		isMatchingOrOlder := (state.finalGen == obj.Key.Gen || isOlderGeneration(obj.Key.Gen, state.finalGen))
		state.mu.Unlock()
		if isMatchingOrOlder {
			// Leftover chunk from completed generation: Ack durable and discard
			if err := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); err != nil {
				return processResult{phaseObjectRetryable, fmt.Errorf("ack completed task leftover object: %w", err)}
			}
			return processResult{phase: phaseStaleDiscarded}
		} else {
			// Object from newer generation that is not yet registered: Requeue to preserve pending-delete conservation
			return processResult{phase: phaseObjectRetryable, err: fmt.Errorf("requeue newer gen %s for completed task %s", obj.Key.Gen, taskID)}
		}
	}

	if state.manifest.Gen != obj.Key.Gen {
		isOlder := isOlderGeneration(obj.Key.Gen, state.manifest.Gen)
		state.mu.Unlock()
		if isOlder {
			// Stale chunk from older generation: Ack durable and discard
			if err := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); err != nil {
				return processResult{phaseObjectRetryable, fmt.Errorf("ack stale object: %w", err)}
			}
			return processResult{phase: phaseStaleDiscarded}
		} else {
			// Object from newer generation before register cutover completes: Requeue
			return processResult{phase: phaseObjectRetryable, err: fmt.Errorf("requeue newer gen %s before cutover for task %s", obj.Key.Gen, taskID)}
		}
	}

	if state.phase == PhaseFailed {
		state.mu.Unlock()
		if ackErr := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); ackErr != nil {
			_ = w.bkt.DeleteObjects([]bucket.ObjectKey{obj.Key})
		}
		return processResult{phase: phaseStaleDiscarded, err: fmt.Errorf("attempt %s gen %s already in PhaseFailed", taskID, obj.Key.Gen)}
	}

	if state.phase != PhaseActive {
		state.mu.Unlock()
		return processResult{phase: phaseObjectRetryable, err: errors.New("attempt is closing or finalizing")}
	}

	manifest := state.manifest
	bm := state.bitmap
	state.mu.Unlock()

	// Grant operation lease using unified AcquireOp
	if !state.AcquireOp() {
		return processResult{phase: phaseObjectRetryable, err: errors.New("attempt lease acquire failed")}
	}

	f, err := w.getOrOpenFile(state)
	if err != nil {
		if isPermanentObjectError(err) {
			return w.failAttemptPermanent(state, obj.Key, manifest, fmt.Errorf("open target moving file: %w", err))
		}
		state.ReleaseOp()
		return processResult{phaseObjectRetryable, fmt.Errorf("open target moving file: %w", err)}
	}

	// Phase 2: Fetch data from buffer
	var data []byte
	if len(obj.Data) > 0 {
		data = obj.Data
	} else {
		readData, err := w.bkt.ReadObject(obj.Key)
		if err != nil {
			if isPermanentObjectError(err) {
				return w.failAttemptPermanent(state, obj.Key, manifest, fmt.Errorf("read buffer object %s: %w", obj.Key, err))
			}
			state.ReleaseOp()
			return processResult{phaseObjectRetryable, fmt.Errorf("read buffer object %s: %w", obj.Key, err)}
		}
		data = readData
	}

	// Phase 3: Write data to target .moving file
	nw, err := f.WriteAt(data, obj.Key.Offset)
	if err != nil {
		if isPermanentObjectError(err) {
			return w.failAttemptPermanent(state, obj.Key, manifest, fmt.Errorf("writeAt target moving file: %w", err))
		}
		state.ReleaseOp()
		return processResult{phaseObjectRetryable, fmt.Errorf("writeAt target moving file: %w", err)}
	}
	if int64(nw) != obj.Key.Length {
		state.ReleaseOp()
		return processResult{phaseObjectRetryable, fmt.Errorf("short write to target: %d of %d bytes", nw, obj.Key.Length)}
	}

	// Invalidate any prior dataSynced flag since new bytes were written (P1-9 fix)
	state.mu.Lock()
	state.dataSynced = false
	state.mu.Unlock()

	if err := f.Sync(); err != nil {
		state.ReleaseOp()
		return processResult{phaseObjectRetryable, fmt.Errorf("sync target file: %w", err)}
	}

	// Phase 4: Bitmap + sidecar durable commit (transactional via snapshot/restore)
	snapshot := bm.Snapshot()
	bm.AddMark(obj.Key.Offset, obj.Key.Length)

	manifest.Ranges = bm.Ranges()
	manifest.Version = SidecarVersion
	if err := w.persistMeta(manifest); err != nil {
		bm.Restore(snapshot)
		state.ReleaseOp()
		return processResult{phaseObjectRetryable, fmt.Errorf("persist sidecar metadata: %w", err)}
	}

	// ──── DURABILITY BOUNDARY ────
	if err := w.bkt.AckDurable([]bucket.ObjectKey{obj.Key}); err != nil {
		state.ReleaseOp()
		return processResult{phaseObjectRetryable, fmt.Errorf("ack durable object: %w", err)}
	}

	// Track write metrics
	atomic.AddInt64(&w.writtenBytes, obj.Key.Length)
	atomic.AddInt64(&w.totalWrites, 1)
	if isContiguous {
		atomic.AddInt64(&w.contiguousWrites, 1)
	}

	durableBytes := bm.DurableBytes()
	isComplete := bm.IsComplete()

	// RELEASE LEASE BEFORE CALLBACKS TO PREVENT RE-ENTRY DEADLOCKS (P1-7 fix)
	state.ReleaseOp()

	if w.onProgress != nil {
		w.onProgress(taskID, durableBytes, manifest.ExpectedSize)
	}

	// Phase 5: Finalize if all ranges are complete
	if isComplete {
		if err := w.finalizeTask(manifest); err != nil {
			if isFinalizePermError(err) {
				if w.onError != nil {
					w.onError(manifest.TaskID, manifest.Gen, err)
				}
				return processResult{phase: phaseStaleDiscarded, err: err}
			}
			return processResult{phaseFinalizeRetryable, err}
		}
	}

	return processResult{phase: phaseDurableOK}
}

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

func (w *TargetWriter) failAttemptPermanent(state *AttemptWriteState, key bucket.ObjectKey, manifest TaskManifest, err error) processResult {
	state.mu.Lock()
	shouldNotify := !state.failedOnce
	state.failedOnce = true
	state.phase = PhaseFailed
	if state.file != nil {
		_ = state.file.Sync()
		_ = state.file.Close()
		state.file = nil
		atomic.AddInt64(&w.openFilesCount, -1)
	}
	state.mu.Unlock()

	state.ReleaseOp()

	if ackErr := w.bkt.AckDurable([]bucket.ObjectKey{key}); ackErr != nil {
		_ = w.bkt.DeleteObjects([]bucket.ObjectKey{key})
	}
	if shouldNotify && w.onError != nil {
		w.onError(manifest.TaskID, manifest.Gen, err)
	}
	return processResult{phase: phaseStaleDiscarded, err: fmt.Errorf("permanent attempt failure: %w", err)}
}

func (w *TargetWriter) getOrOpenFile(state *AttemptWriteState) (*os.File, error) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.file != nil {
		return state.file, nil
	}
	if state.phase != PhaseActive {
		return nil, errors.New("attempt is no longer active")
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
	atomic.AddInt64(&w.openFilesCount, 1)
	return f, nil
}

type StorageErrorKind int

const (
	StorageErrTransient StorageErrorKind = iota
	StorageErrPermanent
	StorageErrENOSPC
)

func isPermanentObjectError(err error) bool {
	return classifyStorageError(err) == StorageErrPermanent
}

func classifyStorageError(err error) StorageErrorKind {
	if err == nil {
		return StorageErrTransient
	}
	if errors.Is(err, os.ErrPermission) || errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.EROFS) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EINVAL) {
		return StorageErrPermanent
	}
	if errors.Is(err, syscall.ENOSPC) {
		return StorageErrENOSPC
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "no space left on device") || strings.Contains(msg, "disk full") || strings.Contains(msg, "quota exceeded") {
		return StorageErrENOSPC
	}
	if strings.Contains(msg, "permission denied") ||
		strings.Contains(msg, "read-only file system") ||
		strings.Contains(msg, "is a directory") ||
		strings.Contains(msg, "invalid argument") ||
		strings.Contains(msg, "bad file descriptor") {
		return StorageErrPermanent
	}
	return StorageErrTransient
}

type CommitProof struct {
	TaskID       string `json:"task_id"`
	Gen          string `json:"gen"`
	FinalPath    string `json:"final_path"`
	ExpectedSize int64  `json:"expected_size"`
	SHA256       string `json:"sha256"`
	CommittedAt  int64  `json:"committed_at"`
}

func (w *TargetWriter) finalizeTask(manifest TaskManifest) (finalErr error) {
	taskID := manifest.TaskID

	w.stateMu.RLock()
	state, ok := w.tasks[taskID]
	w.stateMu.RUnlock()

	if !ok {
		return errors.New("task not found for finalize")
	}

	fToClose, dataSynced, ok := state.AcquireFinalizeOp(manifest.Gen)
	if !ok {
		return nil // already finalized or superseded by newer generation
	}
	if fToClose != nil {
		atomic.AddInt64(&w.openFilesCount, -1)
	}

	finalPath := filepath.Join(w.outputDir, manifest.FinalPath)
	movingPath := finalPath + ".moving"
	metaPath := finalPath + ".moving.meta"
	proofPath := finalPath + ".tgx_commit"

	// Step 1: Ensure data is 100% durable synced to disk (P0-2 fix)
	if !dataSynced {
		if fToClose != nil {
			syncErr := fToClose.Sync()
			closeErr := fToClose.Close()
			if syncErr != nil {
				state.ReleaseOp()
				return fmt.Errorf("final sync moving file: %w", syncErr)
			}
			if closeErr != nil {
				state.ReleaseOp()
				return fmt.Errorf("close moving file: %w", closeErr)
			}
		} else {
			// Re-open staging file on retry to guarantee a clean sync/close cycle
			f, openErr := os.OpenFile(movingPath, os.O_RDWR, 0644)
			if openErr != nil {
				state.ReleaseOp()
				return fmt.Errorf("reopen moving file for final sync: %w", openErr)
			}
			syncErr := f.Sync()
			closeErr := f.Close()
			if syncErr != nil {
				state.ReleaseOp()
				return fmt.Errorf("retry sync moving file: %w", syncErr)
			}
			if closeErr != nil {
				state.ReleaseOp()
				return fmt.Errorf("retry close moving file: %w", closeErr)
			}
		}
		state.mu.Lock()
		state.dataSynced = true
		state.mu.Unlock()
	}

	// Step 2: Verify size
	stat, err := os.Stat(movingPath)
	if err != nil {
		state.ReleaseOp()
		return fmt.Errorf("stat completed moving file: %w", err)
	}
	if manifest.ExpectedSize > 0 && stat.Size() != manifest.ExpectedSize {
		state.ReleaseOp()
		return fmt.Errorf("final size %d does not match expected %d: %w", stat.Size(), manifest.ExpectedSize, ErrSizeMismatch)
	}

	// Step 3: Set modification time
	if manifest.Date > 0 {
		when := time.Unix(manifest.Date, 0)
		_ = os.Chtimes(movingPath, when, when)
	}

	// Step 4: Compute SHA256
	shaHash, shaErr := computeFileSHA256(movingPath)
	if shaErr != nil {
		state.ReleaseOp()
		return fmt.Errorf("compute SHA256 of completed file: %w", shaErr)
	}

	// Step 5: Pre-Commit CAS check under stateMu + state.mu (P0-3 fix)
	w.stateMu.RLock()
	currentAttempt, stillCurrent := w.tasks[taskID]
	w.stateMu.RUnlock()

	state.mu.Lock()
	if !stillCurrent || currentAttempt != state || state.manifest.Gen != manifest.Gen || state.phase != PhaseFinalizing {
		state.mu.Unlock()
		state.ReleaseOp()
		return fmt.Errorf("attempt superseded before commit: task %s gen %s", taskID, manifest.Gen)
	}
	state.mu.Unlock()

	// Step 6: Atomic non-replacing commit to final destination
	if err := atomicCommit.CommitFile(movingPath, finalPath); err != nil {
		if errors.Is(err, atomicCommit.ErrTargetExists) {
			existingSHA, existErr := computeFileSHA256(finalPath)
			if existErr == nil && existingSHA == shaHash {
				_ = os.Remove(movingPath)
				_ = os.Remove(metaPath)
			} else if existErr != nil {
				state.ReleaseOp()
				return fmt.Errorf("read existing target file for sha verification: %w", existErr)
			} else {
				state.ReleaseOp()
				return fmt.Errorf("existing_sha=%s, new_sha=%s: %w",
					existingSHA, shaHash, ErrContentConflict)
			}
		} else {
			state.ReleaseOp()
			return fmt.Errorf("commit target file: %w", err)
		}
	}

	_ = os.Remove(metaPath)

	// Persist immutable commit proof sidecar for 100% verifiable recovery
	proof := CommitProof{
		TaskID:       manifest.TaskID,
		Gen:          manifest.Gen,
		FinalPath:    manifest.FinalPath,
		ExpectedSize: manifest.ExpectedSize,
		SHA256:       shaHash,
		CommittedAt:  time.Now().Unix(),
	}
	if proofData, err := json.Marshal(proof); err == nil {
		_ = os.WriteFile(proofPath, proofData, 0644)
	}

	// Step 7: Clean tracking state — atomically mark finalized, release heavy objects, record tombstone
	w.stateMu.Lock()
	state.mu.Lock()
	if state.manifest.Gen == manifest.Gen {
		state.finalized = true
		state.finalGen = manifest.Gen
		state.finalSHA = shaHash
		state.finalPath = manifest.FinalPath
		state.pendingFinalize = false
		state.phase = PhaseFinalized
		state.bitmap = nil
		state.manifest.Ranges = nil
		w.recordTombstoneLocked(taskID)
	}
	state.mu.Unlock()
	w.stateMu.Unlock()

	// RELEASE LEASE BEFORE CALLING onComplete TO PREVENT RE-ENTRY DEADLOCKS (P1-7 fix)
	state.ReleaseOp()

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
		state.mu.Lock()
		if state.file != nil {
			_ = state.file.Sync()
			_ = state.file.Close()
			state.file = nil
			atomic.AddInt64(&w.openFilesCount, -1)
		}
		state.mu.Unlock()
	}
	w.stateMu.Unlock()
	return nil
}
