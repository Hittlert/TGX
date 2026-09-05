package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrQueueFull  = errors.New("task queue is full")
	ErrIDConflict = errors.New("task id already exists with different input")
)

type TaskState string

const (
	StateQueued      TaskState = "queued"
	StateResolving   TaskState = "resolving"
	StateDownloading TaskState = "downloading"
	StatePublishing  TaskState = "publishing"
	StateSuccess     TaskState = "success"
	StateFailed      TaskState = "failed"
	StateUnavailable TaskState = "unavailable"
)

type TaskRequest struct {
	ID           string `json:"id"`
	Peer         string `json:"peer"`
	MessageID    int    `json:"message_id"`
	FinalPath    string `json:"final_path"`
	ExpectedSize int64  `json:"expected_size"`
	Retry        bool   `json:"retry,omitempty"`
	TargetTitle  string `json:"target_title,omitempty"`
	MediaType    string `json:"media_type,omitempty"`
	FileName     string `json:"file_name,omitempty"`
	Date         int64  `json:"date,omitempty"`
	MaxRetries   int    `json:"max_retries,omitempty"`
}

type TaskSnapshot struct {
	TaskRequest
	State             TaskState `json:"state"`
	FileName          string    `json:"file_name,omitempty"`
	TotalSize         int64     `json:"total_size"`
	Downloaded        int64     `json:"downloaded"`
	NetDownloaded     int64     `json:"net_downloaded,omitempty"`
	WireBytes         int64     `json:"wire_bytes,omitempty"`
	ReplayBytes       int64     `json:"replay_bytes,omitempty"`
	RequestCount      int64     `json:"request_count,omitempty"`
	PhysicalRetries   int64     `json:"physical_retries,omitempty"`
	PhysicalAttemptID string    `json:"physical_attempt_id,omitempty"`
	Progress          float64   `json:"progress"`
	Rolling5sBPS      int64     `json:"rolling_5s_bps"`
	DCID              int       `json:"dc_id,omitempty"`
	AlreadyExists     bool      `json:"already_exists,omitempty"`
	SHA256            string    `json:"sha256,omitempty"`
	ErrorStage        string    `json:"error_stage,omitempty"`
	ErrorOp           string    `json:"error_op,omitempty"`
	ErrorClass        string    `json:"error_class,omitempty"`
	Error             string    `json:"error,omitempty"`
	Retryable         bool      `json:"retryable,omitempty"`
	RetryOwner        string    `json:"retry_owner,omitempty"`
	AttemptGeneration string    `json:"attempt_generation,omitempty"`
	AttemptCount      int       `json:"attempt_count,omitempty"`
	CreatedAt         int64     `json:"created_at"`
	StartedAt         int64     `json:"started_at,omitempty"`
	FinishedAt        int64     `json:"finished_at,omitempty"`
}

type PoolSnapshot struct {
	Size        int   `json:"size"`
	ActiveFiles int   `json:"active_files"`
	Reconnects  int64 `json:"reconnects"`
}

func readProcessRSS() int64 {
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) >= 2 {
			if pages, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				return pages * int64(os.Getpagesize())
			}
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.Sys)
}

type StatusSnapshot struct {
	Backend            string         `json:"backend"`
	Paused             bool           `json:"paused"`
	Rolling5sBPS       int64          `json:"rolling_5s_bps"`
	ActiveFiles        []TaskSnapshot `json:"active_files"`
	QueueDepth         int            `json:"queue_depth"`
	Pool               PoolSnapshot   `json:"pool"`
	LastError          string         `json:"last_error"`
	UpdatedAt          int64          `json:"updated_at"`
	WireRxBytes        int64          `json:"wire_rx_bytes"`
	UniquePayloadBytes int64          `json:"unique_payload_bytes"`
	RetryCount         int64          `json:"retry_count"`
	ProcessRSS         int64          `json:"process_rss"`
	HeapAlloc          int64          `json:"heap_alloc"`
	HeapInuse          int64          `json:"heap_inuse"`
	HeapObjects        int64          `json:"heap_objects"`
	GCCount            int64          `json:"gc_count"`
	GCPauseTotal       int64          `json:"gc_pause_total"`
}

func (s StatusSnapshot) MarshalJSON() ([]byte, error) {
	type Alias StatusSnapshot
	aux := Alias(s)
	if aux.ActiveFiles == nil {
		aux.ActiveFiles = make([]TaskSnapshot, 0)
	}
	return json.Marshal(aux)
}

type byteRange struct {
	start int64
	end   int64
}

type byteEvent struct {
	at    time.Time
	bytes int64
}

type taskState struct {
	request            TaskRequest
	state              TaskState
	attemptGen         string // generation ID for this attempt, set once at Submit time
	attemptCount       int
	fileName           string
	totalSize          int64
	downloaded         int64
	wireBytes          int64
	replayBytes        int64
	requestCount       int64
	physicalRetries    int64
	physicalAttemptID  string
	dcID               int
	alreadyExists      bool
	sha256             string
	errorStage         string
	errorOp            string
	errorClass         string
	errorText          string
	retryable          bool
	retryOwner         string
	createdAt          time.Time
	startedAt          time.Time
	finishedAt         time.Time
	ranges             []byteRange
	events             []byteEvent
	hasNetworkProgress bool
	netDownloaded      int64
	firstByte          time.Time
	lastByte           time.Time
	smoothedSpeed      float64
	ctx                context.Context
	cancel             context.CancelFunc
}

type Registry struct {
	mu                     sync.Mutex
	parentCtx              context.Context
	queueCapacity          int
	terminalLimit          int
	now                    func() time.Time
	tasks                  map[string]*taskState
	queue                  []*taskState
	terminalOrder          []string
	wake                   chan struct{}
	paused                 bool
	events                 []byteEvent
	firstByte              time.Time
	lastByte               time.Time
	lastError              string
	pool                   PoolSnapshot
	smoothedSpeed          float64
	cumulativeWireBytes    int64
	cumulativePayloadBytes int64
	cumulativeRetries      int64
}

type Task struct {
	registry   *Registry
	state      *taskState
	attemptGen string
}

func NewRegistry(queueCapacity, terminalLimit int, now func() time.Time) *Registry {
	return NewRegistryWithContext(context.Background(), queueCapacity, terminalLimit, now)
}

func NewRegistryWithContext(ctx context.Context, queueCapacity, terminalLimit int, now func() time.Time) *Registry {
	if queueCapacity < 1 {
		queueCapacity = 1
	}
	if terminalLimit < 1 {
		terminalLimit = 1
	}
	if now == nil {
		now = time.Now
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &Registry{
		parentCtx:     ctx,
		queueCapacity: queueCapacity,
		terminalLimit: terminalLimit,
		now:           now,
		tasks:         make(map[string]*taskState),
		wake:          make(chan struct{}, 1),
	}
}

func (r *Registry) SetParentContext(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx != nil {
		r.parentCtx = ctx
	}
}

func (r *Registry) Submit(request TaskRequest) (TaskSnapshot, bool, error) {
	if err := validateRequest(request); err != nil {
		return TaskSnapshot{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	retry := request.Retry
	request.Retry = false
	pCtx := r.parentCtx
	if pCtx == nil {
		pCtx = context.Background()
	}
	if existing, ok := r.tasks[request.ID]; ok {
		if existing.request != request {
			return TaskSnapshot{}, false, ErrIDConflict
		}
		if retry && (existing.state == StateFailed || existing.state == StateUnavailable) {
			if len(r.queue) >= r.queueCapacity {
				return TaskSnapshot{}, false, ErrQueueFull
			}
			now := r.now()
			taskCtx, taskCancel := context.WithCancel(pCtx)
			// Immutable Attempt: create a brand new taskState pointer for this retry attempt.
			// Old Task instances continue to hold their original, canceled taskState pointer.
			newState := &taskState{
				request: request, state: StateQueued, totalSize: request.ExpectedSize, createdAt: now,
				attemptGen:   fmt.Sprintf("retry_%d", now.UnixNano()),
				attemptCount: existing.attemptCount + 1,
				ctx:          taskCtx, cancel: taskCancel,
			}
			r.tasks[request.ID] = newState
			r.removeTerminalLocked(request.ID)
			r.queue = append(r.queue, newState)
			r.signalLocked()
			return r.snapshotTaskLocked(newState, now), true, nil
		}
		return r.snapshotTaskLocked(existing, r.now()), false, nil
	}
	if len(r.queue) >= r.queueCapacity {
		return TaskSnapshot{}, false, ErrQueueFull
	}
	now := r.now()
	taskCtx, taskCancel := context.WithCancel(pCtx)
	state := &taskState{
		request: request, state: StateQueued, totalSize: request.ExpectedSize, createdAt: now,
		attemptGen:   "1", // First attempt always uses generation "1"
		attemptCount: 1,
		ctx:          taskCtx, cancel: taskCancel,
	}
	r.tasks[request.ID] = state
	r.queue = append(r.queue, state)
	r.signalLocked()
	return r.snapshotTaskLocked(state, now), true, nil
}

func (r *Registry) removeTerminalLocked(id string) {
	for index, value := range r.terminalOrder {
		if value != id {
			continue
		}
		r.terminalOrder = append(r.terminalOrder[:index], r.terminalOrder[index+1:]...)
		return
	}
}

// RetryTask forces a retry of an existing terminal task with a brand new attempt generation,
// transitioning it back into the active queue without requiring daemon restart.
func (r *Registry) RetryTask(id string) (TaskSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.tasks[id]
	if !ok || existing == nil {
		return TaskSnapshot{}, errors.New("task not found in registry")
	}
	if !isTerminal(existing.state) {
		return TaskSnapshot{}, errors.New("task is not in terminal state")
	}
	if len(r.queue) >= r.queueCapacity {
		return TaskSnapshot{}, ErrQueueFull
	}
	now := r.now()
	pCtx := r.parentCtx
	if pCtx == nil {
		pCtx = context.Background()
	}
	taskCtx, taskCancel := context.WithCancel(pCtx)
	newState := &taskState{
		request:      existing.request,
		state:        StateQueued,
		totalSize:    existing.totalSize,
		createdAt:    now,
		attemptGen:   fmt.Sprintf("retry_%d", now.UnixNano()),
		attemptCount: existing.attemptCount + 1,
		ctx:          taskCtx,
		cancel:       taskCancel,
	}
	r.tasks[id] = newState
	r.removeTerminalLocked(id)
	r.queue = append(r.queue, newState)
	r.signalLocked()
	return r.snapshotTaskLocked(newState, now), nil
}

func (r *Registry) Next(ctx context.Context) (*Task, error) {
	for {
		r.mu.Lock()
		if !r.paused && len(r.queue) > 0 {
			state := r.queue[0]
			r.queue = r.queue[1:]
			state.state = StateResolving
			state.startedAt = r.now()
			if len(r.queue) > 0 {
				r.signalLocked()
			}
			r.mu.Unlock()
			return &Task{registry: r, state: state, attemptGen: state.attemptGen}, nil
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.wake:
		}
	}
}

func (r *Registry) Requeue(t *Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := t.state
	if state == nil || isTerminal(state.state) {
		return errors.New("cannot requeue terminal task")
	}
	state.state = StateQueued
	r.queue = append(r.queue, state)
	r.signalLocked()
	return nil
}

func (r *Registry) SetPaused(paused bool) {
	r.mu.Lock()
	r.paused = paused
	r.signalLocked()
	r.mu.Unlock()
}

func (r *Registry) SetPool(snapshot PoolSnapshot) {
	r.mu.Lock()
	r.pool = snapshot
	r.mu.Unlock()
}

func (r *Registry) SetLastError(message string) {
	r.mu.Lock()
	r.lastError = message
	r.mu.Unlock()
}

func (r *Registry) Task(id string) (TaskSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.tasks[id]
	if !ok {
		return TaskSnapshot{}, false
	}
	return r.snapshotTaskLocked(state, r.now()), true
}

func (r *Registry) Tasks() []TaskSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	result := make([]TaskSnapshot, 0, len(r.tasks))
	for _, state := range r.tasks {
		result = append(result, r.snapshotTaskLocked(state, now))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (r *Registry) Status() StatusSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.events = trimEvents(r.events, now.Add(-5*time.Second))
	var active []TaskSnapshot
	for _, state := range r.tasks {
		if state.state != StateResolving && state.state != StateDownloading && state.state != StatePublishing {
			continue
		}
		active = append(active, r.snapshotTaskLocked(state, now))
	}
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	pool := r.pool
	pool.ActiveFiles = len(active)

	currentBPS := rollingRate(r.events, r.firstByte, r.lastByte, now)
	if currentBPS == 0 {
		r.smoothedSpeed = 0
	} else if r.smoothedSpeed == 0 {
		r.smoothedSpeed = float64(currentBPS)
	} else {
		r.smoothedSpeed = 0.4*float64(currentBPS) + 0.6*r.smoothedSpeed
	}

	totalWire := r.cumulativeWireBytes
	totalPayload := r.cumulativePayloadBytes
	totalRetries := r.cumulativeRetries
	for _, state := range r.tasks {
		totalWire += state.wireBytes
		totalPayload += state.downloaded
		totalRetries += state.physicalRetries
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	rss := readProcessRSS()

	return StatusSnapshot{
		Backend: "tgx", Paused: r.paused,
		Rolling5sBPS:       int64(r.smoothedSpeed),
		ActiveFiles:        active,
		QueueDepth:         len(r.queue),
		Pool:               pool,
		LastError:          r.lastError,
		UpdatedAt:          now.Unix(),
		WireRxBytes:        totalWire,
		UniquePayloadBytes: totalPayload,
		RetryCount:         totalRetries,
		ProcessRSS:         rss,
		HeapAlloc:          int64(m.Alloc),
		HeapInuse:          int64(m.HeapInuse),
		HeapObjects:        int64(m.HeapObjects),
		GCCount:            int64(m.NumGC),
		GCPauseTotal:       int64(m.PauseTotalNs),
	}
}

func (t *Task) Request() TaskRequest {
	return t.state.request
}

type FinishResult int

const (
	FinishAcceptedNewTerminal FinishResult = iota + 1 // Accepted and transitioned to terminal state (DB update authorized)
	FinishAlreadySameTerminal                         // Already in the exact same terminal state (Idempotent OK, no DB update needed)
	FinishConflictingTerminal                         // Conflict: already terminal in different state (Reject DB update)
	FinishRejectedStale                               // Stale generation callback (Reject DB update)
	FinishNotFound                                    // Task not found in active registry (Reject DB update)
)

// AttemptGen returns the generation ID for this task attempt.
// Set once at Task creation time: "1" for first attempt, "retry_<nanos>" for retries.
func (t *Task) AttemptGen() string {
	if t.attemptGen != "" {
		return t.attemptGen
	}
	if t.state != nil && t.state.attemptGen != "" {
		return t.state.attemptGen
	}
	return "1"
}

// Generation is an alias for AttemptGen.
func (t *Task) Generation() string {
	return t.AttemptGen()
}

func (t *Task) Snapshot() TaskSnapshot {
	r := t.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotTaskLocked(t.state, r.now())
}

func (t *Task) SetResolved(fileName string, totalSize int64, dcID int) {
	t.update(func(state *taskState) {
		state.fileName = fileName
		state.totalSize = totalSize
		state.dcID = dcID
	})
}

func (t *Task) SetFinalPath(finalPath string) {
	t.update(func(state *taskState) {
		state.request.FinalPath = finalPath
	})
}

func (t *Task) SetDownloading() {
	t.update(func(state *taskState) { state.state = StateDownloading })
}

func (t *Task) IsTerminal() bool {
	r := t.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.attemptGen != "" && t.state.attemptGen != t.attemptGen {
		return true // stale attempt is effectively terminal/discarded
	}
	return isTerminal(t.state.state)
}

func (t *Task) SetPublishing() bool {
	var ok bool
	t.update(func(state *taskState) {
		state.state = StatePublishing
		ok = true
	})
	return ok
}

func (t *Task) RecordTransferTelemetry(written, wireBytes, replayBytes, reqCount, physicalRetries int64, physicalAttemptID ...string) {
	if t == nil || t.state == nil {
		return
	}
	t.registry.mu.Lock()
	defer t.registry.mu.Unlock()
	if written > 0 {
		t.state.downloaded = written
	}
	t.state.wireBytes = wireBytes
	t.state.replayBytes = replayBytes
	t.state.requestCount = reqCount
	t.state.physicalRetries = physicalRetries
	if len(physicalAttemptID) > 0 && physicalAttemptID[0] != "" {
		t.state.physicalAttemptID = physicalAttemptID[0]
	}
}

func (t *Task) Succeed(finalPath string, alreadyExists bool) {
	t.SucceedResult(PublishResult{Path: finalPath, AlreadyExists: alreadyExists})
}

func (t *Task) SucceedResult(result PublishResult) {
	if t != nil && t.state != nil {
		t.registry.mu.Lock()
		if result.WireBytes > 0 {
			t.state.wireBytes = result.WireBytes
		}
		if result.ReplayBytes > 0 {
			t.state.replayBytes = result.ReplayBytes
		}
		if result.RequestCount > 0 {
			t.state.requestCount = result.RequestCount
		}
		if result.PhysicalRetries > 0 {
			t.state.physicalRetries = result.PhysicalRetries
		}
		if result.PhysicalAttemptID != "" {
			t.state.physicalAttemptID = result.PhysicalAttemptID
		}
		t.registry.mu.Unlock()
	}
	t.registry.finishWithGen(t.state, t.attemptGen, StateSuccess, "", "", "", "", false, "", result.Path, result.AlreadyExists, result.SHA256)
}

// FailureDisposition captures structured failure semantics across the orchestrator, registry, and db.
type FailureDisposition struct {
	Stage             string `json:"stage"`
	Op                string `json:"op"`
	Class             string `json:"class"`
	Unavailable       bool   `json:"unavailable"`
	Retryable         bool   `json:"retryable"`
	RetryOwner        string `json:"retry_owner,omitempty"`
	PhysicalAttemptID string `json:"physical_attempt_id,omitempty"`
	Message           string `json:"message"`
	Cause             error  `json:"-"`
}

func (d FailureDisposition) Error() string {
	msg := d.Message
	if msg == "" && d.Cause != nil {
		msg = d.Cause.Error()
	}
	if d.Stage != "" || d.Op != "" {
		owner := d.RetryOwner
		if owner == "" {
			owner = "none"
		}
		class := d.Class
		if class == "" {
			class = "error"
		}
		return fmt.Sprintf("[%s:%s:%s] %s (owner=%s, retryable=%t)", d.Stage, d.Op, class, msg, owner, d.Retryable)
	}
	return msg
}

func (t *Task) FailDisposition(disp FailureDisposition) {
	state := StateFailed
	if disp.Unavailable {
		state = StateUnavailable
	}
	if disp.PhysicalAttemptID != "" && t.state != nil {
		t.registry.mu.Lock()
		t.state.physicalAttemptID = disp.PhysicalAttemptID
		t.registry.mu.Unlock()
	}
	t.registry.finishWithGen(t.state, t.attemptGen, state, disp.Stage, disp.Op, disp.Class, disp.Error(), disp.Retryable, disp.RetryOwner, "", false, "")
}

func (t *Task) Fail(class, message string, unavailable bool) {
	t.FailDisposition(FailureDisposition{
		Class:       class,
		Message:     message,
		Unavailable: unavailable,
	})
}

func (t *Task) RecordProgress(downloaded int64) {
	if downloaded <= 0 {
		return
	}
	r := t.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.attemptGen != "" && t.state.attemptGen != t.attemptGen {
		return
	}
	if isTerminal(t.state.state) {
		return
	}
	t.state.hasNetworkProgress = true
	if t.state.totalSize > 0 && downloaded > t.state.totalSize {
		downloaded = t.state.totalSize
	}
	diff := downloaded - t.state.netDownloaded
	if diff <= 0 {
		return
	}
	t.state.netDownloaded = downloaded
	now := r.now()
	event := byteEvent{at: now, bytes: diff}
	t.state.events = append(t.state.events, event)
	r.events = append(r.events, event)
	if t.state.firstByte.IsZero() {
		t.state.firstByte = now
	}
	if r.firstByte.IsZero() {
		r.firstByte = now
	}
	t.state.lastByte = now
	r.lastByte = now
}

func (t *Task) RecordWrite(offset int64, size int) int64 {
	if size <= 0 || offset < 0 {
		return 0
	}
	r := t.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.attemptGen != "" && t.state.attemptGen != t.attemptGen {
		return 0
	}
	end := offset + int64(size)
	if end < offset {
		return 0
	}
	if t.state.totalSize > 0 && end > t.state.totalSize {
		end = t.state.totalSize
	}
	if end <= offset {
		return 0
	}
	unique := addRange(&t.state.ranges, offset, end)
	if unique == 0 {
		return 0
	}
	t.state.downloaded += unique
	if !t.state.hasNetworkProgress {
		now := r.now()
		event := byteEvent{at: now, bytes: unique}
		t.state.events = append(t.state.events, event)
		r.events = append(r.events, event)
		if t.state.firstByte.IsZero() {
			t.state.firstByte = now
		}
		if r.firstByte.IsZero() {
			r.firstByte = now
		}
		t.state.lastByte = now
		r.lastByte = now
	}
	return unique
}

func (t *Task) update(update func(*taskState)) {
	r := t.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.attemptGen != "" && t.state.attemptGen != t.attemptGen {
		return
	}
	if !isTerminal(t.state.state) {
		update(t.state)
	}
}

func (r *Registry) pruneTerminalOrderLocked() {
	for len(r.terminalOrder) > r.terminalLimit {
		oldest := r.terminalOrder[0]
		r.terminalOrder = r.terminalOrder[1:]
		if old, exists := r.tasks[oldest]; exists {
			r.cumulativeWireBytes += old.wireBytes
			r.cumulativePayloadBytes += old.downloaded
			r.cumulativeRetries += old.physicalRetries
			delete(r.tasks, oldest)
		}
	}
}

func (r *Registry) finishWithGen(state *taskState, gen string, status TaskState, stage, op, class, message string, retryable bool, retryOwner string, finalPath string, already bool, sha256 string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gen != "" && state.attemptGen != gen {
		return false
	}
	if isTerminal(state.state) {
		return false
	}
	state.state = status
	state.errorStage = stage
	state.errorOp = op
	state.errorClass = class
	state.errorText = message
	state.retryable = retryable
	state.retryOwner = retryOwner
	state.alreadyExists = already
	state.sha256 = sha256
	state.finishedAt = r.now()
	if isTerminal(status) && state.cancel != nil {
		state.cancel()
	}
	if finalPath != "" {
		state.request.FinalPath = finalPath
	}
	if message != "" {
		r.lastError = message
	}
	r.terminalOrder = append(r.terminalOrder, state.request.ID)
	r.pruneTerminalOrderLocked()
	return true
}

// FinishTask attempts to transition a task to terminal state.
// Returns FinishAcceptedNewTerminal if generation matches and transitions from active to terminal.
// Returns FinishAlreadySameTerminal if already in the exact same terminal state.
// Returns FinishConflictingTerminal if already in a different terminal state.
// Returns FinishRejectedStale if generation does not match (stale callback).
// Returns FinishNotFound if task is not in active registry.
func (r *Registry) FinishTask(id, gen string, status TaskState, class, message, finalPath string, already bool, sha256 string) FinishResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.tasks[id]
	if !ok || state == nil {
		return FinishNotFound
	}
	// Generation guard: reject stale attempt callbacks.
	// Empty gen in callback is never accepted (would bypass the guard).
	if gen == "" || state.attemptGen != gen {
		return FinishRejectedStale
	}
	if isTerminal(state.state) {
		if state.state == status {
			return FinishAlreadySameTerminal
		}
		return FinishConflictingTerminal
	}
	state.state = status
	state.errorClass = class
	state.errorText = message
	state.alreadyExists = already
	state.sha256 = sha256
	state.finishedAt = r.now()
	if isTerminal(status) && state.cancel != nil {
		state.cancel()
	}
	if finalPath != "" {
		state.request.FinalPath = finalPath
	}
	if message != "" {
		r.lastError = message
	}
	r.terminalOrder = append(r.terminalOrder, state.request.ID)
	r.pruneTerminalOrderLocked()
	return FinishAcceptedNewTerminal
}

// FinishTaskByMessage finishes an active task matching chatID, messageID, and attempt generation.
func (r *Registry) FinishTaskByMessage(chatID string, messageID int, gen string, status TaskState, class, message, finalPath string, already bool, sha256 string) FinishResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	cleanChatID := strings.TrimPrefix(chatID, "@")
	for _, state := range r.tasks {
		peer := strings.TrimPrefix(state.request.Peer, "@")
		if (peer == cleanChatID || peer == chatID) && state.request.MessageID == messageID {
			if gen != "" && state.attemptGen != gen {
				return FinishRejectedStale
			}
			if isTerminal(state.state) {
				if state.state == status {
					return FinishAlreadySameTerminal
				}
				return FinishConflictingTerminal
			}
			state.state = status
			state.errorClass = class
			state.errorText = message
			state.alreadyExists = already
			state.sha256 = sha256
			state.finishedAt = r.now()
			if isTerminal(status) && state.cancel != nil {
				state.cancel()
			}
			if finalPath != "" {
				state.request.FinalPath = finalPath
			}
			if message != "" {
				r.lastError = message
			}
			r.terminalOrder = append(r.terminalOrder, state.request.ID)
			r.pruneTerminalOrderLocked()
			return FinishAcceptedNewTerminal
		}
	}
	return FinishNotFound
}

// RegisterRecoveredTask registers a task recovered from startup crash recovery into the
// active Registry so its finalization callback can be authoritatively validated and accepted.
func (r *Registry) RegisterRecoveredTask(id, gen, finalPath string, expectedSize int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	taskCtx, taskCancel := context.WithCancel(r.parentCtx)
	state := &taskState{
		request: TaskRequest{
			ID:           id,
			FinalPath:    finalPath,
			ExpectedSize: expectedSize,
		},
		state:      StatePublishing,
		attemptGen: gen,
		totalSize:  expectedSize,
		createdAt:  r.now(),
		startedAt:  r.now(),
		ctx:        taskCtx,
		cancel:     taskCancel,
	}
	r.tasks[id] = state
}

func (r *Registry) Cancel(id string, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var newQueue []*taskState
	for _, state := range r.queue {
		if state.request.ID == id {
			state.state = StateFailed
			state.errorClass = "canceled"
			if reason != "" {
				state.errorText = reason
			} else {
				state.errorText = "task canceled"
			}
			state.finishedAt = r.now()
			if state.cancel != nil {
				state.cancel()
			}
		} else {
			newQueue = append(newQueue, state)
		}
	}
	r.queue = newQueue

	if state, ok := r.tasks[id]; ok {
		if !isTerminal(state.state) {
			state.state = StateFailed
			state.errorClass = "canceled"
			if reason != "" {
				state.errorText = reason
			} else {
				state.errorText = "task canceled"
			}
			state.finishedAt = r.now()
			if state.cancel != nil {
				state.cancel()
			}
		}
	}
	r.signalLocked()
}

// CancelTasksByChatID cancels running and queued tasks for the given chat ID.
func (r *Registry) CancelTasksByChatID(chatID string) {
	r.CancelTasksByChatIDWithDecider(chatID, nil)
}

// CancelTasksByChatIDWithDecider cancels running and queued tasks for the given chat ID,
// consulting decider (e.g. durable DB transition) before setting StateFailed.
// If decider returns an error for a task, that task's state is NOT transitioned to canceled.
func (r *Registry) CancelTasksByChatIDWithDecider(chatID string, decider func(peer string, messageID int, gen string) error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cleanChatID := strings.TrimPrefix(chatID, "@")
	var newQueue []*taskState
	for _, state := range r.queue {
		peer := strings.TrimPrefix(state.request.Peer, "@")
		if peer == cleanChatID || peer == chatID {
			if decider != nil {
				if err := decider(state.request.Peer, state.request.MessageID, state.attemptGen); err != nil {
					newQueue = append(newQueue, state)
					continue
				}
			}
			state.state = StateFailed
			state.errorClass = "canceled"
			state.errorText = "target disabled by user"
			state.finishedAt = r.now()
			if state.cancel != nil {
				state.cancel()
			}
		} else {
			newQueue = append(newQueue, state)
		}
	}
	r.queue = newQueue

	for _, state := range r.tasks {
		if !isTerminal(state.state) {
			peer := strings.TrimPrefix(state.request.Peer, "@")
			if peer == cleanChatID || peer == chatID {
				if decider != nil {
					if err := decider(state.request.Peer, state.request.MessageID, state.attemptGen); err != nil {
						continue
					}
				}
				state.state = StateFailed
				state.errorClass = "canceled"
				state.errorText = "target disabled by user"
				state.finishedAt = r.now()
				if state.cancel != nil {
					state.cancel()
				}
			}
		}
	}
	r.signalLocked()
}

func (t *Task) Context() context.Context {
	r := t.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.state.ctx != nil {
		return t.state.ctx
	}
	return context.Background()
}

func (r *Registry) snapshotTaskLocked(state *taskState, now time.Time) TaskSnapshot {
	state.events = trimEvents(state.events, now.Add(-5*time.Second))
	progress := float64(0)
	if state.totalSize > 0 {
		progress = float64(state.downloaded) * 100 / float64(state.totalSize)
		if progress > 100 {
			progress = 100
		}
	}

	currentTaskBPS := rollingRate(state.events, state.firstByte, state.lastByte, now)
	if currentTaskBPS == 0 {
		state.smoothedSpeed = 0
	} else if state.smoothedSpeed == 0 {
		state.smoothedSpeed = float64(currentTaskBPS)
	} else {
		state.smoothedSpeed = 0.4*float64(currentTaskBPS) + 0.6*state.smoothedSpeed
	}

	netDownloaded := state.netDownloaded
	if netDownloaded < state.downloaded {
		netDownloaded = state.downloaded
	}

	attemptGen := state.attemptGen
	if attemptGen == "" {
		attemptGen = "1"
	}
	attemptCount := state.attemptCount
	if attemptCount <= 0 {
		attemptCount = 1
	}

	return TaskSnapshot{
		TaskRequest: state.request, State: state.state, FileName: state.fileName,
		TotalSize: state.totalSize, Downloaded: state.downloaded, NetDownloaded: netDownloaded,
		WireBytes: state.wireBytes, ReplayBytes: state.replayBytes,
		RequestCount: state.requestCount, PhysicalRetries: state.physicalRetries,
		PhysicalAttemptID: state.physicalAttemptID,
		Progress:          progress,
		Rolling5sBPS:      int64(state.smoothedSpeed),
		DCID:              state.dcID, AlreadyExists: state.alreadyExists, SHA256: state.sha256,
		ErrorStage: state.errorStage, ErrorOp: state.errorOp,
		ErrorClass: state.errorClass, Error: state.errorText,
		Retryable: state.retryable, RetryOwner: state.retryOwner,
		AttemptGeneration: attemptGen, AttemptCount: attemptCount,
		CreatedAt: state.createdAt.Unix(), StartedAt: unixOrZero(state.startedAt),
		FinishedAt: unixOrZero(state.finishedAt),
	}
}

func (r *Registry) signalLocked() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func validateRequest(request TaskRequest) error {
	if request.ID == "" || len(request.ID) > 256 || strings.ContainsAny(request.ID, "/\\\x00") {
		return errors.New("id must be 1-256 characters without slashes")
	}
	if strings.TrimSpace(request.Peer) == "" {
		return errors.New("peer is required")
	}
	if request.MessageID <= 0 {
		return errors.New("message_id must be positive")
	}
	if request.ExpectedSize < 0 {
		return errors.New("expected_size cannot be negative")
	}
	if request.FinalPath != "" {
		if strings.ContainsAny(request.FinalPath, "\\\x00") || strings.HasPrefix(request.FinalPath, "/") {
			return errors.New("final_path must be a relative slash-separated path")
		}
		cleaned := path.Clean(request.FinalPath)
		if cleaned != request.FinalPath || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return fmt.Errorf("final_path escapes output root: %q", request.FinalPath)
		}
	}
	return nil
}

func isTerminal(state TaskState) bool {
	return state == StateSuccess || state == StateFailed || state == StateUnavailable
}

func trimEvents(events []byteEvent, cutoff time.Time) []byteEvent {
	index := 0
	for index < len(events) && events[index].at.Before(cutoff) {
		index++
	}
	if index == 0 {
		return events
	}
	trimmed := make([]byteEvent, len(events)-index)
	copy(trimmed, events[index:])
	return trimmed
}

func rollingRate(events []byteEvent, firstByte, lastByte, now time.Time) int64 {
	if len(events) == 0 || lastByte.IsZero() || now.Sub(lastByte) >= 5*time.Second {
		return 0
	}
	var total int64
	for _, event := range events {
		total += event.bytes
	}
	start := firstByte
	if cutoff := now.Add(-5 * time.Second); start.Before(cutoff) {
		start = cutoff
	}
	elapsed := now.Sub(start).Seconds()
	if elapsed < 1.0 {
		elapsed = 1.0
	}
	return int64(float64(total) / elapsed)
}

func addRange(ranges *[]byteRange, start, end int64) int64 {
	var overlap int64
	for _, existing := range *ranges {
		left, right := max64(start, existing.start), min64(end, existing.end)
		if right > left {
			overlap += right - left
		}
	}
	next := byteRange{start: start, end: end}
	merged := make([]byteRange, 0, len(*ranges)+1)
	inserted := false
	for _, existing := range *ranges {
		switch {
		case existing.end < next.start:
			merged = append(merged, existing)
		case next.end < existing.start:
			if !inserted {
				merged = append(merged, next)
				inserted = true
			}
			merged = append(merged, existing)
		default:
			next.start = min64(next.start, existing.start)
			next.end = max64(next.end, existing.end)
		}
	}
	if !inserted {
		merged = append(merged, next)
	}
	*ranges = merged
	return end - start - overlap
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func unixOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}
