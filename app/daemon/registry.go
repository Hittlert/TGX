package daemon

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
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
}

type TaskSnapshot struct {
	TaskRequest
	State         TaskState `json:"state"`
	FileName      string    `json:"file_name,omitempty"`
	TotalSize     int64     `json:"total_size"`
	Downloaded    int64     `json:"downloaded"`
	NetDownloaded int64     `json:"net_downloaded,omitempty"`
	Progress      float64   `json:"progress"`
	Rolling5sBPS  int64     `json:"rolling_5s_bps"`
	DCID          int       `json:"dc_id,omitempty"`
	AlreadyExists bool      `json:"already_exists,omitempty"`
	SHA256        string    `json:"sha256,omitempty"`
	ErrorClass    string    `json:"error_class,omitempty"`
	Error         string    `json:"error,omitempty"`
	CreatedAt     int64     `json:"created_at"`
	StartedAt     int64     `json:"started_at,omitempty"`
	FinishedAt    int64     `json:"finished_at,omitempty"`
}

type PoolSnapshot struct {
	Size        int   `json:"size"`
	ActiveFiles int   `json:"active_files"`
	Reconnects  int64 `json:"reconnects"`
}

type StatusSnapshot struct {
	Backend      string         `json:"backend"`
	Paused       bool           `json:"paused"`
	Rolling5sBPS int64          `json:"rolling_5s_bps"`
	ActiveFiles  []TaskSnapshot `json:"active_files"`
	QueueDepth   int            `json:"queue_depth"`
	Pool         PoolSnapshot   `json:"pool"`
	LastError    string         `json:"last_error"`
	UpdatedAt    int64          `json:"updated_at"`
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
	request       TaskRequest
	state         TaskState
	attemptGen    string // generation ID for this attempt, set once at Submit time
	fileName      string
	totalSize     int64
	downloaded    int64
	dcID          int
	alreadyExists bool
	sha256        string
	errorClass    string
	errorText     string
	createdAt     time.Time
	startedAt     time.Time
	finishedAt    time.Time
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
	mu            sync.Mutex
	parentCtx     context.Context
	queueCapacity int
	terminalLimit int
	now           func() time.Time
	tasks         map[string]*taskState
	queue         []*taskState
	terminalOrder []string
	wake          chan struct{}
	paused        bool
	events        []byteEvent
	firstByte     time.Time
	lastByte      time.Time
	lastError     string
	pool          PoolSnapshot
	smoothedSpeed float64
}

type Task struct {
	registry *Registry
	state    *taskState
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
			// Unique generation for this retry attempt, determined at submission
			*existing = taskState{
				request: request, state: StateQueued, totalSize: request.ExpectedSize, createdAt: now,
				attemptGen: fmt.Sprintf("retry_%d", now.UnixNano()),
				ctx: taskCtx, cancel: taskCancel,
			}
			r.removeTerminalLocked(request.ID)
			r.queue = append(r.queue, existing)
			r.signalLocked()
			return r.snapshotTaskLocked(existing, now), true, nil
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
		attemptGen: "1", // First attempt always uses generation "1"
		ctx: taskCtx, cancel: taskCancel,
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

func (r *Registry) Next(ctx context.Context) (*Task, error) {
	for {
		r.mu.Lock()
		if !r.paused && len(r.queue) > 0 {
			state := r.queue[0]
			r.queue = r.queue[1:]
			state.state = StateResolving
			state.startedAt = r.now()
			r.mu.Unlock()
			return &Task{registry: r, state: state}, nil
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.wake:
		}
	}
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

	return StatusSnapshot{
		Backend: "tgx", Paused: r.paused,
		Rolling5sBPS: int64(r.smoothedSpeed),
		ActiveFiles:  active, QueueDepth: len(r.queue), Pool: pool,
		LastError: r.lastError, UpdatedAt: now.Unix(),
	}
}

func (t *Task) Request() TaskRequest {
	return t.state.request
}

// AttemptGen returns the generation ID for this task attempt.
// Set once at Submit time: "1" for first attempt, "retry_<nanos>" for retries.
func (t *Task) AttemptGen() string {
	gen := t.state.attemptGen
	if gen == "" {
		return "1"
	}
	return gen
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

func (t *Task) Succeed(finalPath string, alreadyExists bool) {
	t.SucceedResult(PublishResult{Path: finalPath, AlreadyExists: alreadyExists})
}

func (t *Task) SucceedResult(result PublishResult) {
	t.registry.finish(t.state, StateSuccess, "", "", result.Path, result.AlreadyExists, result.SHA256)
}

func (t *Task) Fail(class, message string, unavailable bool) {
	state := StateFailed
	if unavailable {
		state = StateUnavailable
	}
	t.registry.finish(t.state, state, class, message, "", false, "")
}

func (t *Task) RecordProgress(downloaded int64) {
	if downloaded <= 0 {
		return
	}
	r := t.registry
	r.mu.Lock()
	defer r.mu.Unlock()
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
	if !isTerminal(t.state.state) {
		update(t.state)
	}
	r.mu.Unlock()
}

func (r *Registry) finish(state *taskState, status TaskState, class, message, finalPath string, already bool, sha256 string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if isTerminal(state.state) {
		return
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
	for len(r.terminalOrder) > r.terminalLimit {
		oldest := r.terminalOrder[0]
		r.terminalOrder = r.terminalOrder[1:]
		delete(r.tasks, oldest)
	}
}

// FinishTask attempts to transition a task to terminal state.
// Returns true if the transition was accepted (generation matches current attempt).
// Returns false if rejected (stale generation or unknown task).
// The generation check and state transition happen atomically under the same lock.
func (r *Registry) FinishTask(id, gen string, status TaskState, class, message, finalPath string, already bool, sha256 string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.tasks[id]
	if !ok || state == nil {
		return false
	}
	// Generation guard: reject stale attempt callbacks.
	// Empty gen in callback is never accepted (would bypass the guard).
	if gen == "" || state.attemptGen != gen {
		return false
	}
	if isTerminal(state.state) {
		return false
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
	for len(r.terminalOrder) > r.terminalLimit {
		oldest := r.terminalOrder[0]
		r.terminalOrder = r.terminalOrder[1:]
		delete(r.tasks, oldest)
	}
	return true
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

func (r *Registry) CancelTasksByChatID(chatID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cleanChatID := strings.TrimPrefix(chatID, "@")
	var newQueue []*taskState
	for _, state := range r.queue {
		peer := strings.TrimPrefix(state.request.Peer, "@")
		if peer == cleanChatID || peer == chatID {
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

	return TaskSnapshot{
		TaskRequest: state.request, State: state.state, FileName: state.fileName,
		TotalSize: state.totalSize, Downloaded: state.downloaded, NetDownloaded: netDownloaded,
		Progress: progress,
		Rolling5sBPS: int64(state.smoothedSpeed),
		DCID:         state.dcID, AlreadyExists: state.alreadyExists, SHA256: state.sha256,
		ErrorClass: state.errorClass, Error: state.errorText,
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
	if request.FinalPath == "" || strings.ContainsAny(request.FinalPath, "\\\x00") || strings.HasPrefix(request.FinalPath, "/") {
		return errors.New("final_path must be a relative slash-separated path")
	}
	cleaned := path.Clean(request.FinalPath)
	if cleaned != request.FinalPath || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return fmt.Errorf("final_path escapes output root: %q", request.FinalPath)
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
	if len(events) == 0 || lastByte.IsZero() || now.Sub(lastByte) >= 3*time.Second {
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
