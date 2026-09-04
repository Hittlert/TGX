package daemon

import (
	"sync"

	"go.uber.org/zap"
)

// Required stable lifecycle event chain constants.
const (
	EventItemIngested         = "item.ingested"
	EventItemAdmitted         = "item.admitted"
	EventItemResolved         = "item.resolved"
	EventDownloadStarted      = "download.started"
	EventRPCRetry             = "rpc.retry"
	EventSSDCommitPrepared    = "ssd.commit_prepared"
	EventSSDCommitted         = "ssd.committed"
	EventItemTerminal         = "item.terminal"
	EventArchiveQueued        = "archive.queued"
	EventArchiveStarted       = "archive.started"
	EventArchiveCommitted     = "archive.committed"
	EventArchiveSourceRemoved = "archive.source_removed"
)

// LifecycleEvent represents a structured, correlated event across download and archive lifecycles.
type LifecycleEvent struct {
	Event           string         `json:"event"`
	TaskID          string         `json:"task_id,omitempty"`
	AttemptID       string         `json:"attempt_id,omitempty"`
	ChatID          string         `json:"chat_id,omitempty"`
	MessageID       int            `json:"message_id,omitempty"`
	Stage           string         `json:"stage,omitempty"`
	Op              string         `json:"op,omitempty"`
	Path            string         `json:"path,omitempty"`
	Size            int64          `json:"size,omitempty"`
	SHA256          string         `json:"sha256,omitempty"`
	DC              int            `json:"dc,omitempty"`
	Error           string         `json:"error,omitempty"`
	ErrorClass      string         `json:"error_class,omitempty"`
	Retryable       bool           `json:"retryable,omitempty"`
	RetryOwner      string         `json:"retry_owner,omitempty"`
	Status          string         `json:"status,omitempty"`
	PhysicalRetries int64          `json:"physical_retries,omitempty"`
	RequestCount    int64          `json:"request_count,omitempty"`
	WireBytes       int64          `json:"wire_bytes,omitempty"`
	ReplayBytes     int64          `json:"replay_bytes,omitempty"`
	Extra           map[string]any `json:"extra,omitempty"`
}

// LifecycleObserver observes emitted lifecycle events for testing and telemetry.
type LifecycleObserver interface {
	OnLifecycleEvent(evt LifecycleEvent)
}

// LifecycleObserverFunc implements LifecycleObserver for a simple function callback.
type LifecycleObserverFunc func(evt LifecycleEvent)

func (f LifecycleObserverFunc) OnLifecycleEvent(evt LifecycleEvent) {
	f(evt)
}

var (
	lifecycleMu        sync.RWMutex
	nextObserverID     uint64
	lifecycleObservers = make(map[uint64]LifecycleObserver)
)

// RegisterLifecycleObserver registers an observer and returns an unregister closure.
func RegisterLifecycleObserver(obs LifecycleObserver) func() {
	if obs == nil {
		return func() {}
	}
	lifecycleMu.Lock()
	nextObserverID++
	id := nextObserverID
	lifecycleObservers[id] = obs
	lifecycleMu.Unlock()

	return func() {
		lifecycleMu.Lock()
		delete(lifecycleObservers, id)
		lifecycleMu.Unlock()
	}
}

// EmitLifecycle records a lifecycle event to the logger and broadcasts to registered observers.
func EmitLifecycle(logger *zap.Logger, evt LifecycleEvent) {
	if logger != nil {
		fields := []zap.Field{
			zap.String("event", evt.Event),
		}
		if evt.TaskID != "" {
			fields = append(fields, zap.String("task_id", evt.TaskID))
		}
		if evt.AttemptID != "" {
			fields = append(fields, zap.String("attempt_id", evt.AttemptID))
		}
		if evt.ChatID != "" {
			fields = append(fields, zap.String("chat_id", evt.ChatID))
		}
		if evt.MessageID != 0 {
			fields = append(fields, zap.Int("message_id", evt.MessageID))
		}
		if evt.Stage != "" {
			fields = append(fields, zap.String("stage", evt.Stage))
		}
		if evt.Op != "" {
			fields = append(fields, zap.String("op", evt.Op))
		}
		if evt.Path != "" {
			fields = append(fields, zap.String("path", evt.Path))
		}
		if evt.Size != 0 {
			fields = append(fields, zap.Int64("size", evt.Size))
		}
		if evt.SHA256 != "" {
			fields = append(fields, zap.String("sha256", evt.SHA256))
		}
		if evt.DC != 0 {
			fields = append(fields, zap.Int("dc", evt.DC))
		}
		if evt.ErrorClass != "" {
			fields = append(fields, zap.String("error_class", evt.ErrorClass))
		}
		if evt.Error != "" {
			fields = append(fields, zap.String("error", evt.Error))
		}
		if evt.RetryOwner != "" {
			fields = append(fields, zap.String("retry_owner", evt.RetryOwner))
		}
		if evt.Status != "" {
			fields = append(fields, zap.String("status", evt.Status))
		}
		if evt.PhysicalRetries != 0 {
			fields = append(fields, zap.Int64("physical_retries", evt.PhysicalRetries))
		}
		if evt.RequestCount != 0 {
			fields = append(fields, zap.Int64("request_count", evt.RequestCount))
		}
		if evt.WireBytes != 0 {
			fields = append(fields, zap.Int64("wire_bytes", evt.WireBytes))
		}
		if evt.ReplayBytes != 0 {
			fields = append(fields, zap.Int64("replay_bytes", evt.ReplayBytes))
		}
		if evt.Extra != nil {
			for k, v := range evt.Extra {
				fields = append(fields, zap.Any(k, v))
			}
		}

		logger.Info("lifecycle_event", fields...)
	}

	lifecycleMu.RLock()
	observers := make([]LifecycleObserver, 0, len(lifecycleObservers))
	for _, obs := range lifecycleObservers {
		observers = append(observers, obs)
	}
	lifecycleMu.RUnlock()

	for _, obs := range observers {
		obs.OnLifecycleEvent(evt)
	}
}
