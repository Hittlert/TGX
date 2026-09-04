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
	Event      string         `json:"event"`
	TaskID     string         `json:"task_id,omitempty"`
	AttemptID  string         `json:"attempt_id,omitempty"`
	ChatID     string         `json:"chat_id,omitempty"`
	MessageID  int            `json:"message_id,omitempty"`
	Stage      string         `json:"stage,omitempty"`
	Op         string         `json:"op,omitempty"`
	Path       string         `json:"path,omitempty"`
	Size       int64          `json:"size,omitempty"`
	SHA256     string         `json:"sha256,omitempty"`
	DC         int            `json:"dc,omitempty"`
	Error      string         `json:"error,omitempty"`
	ErrorClass string         `json:"error_class,omitempty"`
	Retryable  bool           `json:"retryable,omitempty"`
	RetryOwner string         `json:"retry_owner,omitempty"`
	Status     string         `json:"status,omitempty"`
	Extra      map[string]any `json:"extra,omitempty"`
}

// LifecycleObserver observes emitted lifecycle events for testing and telemetry.
type LifecycleObserver interface {
	OnLifecycleEvent(evt LifecycleEvent)
}

var (
	lifecycleMu        sync.RWMutex
	lifecycleObservers []LifecycleObserver
)

// RegisterLifecycleObserver registers an observer and returns an unregister closure.
func RegisterLifecycleObserver(obs LifecycleObserver) func() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	lifecycleObservers = append(lifecycleObservers, obs)
	return func() {
		lifecycleMu.Lock()
		defer lifecycleMu.Unlock()
		for i, o := range lifecycleObservers {
			if o == obs {
				lifecycleObservers = append(lifecycleObservers[:i], lifecycleObservers[i+1:]...)
				break
			}
		}
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

		logger.Info("lifecycle_event", fields...)
	}

	lifecycleMu.RLock()
	observers := append([]LifecycleObserver(nil), lifecycleObservers...)
	lifecycleMu.RUnlock()

	for _, obs := range observers {
		obs.OnLifecycleEvent(evt)
	}
}
