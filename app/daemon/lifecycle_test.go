package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/Hittlert/TGX/core/transfer"
	"github.com/Hittlert/TGX/internal/fscommit"
)

type eventRecorder struct {
	mu     sync.Mutex
	events []LifecycleEvent
}

func (r *eventRecorder) OnLifecycleEvent(evt LifecycleEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
}

func (r *eventRecorder) Events() []LifecycleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]LifecycleEvent, len(r.events))
	copy(cp, r.events)
	return cp
}

// 1. Acceptance Test: Complete lifecycle event chain in exact causal order.
func TestLifecycle_EventChainCompleteSuccess(t *testing.T) {
	recorder := &eventRecorder{}
	unregister := RegisterLifecycleObserver(recorder)
	defer unregister()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	archiveDir := filepath.Join(tempDir, "archive")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	require.NoError(t, os.MkdirAll(archiveDir, 0o755))

	logger := zap.NewNop()
	archiveWorker, err := NewArchiveWorker(db, ssdDir, archiveDir, logger)
	require.NoError(t, err)

	registry := NewRegistry(10, 100, time.Now)
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 2,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
	})
	ssdAdmission := fscommit.NewSSDAdmission(ssdDir, 1<<20)

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	h := sha256.Sum256(payload)
	expectedSHA := hex.EncodeToString(h[:])

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, payload)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 101, AccessHash: 202}, sz: 1024, dc: 2},
				Name:      "video.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      time.Now().Unix(),
			}, nil
		},
	}

	orch := NewOrchestrator(
		db,
		transferMgr,
		ssdAdmission,
		nil,
		access,
		registry,
		logger,
		ssdDir,
	)
	orch.SetArchiveWorker(archiveWorker)

	// Seed target in DB
	_, _ = db.Execute(`
		INSERT INTO listen_targets (chat_id, enabled, title, username, priority, created_at, updated_at)
		VALUES ('-100777', 1, 'My Channel', 'chan', 10, 1000, 1000)
	`)

	// 1. Submit record (item.ingested)
	rec := DownloadRecord{
		ChatID:      "-100777",
		MessageID:   501,
		TargetTitle: "My Channel",
		MediaType:   "video",
		FileName:    "video.mp4",
		FileSize:    1024,
		CreatedAt:   1000,
	}
	require.NoError(t, orch.SubmitRecord(rec))

	// 2. Run orchestrator download loop for task
	task, err := registry.Next(context.Background())
	require.NoError(t, err)

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	require.Equal(t, StateSuccess, snap.State)

	// 3. Process archive job
	processed := archiveWorker.ProcessNext(context.Background())
	require.True(t, processed, "due archive job should be processed successfully")

	events := recorder.Events()

	expectedSequence := []string{
		EventItemIngested,
		EventItemAdmitted,
		EventItemResolved,
		EventDownloadStarted,
		EventSSDCommitPrepared,
		EventSSDCommitted,
		EventArchiveQueued,
		EventItemTerminal,
		EventArchiveStarted,
		EventArchiveCommitted,
		EventArchiveSourceRemoved,
	}

	eventTypes := make([]string, 0, len(events))
	for _, e := range events {
		eventTypes = append(eventTypes, e.Event)
		require.NotEmpty(t, e.TaskID, "every event must carry TaskID")
	}

	// Verify each required event in expectedSequence occurred in order
	seqIdx := 0
	for _, actual := range eventTypes {
		if seqIdx < len(expectedSequence) && actual == expectedSequence[seqIdx] {
			seqIdx++
		}
	}
	require.Equal(t, len(expectedSequence), seqIdx, "lifecycle event sequence incomplete: got %v, expected in-order %v", eventTypes, expectedSequence)

	// Assert no success event appears before SSD and DB durability
	var terminalSuccessSeen, ssdCommittedSeen bool
	for _, e := range events {
		if e.Event == EventSSDCommitted {
			ssdCommittedSeen = true
		}
		if e.Event == EventItemTerminal && e.Status == "success" {
			terminalSuccessSeen = true
			require.True(t, ssdCommittedSeen, "EventItemTerminal(success) cannot appear before EventSSDCommitted")
		}
	}
	require.True(t, terminalSuccessSeen)
	require.Equal(t, expectedSHA, snap.SHA256)
}

// 2. Acceptance Test: Failure injection across stages carries full attribution, cause, and owner.
func TestLifecycle_FailureAttributionMatrix(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "fail.db")
	db, err := NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	logger := zap.NewNop()

	registry := NewRegistry(10, 100, time.Now)
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 1,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
	})
	ssdAdmission := fscommit.NewSSDAdmission(ssdDir, 1<<20)

	// 2.1 Resolve Failure
	resolveErr := NewTaskError("resolve", "get_message", "unavailable", true, false, errors.New("message was deleted"))
	accessFail := &mockAccessWithPool{
		pool:       &mockPool{invoker: nil},
		resolveErr: resolveErr,
	}

	orchResolveFail := NewOrchestrator(db, transferMgr, ssdAdmission, nil, accessFail, registry, logger, ssdDir)
	req1 := TaskRequest{ID: "fail_resolve", Peer: "-1001", MessageID: 10, ExpectedSize: 100}
	_, _, _ = registry.Submit(req1)
	task1, _ := registry.Next(context.Background())
	orchResolveFail.downloadOne(context.Background(), task1)

	snap1 := task1.Snapshot()
	require.Equal(t, StateUnavailable, snap1.State)
	require.Equal(t, "resolve", snap1.ErrorStage)
	require.Equal(t, "get_message", snap1.ErrorOp)
	require.Equal(t, "unavailable", snap1.ErrorClass)
	require.False(t, snap1.Retryable)
	require.Equal(t, "none", snap1.RetryOwner)

	// 2.2 Transfer RPC Failure (classified as network with owner gotd)
	invokerRPCFail := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return errors.New("rpc connection reset")
	})
	accessRPCFail := &mockAccessWithPool{
		pool: &mockPool{invoker: invokerRPCFail},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 201}, sz: 500, dc: 2},
				Name:      "test.mp4",
				Size:      500,
				DCID:      2,
				MediaType: "video",
			}, nil
		},
	}

	orchRPCFail := NewOrchestrator(db, transferMgr, ssdAdmission, nil, accessRPCFail, registry, logger, ssdDir)
	req2 := TaskRequest{ID: "fail_rpc", Peer: "-1001", MessageID: 20, ExpectedSize: 500}
	_, _, _ = registry.Submit(req2)
	task2, _ := registry.Next(context.Background())
	orchRPCFail.downloadOne(context.Background(), task2)

	snap2 := task2.Snapshot()
	require.Equal(t, StateFailed, snap2.State)
	require.Equal(t, "transfer", snap2.ErrorStage)
	require.Equal(t, "download", snap2.ErrorOp)
	require.Equal(t, "network", snap2.ErrorClass)
	require.Equal(t, "gotd", snap2.RetryOwner)
}

// 3. Acceptance Test: Recovery error propagation returns errors on DB/filesystem failures.
func TestLifecycle_RecoveryErrorPropagation(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "closed.db")
	db, err := NewDatabase(dbPath)
	require.NoError(t, err)

	// Close database to force DB query failure during startup recovery
	db.Close()

	logger := zap.NewNop()
	err = ReconcileOnStartup(context.Background(), db, filepath.Join(tempDir, "ssd"), "", logger)
	require.Error(t, err, "ReconcileOnStartup must return combined errors when DB operations fail")
}

// 4. Acceptance Test: TaskRetryHandler captures physical retries, task correlation, and DC identity via production gotd retry.
func TestLifecycle_RPCRetryHandler(t *testing.T) {
	recorder := &eventRecorder{}
	unregister := RegisterLifecycleObserver(recorder)
	defer unregister()

	core, observedLogs := observer.New(zap.InfoLevel)
	logger := zap.New(core)

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "retry_state.db")
	db, err := NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	ssdDir := filepath.Join(tempDir, "ssd")
	require.NoError(t, os.MkdirAll(ssdDir, 0o755))
	ssdAdmission := fscommit.NewSSDAdmission(ssdDir, 1<<20)

	registry := NewRegistry(10, 100, time.Now)

	var retryEventFired bool
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 1,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
		TaskRetryHandler: func(taskCtx context.Context, event downloader.RetryEvent) {
			retryEventFired = true
			tc, _ := transfer.TransferTaskFromContext(taskCtx)
			physAttemptID, _, ok := transfer.ExtractPhysicalAttempt(event.Err)
			if !ok || physAttemptID == "" {
				physAttemptID = tc.GetFailedAttempt(event.Operation)
				if physAttemptID == "" {
					physAttemptID = fmt.Sprintf("%s-p%d", tc.AttemptID, event.Attempt)
				}
			}
			EmitLifecycle(logger, LifecycleEvent{
				Event:             EventRPCRetry,
				TaskID:            tc.TaskID,
				AttemptID:         tc.AttemptID,
				PhysicalAttemptID: physAttemptID,
				ChatID:            tc.ChatID,
				MessageID:         tc.MessageID,
				DC:                tc.DCID,
				Op:                event.Operation,
				PhysicalRetries:   int64(event.Attempt),
				Error:             fmt.Sprintf("%v", event.Err),
				Extra: map[string]any{
					"operation":           event.Operation,
					"attempt":             event.Attempt,
					"physical_attempt_id": physAttemptID,
				},
			})
		},
	})

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	var callCount int64
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		cnt := atomic.AddInt64(&callCount, 1)
		if cnt == 1 {
			// First call fails with transient timeout -> gotd downloader retries and invokes TaskRetryHandler
			return tgerr.New(500, tg.ErrTimeout)
		}
		setUploadFile(output, payload)
		return nil
	})

	targetDC := 4
	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 301, AccessHash: 401}, sz: 1024, dc: targetDC},
				Name:      "retry_stream.mp4",
				Size:      1024,
				DCID:      targetDC,
				MediaType: "video",
				Date:      time.Now().Unix(),
			}, nil
		},
	}

	orch := NewOrchestrator(
		db,
		transferMgr,
		ssdAdmission,
		nil,
		access,
		registry,
		logger,
		ssdDir,
	)

	// Seed target
	_, _ = db.Execute(`
		INSERT INTO listen_targets (chat_id, enabled, title, username, priority, created_at, updated_at)
		VALUES ('-100888', 1, 'Retry Channel', 'retry_chan', 10, 1000, 1000)
	`)

	rec := DownloadRecord{
		ChatID:      "-100888",
		MessageID:   888,
		TargetTitle: "Retry Channel",
		MediaType:   "video",
		FileName:    "retry_stream.mp4",
		FileSize:    1024,
		CreatedAt:   1000,
	}
	require.NoError(t, orch.SubmitRecord(rec))

	task, err := registry.Next(context.Background())
	require.NoError(t, err)

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	require.Equal(t, StateSuccess, snap.State)
	require.True(t, retryEventFired, "registered gotd TaskRetryHandler must be invoked on physical retry")

	// 1. Assert observer captured EventRPCRetry with full correlation
	events := recorder.Events()
	var rpcRetryEvent *LifecycleEvent
	for i := range events {
		if events[i].Event == EventRPCRetry {
			rpcRetryEvent = &events[i]
			break
		}
	}
	require.NotNil(t, rpcRetryEvent, "EventRPCRetry must be emitted during gotd physical retry")
	require.Equal(t, task.Request().ID, rpcRetryEvent.TaskID)
	require.Equal(t, task.Generation(), rpcRetryEvent.AttemptID)
	require.Equal(t, "-100888", rpcRetryEvent.ChatID)
	require.Equal(t, 888, rpcRetryEvent.MessageID)
	require.Equal(t, targetDC, rpcRetryEvent.DC)
	require.Equal(t, "reader.chunk", rpcRetryEvent.Op)
	require.Equal(t, int64(1), rpcRetryEvent.PhysicalRetries)
	require.Equal(t, "reader.chunk", rpcRetryEvent.Extra["operation"])
	expectedPhysID := fmt.Sprintf("%s-chunk-0-a1", task.Generation())
	require.Equal(t, expectedPhysID, rpcRetryEvent.PhysicalAttemptID)
	require.Equal(t, expectedPhysID, rpcRetryEvent.Extra["physical_attempt_id"])

	// 2. Assert zap logger actually emitted all fields including Extra (operation, attempt)
	var loggedRetryEntry *observer.LoggedEntry
	for _, entry := range observedLogs.All() {
		ctxMap := entry.ContextMap()
		if ctxMap["event"] == EventRPCRetry {
			e := entry
			loggedRetryEntry = &e
			break
		}
	}
	require.NotNil(t, loggedRetryEntry, "zap logger must emit lifecycle_event for rpc.retry")
	logCtx := loggedRetryEntry.ContextMap()
	require.Equal(t, task.Request().ID, logCtx["task_id"])
	require.Equal(t, task.Generation(), logCtx["attempt_id"])
	require.Equal(t, expectedPhysID, logCtx["physical_attempt_id"])
	require.Equal(t, "-100888", logCtx["chat_id"])
	require.Equal(t, int64(888), logCtx["message_id"])
	require.Equal(t, int64(targetDC), logCtx["dc"])
	require.Equal(t, "reader.chunk", logCtx["op"])
	require.Equal(t, int64(1), logCtx["physical_retries"])
	require.Equal(t, "reader.chunk", logCtx["operation"], "operation from Extra must be emitted in zap fields")
	require.Equal(t, int64(1), logCtx["attempt"], "attempt from Extra must be emitted in zap fields")

	// 3. Assert lifecycle event order: ingested -> admitted -> resolved -> download.started -> rpc.retry -> ssd.commit_prepared -> ssd.committed -> item.terminal
	expectedSequence := []string{
		EventItemIngested,
		EventItemAdmitted,
		EventItemResolved,
		EventDownloadStarted,
		EventRPCRetry,
		EventSSDCommitPrepared,
		EventSSDCommitted,
		EventItemTerminal,
	}
	actualTypes := make([]string, 0, len(events))
	for _, e := range events {
		actualTypes = append(actualTypes, e.Event)
	}
	seqIdx := 0
	for _, act := range actualTypes {
		if seqIdx < len(expectedSequence) && act == expectedSequence[seqIdx] {
			seqIdx++
		}
	}
	require.Equal(t, len(expectedSequence), seqIdx, "lifecycle event sequence incomplete: got %v, expected in-order %v", actualTypes, expectedSequence)
}
