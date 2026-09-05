package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/transfer"
	"github.com/Hittlert/TGX/internal/fscommit"
)

type orchMockMediaFile struct {
	loc tg.InputFileLocationClass
	sz  int64
	dc  int
}

func (m *orchMockMediaFile) Location() tg.InputFileLocationClass { return m.loc }
func (m *orchMockMediaFile) Size() int64                         { return m.sz }
func (m *orchMockMediaFile) DC() int                             { return m.dc }

type invokerFunc func(ctx context.Context, input bin.Encoder, output bin.Decoder) error

func (f invokerFunc) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return f(ctx, input, output)
}

type mockPool struct {
	invoker tg.Invoker
}

func (m *mockPool) Client(ctx context.Context, dc int) *tg.Client         { return nil }
func (m *mockPool) Takeout(ctx context.Context, dc int) *tg.Client        { return nil }
func (m *mockPool) TakeoutInvoker(ctx context.Context, dc int) tg.Invoker { return nil }
func (m *mockPool) Default(ctx context.Context) *tg.Client                { return nil }
func (m *mockPool) Invoker(ctx context.Context, dc int) tg.Invoker        { return m.invoker }
func (m *mockPool) DefaultInvoker(ctx context.Context) tg.Invoker         { return m.invoker }
func (m *mockPool) CDN(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error) {
	return nil, nil, nil
}
func (m *mockPool) Close() error { return nil }

type mockAccessWithPool struct {
	pool       dcpool.Pool
	resolveFn  func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error)
	resolveErr error
}

func (m *mockAccessWithPool) GetDialogs(ctx context.Context) ([]DialogDTO, error) { return nil, nil }
func (m *mockAccessWithPool) GetHistory(ctx context.Context, req HistoryRequest) ([]MessageDTO, error) {
	return nil, nil
}
func (m *mockAccessWithPool) ResolvePeerInfo(ctx context.Context, queryStr string) (DialogDTO, error) {
	return DialogDTO{}, nil
}
func (m *mockAccessWithPool) SyncPeers(ctx context.Context) error { return nil }
func (m *mockAccessWithPool) ResolveBatch(ctx context.Context, peer string, messageIDs []int) (map[int]ResolvedMedia, error) {
	return nil, nil
}
func (m *mockAccessWithPool) Pool() dcpool.Pool { return m.pool }

func (m *mockAccessWithPool) Resolve(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
	if m.resolveFn != nil {
		return m.resolveFn(ctx, peer, messageID)
	}
	if m.resolveErr != nil {
		return ResolvedMedia{}, m.resolveErr
	}
	return ResolvedMedia{
		File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
		Name:      "test.mp4",
		Size:      1024,
		DCID:      2,
		MediaType: "video",
		Date:      time.Now().Unix(),
	}, nil
}

func setupTestOrchestrator(t *testing.T, invoker tg.Invoker, resolveErr error) (*Orchestrator, *Registry, *Database, string) {
	access := &mockAccessWithPool{
		pool:       &mockPool{invoker: invoker},
		resolveErr: resolveErr,
	}
	return setupTestOrchestratorWithAccess(t, access)
}

func setupTestOrchestratorWithAccess(t *testing.T, access TelegramAccess) (*Orchestrator, *Registry, *Database, string) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to init test database: %v", err)
	}

	saveDir := filepath.Join(tempDir, "downloads")
	if err := os.MkdirAll(saveDir, 0o755); err != nil {
		t.Fatalf("failed to create save dir: %v", err)
	}

	registry := NewRegistry(10, 100, time.Now)
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 4,
		MaxFileThreads:  2,
		MaxDataInFlight: 10,
		TaskRetryHandler: func(taskCtx context.Context, event downloader.RetryEvent) {
			tc, _ := transfer.TransferTaskFromContext(taskCtx)
			physAttemptID, rangeLabel, ok := transfer.ExtractPhysicalAttempt(event.Err)
			if !ok || physAttemptID == "" {
				physAttemptID = tc.GetFailedAttempt(event.Operation)
				if physAttemptID == "" {
					physAttemptID = fmt.Sprintf("%s-p%d", tc.AttemptID, event.Attempt)
				}
			}
			op := event.Operation
			if rangeLabel != "" {
				op = fmt.Sprintf("%s:%s", event.Operation, rangeLabel)
			}
			extra := map[string]any{
				"operation":           event.Operation,
				"attempt":             event.Attempt,
				"physical_attempt_id": physAttemptID,
			}
			if rangeLabel != "" {
				extra["range"] = rangeLabel
			}
			EmitLifecycle(zap.NewNop(), LifecycleEvent{
				Event:             EventRPCRetry,
				TaskID:            tc.TaskID,
				AttemptID:         tc.AttemptID,
				PhysicalAttemptID: physAttemptID,
				ChatID:            tc.ChatID,
				MessageID:         tc.MessageID,
				DC:                tc.DCID,
				Op:                op,
				PhysicalRetries:   int64(event.Attempt),
				Error:             fmt.Sprintf("%v", event.Err),
				Extra:             extra,
			})
		},
	})
	ssdAdmission := fscommit.NewSSDAdmission(saveDir, 1<<20)

	orch := NewOrchestrator(
		db,
		transferMgr,
		ssdAdmission,
		nil,
		access,
		registry,
		zap.NewNop(),
		saveDir,
	)

	return orch, registry, db, saveDir
}

// 1. Unavailable: Telegram server returns FILE_REFERENCE_EXPIRED or unavailable.
// Assert: StateUnavailable, ErrorClass="unavailable", DB="unavailable", retry eligibility rejected.
func TestOrchestrator_Matrix_Unavailable(t *testing.T) {
	expiredErr := tgerr.New(400, "FILE_REFERENCE_EXPIRED")
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return expiredErr
	})

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_unavailable",
		Peer:         "-1001234567",
		MessageID:    101,
		FinalPath:    "Channel/2026_09/test.mp4",
		ExpectedSize: 1024,
	}

	snap, created, err := registry.Submit(req)
	if err != nil || !created {
		t.Fatalf("submit failed: %v", err)
	}

	task, err := registry.Next(context.Background())
	if err != nil {
		t.Fatalf("next task: %v", err)
	}

	orch.downloadOne(context.Background(), task)

	// Check Task Snapshot
	snap = task.Snapshot()
	if snap.State != StateUnavailable {
		t.Fatalf("expected StateUnavailable, got: %s", snap.State)
	}
	if snap.ErrorClass != "unavailable" {
		t.Fatalf("expected ErrorClass unavailable, got: %s", snap.ErrorClass)
	}

	// Check Database Record
	rec, err := db.GetDownloadRecord(req.Peer, req.MessageID)
	if err != nil || rec == nil {
		t.Fatalf("get record failed: %v", err)
	}
	if rec.Status != "unavailable" {
		t.Fatalf("expected db status unavailable, got: %s", rec.Status)
	}

	// Check Retry Eligibility: ordinary retry should be rejected
	retrySnap, retryCreated, err := registry.Submit(req)
	if err != nil {
		t.Fatalf("submit check error: %v", err)
	}
	if retryCreated {
		t.Fatalf("unavailable task should not be accepted as newly created without explicit retry")
	}
	if retrySnap.State != StateUnavailable {
		t.Fatalf("expected state to remain unavailable, got: %s", retrySnap.State)
	}

	// Check that no orphaned .part file exists
	partPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath)+".part")
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("expected .part to be cleaned up, but exists: %s", partPath)
	}
}

// 2. Permanent I/O: Writer failure during transfer.
// Assert: StateFailed, ErrorClass="io", DB="failed", cleanup verified.
func TestOrchestrator_Matrix_PermanentIO(t *testing.T) {
	data := make([]byte, 1024)
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		req, ok := input.(*tg.UploadGetFileRequest)
		if !ok {
			return errors.New("unexpected input")
		}
		resp, ok := output.(*tg.UploadFile)
		if !ok {
			return errors.New("unexpected output")
		}
		resp.Type = &tg.StorageFilePartial{}
		resp.Bytes = data[req.Offset : req.Offset+int64(req.Limit)]
		return nil
	})

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_io",
		Peer:         "-1001234567",
		MessageID:    102,
		FinalPath:    "ReadOnly/2026_09/test.mp4",
		ExpectedSize: 1024,
	}

	// Make directory read-only so part file creation or writing fails
	targetDir := filepath.Join(saveDir, "ReadOnly", "2026_09")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	partPath := filepath.Join(targetDir, "test.mp4.part")
	// Pre-create read-only part file to trigger open error
	f, _ := os.Create(partPath)
	_ = f.Close()
	_ = os.Chmod(partPath, 0o400)
	defer func() {
		_ = os.Chmod(partPath, 0o644)
		_ = os.Remove(partPath)
	}()

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed on I/O error, got: %s", snap.State)
	}
	if snap.ErrorClass != "io" {
		t.Fatalf("expected ErrorClass io, got: %s", snap.ErrorClass)
	}

	rec, err := db.GetDownloadRecord(req.Peer, req.MessageID)
	if err != nil || rec == nil {
		t.Fatalf("db record error: %v", err)
	}
	if rec.Status != "failed" {
		t.Fatalf("expected db status failed, got: %s", rec.Status)
	}
}

func setUploadFile(output bin.Decoder, data []byte) {
	if box, ok := output.(*tg.UploadFileBox); ok {
		box.File = &tg.UploadFile{
			Type:  &tg.StorageFilePartial{},
			Bytes: data,
		}
	} else if f, ok := output.(*tg.UploadFile); ok {
		f.Type = &tg.StorageFilePartial{}
		f.Bytes = data
	}
}

// 3. Incomplete Coverage: server returns short bytes.
// Assert: StateFailed, ErrorClass="corrupt", cleanup verified.
func TestOrchestrator_Matrix_IncompleteCoverage(t *testing.T) {
	// Server only returns 500 bytes for a 1024 byte expected file
	shortData := make([]byte, 500)
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, shortData)
		return nil
	})

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_corrupt",
		Peer:         "-1001234567",
		MessageID:    103,
		FinalPath:    "Corrupt/2026_09/test.mp4",
		ExpectedSize: 1024,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed on corrupt coverage, got: %s", snap.State)
	}
	if snap.ErrorClass != "corrupt" {
		t.Fatalf("expected ErrorClass corrupt, got: %s", snap.ErrorClass)
	}

	partPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath)+".part")
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file was not cleaned up: %s", partPath)
	}
}

// 4. Cancellation: context canceled during transfer.
// Assert: StateFailed, ErrorClass="canceled", cleanup verified.
func TestOrchestrator_Matrix_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	invoker := invokerFunc(func(invCtx context.Context, input bin.Encoder, output bin.Decoder) error {
		cancel() // cancel context on first chunk
		<-invCtx.Done()
		return invCtx.Err()
	})

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_canceled",
		Peer:         "-1001234567",
		MessageID:    104,
		FinalPath:    "Canceled/2026_09/test.mp4",
		ExpectedSize: 1024,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(ctx, task)

	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed on context cancel, got: %s", snap.State)
	}
	if snap.ErrorClass != "canceled" {
		t.Fatalf("expected ErrorClass canceled, got: %s", snap.ErrorClass)
	}

	partPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath)+".part")
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file was not cleaned up: %s", partPath)
	}
}

// 5. Gotd Retry Exhaustion: transport returns network errors until gotd retry limit.
// Assert: StateFailed, ErrorClass="network", cleanup verified.
func TestOrchestrator_Matrix_GotdExhaustion(t *testing.T) {
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return errors.New("connection reset by peer")
	})

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_exhaustion",
		Peer:         "-1001234567",
		MessageID:    105,
		FinalPath:    "Exhaustion/2026_09/test.mp4",
		ExpectedSize: 1024,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed on network exhaustion, got: %s", snap.State)
	}
	if snap.ErrorClass != "network" {
		t.Fatalf("expected ErrorClass network, got: %s", snap.ErrorClass)
	}

	partPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath)+".part")
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file was not cleaned up: %s", partPath)
	}
}

// 6. Success with physical retries: gotd recovers after 2 transient retries.
// Assert: StateSuccess, SHA-256 match, final file exists, .part cleaned up,
// physicalRetries recorded, exact request count, bounded request budget, wire bytes,
// and physical attempt correlation in lifecycle observer.
func TestOrchestrator_Matrix_SuccessWithPhysicalRetries(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	hash := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(hash[:])

	var callCount int64
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		count := atomic.AddInt64(&callCount, 1)
		if count <= 2 {
			return tgerr.New(500, tg.ErrTimeout)
		}
		setUploadFile(output, data)
		return nil
	})

	var observedEvents []LifecycleEvent
	var obsMu sync.Mutex
	unreg := RegisterLifecycleObserver(LifecycleObserverFunc(func(evt LifecycleEvent) {
		obsMu.Lock()
		defer obsMu.Unlock()
		observedEvents = append(observedEvents, evt)
	}))
	defer unreg()

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_success_retries",
		Peer:         "-1001234567",
		MessageID:    106,
		FinalPath:    "Success/2026_09/test.mp4",
		ExpectedSize: 1024,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateSuccess {
		t.Fatalf("expected StateSuccess, got: %s (err: %s)", snap.State, snap.Error)
	}
	if snap.SHA256 != expectedHex {
		t.Fatalf("expected sha256 %s, got: %s", expectedHex, snap.SHA256)
	}

	// 1. Physical retries recorded on task snapshot
	if snap.PhysicalRetries != 2 {
		t.Fatalf("expected physical_retries 2, got %d", snap.PhysicalRetries)
	}
	// 2. Exact request count: 2 timeouts + 1 success = 3 requests
	if snap.RequestCount != 3 {
		t.Fatalf("expected request_count 3, got %d", snap.RequestCount)
	}
	// 3. Request budget: bounded within 21 (1 chunk * 21 max attempts)
	expectedBudget := transfer.ComputeRequestBudget(1024, transfer.DefaultMaxRetryAttempts)
	if snap.RequestCount > expectedBudget {
		t.Fatalf("request_count %d exceeded bounded budget %d", snap.RequestCount, expectedBudget)
	}
	// 4. Wire bytes vs committed payload bytes
	if snap.Downloaded != 1024 {
		t.Fatalf("expected downloaded payload 1024, got %d", snap.Downloaded)
	}
	if snap.WireBytes != 1024 {
		t.Fatalf("expected wire bytes 1024, got %d", snap.WireBytes)
	}
	if snap.ReplayBytes != 0 {
		t.Fatalf("expected replay bytes 0 (timeouts failed before payload arrival), got %d", snap.ReplayBytes)
	}

	// Final destination file must exist
	finalPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath))
	finInfo, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("final file does not exist: %v", err)
	}
	if finInfo.Size() != 1024 {
		t.Fatalf("expected final file size 1024, got %d", finInfo.Size())
	}

	// .part must not exist
	partPath := finalPath + ".part"
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file was not cleaned up: %s", partPath)
	}

	// Verify lifecycle events carry physical-attempt identity and wire metrics
	obsMu.Lock()
	defer obsMu.Unlock()
	var terminalEvt *LifecycleEvent
	for _, e := range observedEvents {
		if e.Event == EventItemTerminal && e.TaskID == req.ID {
			terminalEvt = &e
			break
		}
	}
	if terminalEvt == nil {
		t.Fatal("expected EventItemTerminal lifecycle event")
	}
	if terminalEvt.PhysicalRetries != 2 {
		t.Fatalf("expected lifecycle physical_retries 2, got %d", terminalEvt.PhysicalRetries)
	}
	if terminalEvt.RequestCount != 3 {
		t.Fatalf("expected lifecycle request_count 3, got %d", terminalEvt.RequestCount)
	}
	if terminalEvt.WireBytes != 1024 {
		t.Fatalf("expected lifecycle wire_bytes 1024, got %d", terminalEvt.WireBytes)
	}

	expectedPhysID := fmt.Sprintf("%s-chunk-0-a3", task.Generation())
	if snap.PhysicalAttemptID != expectedPhysID {
		t.Fatalf("expected task physical_attempt_id %s, got %s", expectedPhysID, snap.PhysicalAttemptID)
	}
	if terminalEvt.PhysicalAttemptID != expectedPhysID {
		t.Fatalf("expected lifecycle terminal physical_attempt_id %s, got %s", expectedPhysID, terminalEvt.PhysicalAttemptID)
	}
	if terminalEvt.Extra["physical_attempt_id"] != expectedPhysID {
		t.Fatalf("expected lifecycle extra physical_attempt_id %s, got %v", expectedPhysID, terminalEvt.Extra["physical_attempt_id"])
	}

	// Verify distinct physical attempt IDs across EventRPCRetry events
	var rpcRetries []*LifecycleEvent
	for i := range observedEvents {
		if observedEvents[i].Event == EventRPCRetry && observedEvents[i].TaskID == req.ID {
			rpcRetries = append(rpcRetries, &observedEvents[i])
		}
	}
	if len(rpcRetries) != 2 {
		t.Fatalf("expected 2 EventRPCRetry events, got %d", len(rpcRetries))
	}
	if rpcRetries[0].PhysicalAttemptID != fmt.Sprintf("%s-chunk-0-a1", task.Generation()) {
		t.Fatalf("expected retry 1 physical attempt ID %s-chunk-0-a1, got %s", task.Generation(), rpcRetries[0].PhysicalAttemptID)
	}
	if rpcRetries[1].PhysicalAttemptID != fmt.Sprintf("%s-chunk-0-a2", task.Generation()) {
		t.Fatalf("expected retry 2 physical attempt ID %s-chunk-0-a2, got %s", task.Generation(), rpcRetries[1].PhysicalAttemptID)
	}
}

// 7. Production-path Wire and Replay Bytes Accounting test:
// Proves:
// - Physical replay bytes are distinguishable from unique committed payload bytes;
// - Total wire bytes equals committed payload bytes + physical replay bytes;
// - Exact accounting is captured on TaskSnapshot and Lifecycle telemetry.
func TestOrchestrator_ProductionPath_WireAndReplayBytesAccounting(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte((i * 19) % 256)
	}
	hash := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(hash[:])

	var callCount int64
	// In this test, the invoker delivers 1024 bytes over the wire on call 1, but then gotd triggers
	// a retry because call 1 returns a transient error after payload delivery.
	// Call 2 delivers 1024 bytes over the wire and succeeds.
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		count := atomic.AddInt64(&callCount, 1)
		setUploadFile(output, data)
		if count == 1 {
			return tgerr.New(500, tg.ErrTimeout)
		}
		return nil
	})

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_wire_replay_bytes",
		Peer:         "-1001234567",
		MessageID:    107,
		FinalPath:    "Replay/test_replay.mp4",
		ExpectedSize: 1024,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateSuccess {
		t.Fatalf("expected StateSuccess, got: %s (err: %s)", snap.State, snap.Error)
	}
	if snap.SHA256 != expectedHex {
		t.Fatalf("expected sha256 %s, got: %s", expectedHex, snap.SHA256)
	}

	// Unique committed payload bytes
	if snap.Downloaded != 1024 {
		t.Fatalf("expected downloaded payload 1024, got %d", snap.Downloaded)
	}
	// Wire bytes delivered across 2 RPC attempts: 1024 + 1024 = 2048 bytes
	if snap.WireBytes != 2048 {
		t.Fatalf("expected wire bytes 2048, got %d", snap.WireBytes)
	}
	// Physical replay bytes: 2048 - 1024 = 1024 bytes
	if snap.ReplayBytes != 1024 {
		t.Fatalf("expected replay bytes 1024, got %d", snap.ReplayBytes)
	}
	if snap.WireBytes != snap.Downloaded+snap.ReplayBytes {
		t.Fatalf("invariant violation: wire_bytes (%d) != downloaded (%d) + replay_bytes (%d)",
			snap.WireBytes, snap.Downloaded, snap.ReplayBytes)
	}
	if snap.PhysicalRetries != 1 {
		t.Fatalf("expected physical retries 1, got %d", snap.PhysicalRetries)
	}
	if snap.RequestCount != 2 {
		t.Fatalf("expected request count 2, got %d", snap.RequestCount)
	}

	finalPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath))
	finInfo, err := os.Stat(finalPath)
	if err != nil || finInfo.Size() != 1024 {
		t.Fatalf("final destination file invalid: %v", err)
	}
}

// 8. FloodWait ownership and shared-cooldown test:
// Proves:
// - gotd handles FloodWait retry internally;
// - DataGate enforces shared cooldown across tasks targeting the same DC;
// - Outer layer (Orchestrator) does NOT multiply requests or replay whole files;
// - Exactly 2 requests for Task 1 (1 FloodWait + 1 success retry), exactly 1 request for Task 2.
func TestOrchestrator_ProductionPath_FloodWaitSharedCooldownNoMultiplication(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte((i * 13) % 256)
	}

	var task1Calls, task2Calls int64
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		tc, _ := transfer.TransferTaskFromContext(ctx)
		if tc.TaskID == "flood_task_1" {
			c := atomic.AddInt64(&task1Calls, 1)
			if c == 1 {
				// Server responds with 1-second FloodWait
				return tgerr.New(420, "FLOOD_WAIT_1")
			}
			setUploadFile(output, data)
			return nil
		}
		// Task 2
		atomic.AddInt64(&task2Calls, 1)
		setUploadFile(output, data)
		return nil
	})

	orch, registry, db, _ := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	req1 := TaskRequest{
		ID:           "flood_task_1",
		Peer:         "-1001234567",
		MessageID:    201,
		FinalPath:    "Flood/test1.mp4",
		ExpectedSize: 1024,
	}
	req2 := TaskRequest{
		ID:           "flood_task_2",
		Peer:         "-1001234567",
		MessageID:    202,
		FinalPath:    "Flood/test2.mp4",
		ExpectedSize: 1024,
	}

	_, _, _ = registry.Submit(req1)
	_, _, _ = registry.Submit(req2)
	task1, _ := registry.Next(context.Background())
	task2, _ := registry.Next(context.Background())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		orch.downloadOne(context.Background(), task1)
	}()
	// Start task2 concurrently shortly after task1 triggers FloodWait
	time.Sleep(50 * time.Millisecond)
	go func() {
		defer wg.Done()
		orch.downloadOne(context.Background(), task2)
	}()
	wg.Wait()

	snap1 := task1.Snapshot()
	snap2 := task2.Snapshot()

	if snap1.State != StateSuccess {
		t.Fatalf("task1 failed: state=%s, err=%s", snap1.State, snap1.Error)
	}
	if snap2.State != StateSuccess {
		t.Fatalf("task2 failed: state=%s, err=%s", snap2.State, snap2.Error)
	}

	// Task 1: exactly 2 requests (1 initial FloodWait + 1 internal gotd retry).
	// Crucially: NOT multiplied by the outer layer!
	if snap1.RequestCount != 2 {
		t.Fatalf("expected task1 request count 2 (1 flood + 1 retry), got %d (outer layer multiplied requests!)", snap1.RequestCount)
	}
	if snap1.PhysicalRetries != 1 {
		t.Fatalf("expected task1 physical retries 1, got %d", snap1.PhysicalRetries)
	}

	// Task 2: exactly 1 request because DataGate waited out the shared cooldown on DC 2 before dispatching
	if snap2.RequestCount != 1 {
		t.Fatalf("expected task2 request count 1, got %d", snap2.RequestCount)
	}
	if snap2.PhysicalRetries != 0 {
		t.Fatalf("expected task2 physical retries 0, got %d", snap2.PhysicalRetries)
	}
}

func TestOrchestrator_CanonicalPathAssertedInE2E(t *testing.T) {
	expectedPayload := make([]byte, 1024)
	for i := range expectedPayload {
		expectedPayload[i] = byte(i % 251)
	}
	h := sha256.Sum256(expectedPayload)
	expectedHex := hex.EncodeToString(h[:])

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, expectedPayload)
		return nil
	})

	fixedDate := int64(1725148800) // 2024-09-01 -> 2024_09
	chatID := "-100777888"
	msgID := 888

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "clip.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      fixedDate,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	// Seed target in DB
	_, _ = db.Execute(`
		INSERT INTO listen_targets (chat_id, enabled, title, username, priority, created_at, updated_at)
		VALUES (?, 1, 'My Special Channel', 'special', 10, ?, ?)
	`, chatID, fixedDate, fixedDate)

	req := TaskRequest{
		ID:          "case_canonical_path_e2e",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "My Special Channel",
		MediaType:   "video",
		FileName:    "clip.mp4",
		Date:        fixedDate,
		// FinalPath is deliberately EMPTY so PathPlanner plans it!
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateSuccess {
		t.Fatalf("expected StateSuccess, got: %s (err: %s)", snap.State, snap.Error)
	}
	if snap.SHA256 != expectedHex {
		t.Fatalf("expected sha256 %s, got: %s", expectedHex, snap.SHA256)
	}

	// Assert the EXACT canonical relative path
	expectedRelPath := "My Special Channel/2024_09/888 - clip.mp4"
	if snap.FinalPath != expectedRelPath {
		t.Fatalf("expected canonical path %q, got: %q", expectedRelPath, snap.FinalPath)
	}

	// Final destination file must exist at the expected canonical path on disk
	finalPath := filepath.Join(saveDir, filepath.FromSlash(expectedRelPath))
	finInfo, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("final file does not exist at canonical path %q: %v", finalPath, err)
	}
	if finInfo.Size() != 1024 {
		t.Fatalf("expected final file size 1024, got %d", finInfo.Size())
	}

	// Also verify durable DB record holds the exact canonical path
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("failed to query download record: %v", err)
	}
	if rec.SavePath != expectedRelPath {
		t.Fatalf("expected DB SavePath %q, got %q", expectedRelPath, rec.SavePath)
	}
}

// 9. Exact request budget exhaustion at boundary test:
// Proves:
// - Declared request budget is strictly enforced at the authoritative request/retry boundary;
// - Requests beyond declared budget are rejected with ErrRequestBudgetExhausted;
// - Raw invoker is NOT called beyond declared budget;
// - Task fails cleanly with StateFailed, error class "network", non-retryable;
// - .part file is removed, DB record is marked failed;
// - Distinct physical attempt identity is emitted and correlated across retries and terminal event.
func TestOrchestrator_ExactBudgetExhaustionAtBoundary(t *testing.T) {
	var rawCalls int64
	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		atomic.AddInt64(&rawCalls, 1)
		return tgerr.New(500, tg.ErrTimeout)
	})

	var observedEvents []LifecycleEvent
	var obsMu sync.Mutex
	unreg := RegisterLifecycleObserver(LifecycleObserverFunc(func(evt LifecycleEvent) {
		obsMu.Lock()
		defer obsMu.Unlock()
		observedEvents = append(observedEvents, evt)
	}))
	defer unreg()

	orch, registry, db, saveDir := setupTestOrchestrator(t, invoker, nil)
	defer db.Close()

	// MaxRetries = 2: 1 initial chunk request + 2 retries = declared budget 3
	req := TaskRequest{
		ID:           "case_exact_budget_exhaustion",
		Peer:         "-1001234567",
		MessageID:    109,
		FinalPath:    "Exhaust/exhaust_test.mp4",
		ExpectedSize: 1024,
		MaxRetries:   2,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed, got: %s (err: %s)", snap.State, snap.Error)
	}
	if snap.ErrorClass != "network" {
		t.Fatalf("expected error class network, got: %s", snap.ErrorClass)
	}
	if snap.Retryable {
		t.Fatalf("expected retryable=false on budget exhaustion, got %t", snap.Retryable)
	}
	if !strings.Contains(snap.Error, "request budget exhausted") {
		t.Fatalf("expected 'request budget exhausted' error message, got: %s", snap.Error)
	}

	// Raw invoker was called strictly 3 times (the 4th attempt was blocked at boundary before dispatch!)
	if rawCalls != 3 {
		t.Fatalf("expected strictly 3 raw calls executed to Telegram, got %d", rawCalls)
	}
	if snap.RequestCount != 3 {
		t.Fatalf("expected snap.RequestCount == 3, got %d", snap.RequestCount)
	}

	expectedPhysID := fmt.Sprintf("%s-chunk-0-a4", task.Generation())
	if snap.PhysicalAttemptID != expectedPhysID {
		t.Fatalf("expected task physical attempt ID %s, got %s", expectedPhysID, snap.PhysicalAttemptID)
	}

	// .part must not exist
	finalPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath))
	partPath := finalPath + ".part"
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file was not cleaned up after failure: %s", partPath)
	}

	// DB record must be failed
	rec, err := db.GetDownloadRecord(req.Peer, req.MessageID)
	if err != nil || rec == nil {
		t.Fatalf("failed to query download record: %v", err)
	}
	if rec.Status != "failed" {
		t.Fatalf("expected DB record status failed, got: %s", rec.Status)
	}
	if !strings.Contains(rec.Error, "network") || !strings.Contains(rec.Error, "request budget exhausted") {
		t.Fatalf("expected DB error to record network class and request budget exhausted, got: %s", rec.Error)
	}

	// Verify lifecycle events
	obsMu.Lock()
	defer obsMu.Unlock()
	var terminalEvt *LifecycleEvent
	var rpcRetries []*LifecycleEvent
	for i := range observedEvents {
		if observedEvents[i].TaskID == req.ID {
			if observedEvents[i].Event == EventItemTerminal {
				terminalEvt = &observedEvents[i]
			} else if observedEvents[i].Event == EventRPCRetry {
				rpcRetries = append(rpcRetries, &observedEvents[i])
			}
		}
	}

	if terminalEvt == nil {
		t.Fatal("expected EventItemTerminal lifecycle event for budget exhaustion failure")
	}
	if terminalEvt.Status != "failed" {
		t.Fatalf("expected terminal status failed, got: %s", terminalEvt.Status)
	}
	if terminalEvt.ErrorClass != "network" {
		t.Fatalf("expected terminal error_class network, got: %s", terminalEvt.ErrorClass)
	}
	if terminalEvt.PhysicalAttemptID != expectedPhysID {
		t.Fatalf("expected terminal physical_attempt_id %s, got %s", expectedPhysID, terminalEvt.PhysicalAttemptID)
	}
	if terminalEvt.RequestCount != 3 {
		t.Fatalf("expected terminal request_count 3, got %d", terminalEvt.RequestCount)
	}
	if terminalEvt.Extra["request_budget"] != int64(3) {
		t.Fatalf("expected request_budget 3 in terminal extra, got %v", terminalEvt.Extra["request_budget"])
	}

	// Check that physical attempt IDs across retries are distinct and tied to chunk-0
	if len(rpcRetries) != 3 {
		t.Fatalf("expected 3 EventRPCRetry events, got %d", len(rpcRetries))
	}
	if rpcRetries[0].PhysicalAttemptID != fmt.Sprintf("%s-chunk-0-a1", task.Generation()) {
		t.Fatalf("expected retry 1 physical attempt ID %s-chunk-0-a1, got %s", task.Generation(), rpcRetries[0].PhysicalAttemptID)
	}
	if rpcRetries[1].PhysicalAttemptID != fmt.Sprintf("%s-chunk-0-a2", task.Generation()) {
		t.Fatalf("expected retry 2 physical attempt ID %s-chunk-0-a2, got %s", task.Generation(), rpcRetries[1].PhysicalAttemptID)
	}
	if rpcRetries[2].PhysicalAttemptID != fmt.Sprintf("%s-chunk-0-a3", task.Generation()) {
		t.Fatalf("expected retry 3 physical attempt ID %s-chunk-0-a3, got %s", task.Generation(), rpcRetries[2].PhysicalAttemptID)
	}
}

// 10. Multi-part concurrent retry fixture proving uniqueness and causality:
// Proves:
// - Parallel chunk requests do NOT share a physical attempt ID (no collision, no shared p0);
// - Concurrently executing retries receive unique identities bound to the chunk range being attempted;
// - Retry events carry physical attempt IDs causally linked to the specific failing range;
// - All physical attempt IDs across concurrent chunks are strictly unique.
func TestOrchestrator_MultiPartConcurrentRetry_UniquenessAndCausality(t *testing.T) {
	var (
		invocationsMu sync.Mutex
		allPhysIDs    []string
		chunk1Calls   int64
	)

	// File size: 2 * 512 KiB = 1048576 bytes (2 chunks: offset 0 and offset 524288)
	chunkSize := 512 * 1024
	totalSize := int64(chunkSize * 2)

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		req, ok := input.(*tg.UploadGetFileRequest)
		if !ok {
			return errors.New("unsupported request type")
		}

		physID := transfer.PhysicalAttemptIDFromContext(ctx)
		invocationsMu.Lock()
		allPhysIDs = append(allPhysIDs, physID)
		invocationsMu.Unlock()

		if req.Offset == int64(chunkSize) {
			call := atomic.AddInt64(&chunk1Calls, 1)
			if call == 1 {
				// Inject transient timeout on attempt 1 of chunk 1 to trigger gotd retry
				return tgerr.New(500, tg.ErrTimeout)
			}
		}

		remaining := totalSize - req.Offset
		if remaining <= 0 {
			setUploadFile(output, []byte{})
			return nil
		}
		limit := req.Limit
		if remaining < int64(limit) {
			limit = int(remaining)
		}
		data := make([]byte, limit)
		for i := range data {
			data[i] = byte((int(req.Offset) + i) % 256)
		}
		setUploadFile(output, data)
		return nil
	})

	var observedEvents []LifecycleEvent
	var obsMu sync.Mutex
	unreg := RegisterLifecycleObserver(LifecycleObserverFunc(func(evt LifecycleEvent) {
		obsMu.Lock()
		defer obsMu.Unlock()
		observedEvents = append(observedEvents, evt)
	}))
	defer unreg()

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 9999, AccessHash: 8888}, sz: totalSize, dc: 2},
				Name:      "multipart_test.bin",
				Size:      totalSize,
				DCID:      2,
				MediaType: "document",
				Date:      time.Now().Unix(),
			}, nil
		},
	}
	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_multipart_concurrent_retry",
		Peer:         "-1009999",
		MessageID:    555,
		FinalPath:    "Concurrent/multipart_test.bin",
		ExpectedSize: totalSize,
		MaxRetries:   3,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateSuccess {
		t.Fatalf("expected StateSuccess, got: %s (err: %s)", snap.State, snap.Error)
	}

	// Verify file was written to disk
	finalPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath))
	info, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("final file does not exist: %v", err)
	}
	if info.Size() != totalSize {
		t.Fatalf("expected size %d, got %d", totalSize, info.Size())
	}

	// Verify that physical attempt IDs are strictly unique across all invocations
	invocationsMu.Lock()
	defer invocationsMu.Unlock()
	seen := make(map[string]bool)
	for _, id := range allPhysIDs {
		if seen[id] {
			t.Fatalf("detected COLLISION in physical attempt ID: %s (all: %v)", id, allPhysIDs)
		}
		seen[id] = true
	}

	// Verify Chunk 0 and Chunk 1 have distinct range labels in their attempt IDs
	hasChunk0 := false
	hasChunk1A1 := false
	hasChunk1A2 := false
	gen := task.Generation()
	for _, id := range allPhysIDs {
		if strings.Contains(id, fmt.Sprintf("%s-chunk-0-a1", gen)) {
			hasChunk0 = true
		}
		if strings.Contains(id, fmt.Sprintf("%s-chunk-%d-a1", gen, chunkSize)) {
			hasChunk1A1 = true
		}
		if strings.Contains(id, fmt.Sprintf("%s-chunk-%d-a2", gen, chunkSize)) {
			hasChunk1A2 = true
		}
	}
	if !hasChunk0 {
		t.Fatalf("missing physical invocation for chunk 0: all = %v", allPhysIDs)
	}
	if !hasChunk1A1 {
		t.Fatalf("missing physical invocation for chunk 1 attempt 1: all = %v", allPhysIDs)
	}
	if !hasChunk1A2 {
		t.Fatalf("missing physical invocation for chunk 1 attempt 2 (retry): all = %v", allPhysIDs)
	}

	// Verify Lifecycle EventRPCRetry captured the exact chunk 1 attempt 1 that failed
	obsMu.Lock()
	defer obsMu.Unlock()
	var retryEvt *LifecycleEvent
	for i := range observedEvents {
		if observedEvents[i].TaskID == req.ID && observedEvents[i].Event == EventRPCRetry {
			retryEvt = &observedEvents[i]
			break
		}
	}
	if retryEvt == nil {
		t.Fatal("expected EventRPCRetry to be emitted for chunk 1 failure")
	}
	expectedRetryPhysID := fmt.Sprintf("%s-chunk-%d-a1", gen, chunkSize)
	if retryEvt.PhysicalAttemptID != expectedRetryPhysID {
		t.Fatalf("expected retry event to carry causally linked physical attempt %s, got %s", expectedRetryPhysID, retryEvt.PhysicalAttemptID)
	}
}

// 11. Two concurrent failing chunks with interleaved retries:
// Strictly asserts that when multiple chunks fail concurrently, each RetryEvent authoritatively
// carries its OWN physical attempt ID (derived directly from the failed invocation error) without
// any cross-talk, race conditions, or fallback to the other chunk's ID.
func TestOrchestrator_TwoConcurrentChunkFailures_InterleavedCausality(t *testing.T) {
	var (
		invocationsMu sync.Mutex
		allPhysIDs    []string
		chunk0Calls   int64
		chunk1Calls   int64
	)

	chunkSize := 512 * 1024
	totalSize := int64(chunkSize * 2)

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		req, ok := input.(*tg.UploadGetFileRequest)
		if !ok {
			return errors.New("unsupported request type")
		}

		physID := transfer.PhysicalAttemptIDFromContext(ctx)
		invocationsMu.Lock()
		allPhysIDs = append(allPhysIDs, physID)
		invocationsMu.Unlock()

		if req.Offset == 0 {
			call := atomic.AddInt64(&chunk0Calls, 1)
			if call == 1 {
				// Chunk 0 fails on attempt 1 with transient timeout
				return tgerr.New(500, tg.ErrTimeout)
			}
		} else if req.Offset == int64(chunkSize) {
			call := atomic.AddInt64(&chunk1Calls, 1)
			if call == 1 {
				// Chunk 1 fails on attempt 1 with transient timeout
				return tgerr.New(500, tg.ErrTimeout)
			}
		}

		remaining := totalSize - req.Offset
		if remaining <= 0 {
			setUploadFile(output, []byte{})
			return nil
		}
		limit := req.Limit
		if remaining < int64(limit) {
			limit = int(remaining)
		}
		data := make([]byte, limit)
		for i := range data {
			data[i] = byte((int(req.Offset) + i) % 256)
		}
		setUploadFile(output, data)
		return nil
	})

	var observedEvents []LifecycleEvent
	var obsMu sync.Mutex
	unreg := RegisterLifecycleObserver(LifecycleObserverFunc(func(evt LifecycleEvent) {
		obsMu.Lock()
		defer obsMu.Unlock()
		observedEvents = append(observedEvents, evt)
	}))
	defer unreg()

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 7777, AccessHash: 6666}, sz: totalSize, dc: 2},
				Name:      "concurrent_failures.bin",
				Size:      totalSize,
				DCID:      2,
				MediaType: "document",
				Date:      time.Now().Unix(),
			}, nil
		},
	}
	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	req := TaskRequest{
		ID:           "case_two_concurrent_failures",
		Peer:         "-1007777",
		MessageID:    666,
		FinalPath:    "Concurrent/concurrent_failures.bin",
		ExpectedSize: totalSize,
		MaxRetries:   3,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateSuccess {
		t.Fatalf("expected StateSuccess, got: %s (err: %s)", snap.State, snap.Error)
	}

	// Verify file on disk
	finalPath := filepath.Join(saveDir, filepath.FromSlash(req.FinalPath))
	info, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("final file missing: %v", err)
	}
	if info.Size() != totalSize {
		t.Fatalf("expected size %d, got %d", totalSize, info.Size())
	}

	// Find the retry events for this task
	obsMu.Lock()
	defer obsMu.Unlock()
	var retries []LifecycleEvent
	for _, evt := range observedEvents {
		if evt.TaskID == req.ID && evt.Event == EventRPCRetry {
			retries = append(retries, evt)
		}
	}

	if len(retries) != 2 {
		t.Fatalf("expected exactly 2 EventRPCRetry events for the two failing chunks, got %d", len(retries))
	}

	gen := task.Generation()
	expected0 := fmt.Sprintf("%s-chunk-0-a1", gen)
	expected1 := fmt.Sprintf("%s-chunk-%d-a1", gen, chunkSize)

	var found0, found1 bool
	for _, r := range retries {
		if r.PhysicalAttemptID == expected0 {
			found0 = true
			if r.Op != "reader.chunk:chunk-0" {
				t.Fatalf("expected op reader.chunk:chunk-0 for chunk 0, got %s", r.Op)
			}
			if r.Extra["range"] != "chunk-0" {
				t.Fatalf("expected extra.range chunk-0, got %v", r.Extra["range"])
			}
		}
		if r.PhysicalAttemptID == expected1 {
			found1 = true
			if r.Op != fmt.Sprintf("reader.chunk:chunk-%d", chunkSize) {
				t.Fatalf("expected op reader.chunk:chunk-%d for chunk 1, got %s", chunkSize, r.Op)
			}
			if r.Extra["range"] != fmt.Sprintf("chunk-%d", chunkSize) {
				t.Fatalf("expected extra.range chunk-%d, got %v", chunkSize, r.Extra["range"])
			}
		}
	}

	if !found0 {
		t.Fatalf("did not find RetryEvent for chunk 0 with attempt ID %s (got: %v)", expected0, retries)
	}
	if !found1 {
		t.Fatalf("did not find RetryEvent for chunk 1 with attempt ID %s (got: %v)", expected1, retries)
	}
}

func TestOrchestrator_BeginDownloadRejectionFailsAdmissionImmediately(t *testing.T) {
	resolveCalled := false
	access := &mockAccessWithPool{
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			resolveCalled = true
			return ResolvedMedia{}, errors.New("should not be called when BeginDownload fails")
		},
	}
	orch, registry, db, _ := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	chatID := "-1008888"
	msgID := 999
	now := time.Now().Unix()

	// Pre-seed an unavailable record in DB
	_, _ = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, media_type, file_size, attempt_generation, created_at, updated_at)
		VALUES (?, ?, 'unavailable', 'test.bin', 'test.bin', 'document', 100, 'old_gen', ?, ?)
	`, chatID, msgID, now, now)

	req := TaskRequest{
		ID:           "case_begin_download_reject",
		Peer:         chatID,
		MessageID:    msgID,
		FinalPath:    "Vault/test.bin",
		ExpectedSize: 100,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed, got: %s", snap.State)
	}
	if snap.ErrorClass != "db_conflict" {
		t.Fatalf("expected ErrorClass 'db_conflict', got: %s", snap.ErrorClass)
	}
	if resolveCalled {
		t.Fatal("TelegramAccess.Resolve was called despite BeginDownload rejection at admission!")
	}
}

func TestOrchestrator_PlannedPathPersistedBeforeFilesystem_RecoveryHandlesRealPart(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	transferActive := make(chan struct{}, 1)
	unblockTransfer := make(chan struct{})

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		select {
		case transferActive <- struct{}{}:
		default:
		}
		select {
		case <-unblockTransfer:
			setUploadFile(output, data)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "test_video.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400, // 2024-09-05 UTC
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	chatID := "-1008888"
	msgID := 777
	req := TaskRequest{
		ID:          "case_planned_path_recovery",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "AlphaChannel",
		Date:        1725518400,
	}

	_, _, err := registry.Submit(req)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	task, _ := registry.Next(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		orch.downloadOne(ctx, task)
	}()

	// Wait until transfer is active
	select {
	case <-transferActive:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for transfer to become active")
	}

	// 1. Assert canonical path is expected: "AlphaChannel/2024_09/777 - test_video.mp4"
	expectedCanonicalRelPath := "AlphaChannel/2024_09/777 - test_video.mp4"
	expectedPartPath := filepath.Join(saveDir, filepath.FromSlash(expectedCanonicalRelPath)+".part")

	// 2. Verify that while transfer is active, DB already contains the exact canonical path (not empty!)
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("failed to query DB record: %v", err)
	}
	if rec.SavePath != expectedCanonicalRelPath {
		t.Fatalf("DB save_path not updated before transfer: got %q, want %q", rec.SavePath, expectedCanonicalRelPath)
	}
	if rec.Status != "downloading" {
		t.Fatalf("expected DB status 'downloading', got %q", rec.Status)
	}

	// 3. Verify .part file physically exists on disk
	if _, err := os.Stat(expectedPartPath); err != nil {
		t.Fatalf("expected .part file to exist at %q, err: %v", expectedPartPath, err)
	}

	// 4. Simulate crash / cancellation
	cancel()
	<-done

	// Re-create the .part file to simulate an interrupted process leaving the .part file on disk before restart
	if err := os.WriteFile(expectedPartPath, []byte("interrupted data"), 0o644); err != nil {
		t.Fatalf("failed to write simulated crash part file: %v", err)
	}
	// And ensure DB status is 'downloading' (as it would be during a sudden crash)
	_, _ = db.Execute(`UPDATE download_records SET status = 'downloading' WHERE chat_id = ? AND message_id = ?`, chatID, msgID)

	// 5. Execute ReconcileOnStartup
	recErr := ReconcileOnStartup(context.Background(), db, saveDir, "", zap.NewNop())
	if recErr != nil {
		t.Fatalf("ReconcileOnStartup failed: %v", recErr)
	}

	// 6. Assert real .part file was found and cleaned up, NOT orphaned!
	if _, err := os.Stat(expectedPartPath); !os.IsNotExist(err) {
		t.Fatalf("expected real .part file to be removed by recovery, but it still exists: %v", err)
	}

	// 7. Assert DB record was reset to pending with canonical save_path preserved
	recAfter, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || recAfter == nil {
		t.Fatalf("failed to query DB record after recovery: %v", err)
	}
	if recAfter.Status != "pending" {
		t.Fatalf("expected status 'pending' after recovery, got %q", recAfter.Status)
	}
	if recAfter.SavePath != expectedCanonicalRelPath {
		t.Fatalf("expected canonical path preserved after recovery, got %q", recAfter.SavePath)
	}
}

func TestOrchestrator_CancelDuringFsyncHashRename_CleansPartAndDoesNotPublish(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	transferDone := make(chan struct{})
	var orchRef *Orchestrator
	var chatID = "-1007777"
	var msgID = 888

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		// Cancel target as soon as transfer finishes (entering fsync/hash/rename window)
		if orchRef != nil {
			orchRef.CancelTasksByChatID(chatID)
		}
		close(transferDone)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "cancel_test.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	orchRef = orch
	defer db.Close()

	req := TaskRequest{
		ID:          "case_cancel_production_path",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "CancelChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)
	<-transferDone

	finalRelPath := "CancelChannel/2024_09/888 - cancel_test.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))
	partAbsPath := finalAbsPath + ".part"

	// 1. Final file must NOT be published
	if _, err := os.Stat(finalAbsPath); !os.IsNotExist(err) {
		t.Fatalf("final file should not exist after cancellation, but was found: %v", err)
	}

	// 2. .part file must NOT remain on disk
	if _, err := os.Stat(partAbsPath); !os.IsNotExist(err) {
		t.Fatalf(".part file should be removed after cancellation, but was found: %v", err)
	}

	// 3. DB status must be 'failed' (canceled), NEVER 'success'
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("failed to query DB record: %v", err)
	}
	if rec.Status != "failed" {
		t.Fatalf("expected DB status 'failed', got: %q", rec.Status)
	}

	// 4. Registry task state must be StateFailed with canceled class
	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected Registry StateFailed, got: %s", snap.State)
	}
	if snap.ErrorClass != "canceled" {
		t.Fatalf("expected ErrorClass 'canceled', got: %s", snap.ErrorClass)
	}
}

func TestOrchestrator_AlreadySuccessVerifiesPhysicalFileOnDisk(t *testing.T) {
	orch, registry, db, saveDir := setupTestOrchestrator(t, nil, nil)
	defer db.Close()

	chatID := "-1006666"
	msgID := 333
	now := time.Now().Unix()
	relPath := "Vault/2026_09/333 - doc.bin"
	absPath := filepath.Join(saveDir, filepath.FromSlash(relPath))

	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	realHash := sha256.Sum256(payload)
	realSHA := hex.EncodeToString(realHash[:])

	// Pre-seed DB with success
	_, err := db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, media_type, file_size, sha256, attempt_generation, created_at, updated_at)
		VALUES (?, ?, 'success', 'doc.bin', ?, 'document', 512, ?, 'gen_done', ?, ?)
	`, chatID, msgID, relPath, realSHA, now, now)
	if err != nil {
		t.Fatalf("seed DB failed: %v", err)
	}

	// 1. Physical file is MISSING on disk: downloadOne must fail with missing_file
	t.Logf("[BRANCH 1: MISSING] Testing AlreadySuccess when physical file is missing from disk: %s", absPath)
	reqMissing := TaskRequest{
		ID:           "case_already_success_missing",
		Peer:         chatID,
		MessageID:    msgID,
		FinalPath:    relPath,
		ExpectedSize: 512,
	}
	_, _, _ = registry.Submit(reqMissing)
	task1, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task1)

	snap1 := task1.Snapshot()
	t.Logf("[BRANCH 1: MISSING] Result: state=%s, error_class=%s, err=%s", snap1.State, snap1.ErrorClass, snap1.Error)
	if snap1.State != StateFailed {
		t.Fatalf("expected StateFailed when physical file is missing, got: %s", snap1.State)
	}
	if snap1.ErrorClass != "missing_file" {
		t.Fatalf("expected ErrorClass 'missing_file', got: %s", snap1.ErrorClass)
	}

	// 2. Physical file exists but is CORRUPTED (same size, wrong SHA): must fail with class 'corrupt'
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(absPath, make([]byte, 512), 0o644); err != nil { // all zero bytes != payload
		t.Fatalf("write file failed: %v", err)
	}
	t.Logf("[BRANCH 2: CORRUPT] Created corrupted physical file with same size (512 bytes zeros, wrong hash)")

	reqCorrupt := TaskRequest{
		ID:           "case_already_success_corrupt",
		Peer:         chatID,
		MessageID:    msgID,
		FinalPath:    relPath,
		ExpectedSize: 512,
	}
	_, _, _ = registry.Submit(reqCorrupt)
	taskCorrupt, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), taskCorrupt)

	snapCorrupt := taskCorrupt.Snapshot()
	t.Logf("[BRANCH 2: CORRUPT] Result: state=%s, error_class=%s, err=%s", snapCorrupt.State, snapCorrupt.ErrorClass, snapCorrupt.Error)
	if snapCorrupt.State != StateFailed {
		t.Fatalf("expected StateFailed on corrupted physical file, got: %s", snapCorrupt.State)
	}
	if snapCorrupt.ErrorClass != "corrupt" {
		t.Fatalf("expected ErrorClass 'corrupt', got: %s", snapCorrupt.ErrorClass)
	}

	// 3. Physical file EXISTS with matching size and matching SHA-256: downloadOne must succeed
	if err := os.WriteFile(absPath, payload, 0o644); err != nil {
		t.Fatalf("write authentic payload failed: %v", err)
	}
	t.Logf("[BRANCH 3: AUTHENTIC] Written authentic payload matching DB proof (512 bytes, sha=%s)", realSHA)

	reqPresent := TaskRequest{
		ID:           "case_already_success_present",
		Peer:         chatID,
		MessageID:    msgID,
		FinalPath:    relPath,
		ExpectedSize: 512,
	}
	_, _, _ = registry.Submit(reqPresent)
	task2, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task2)

	snap2 := task2.Snapshot()
	t.Logf("[BRANCH 3: AUTHENTIC] Result: state=%s, final_path=%s", snap2.State, snap2.FinalPath)
	if snap2.State != StateSuccess {
		t.Fatalf("expected StateSuccess when physical file is verified, got: %s", snap2.State)
	}
	if snap2.FinalPath != relPath {
		t.Fatalf("expected FinalPath %q, got %q", relPath, snap2.FinalPath)
	}

	// 4. Conflicting request path with DB success proof: admission rejected
	t.Logf("[BRANCH 4: CONFLICT] Submitting conflicting path request against authoritative DB proof...")
	reqConflict := TaskRequest{
		ID:           "case_already_success_conflict",
		Peer:         chatID,
		MessageID:    msgID,
		FinalPath:    "Different/Path/333 - doc.bin",
		ExpectedSize: 512,
	}
	_, _, _ = registry.Submit(reqConflict)
	task3, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task3)

	snap3 := task3.Snapshot()
	t.Logf("[BRANCH 4: CONFLICT] Result: state=%s, error_class=%s, err=%s", snap3.State, snap3.ErrorClass, snap3.Error)
	if snap3.State != StateFailed {
		t.Fatalf("expected StateFailed on conflicting path, got: %s", snap3.State)
	}
	if snap3.ErrorClass != "db_conflict" {
		t.Fatalf("expected ErrorClass 'db_conflict', got: %s", snap3.ErrorClass)
	}
}

func TestOrchestrator_Deterministic_CancelAfterSetPublishingBeforeRename_RejectedAndPublishesSuccess(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var chatID = "-1007779"
	var msgID = 889
	var orchRef *Orchestrator

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "commit_cancel_test.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	orchRef = orch
	defer db.Close()

	finalRelPath := "RaceChannel/2024_09/889 - commit_cancel_test.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))
	partAbsPath := finalAbsPath + ".part"

	hookExecuted := false
	orch.SetTestHooks(OrchestratorTestHooks{
		AfterSetPublishing: func(taskID, cID string, mID int) {
			hookExecuted = true
			t.Logf("[HOOK: AfterSetPublishing] Arrived at publishing window: taskID=%s, chatID=%s, msgID=%d", taskID, cID, mID)

			// 1. Assert DB is in 'committing' state
			rec, err := db.GetDownloadRecord(cID, mID)
			if err != nil || rec == nil {
				t.Fatalf("failed to query DB record during hook: %v", err)
			}
			t.Logf("[HOOK: AfterSetPublishing] Verified DB status: %s, gen=%s", rec.Status, rec.AttemptGeneration)
			if rec.Status != "committing" {
				t.Fatalf("expected DB status 'committing', got: %s", rec.Status)
			}

			// 2. Assert .part exists and final file does NOT exist yet
			if _, statErr := os.Stat(partAbsPath); statErr != nil {
				t.Fatalf("expected .part file to exist before rename, got err: %v", statErr)
			}
			if _, statErr := os.Stat(finalAbsPath); !os.IsNotExist(statErr) {
				t.Fatalf("final file should not exist before rename, but found: %s", finalAbsPath)
			}
			t.Logf("[HOOK: AfterSetPublishing] Physical check: .part exists, final file does not exist yet")

			// 3. Attempt concurrent cancel while in publishing window
			t.Logf("[HOOK: AfterSetPublishing] Concurrently injecting CancelTasksByChatID(%s)...", cID)
			orchRef.CancelTasksByChatID(cID)

			// 4. Assert DB rejected cancel due to committing status barrier
			recAfter, err := db.GetDownloadRecord(cID, mID)
			if err != nil || recAfter == nil {
				t.Fatalf("failed to query DB record after cancel: %v", err)
			}
			t.Logf("[HOOK: AfterSetPublishing] DB status after cancel injection: %s (must remain 'committing')", recAfter.Status)
			if recAfter.Status != "committing" {
				t.Fatalf("expected DB status to remain 'committing', got: %s", recAfter.Status)
			}

			// 5. Assert Registry task state is NOT transitioned to failed
			if snap, ok := registry.Task(taskID); ok {
				t.Logf("[HOOK: AfterSetPublishing] Registry task state after cancel injection: %s", snap.State)
				if snap.State == StateFailed {
					t.Fatalf("Registry task was illegally canceled during publishing window!")
				}
			}
		},
	})

	req := TaskRequest{
		ID:          "case_cancel_after_set_publishing",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task)

	if !hookExecuted {
		t.Fatal("expected AfterSetPublishing test hook to be executed!")
	}

	// Post-execution assertions:
	// 1. Final file MUST be published
	if _, err := os.Stat(finalAbsPath); err != nil {
		t.Fatalf("expected final file to be published, but got err: %v", err)
	}
	t.Logf("[POST-ASSERT] Physical file published: %s", finalAbsPath)

	// 2. DB status MUST be 'success'
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("failed to query DB record: %v", err)
	}
	t.Logf("[POST-ASSERT] DB final record: status=%s, path=%s, size=%d, sha=%s", rec.Status, rec.SavePath, rec.FileSize, rec.SHA256)
	if rec.Status != "success" {
		t.Fatalf("expected DB status 'success', got: %q", rec.Status)
	}

	// 3. Registry task state MUST be StateSuccess
	snap := task.Snapshot()
	t.Logf("[POST-ASSERT] Registry task final state: %s", snap.State)
	if snap.State != StateSuccess {
		t.Fatalf("expected Registry StateSuccess, got: %s", snap.State)
	}
}

func TestOrchestrator_Deterministic_CancelAfterRenameBeforeCompleteDB_RejectedAndCompletesSuccess(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var chatID = "-1007779"
	var msgID = 890
	var orchRef *Orchestrator

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "rename_window_test.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	orchRef = orch
	defer db.Close()

	finalRelPath := "RaceChannel/2024_09/890 - rename_window_test.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))

	hookExecuted := false
	orch.SetTestHooks(OrchestratorTestHooks{
		AfterRename: func(taskID, cID string, mID int) {
			hookExecuted = true
			t.Logf("[HOOK: AfterRename] Arrived after atomic rename: taskID=%s, chatID=%s, msgID=%d", taskID, cID, mID)

			// 1. Final file is now physically on disk!
			if _, statErr := os.Stat(finalAbsPath); statErr != nil {
				t.Fatalf("expected final file to exist after rename, got err: %v", statErr)
			}
			t.Logf("[HOOK: AfterRename] Verified physical file is already on disk: %s", finalAbsPath)

			// 2. DB status is still 'committing' (before CompleteDownload is called)
			rec, err := db.GetDownloadRecord(cID, mID)
			if err != nil || rec == nil {
				t.Fatalf("failed to query DB record: %v", err)
			}
			t.Logf("[HOOK: AfterRename] Verified DB status is 'committing': %s", rec.Status)
			if rec.Status != "committing" {
				t.Fatalf("expected DB status 'committing', got: %s", rec.Status)
			}

			// 3. Attempt cancel right between rename and DB completion
			t.Logf("[HOOK: AfterRename] Concurrently injecting CancelTasksByChatID(%s)...", cID)
			orchRef.CancelTasksByChatID(cID)

			// 4. Assert DB rejected cancel
			recAfter, err := db.GetDownloadRecord(cID, mID)
			if err != nil || recAfter == nil {
				t.Fatalf("failed to query DB record after cancel: %v", err)
			}
			t.Logf("[HOOK: AfterRename] DB status after cancel injection: %s (must remain 'committing')", recAfter.Status)
			if recAfter.Status != "committing" {
				t.Fatalf("expected DB status to remain 'committing', got: %s", recAfter.Status)
			}

			// 5. Assert Registry task state is NOT failed
			if snap, ok := registry.Task(taskID); ok {
				t.Logf("[HOOK: AfterRename] Registry task state after cancel injection: %s", snap.State)
				if snap.State == StateFailed {
					t.Fatalf("Registry task was illegally canceled after rename!")
				}
			}
		},
	})

	req := TaskRequest{
		ID:          "case_cancel_after_rename",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task)

	if !hookExecuted {
		t.Fatal("expected AfterRename test hook to be executed!")
	}

	// 1. Final file MUST be present on disk
	if _, err := os.Stat(finalAbsPath); err != nil {
		t.Fatalf("expected final file to be present, but got err: %v", err)
	}

	// 2. DB status MUST be 'success'
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("failed to query DB record: %v", err)
	}
	t.Logf("[POST-ASSERT] DB final record: status=%s, path=%s, size=%d, sha=%s", rec.Status, rec.SavePath, rec.FileSize, rec.SHA256)
	if rec.Status != "success" {
		t.Fatalf("expected DB status 'success', got: %q", rec.Status)
	}

	// 3. Registry state MUST be StateSuccess
	snap := task.Snapshot()
	t.Logf("[POST-ASSERT] Registry task final state: %s", snap.State)
	if snap.State != StateSuccess {
		t.Fatalf("expected Registry StateSuccess, got: %s", snap.State)
	}
}

func TestOrchestrator_Deterministic_DBCompleteTransientFailure_RetriesAndConvergesToSuccess(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var chatID = "-1007779"
	var msgID = 891

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "db_complete_retry.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer db.Close()

	saveDir := filepath.Join(tempDir, "downloads")
	_ = os.MkdirAll(saveDir, 0o755)

	registry := NewRegistry(10, 100, time.Now)
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 4,
		MaxFileThreads:  2,
		MaxDataInFlight: 10,
	})
	ssdAdmission := fscommit.NewSSDAdmission(saveDir, 100*1024*1024)
	orch := NewOrchestrator(db, transferMgr, ssdAdmission, nil, access, registry, zap.NewNop(), saveDir)

	finalRelPath := "RaceChannel/2024_09/891 - db_complete_retry.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))

	var hookInvocations int32
	orch.SetTestHooks(OrchestratorTestHooks{
		BeforeCompleteDB: func(taskID, cID string, mID int) {
			calls := atomic.AddInt32(&hookInvocations, 1)
			t.Logf("[HOOK: BeforeCompleteDB] Invocation #%d for taskID=%s", calls, taskID)
			if calls == 1 {
				// Inject a temporary exclusive lock on the database using an external connection
				t.Logf("[HOOK: BeforeCompleteDB] Injecting SQLite database lock to simulate transient DB busy...")
				lockConn, lockErr := sql.Open("sqlite", dbPath)
				if lockErr == nil {
					tx, txErr := lockConn.Begin()
					if txErr == nil {
						_, _ = tx.Exec("BEGIN EXCLUSIVE")
						// Release lock after 30ms to allow online retry loop to succeed!
						go func() {
							time.Sleep(30 * time.Millisecond)
							_ = tx.Rollback()
							_ = lockConn.Close()
							t.Logf("[HOOK: BeforeCompleteDB] Released transient DB lock; online retry should now succeed")
						}()
					}
				}
			}
		},
	})

	req := TaskRequest{
		ID:          "case_db_complete_retry",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task)

	// Assertions:
	// 1. Physical file must be on disk
	if _, err := os.Stat(finalAbsPath); err != nil {
		t.Fatalf("physical file missing on disk: %v", err)
	}
	t.Logf("[POST-ASSERT] Physical file confirmed on disk: %s", finalAbsPath)

	// 2. Immediately after downloadOne failed on lock, Registry MUST NOT be failed!
	// It remains non-terminal (publishing), never faking failed!
	initialSnap := task.Snapshot()
	t.Logf("[POST-ASSERT] Registry state immediately after DB error: %s (must not be failed)", initialSnap.State)
	if initialSnap.State == StateFailed {
		t.Fatalf("Registry was improperly marked failed on DB error: state=%s", initialSnap.State)
	}

	// 3. DB status must be 'committing' (durable commit intent preserved)
	initialRec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || initialRec == nil {
		t.Fatalf("query DB record failed: %v", err)
	}
	t.Logf("[POST-ASSERT] DB status immediately after transient error: %s", initialRec.Status)
	if initialRec.Status != "committing" {
		t.Fatalf("expected DB status 'committing', got: %q", initialRec.Status)
	}

	// 4. Wait for transient DB lock to release (30ms)
	time.Sleep(50 * time.Millisecond)

	// 5. Invoke online continuous committing reconciler (without restarting daemon process!)
	if recErr := orch.ReconcileCommitting(context.Background()); recErr != nil {
		t.Fatalf("ReconcileCommitting failed: %v", recErr)
	}

	// 6. DB must have converged to success!
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("query DB record failed: %v", err)
	}
	t.Logf("[POST-ASSERT] Final DB record: status=%s, path=%s, size=%d, sha=%s", rec.Status, rec.SavePath, rec.FileSize, rec.SHA256)
	if rec.Status != "success" {
		t.Fatalf("expected DB status 'success' after online reconciliation, got: %q", rec.Status)
	}

	// 7. Registry task MUST have converged to StateSuccess without restart!
	snap := task.Snapshot()
	t.Logf("[POST-ASSERT] Final Registry task state: %s", snap.State)
	if snap.State != StateSuccess {
		t.Fatalf("expected Registry StateSuccess, got: %s", snap.State)
	}
}

func TestOrchestrator_Deterministic_RenameFailure_SyncsDBFailureBeforeRegistryTerminal(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var chatID = "-1007779"
	var msgID = 892

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "rename_fail_test.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	finalRelPath := "RaceChannel/2024_09/892 - rename_fail_test.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))

	// In BeforeRename hook, replace the target destination with a non-writable directory file
	orch.SetTestHooks(OrchestratorTestHooks{
		BeforeRename: func(taskID, cID string, mID int) {
			t.Logf("[HOOK: BeforeRename] Sabotaging target destination %s to trigger atomic commit failure", finalAbsPath)
			_ = os.MkdirAll(finalAbsPath, 0o755) // directory at file path causes rename failure!
		},
	})

	req := TaskRequest{
		ID:          "case_rename_fail",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task)

	// Assertions:
	// 1. Registry task must be failed with class "io"
	snap := task.Snapshot()
	t.Logf("[POST-ASSERT] Registry task state=%s, error_class=%s, err=%s", snap.State, snap.ErrorClass, snap.Error)
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed on rename failure, got: %s", snap.State)
	}
	if snap.ErrorClass != "io" {
		t.Fatalf("expected error class 'io', got: %s", snap.ErrorClass)
	}

	// 2. DB record MUST also be 'failed', synchronized before Registry
	rec, err := db.GetDownloadRecord(chatID, msgID)
	if err != nil || rec == nil {
		t.Fatalf("failed to query DB record: %v", err)
	}
	t.Logf("[POST-ASSERT] DB record status=%s, error=%s", rec.Status, rec.Error)
	if rec.Status != "failed" {
		t.Fatalf("expected DB record status 'failed', got: %s", rec.Status)
	}
}

func TestOrchestrator_Deterministic_RenameFailure_DBFailureFailsClosedWithoutRegistryTerminal(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var chatID = "-1007779"
	var msgID = 893

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "rename_fail_db_lock.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	finalRelPath := "RaceChannel/2024_09/893 - rename_fail_db_lock.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))

	orch.SetTestHooks(OrchestratorTestHooks{
		BeforeRename: func(taskID, cID string, mID int) {
			t.Logf("[HOOK: BeforeRename] Sabotaging target destination %s to trigger atomic commit failure", finalAbsPath)
			_ = os.MkdirAll(finalAbsPath, 0o755) // directory causes rename failure!

			// Close database connection to simulate DB verdict failure deterministically
			t.Logf("[HOOK: BeforeRename] Closing database to simulate DB failure verdict error...")
			_ = db.Close()
		},
	})

	req := TaskRequest{
		ID:          "case_rename_fail_db_lock",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())
	orch.downloadOne(context.Background(), task)

	// Assertions:
	// 1. Registry task MUST NOT be StateFailed because DB failure verdict was rejected!
	snap := task.Snapshot()
	t.Logf("[POST-ASSERT] Registry task state=%s (must not be failed without DB verdict)", snap.State)
	if snap.State == StateFailed {
		t.Fatalf("expected Registry to NOT be StateFailed when DB verdict rejected, got: %s", snap.State)
	}
}

// Acceptance Test (Issue #4): Active Publisher Exclusion.
// Worker pauses between PrepareDownloadCommit and Rename; Finalizer is triggered concurrently
// and MUST skip this active worker, preserving .part file and allowing the worker to complete normally.
func TestOrchestrator_Deterministic_ActivePublisherExclusion_FinalizerSkipsInFlightWorker(t *testing.T) {
	chatID := "-1008881"
	msgID := 901
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	hash := sha256.Sum256(data)
	shaHex := hex.EncodeToString(hash[:])

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "publisher_exclusion.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	finalRelPath := "RaceChannel/2024_09/901 - publisher_exclusion.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))
	partAbsPath := finalAbsPath + ".part"

	workerPaused := make(chan struct{})
	resumeWorker := make(chan struct{})

	orch.SetTestHooks(OrchestratorTestHooks{
		AfterPrepareCommit: func(taskID, cID string, mID int) {
			t.Logf("[HOOK: AfterPrepareCommit] Worker immediately after PrepareDownloadCommit (DB now committing); signaling pause...")
			close(workerPaused)
			<-resumeWorker
			t.Logf("[HOOK: AfterPrepareCommit] Resuming worker...")
		},
	})

	req := TaskRequest{
		ID:          "case_active_publisher_exclusion",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		orch.downloadOne(context.Background(), task)
	}()

	// Wait for worker to pause after PrepareDownloadCommit and before Rename
	<-workerPaused

	// Assertions during pause:
	// 1. DB status MUST be 'committing'
	var dbStatus string
	err := db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus)
	if err != nil || dbStatus != "committing" {
		t.Fatalf("expected committing DB status, got %q (err: %v)", dbStatus, err)
	}

	// Active publisher registration MUST exist concurrently with committing state!
	pubKey := fmt.Sprintf("%s:%d", chatID, msgID)
	val, exists := orch.activePublishers.Load(pubKey)
	snapGen := task.Snapshot().AttemptGeneration
	if !exists || val != snapGen {
		t.Fatalf("expected activePublishers to hold generation %s, got exists=%t val=%v", snapGen, exists, val)
	}

	// 2. .part file MUST exist
	if _, statErr := os.Stat(partAbsPath); statErr != nil {
		t.Fatalf(".part file missing while worker is paused: %v", statErr)
	}

	// 3. Now trigger online committing reconciler!
	t.Logf("[TEST] Triggering online ReconcileCommitting while worker is active publisher...")
	recErr := orch.ReconcileCommitting(context.Background())
	if recErr != nil {
		t.Fatalf("ReconcileCommitting returned unexpected error: %v", recErr)
	}

	// 4. Assert: Finalizer MUST HAVE SKIPPED the active worker!
	// The DB record must still be 'committing' (NOT reset to pending)
	err = db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus)
	if err != nil || dbStatus != "committing" {
		t.Fatalf("expected committing DB status preserved by exclusion, got %q", dbStatus)
	}
	// The .part file must NOT have been removed by finalizer
	if _, statErr := os.Stat(partAbsPath); statErr != nil {
		t.Fatalf(".part file was deleted by finalizer despite active publisher exclusion: %v", statErr)
	}

	// Resume worker to allow complete
	close(resumeWorker)
	<-workerDone

	// 5. Final assertion: Worker completes cleanly to success
	snap := task.Snapshot()
	if snap.State != StateSuccess {
		t.Fatalf("expected Registry task state success, got %s", snap.State)
	}
	err = db.db.QueryRow(`SELECT status, sha256 FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus, &shaHex)
	if err != nil || dbStatus != "success" {
		t.Fatalf("expected final DB status success, got %q", dbStatus)
	}

	// Active publisher registration MUST be deleted after completion!
	if _, exists := orch.activePublishers.Load(pubKey); exists {
		t.Fatalf("expected activePublishers to be cleaned up after completion, but key still exists")
	}
	t.Logf("[POST-ASSERT] Verified: Active publisher exclusion protected in-flight worker; completed to success and deregistered!")
}

// Acceptance Test (Issue #4): Concurrent wakeFinalizer calls execute single-flight without race or blocking.
func TestOrchestrator_Deterministic_ConcurrentWakeup_SingleFlightNoRace(t *testing.T) {
	orch := &Orchestrator{
		finalizerWakeCh: make(chan struct{}, 1),
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				orch.wakeFinalizer()
			}
		}()
	}
	wg.Wait()

	// Drain channel, at most 1 token present
	count := 0
	for {
		select {
		case <-orch.finalizerWakeCh:
			count++
		default:
			goto drained
		}
	}
drained:
	if count > 1 {
		t.Fatalf("expected at most 1 debounced signal in finalizerWakeCh, got %d", count)
	}
	t.Logf("[POST-ASSERT] Verified: 1000 concurrent wakeFinalizer calls safely debounced into single-flight signal")
}

// Acceptance Test (Issue #4): CommitSiblingPart encountering ErrTargetExists re-reads and validates proof, promoting to success.
func TestRecovery_ReconcileCommittingRecord_TargetExists_RevalidatesProofAndPromotesSuccess(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to init database: %v", err)
	}
	defer db.Close()

	saveDir := filepath.Join(tempDir, "downloads")
	_ = os.MkdirAll(saveDir, 0o755)

	chatID := "-100999"
	msgID := 555
	now := time.Now().Unix()
	payload := []byte("target exists revalidation test payload 12345")
	hash := sha256.Sum256(payload)
	shaHex := hex.EncodeToString(hash[:])
	size := int64(len(payload))
	relPath := "Channel/2026_09/555 - target_exists.bin"

	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(relPath))
	partAbsPath := finalAbsPath + ".part"
	_ = os.MkdirAll(filepath.Dir(finalAbsPath), 0o755)

	// CRUCIAL: finalAbsPath DOES NOT exist initially! Only partAbsPath exists.
	// This ensures Phase 1 (initial final check) fails, forcing Phase 2 to execute.
	_ = os.WriteFile(partAbsPath, payload, 0o644)

	// Inject hook to create final file right before CommitSiblingPart is called
	var hookCalled bool
	SetRecoveryTestHooks(RecoveryTestHooks{
		BeforeCommitSiblingPart: func(part, final string) {
			hookCalled = true
			// Create concurrent final file with matching valid proof to trigger ErrTargetExists
			if err := os.WriteFile(final, payload, 0o644); err != nil {
				t.Fatalf("write concurrent final file: %v", err)
			}
		},
	})
	defer SetRecoveryTestHooks(RecoveryTestHooks{})

	// Seed DB in committing status
	_, err = db.Execute(`
		INSERT INTO download_records (
			chat_id, message_id, status, file_name, save_path, file_size, sha256,
			attempt_generation, created_at, updated_at
		) VALUES (?, ?, 'committing', 'target_exists.bin', ?, ?, ?, 'gen_te', ?, ?)
	`, chatID, msgID, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}

	rec := DownloadRecord{
		ChatID:            chatID,
		MessageID:         msgID,
		Status:            "committing",
		FileName:          "target_exists.bin",
		SavePath:          relPath,
		FileSize:          size,
		SHA256:            shaHex,
		AttemptGeneration: "gen_te",
	}

	logger := zap.NewNop()
	recErr := ReconcileCommittingRecord(context.Background(), db, saveDir, false, rec, nil, logger)
	if recErr != nil {
		t.Fatalf("ReconcileCommittingRecord returned error: %v", recErr)
	}

	// 1. Assert hook was called proving CommitSiblingPart branch was reached
	if !hookCalled {
		t.Fatal("expected BeforeCommitSiblingPart hook to be called; ErrTargetExists branch was not exercised!")
	}

	// 2. Assert DB status is now 'success' (NOT reset to pending!)
	var status string
	err = db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&status)
	if err != nil || status != "success" {
		t.Fatalf("expected status promoted to 'success', got %q (err: %v)", status, err)
	}

	// 3. Residual .part file must have been cleaned
	if _, statErr := os.Stat(partAbsPath); !os.IsNotExist(statErr) {
		t.Fatalf("residual .part file was not cleaned: %v", statErr)
	}

	// 4. Final file intact
	content, readErr := os.ReadFile(finalAbsPath)
	if readErr != nil || string(content) != string(payload) {
		t.Fatalf("final file was corrupted or missing: %v", readErr)
	}
	t.Logf("[POST-ASSERT] Verified: ReconcileCommittingRecord exercised ErrTargetExists branch, re-read matching target file and converged to success")
}

// Acceptance Test (Issue #4): Valid part + conflicting final must preserve both proofs and produce a durable conflict, never delete part/reset pending.
func TestRecovery_ReconcileCommittingRecord_ConflictingFinal_PreservesProofsAndSetsDurableConflict(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to init database: %v", err)
	}
	defer db.Close()

	saveDir := filepath.Join(tempDir, "downloads")
	_ = os.MkdirAll(saveDir, 0o755)

	chatID := "-100999"
	msgID := 556
	now := time.Now().Unix()
	validPayload := []byte("authentic valid part payload for conflict test")
	conflictingPayload := []byte("completely different concurrent final payload 99999999")
	hash := sha256.Sum256(validPayload)
	shaHex := hex.EncodeToString(hash[:])
	size := int64(len(validPayload))
	relPath := "Channel/2026_09/556 - target_conflict.bin"

	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(relPath))
	partAbsPath := finalAbsPath + ".part"
	_ = os.MkdirAll(filepath.Dir(finalAbsPath), 0o755)

	// Valid .part file exists on disk
	_ = os.WriteFile(partAbsPath, validPayload, 0o644)

	// Inject hook to create conflicting final file right before CommitSiblingPart
	var hookCalled bool
	SetRecoveryTestHooks(RecoveryTestHooks{
		BeforeCommitSiblingPart: func(part, final string) {
			hookCalled = true
			if err := os.WriteFile(final, conflictingPayload, 0o644); err != nil {
				t.Fatalf("write conflicting final: %v", err)
			}
		},
	})
	defer SetRecoveryTestHooks(RecoveryTestHooks{})

	// Seed DB in committing status
	_, err = db.Execute(`
		INSERT INTO download_records (
			chat_id, message_id, status, file_name, save_path, file_size, sha256,
			attempt_generation, created_at, updated_at
		) VALUES (?, ?, 'committing', 'target_conflict.bin', ?, ?, ?, 'gen_tc', ?, ?)
	`, chatID, msgID, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}

	rec := DownloadRecord{
		ChatID:            chatID,
		MessageID:         msgID,
		Status:            "committing",
		FileName:          "target_conflict.bin",
		SavePath:          relPath,
		FileSize:          size,
		SHA256:            shaHex,
		AttemptGeneration: "gen_tc",
	}

	logger := zap.NewNop()
	recErr := ReconcileCommittingRecord(context.Background(), db, saveDir, false, rec, nil, logger)
	if recErr == nil {
		t.Fatal("expected ReconcileCommittingRecord to return error on target conflict, got nil")
	}

	if !hookCalled {
		t.Fatal("expected hook to be called before CommitSiblingPart")
	}

	// 1. Assert .part file is preserved intact! (MUST NOT BE DELETED!)
	partContent, err := os.ReadFile(partAbsPath)
	if err != nil || string(partContent) != string(validPayload) {
		t.Fatalf("valid .part file was deleted or corrupted: %v", err)
	}

	// 2. Assert conflicting final file is preserved intact! (MUST NOT BE OVERWRITTEN/DELETED!)
	finContent, err := os.ReadFile(finalAbsPath)
	if err != nil || string(finContent) != string(conflictingPayload) {
		t.Fatalf("conflicting final file was deleted or corrupted: %v", err)
	}

	// 3. Assert DB record is failed with target_conflict (MUST NOT BE PENDING!)
	var dbStatus, dbError string
	err = db.db.QueryRow(`SELECT status, COALESCE(error, '') FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus, &dbError)
	if err != nil {
		t.Fatalf("query DB: %v", err)
	}
	if dbStatus != "failed" {
		t.Fatalf("expected durable status 'failed', got %q (must not be reset to pending)", dbStatus)
	}
	if !strings.Contains(dbError, "target_conflict") {
		t.Fatalf("expected error to contain 'target_conflict', got %q", dbError)
	}
	t.Logf("[POST-ASSERT] Verified: valid .part and conflicting final both preserved; DB committed durable conflict (not pending)")
}

// Acceptance Test (Issue #4): If PrepareDownloadCommit fails, active publisher exclusion is immediately cleaned up.
func TestOrchestrator_Deterministic_ActivePublisherCleanupOnPrepareFailure(t *testing.T) {
	chatID := "-100888"
	msgID := 102
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 251)
	}

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 1001, AccessHash: 2002}, sz: 1024, dc: 2},
				Name:      "cleanup_test.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, _ := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	pubKey := fmt.Sprintf("%s:%d", chatID, msgID)

	// In BeforePrepareCommit hook, close the DB to guarantee PrepareDownloadCommit fails!
	orch.SetTestHooks(OrchestratorTestHooks{
		BeforePrepareCommit: func(taskID, cID string, mID int) {
			t.Logf("[HOOK: BeforePrepareCommit] Closing DB to induce PrepareDownloadCommit failure...")
			_ = db.Close()
		},
	})

	req := TaskRequest{
		ID:          "case_active_pub_cleanup",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	// Assert task failed
	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected task state failed, got %s", snap.State)
	}

	// Crucial invariant: activePublishers MUST be cleaned up by defer, no leftover key!
	if _, exists := orch.activePublishers.Load(pubKey); exists {
		t.Fatalf("expected activePublishers to be cleaned up after prepare failure, but key still exists!")
	}
	t.Logf("[POST-ASSERT] Verified: activePublishers cleanly deleted on PrepareDownloadCommit failure")
}

// Acceptance Test (Issue #4): Initial check with conflicting final and valid part preserves both and fails durably.
func TestRecovery_ReconcileCommittingRecord_InitialConflictingFinal_PreservesProofsAndSetsDurableConflict(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to init database: %v", err)
	}
	defer db.Close()

	saveDir := filepath.Join(tempDir, "downloads")
	_ = os.MkdirAll(saveDir, 0o755)

	chatID := "-100999"
	msgID := 557
	now := time.Now().Unix()
	validPayload := []byte("authentic valid part payload for initial conflict test")
	conflictingPayload := []byte("pre-existing conflicting final payload 88888888")
	hash := sha256.Sum256(validPayload)
	shaHex := hex.EncodeToString(hash[:])
	size := int64(len(validPayload))
	relPath := "Channel/2026_09/557 - initial_conflict.bin"

	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(relPath))
	partAbsPath := finalAbsPath + ".part"
	_ = os.MkdirAll(filepath.Dir(finalAbsPath), 0o755)

	// BOTH exist prior to ReconcileCommittingRecord: final is conflicting, part is valid!
	_ = os.WriteFile(partAbsPath, validPayload, 0o644)
	_ = os.WriteFile(finalAbsPath, conflictingPayload, 0o644)

	// Seed DB in committing status
	_, err = db.Execute(`
		INSERT INTO download_records (
			chat_id, message_id, status, file_name, save_path, file_size, sha256,
			attempt_generation, created_at, updated_at
		) VALUES (?, ?, 'committing', 'initial_conflict.bin', ?, ?, ?, 'gen_ic', ?, ?)
	`, chatID, msgID, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}

	rec := DownloadRecord{
		ChatID:            chatID,
		MessageID:         msgID,
		Status:            "committing",
		FileName:          "initial_conflict.bin",
		SavePath:          relPath,
		FileSize:          size,
		SHA256:            shaHex,
		AttemptGeneration: "gen_ic",
	}

	logger := zap.NewNop()
	recErr := ReconcileCommittingRecord(context.Background(), db, saveDir, false, rec, nil, logger)
	if recErr == nil {
		t.Fatal("expected ReconcileCommittingRecord to return error on initial target conflict, got nil")
	}

	// 1. Assert .part file is preserved intact! (MUST NOT BE DELETED!)
	partContent, err := os.ReadFile(partAbsPath)
	if err != nil || string(partContent) != string(validPayload) {
		t.Fatalf("valid .part file was deleted or corrupted: %v", err)
	}

	// 2. Assert conflicting final file is preserved intact! (MUST NOT BE OVERWRITTEN/DELETED!)
	finContent, err := os.ReadFile(finalAbsPath)
	if err != nil || string(finContent) != string(conflictingPayload) {
		t.Fatalf("conflicting final file was deleted or corrupted: %v", err)
	}

	// 3. Assert DB record is failed with target_conflict (MUST NOT BE PENDING!)
	var dbStatus, dbError string
	err = db.db.QueryRow(`SELECT status, COALESCE(error, '') FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus, &dbError)
	if err != nil {
		t.Fatalf("query DB: %v", err)
	}
	if dbStatus != "failed" {
		t.Fatalf("expected durable status 'failed', got %q (must not be reset to pending)", dbStatus)
	}
	if !strings.Contains(dbError, "target_conflict") {
		t.Fatalf("expected error to contain 'target_conflict', got %q", dbError)
	}
	t.Logf("[POST-ASSERT] Verified: initial conflict branch preserved both proofs; DB committed durable conflict (not pending)")
}

// Acceptance Test (Issue #4): In downloadOne, CommitSiblingPart encountering ErrTargetExists re-reads and converges to success if proofs match.
func TestOrchestrator_Deterministic_DownloadOne_ErrTargetExists_RevalidatesProofAndPromotesSuccess(t *testing.T) {
	chatID := "-100777"
	msgID := 201
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 199)
	}

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, data)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 2001, AccessHash: 3002}, sz: 1024, dc: 2},
				Name:      "download_one_match.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	finalRelPath := "RaceChannel/2024_09/201 - download_one_match.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))
	_ = os.MkdirAll(filepath.Dir(finalAbsPath), 0o755)

	var hookCalled bool
	orch.SetTestHooks(OrchestratorTestHooks{
		BeforeRename: func(taskID, cID string, mID int) {
			hookCalled = true
			// Create matching final file right before CommitSiblingPart is called
			if err := os.WriteFile(finalAbsPath, data, 0o644); err != nil {
				t.Fatalf("write matching final: %v", err)
			}
		},
	})

	req := TaskRequest{
		ID:          "case_download_one_match",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	if !hookCalled {
		t.Fatal("expected BeforeRename hook to be called")
	}

	snap := task.Snapshot()
	if snap.State != StateSuccess {
		t.Fatalf("expected task state success, got %s (err: %s)", snap.State, snap.Error)
	}

	var dbStatus string
	err := db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus)
	if err != nil || dbStatus != "success" {
		t.Fatalf("expected DB status success, got %q", dbStatus)
	}

	// .part file must be cleaned
	if _, statErr := os.Stat(finalAbsPath + ".part"); !os.IsNotExist(statErr) {
		t.Fatalf("residual .part file was not cleaned: %v", statErr)
	}
	t.Logf("[POST-ASSERT] Verified: downloadOne successfully converged to success upon matching ErrTargetExists")
}

// Acceptance Test (Issue #4): In downloadOne, CommitSiblingPart encountering ErrTargetExists preserves both proofs and sets durable conflict.
func TestOrchestrator_Deterministic_DownloadOne_ErrTargetExists_ConflictingFinal_PreservesBothProofs(t *testing.T) {
	chatID := "-100777"
	msgID := 202
	validData := make([]byte, 1024)
	for i := range validData {
		validData[i] = byte(i % 173)
	}
	conflictingData := make([]byte, 1024)
	for i := range conflictingData {
		conflictingData[i] = byte((i + 1) % 173)
	}

	invoker := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setUploadFile(output, validData)
		return nil
	})

	access := &mockAccessWithPool{
		pool: &mockPool{invoker: invoker},
		resolveFn: func(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
			return ResolvedMedia{
				File:      &orchMockMediaFile{loc: &tg.InputDocumentFileLocation{ID: 2002, AccessHash: 3003}, sz: 1024, dc: 2},
				Name:      "download_one_conflict.mp4",
				Size:      1024,
				DCID:      2,
				MediaType: "video",
				Date:      1725518400,
			}, nil
		},
	}

	orch, registry, db, saveDir := setupTestOrchestratorWithAccess(t, access)
	defer db.Close()

	finalRelPath := "RaceChannel/2024_09/202 - download_one_conflict.mp4"
	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(finalRelPath))
	partAbsPath := finalAbsPath + ".part"
	_ = os.MkdirAll(filepath.Dir(finalAbsPath), 0o755)

	var hookCalled bool
	orch.SetTestHooks(OrchestratorTestHooks{
		BeforeRename: func(taskID, cID string, mID int) {
			hookCalled = true
			// Create conflicting final file right before CommitSiblingPart is called
			if err := os.WriteFile(finalAbsPath, conflictingData, 0o644); err != nil {
				t.Fatalf("write conflicting final: %v", err)
			}
		},
	})

	req := TaskRequest{
		ID:          "case_download_one_conflict",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "RaceChannel",
		Date:        1725518400,
	}

	_, _, _ = registry.Submit(req)
	task, _ := registry.Next(context.Background())

	orch.downloadOne(context.Background(), task)

	if !hookCalled {
		t.Fatal("expected BeforeRename hook to be called")
	}

	// 1. Task snapshot must be StateFailed with target_conflict
	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected task state failed, got %s", snap.State)
	}
	if snap.ErrorClass != "target_conflict" {
		t.Fatalf("expected error class target_conflict, got %s", snap.ErrorClass)
	}

	// 2. Both physical files MUST be preserved!
	partContent, err := os.ReadFile(partAbsPath)
	if err != nil || string(partContent) != string(validData) {
		t.Fatalf("valid .part file was deleted or corrupted: %v", err)
	}
	finContent, err := os.ReadFile(finalAbsPath)
	if err != nil || string(finContent) != string(conflictingData) {
		t.Fatalf("conflicting final file was deleted or corrupted: %v", err)
	}

	// 3. DB status MUST be 'failed' with target_conflict
	var dbStatus, dbError string
	err = db.db.QueryRow(`SELECT status, COALESCE(error, '') FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus, &dbError)
	if err != nil || dbStatus != "failed" {
		t.Fatalf("expected DB status failed, got %q (err: %v)", dbStatus, err)
	}
	if !strings.Contains(dbError, "target_conflict") {
		t.Fatalf("expected DB error to contain target_conflict, got %q", dbError)
	}
	t.Logf("[POST-ASSERT] Verified: downloadOne preserved both .part and final files; durably committed target_conflict")
}

// Acceptance Test (Issue #4): If CommitSiblingPart fails with non-ErrTargetExists error, recovery fails closed and preserves valid .part.
func TestRecovery_ReconcileCommittingRecord_NonTargetExistsCommitError_PreservesPartAndFailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to init database: %v", err)
	}
	defer db.Close()

	saveDir := filepath.Join(tempDir, "downloads")
	_ = os.MkdirAll(saveDir, 0o755)

	chatID := "-100999"
	msgID := 558
	now := time.Now().Unix()
	validPayload := []byte("payload to test non-target-exists commit error preservation")
	hash := sha256.Sum256(validPayload)
	shaHex := hex.EncodeToString(hash[:])
	size := int64(len(validPayload))
	relPath := "ReadOnlyChannel/2026_09/558 - eperm_commit.bin"

	finalAbsPath := filepath.Join(saveDir, filepath.FromSlash(relPath))
	partAbsPath := finalAbsPath + ".part"
	parentDir := filepath.Dir(finalAbsPath)
	_ = os.MkdirAll(parentDir, 0o755)

	_ = os.WriteFile(partAbsPath, validPayload, 0o644)

	// Inject hook to make parent directory read-only right before CommitSiblingPart
	var hookCalled bool
	SetRecoveryTestHooks(RecoveryTestHooks{
		BeforeCommitSiblingPart: func(part, final string) {
			hookCalled = true
			_ = os.Chmod(parentDir, 0o555) // read-only directory causes link/rename failure with EACCES
		},
	})
	defer func() {
		_ = os.Chmod(parentDir, 0o755)
		SetRecoveryTestHooks(RecoveryTestHooks{})
	}()

	// Seed DB in committing status
	_, err = db.Execute(`
		INSERT INTO download_records (
			chat_id, message_id, status, file_name, save_path, file_size, sha256,
			attempt_generation, created_at, updated_at
		) VALUES (?, ?, 'committing', 'eperm_commit.bin', ?, ?, ?, 'gen_eperm', ?, ?)
	`, chatID, msgID, relPath, size, shaHex, now, now)
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}

	rec := DownloadRecord{
		ChatID:            chatID,
		MessageID:         msgID,
		Status:            "committing",
		FileName:          "eperm_commit.bin",
		SavePath:          relPath,
		FileSize:          size,
		SHA256:            shaHex,
		AttemptGeneration: "gen_eperm",
	}

	logger := zap.NewNop()
	recErr := ReconcileCommittingRecord(context.Background(), db, saveDir, false, rec, nil, logger)
	if recErr == nil {
		t.Fatal("expected ReconcileCommittingRecord to fail due to read-only directory, got nil")
	}

	if !hookCalled {
		t.Fatal("expected BeforeCommitSiblingPart hook to be called")
	}

	// 1. Assert .part file is preserved intact! (MUST NOT BE DELETED!)
	_ = os.Chmod(parentDir, 0o755)
	partContent, err := os.ReadFile(partAbsPath)
	if err != nil || string(partContent) != string(validPayload) {
		t.Fatalf("valid .part file was deleted despite commit error: %v", err)
	}

	// 2. DB status MUST NOT be pending! (Must remain committing so it can be retried)
	var dbStatus string
	err = db.db.QueryRow(`SELECT status FROM download_records WHERE chat_id = ? AND message_id = ?`, chatID, msgID).Scan(&dbStatus)
	if err != nil {
		t.Fatalf("query DB: %v", err)
	}
	if dbStatus == "pending" {
		t.Fatalf("illegal status transition: valid .part was reset to pending!")
	}
	t.Logf("[POST-ASSERT] Verified: non-target-exists commit error preserved valid .part and failed closed (DB status=%s)", dbStatus)
}
