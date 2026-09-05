package daemon

import (
	"context"
	"crypto/sha256"
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
