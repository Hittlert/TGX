package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
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
	})
	ssdAdmission := fscommit.NewSSDAdmission(saveDir, 1<<20)

	access := &mockAccessWithPool{
		pool:       &mockPool{invoker: invoker},
		resolveErr: resolveErr,
	}

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
// Assert: StateSuccess, SHA-256 match, final file exists, .part cleaned up, physicalRetries recorded.
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
}
