package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

func TestComputeFileThreads(t *testing.T) {
	mgr := NewTransferManager(Options{
		MaxFileThreads: 8,
	})

	tests := []struct {
		size    int64
		wantTh  int
		comment string
	}{
		{size: 0, wantTh: 1, comment: "zero size"},
		{size: 300 * 1024, wantTh: 1, comment: "300 KiB image"},
		{size: 512 * 1024, wantTh: 1, comment: "exact 1 chunk"},
		{size: 512*1024 + 1, wantTh: 2, comment: "just over 1 chunk"},
		{size: 900 * 1024, wantTh: 2, comment: "900 KiB image"},
		{size: int64(1.75 * 1024 * 1024), wantTh: 4, comment: "1.75 MiB file (4 chunks)"},
		{size: 100 * 1024 * 1024, wantTh: 8, comment: "100 MiB video (capped at 8)"},
	}

	for _, tt := range tests {
		got := mgr.ComputeFileThreads(tt.size)
		if got != tt.wantTh {
			t.Errorf("ComputeFileThreads(%d [%s]) = %d, want %d", tt.size, tt.comment, got, tt.wantTh)
		}
	}
}

func TestDataGate_CapacityAndFloodWait(t *testing.T) {
	gate := NewDataGate(5)
	ctx := context.Background()

	// 1. Acquire up to capacity (5)
	releases := make([]func(), 5)
	for i := 0; i < 5; i++ {
		rel, err := gate.Acquire(ctx, 1)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases[i] = rel
	}
	if gate.InFlight() != 5 {
		t.Fatalf("expected 5 in flight, got %d", gate.InFlight())
	}

	// 2. 6th acquire should block until release
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err := gate.Acquire(timeoutCtx, 1)
	if err == nil {
		t.Fatal("expected timeout acquiring beyond capacity")
	}

	// 3. Release one, acquire succeeds
	releases[0]()
	releases[0]() // idempotent
	if gate.InFlight() != 4 {
		t.Fatalf("expected 4 in flight after release, got %d", gate.InFlight())
	}

	rel6, err := gate.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	rel6()

	for i := 1; i < 5; i++ {
		releases[i]()
	}
	if gate.InFlight() != 0 {
		t.Fatalf("expected 0 in flight, got %d", gate.InFlight())
	}

	// 4. Test FloodWait on DC 2
	gate.TriggerFloodWait(2, 200*time.Millisecond)
	if !gate.IsDCCooledDown(2) {
		t.Fatal("expected DC 2 to be cooled down")
	}
	if gate.IsDCCooledDown(1) {
		t.Fatal("DC 1 should not be cooled down")
	}

	// Acquiring for DC 2 should wait until cooldown expires
	start := time.Now()
	relDC2, err := gate.Acquire(ctx, 2)
	if err != nil {
		t.Fatalf("acquire DC 2: %v", err)
	}
	relDC2()
	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond {
		t.Fatalf("expected at least 150ms wait for FloodWait, got %v", elapsed)
	}
}

// MemoryWriterAt implements io.WriterAt over a byte slice.
type MemoryWriterAt struct {
	buf []byte
	mu  sync.Mutex
}

func (m *MemoryWriterAt) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	end := int(off) + len(p)
	if end > len(m.buf) {
		newBuf := make([]byte, end)
		copy(newBuf, m.buf)
		m.buf = newBuf
	}
	copy(m.buf[off:], p)
	return len(p), nil
}

type fakeClient struct {
	data     []byte
	partSize int
}

func (f *fakeClient) UploadGetFile(ctx context.Context, req *tg.UploadGetFileRequest) (tg.UploadFileClass, error) {
	offset := int(req.Offset)
	limit := req.Limit
	if offset >= len(f.data) {
		return &tg.UploadFile{
			Type:  &tg.StorageFilePartial{},
			Bytes: []byte{},
		}, nil
	}
	end := offset + limit
	if end > len(f.data) {
		end = len(f.data)
	}
	chunk := f.data[offset:end]
	return &tg.UploadFile{
		Type:  &tg.StorageFilePartial{},
		Bytes: chunk,
	}, nil
}

func (f *fakeClient) UploadGetFileHashes(ctx context.Context, req *tg.UploadGetFileHashesRequest) ([]tg.FileHash, error) {
	return nil, nil
}
func (f *fakeClient) UploadReuploadCDNFile(ctx context.Context, req *tg.UploadReuploadCDNFileRequest) ([]tg.FileHash, error) {
	return nil, nil
}
func (f *fakeClient) UploadGetCDNFileHashes(ctx context.Context, req *tg.UploadGetCDNFileHashesRequest) ([]tg.FileHash, error) {
	return nil, nil
}
func (f *fakeClient) UploadGetWebFile(ctx context.Context, req *tg.UploadGetWebFileRequest) (*tg.UploadWebFile, error) {
	return nil, nil
}

func TestTransferManager_DownloadFile(t *testing.T) {
	totalSize := int64(1.75 * 1024 * 1024) // 1.75 MiB
	payload := make([]byte, totalSize)
	for i := range payload {
		payload[i] = byte((i * 17) % 255)
	}
	expectedHash := sha256.Sum256(payload)
	expectedHex := hex.EncodeToString(expectedHash[:])

	fake := &fakeClient{data: payload, partSize: GotdPartSize}
	mgr := NewTransferManager(Options{
		FileConcurrency: 4,
		MaxFileThreads:  4,
		MaxDataInFlight: 10,
	})

	location := &tg.InputDocumentFileLocation{
		ID:            123456,
		AccessHash:    789012,
		FileReference: []byte("ref"),
	}

	memWriter := &MemoryWriterAt{buf: make([]byte, totalSize)}
	var progressReported int64

	downloaded, err := mgr.DownloadFile(
		context.Background(),
		fake,
		location,
		totalSize,
		memWriter,
		func(cur, tot int64) {
			progressReported = cur
		},
	)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	if downloaded != totalSize {
		t.Fatalf("downloaded bytes mismatch: got %d, want %d", downloaded, totalSize)
	}
	if progressReported != totalSize {
		t.Fatalf("progress reported mismatch: got %d, want %d", progressReported, totalSize)
	}

	actualHash := sha256.Sum256(memWriter.buf)
	actualHex := hex.EncodeToString(actualHash[:])
	if actualHex != expectedHex {
		t.Fatalf("sha256 mismatch: got %s, want %s", actualHex, expectedHex)
	}
}

func TestRangeTracker(t *testing.T) {
	rt := NewRangeTracker()
	if !rt.IsComplete(0) {
		t.Errorf("zero size should be complete")
	}

	// 1. Partial write: [0, 5) for 2MB expected
	rt.AddRange(0, 5)
	if rt.IsComplete(2097152) {
		t.Errorf("expected incomplete for 5/2097152 bytes")
	}
	if rt.CoveredBytes() != 5 {
		t.Errorf("expected 5 covered bytes, got %d", rt.CoveredBytes())
	}

	// 2. Overlapping and out-of-order writes
	rt.AddRange(5, 100)
	rt.AddRange(50, 150) // overlaps
	if rt.CoveredBytes() != 150 {
		t.Errorf("expected 150 covered bytes after overlap merge, got %d", rt.CoveredBytes())
	}

	// 3. Gap write: [200, 300) leaves [150, 200) gap
	rt.AddRange(200, 300)
	if rt.IsComplete(300) {
		t.Errorf("expected incomplete due to gap [150, 200)")
	}

	// 4. Fill the gap: [150, 200)
	rt.AddRange(150, 200)
	if !rt.IsComplete(300) {
		t.Errorf("expected complete once gap is filled")
	}
	if rt.CoveredBytes() != 300 {
		t.Errorf("expected 300 covered bytes, got %d", rt.CoveredBytes())
	}
}

func TestDownloadFile_ShortResponseRejected(t *testing.T) {
	mgr := NewTransferManager(Options{
		MaxFileThreads: 4,
	})

	// Fake client that only serves 5 bytes when 2097152 bytes (2 MiB) are requested
	fake := &fakeClient{
		data:     []byte("hello"),
		partSize: GotdPartSize,
	}

	location := &tg.InputDocumentFileLocation{
		ID:         999,
		AccessHash: 888,
	}

	memWriter := &MemoryWriterAt{buf: make([]byte, 2097152)}

	_, err := mgr.DownloadFile(
		context.Background(),
		fake,
		location,
		2097152,
		memWriter,
		nil,
	)
	if err == nil {
		t.Fatalf("expected error for short response, got nil")
	}
}

type mockRawInvoker struct {
	invoked bool
	err     error
}

func (m *mockRawInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	m.invoked = true
	return m.err
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func TestGatedClient_InvokerAndCDN(t *testing.T) {
	gate := NewDataGate(2)
	raw := &mockRawInvoker{}
	gatedInv := NewGatedInvoker(raw, gate, 1)

	// Invoke through GatedInvoker
	err := gatedInv.Invoke(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected invoke error: %v", err)
	}
	if !raw.invoked {
		t.Fatal("expected raw.Invoke to be called")
	}
	if gate.InFlight() != 0 {
		t.Fatalf("expected 0 in flight after invoke, got %d", gate.InFlight())
	}

	// Test GatedClient with CDN
	cdnInvoker := &mockRawInvoker{}
	cdnCalled := false
	client := NewGatedClient(raw, gate, 1, func(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error) {
		cdnCalled = true
		return cdnInvoker, nopCloser{}, nil
	})

	cdnClient, closer, err := client.CDN(context.Background(), 2, 1024)
	if err != nil {
		t.Fatalf("failed to get CDN: %v", err)
	}
	if !cdnCalled || cdnClient == nil || closer == nil {
		t.Fatal("expected CDN to be returned")
	}
	_ = closer.Close()
}

type failingWriterAt struct {
	mu         sync.Mutex
	writeCalls int
	err        error
}

func (f *failingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeCalls++
	return 0, f.err
}

func TestDownloadFile_PermanentWriterError_ClassifiedAsIO(t *testing.T) {
	mgr := NewTransferManager(Options{
		MaxFileThreads: 1,
	})

	fake := &fakeClient{
		data:     make([]byte, GotdPartSize),
		partSize: GotdPartSize,
	}
	location := &tg.InputDocumentFileLocation{ID: 1, AccessHash: 2}

	expectedErr := io.ErrUnexpectedEOF
	failWriter := &failingWriterAt{err: expectedErr}

	_, err := mgr.DownloadFile(
		context.Background(),
		fake,
		location,
		int64(len(fake.data)),
		failWriter,
		nil,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var tErr *TransferError
	if !errors.As(err, &tErr) {
		t.Fatalf("expected TransferError, got %T: %v", err, err)
	}

	if tErr.Class != "io" {
		t.Errorf("expected Class 'io', got %q", tErr.Class)
	}
	if tErr.Op != "write_chunk" {
		t.Errorf("expected Op 'write_chunk', got %q", tErr.Op)
	}
	if tErr.Retryable {
		t.Error("expected Retryable to be false for permanent I/O error")
	}
	if tErr.RetryOwner != "none" {
		t.Errorf("expected RetryOwner 'none', got %q", tErr.RetryOwner)
	}
	if !errors.Is(tErr.Cause, expectedErr) {
		t.Errorf("expected Cause %v, got %v", expectedErr, tErr.Cause)
	}
}

func TestDownloadFile_IncompleteCoverage_ClassifiedAsCorrupt(t *testing.T) {
	mgr := NewTransferManager(Options{
		MaxFileThreads: 2,
	})

	// fake client with less data than expectedSize
	fake := &fakeClient{
		data:     []byte("short"),
		partSize: GotdPartSize,
	}
	location := &tg.InputDocumentFileLocation{ID: 1, AccessHash: 2}
	mem := &MemoryWriterAt{buf: make([]byte, 1024)}

	_, err := mgr.DownloadFile(
		context.Background(),
		fake,
		location,
		1024,
		mem,
		nil,
	)
	if err == nil {
		t.Fatal("expected error for incomplete coverage, got nil")
	}

	var tErr *TransferError
	if !errors.As(err, &tErr) {
		t.Fatalf("expected TransferError, got %T: %v", err, err)
	}

	if tErr.Class != "corrupt" {
		t.Errorf("expected Class 'corrupt', got %q", tErr.Class)
	}
	if tErr.Op != "verify_coverage" {
		t.Errorf("expected Op 'verify_coverage', got %q", tErr.Op)
	}
	if tErr.Retryable {
		t.Error("expected Retryable to be false for corrupt coverage")
	}
}

func TestDownloadFile_CanceledContext_ClassifiedAsCanceled(t *testing.T) {
	mgr := NewTransferManager(Options{
		MaxFileThreads: 1,
	})

	fake := &fakeClient{
		data:     make([]byte, GotdPartSize),
		partSize: GotdPartSize,
	}
	location := &tg.InputDocumentFileLocation{ID: 1, AccessHash: 2}
	mem := &MemoryWriterAt{buf: make([]byte, GotdPartSize)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled immediately

	_, err := mgr.DownloadFile(
		ctx,
		fake,
		location,
		GotdPartSize,
		mem,
		nil,
	)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}

	var tErr *TransferError
	if !errors.As(err, &tErr) {
		t.Fatalf("expected TransferError, got %T: %v", err, err)
	}

	if tErr.Class != "canceled" {
		t.Errorf("expected Class 'canceled', got %q", tErr.Class)
	}
	if tErr.Retryable {
		t.Error("expected Retryable to be false for cancellation")
	}
}

type invokerFunc func(ctx context.Context, input bin.Encoder, output bin.Decoder) error

func (f invokerFunc) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return f(ctx, input, output)
}

func TestGatedInvoker_RequestAndWireBytesAccounting(t *testing.T) {
	gate := NewDataGate(10)
	var reqCount int64
	var wireBytes int64

	tc := TransferTaskContext{
		TaskID:       "task-1",
		AttemptID:    "gen-1",
		ChatID:       "-1001",
		MessageID:    42,
		DCID:         2,
		RequestCount: &reqCount,
		WireBytes:    &wireBytes,
	}
	ctx := ContextWithTransferTask(context.Background(), tc)

	chunkData := make([]byte, 512)
	calls := 0
	mockInv := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		calls++
		if calls == 1 {
			return errors.New("transient rpc failure")
		}
		if box, ok := output.(*tg.UploadFileBox); ok {
			box.File = &tg.UploadFile{
				Type:  &tg.StorageFilePartial{},
				Bytes: chunkData,
			}
		}
		return nil
	})

	gated := NewGatedInvoker(mockInv, gate, 2)

	// Call 1: fails
	var out1 tg.UploadFileBox
	err1 := gated.Invoke(ctx, nil, &out1)
	if err1 == nil {
		t.Fatal("expected error on call 1")
	}

	// Call 2: succeeds
	var out2 tg.UploadFileBox
	err2 := gated.Invoke(ctx, nil, &out2)
	if err2 != nil {
		t.Fatalf("unexpected error on call 2: %v", err2)
	}

	if reqCount != 2 {
		t.Fatalf("expected 2 requests tracked, got %d", reqCount)
	}
	if wireBytes != 512 {
		t.Fatalf("expected 512 wire bytes, got %d", wireBytes)
	}

	budget := ComputeRequestBudget(1024, 20)
	if budget != 21 {
		t.Fatalf("expected budget 21 for 1024 bytes (1 chunk + 20 retries), got %d", budget)
	}
}

func TestGatedInvoker_RequestBudgetExhaustionBoundary(t *testing.T) {
	gate := NewDataGate(10)
	var reqCount int64
	var wireBytes int64

	tc := TransferTaskContext{
		TaskID:        "task-budget-1",
		AttemptID:     "gen-1",
		ChatID:        "-1001",
		MessageID:     42,
		DCID:          2,
		RequestBudget: 3,
		RequestCount:  &reqCount,
		WireBytes:     &wireBytes,
	}
	ctx := ContextWithTransferTask(context.Background(), tc)

	var rawCalls int64
	mockInv := invokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		atomic.AddInt64(&rawCalls, 1)
		return errors.New("transient upstream error")
	})

	gated := NewGatedInvoker(mockInv, gate, 2)

	// Call 1..3: allowed and reach raw invoker
	for i := 1; i <= 3; i++ {
		var out tg.UploadFileBox
		err := gated.Invoke(ctx, nil, &out)
		if err == nil || err.Error() != "transient upstream error" {
			t.Fatalf("expected transient error on call %d, got %v", i, err)
		}
	}

	if rawCalls != 3 {
		t.Fatalf("expected 3 raw calls, got %d", rawCalls)
	}
	if reqCount != 3 {
		t.Fatalf("expected request count 3, got %d", reqCount)
	}

	// Call 4: exceeds declared budget 3 -> blocked AT BOUNDARY, raw invoker NEVER called!
	var out4 tg.UploadFileBox
	err4 := gated.Invoke(ctx, nil, &out4)
	if !errors.Is(err4, ErrRequestBudgetExhausted) {
		t.Fatalf("expected ErrRequestBudgetExhausted on call 4, got: %v", err4)
	}
	if rawCalls != 3 {
		t.Fatalf("raw invoker was called despite budget exhaustion: got %d calls, want strictly 3", rawCalls)
	}
	if reqCount != 3 {
		t.Fatalf("request count should remain capped at budget 3, got %d", reqCount)
	}

	// Verify PhysicalAttemptID helper
	if id0 := tc.PhysicalAttemptID(0); id0 != "gen-1-p0" {
		t.Fatalf("expected physical attempt ID gen-1-p0, got %s", id0)
	}
	if id2 := tc.PhysicalAttemptID(2); id2 != "gen-1-p2" {
		t.Fatalf("expected physical attempt ID gen-1-p2, got %s", id2)
	}
}
