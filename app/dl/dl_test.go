package dl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/tmedia"
	"github.com/Hittlert/TGX/core/transfer"
)

type mockInvokerFunc func(ctx context.Context, input bin.Encoder, output bin.Decoder) error

func (f mockInvokerFunc) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return f(ctx, input, output)
}

type trackingPool struct {
	mu             sync.Mutex
	invokerCalls   map[int]int
	takeoutCalls   map[int]int
	defaultInvoker tg.Invoker
	dcInvokers     map[int]tg.Invoker
}

func newTrackingPool() *trackingPool {
	return &trackingPool{
		invokerCalls: make(map[int]int),
		takeoutCalls: make(map[int]int),
		dcInvokers:   make(map[int]tg.Invoker),
	}
}

var _ dcpool.Pool = (*trackingPool)(nil)

func (p *trackingPool) Client(ctx context.Context, dc int) *tg.Client  { return nil }
func (p *trackingPool) Takeout(ctx context.Context, dc int) *tg.Client { return nil }
func (p *trackingPool) Default(ctx context.Context) *tg.Client         { return nil }
func (p *trackingPool) DefaultInvoker(ctx context.Context) tg.Invoker  { return p.defaultInvoker }
func (p *trackingPool) Close() error                                   { return nil }
func (p *trackingPool) CDN(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error) {
	return nil, nil, nil
}

func (p *trackingPool) Invoker(ctx context.Context, dc int) tg.Invoker {
	p.mu.Lock()
	p.invokerCalls[dc]++
	inv := p.dcInvokers[dc]
	p.mu.Unlock()
	if inv != nil {
		return inv
	}
	return p.defaultInvoker
}

func (p *trackingPool) TakeoutInvoker(ctx context.Context, dc int) tg.Invoker {
	p.mu.Lock()
	p.takeoutCalls[dc]++
	p.mu.Unlock()
	return p.defaultInvoker
}

type mockElementIterator struct {
	elements []*iterElem
	idx      int
	err      error
}

func (m *mockElementIterator) Next(ctx context.Context) bool {
	if m.idx < len(m.elements) {
		m.idx++
		return true
	}
	return false
}

func (m *mockElementIterator) Value() *iterElem {
	return m.elements[m.idx-1]
}

func (m *mockElementIterator) Err() error {
	return m.err
}

func setMockUploadFile(output bin.Decoder, data []byte) {
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

// 1. Acceptance Test: Files from at least two DCs route through their expected invokers.
func TestCLIRunDownloadLoop_MultiDCRouting(t *testing.T) {
	tempDir := t.TempDir()
	pool := newTrackingPool()

	payloadDC2 := []byte("payload-dc-2")
	payloadDC4 := []byte("payload-dc-4")

	pool.dcInvokers[2] = mockInvokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setMockUploadFile(output, payloadDC2)
		return nil
	})
	pool.dcInvokers[4] = mockInvokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setMockUploadFile(output, payloadDC4)
		return nil
	})

	f2, _ := os.Create(filepath.Join(tempDir, "file2.bin"))
	defer f2.Close()
	f4, _ := os.Create(filepath.Join(tempDir, "file4.bin"))
	defer f4.Close()

	elemDC2 := &iterElem{
		id:   1,
		file: &tmedia.Media{DC: 2, Size: int64(len(payloadDC2)), InputFileLoc: &tg.InputDocumentFileLocation{ID: 201}},
		to:   f2,
		opts: Options{Takeout: false},
	}
	elemDC4 := &iterElem{
		id:   2,
		file: &tmedia.Media{DC: 4, Size: int64(len(payloadDC4)), InputFileLoc: &tg.InputDocumentFileLocation{ID: 401}},
		to:   f4,
		opts: Options{Takeout: false},
	}

	it := &mockElementIterator{elements: []*iterElem{elemDC2, elemDC4}}
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 2,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
	})

	err := runDownloadLoop(context.Background(), it, pool, transferMgr, 2, nil)
	if err != nil {
		t.Fatalf("runDownloadLoop failed: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.invokerCalls[2] != 1 {
		t.Fatalf("expected 1 call to DC 2 invoker, got %d", pool.invokerCalls[2])
	}
	if pool.invokerCalls[4] != 1 {
		t.Fatalf("expected 1 call to DC 4 invoker, got %d", pool.invokerCalls[4])
	}
}

// 2. Acceptance Test: Takeout mode invokes the Takeout path.
func TestCLIRunDownloadLoop_TakeoutRouting(t *testing.T) {
	tempDir := t.TempDir()
	pool := newTrackingPool()

	payload := []byte("takeout-payload")
	pool.defaultInvoker = mockInvokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		setMockUploadFile(output, payload)
		return nil
	})

	f, _ := os.Create(filepath.Join(tempDir, "takeout.bin"))
	defer f.Close()

	elem := &iterElem{
		id:   1,
		file: &tmedia.Media{DC: 1, Size: int64(len(payload)), InputFileLoc: &tg.InputDocumentFileLocation{ID: 101}},
		to:   f,
		opts: Options{Takeout: true},
	}

	it := &mockElementIterator{elements: []*iterElem{elem}}
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 1,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
	})

	err := runDownloadLoop(context.Background(), it, pool, transferMgr, 1, nil)
	if err != nil {
		t.Fatalf("runDownloadLoop failed: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.takeoutCalls[1] != 1 {
		t.Fatalf("expected 1 call to TakeoutInvoker on DC 1, got %d", pool.takeoutCalls[1])
	}
	if pool.invokerCalls[1] != 0 {
		t.Fatalf("expected 0 calls to standard Invoker on DC 1, got %d", pool.invokerCalls[1])
	}
}

// 3. Acceptance Test: Cancellation while waiting for a token returns without deadlock.
func TestCLIRunDownloadLoop_CancellationSafetyNoDeadlock(t *testing.T) {
	tempDir := t.TempDir()
	pool := newTrackingPool()

	started := make(chan struct{})
	payload := []byte("slow-payload")

	pool.defaultInvoker = mockInvokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		close(started)
		// block until context canceled
		<-ctx.Done()
		return ctx.Err()
	})

	f1, _ := os.Create(filepath.Join(tempDir, "f1.bin"))
	defer f1.Close()
	f2, _ := os.Create(filepath.Join(tempDir, "f2.bin"))
	defer f2.Close()

	elem1 := &iterElem{id: 1, file: &tmedia.Media{DC: 2, Size: int64(len(payload))}, to: f1}
	elem2 := &iterElem{id: 2, file: &tmedia.Media{DC: 2, Size: int64(len(payload))}, to: f2}

	it := &mockElementIterator{elements: []*iterElem{elem1, elem2}}
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 1,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doneCh := make(chan error, 1)
	go func() {
		// Limit is 1, so elem2 will block on sem <- struct{}{}
		doneCh <- runDownloadLoop(ctx, it, pool, transferMgr, 1, nil)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("elem1 never started")
	}

	// Cancel context while elem2 is waiting on sem
	cancel()

	select {
	case err := <-doneCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runDownloadLoop deadlocked or timed out during cancellation!")
	}
}

// 4. Acceptance Test: First file failure does not leak a token or hang shutdown.
func TestCLIRunDownloadLoop_FailureTokenLeakProof(t *testing.T) {
	tempDir := t.TempDir()
	pool := newTrackingPool()

	failErr := errors.New("network failure")
	pool.defaultInvoker = mockInvokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return failErr
	})

	f, _ := os.Create(filepath.Join(tempDir, "fail.bin"))
	defer f.Close()

	elem := &iterElem{id: 1, file: &tmedia.Media{DC: 2, Size: 1024}, to: f}
	it := &mockElementIterator{elements: []*iterElem{elem}}
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 1,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
	})

	err := runDownloadLoop(context.Background(), it, pool, transferMgr, 1, nil)
	if err == nil {
		t.Fatal("expected failure error, got nil")
	}
	if !errors.Is(err, failErr) && err.Error() != failErr.Error() {
		t.Fatalf("expected error %v, got %v", failErr, err)
	}
}

// 5. Acceptance Test: CLI output hash and final filename remain correct.
func TestCLIRunDownloadLoop_OutputIntegrity(t *testing.T) {
	tempDir := t.TempDir()
	pool := newTrackingPool()

	expectedData := make([]byte, 2048)
	for i := range expectedData {
		expectedData[i] = byte(i % 251)
	}
	h := sha256.Sum256(expectedData)
	expectedHex := hex.EncodeToString(h[:])

	var callCount int32
	pool.defaultInvoker = mockInvokerFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		atomic.AddInt32(&callCount, 1)
		setMockUploadFile(output, expectedData)
		return nil
	})

	outPath := filepath.Join(tempDir, "verified.mp4")
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	elem := &iterElem{
		id:   1,
		file: &tmedia.Media{DC: 2, Size: int64(len(expectedData)), InputFileLoc: &tg.InputDocumentFileLocation{ID: 999}},
		to:   f,
	}

	it := &mockElementIterator{elements: []*iterElem{elem}}
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 1,
		MaxFileThreads:  1,
		MaxDataInFlight: 10,
	})

	if err := runDownloadLoop(context.Background(), it, pool, transferMgr, 1, nil); err != nil {
		t.Fatalf("runDownloadLoop failed: %v", err)
	}

	_ = f.Sync()
	actualBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if len(actualBytes) != len(expectedData) {
		t.Fatalf("size mismatch: expected %d, got %d", len(expectedData), len(actualBytes))
	}
	actualHash := sha256.Sum256(actualBytes)
	actualHex := hex.EncodeToString(actualHash[:])
	if actualHex != expectedHex {
		t.Fatalf("sha256 mismatch: expected %s, got %s", expectedHex, actualHex)
	}
}
