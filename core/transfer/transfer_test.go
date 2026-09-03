package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
	"time"

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
