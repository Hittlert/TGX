package transfer

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
)

const (
	DefaultFileConcurrency = 5
	DefaultMaxFileThreads  = 8
	GotdPartSize           = 512 * 1024 // 512 KiB gotd protocol chunk size
)

// TransferManager manages gotd parallel transport and global RPC rate limits.
type TransferManager struct {
	fileCapacity   int
	activeFiles    int64
	maxFileThreads int
	gate           *DataGate
	downloader     *downloader.Downloader
}

// Options configures the TransferManager.
type Options struct {
	FileConcurrency int
	MaxFileThreads  int
	MaxDataInFlight int64
	RetryHandler    downloader.RetryHandler
}

// NewTransferManager creates a manager with the official gotd downloader and DataGate.
func NewTransferManager(opts Options) *TransferManager {
	if opts.FileConcurrency <= 0 {
		opts.FileConcurrency = DefaultFileConcurrency
	}
	if opts.MaxFileThreads <= 0 {
		opts.MaxFileThreads = DefaultMaxFileThreads
	}
	if opts.MaxDataInFlight <= 0 {
		opts.MaxDataInFlight = DefaultMaxDataInFlight
	}

	gate := NewDataGate(opts.MaxDataInFlight)
	dl := downloader.NewDownloader().
		WithPartSize(GotdPartSize).
		WithAllowCDN(true)

	if opts.RetryHandler != nil {
		dl = dl.WithRetryHandler(opts.RetryHandler)
	}

	return &TransferManager{
		fileCapacity:   opts.FileConcurrency,
		maxFileThreads: opts.MaxFileThreads,
		gate:           gate,
		downloader:     dl,
	}
}

// Gate returns the underlying DataGate.
func (m *TransferManager) Gate() *DataGate {
	return m.gate
}

// Downloader returns the underlying official gotd downloader instance.
func (m *TransferManager) Downloader() *downloader.Downloader {
	return m.downloader
}

// ActiveFiles returns the count of currently admitted downloading files.
func (m *TransferManager) ActiveFiles() int64 {
	return atomic.LoadInt64(&m.activeFiles)
}

// FileConcurrency returns the maximum concurrent active files capacity.
func (m *TransferManager) FileConcurrency() int {
	if m == nil {
		return 0
	}
	return m.fileCapacity
}

// ComputeFileThreads derives worker goroutines for one file based on logical work.
// Formula: chunk_count = ceil(expected_size / gotd_part_size), file_threads = min(max_file_threads, max(1, chunk_count))
func (m *TransferManager) ComputeFileThreads(expectedSize int64) int {
	if expectedSize <= 0 {
		return 1
	}
	chunkCount := int((expectedSize + GotdPartSize - 1) / GotdPartSize)
	threads := m.maxFileThreads
	if chunkCount < threads {
		threads = chunkCount
	}
	if threads < 1 {
		threads = 1
	}
	return threads
}

// DownloadFile downloads a Telegram file directly into dest using the official gotd parallel downloader.
func (m *TransferManager) DownloadFile(
	ctx context.Context,
	client downloader.Client,
	location tg.InputFileLocationClass,
	expectedSize int64,
	dest io.WriterAt,
	onProgress func(downloaded, total int64),
) (int64, error) {
	atomic.AddInt64(&m.activeFiles, 1)
	defer atomic.AddInt64(&m.activeFiles, -1)

	fileThreads := m.ComputeFileThreads(expectedSize)
	writer := NewCountingWriterAt(dest, expectedSize, onProgress)

	builder := m.downloader.Download(client, location).
		WithThreads(fileThreads)

	_, err := builder.Parallel(ctx, writer)
	downloaded := writer.Downloaded()
	if err != nil {
		return downloaded, fmt.Errorf("gotd parallel download: %w", err)
	}

	if expectedSize > 0 && !writer.IsComplete(expectedSize) {
		covered := writer.CoveredBytes()
		return downloaded, fmt.Errorf("incomplete download coverage: covered %d of %d expected bytes", covered, expectedSize)
	}

	return downloaded, nil
}
