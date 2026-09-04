package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

var (
	// ErrRequestBudgetExhausted is returned when the declared RPC request budget is exceeded.
	ErrRequestBudgetExhausted = errors.New("request budget exhausted")
)

const (
	DefaultFileConcurrency  = 5
	DefaultMaxFileThreads   = 8
	GotdPartSize            = 512 * 1024 // 512 KiB gotd protocol chunk size
	DefaultMaxRetryAttempts = 20         // Maximum gotd chunk retries per operation
)

// ComputeChunkCount calculates the expected number of 512 KiB chunks for a given file size.
func ComputeChunkCount(size int64) int {
	if size <= 0 {
		return 1
	}
	chunks := int((size + GotdPartSize - 1) / GotdPartSize)
	if chunks <= 0 {
		return 1
	}
	return chunks
}

// ComputeRequestBudget derives the declared bounded RPC request budget for a given expected file size.
func ComputeRequestBudget(expectedSize int64, maxRetriesPerChunk int) int64 {
	if maxRetriesPerChunk <= 0 {
		maxRetriesPerChunk = DefaultMaxRetryAttempts
	}
	chunks := int64(ComputeChunkCount(expectedSize))
	return chunks * int64(1+maxRetriesPerChunk)
}

// TransferError preserves classified failure semantics across the transfer boundary.
type TransferError struct {
	Stage       string `json:"stage"`
	Op          string `json:"op"`
	Class       string `json:"class"`
	Unavailable bool   `json:"unavailable"`
	Retryable   bool   `json:"retryable"`
	RetryOwner  string `json:"retry_owner"`
	Cause       error  `json:"-"`
}

func (e *TransferError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("[%s:%s] %s (owner: %s): %v", e.Stage, e.Op, e.Class, e.RetryOwner, e.Cause)
}

func (e *TransferError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type taskCtxKey struct{}

// TransferTaskContext preserves task, attempt generation, and DC correlation for transfer-level retries.
type TransferTaskContext struct {
	TaskID          string
	AttemptID       string
	ChatID          string
	MessageID       int
	DCID            int
	MaxRetries      int
	RequestBudget   int64
	RequestCount    *int64
	WireBytes       *int64
	PhysicalRetries *int64
}

// PhysicalAttemptID formats a unique identity for a physical attempt within this logical attempt.
func (tc TransferTaskContext) PhysicalAttemptID(retry int64) string {
	if tc.AttemptID == "" {
		return fmt.Sprintf("p%d", retry)
	}
	return fmt.Sprintf("%s-p%d", tc.AttemptID, retry)
}

// ContextWithTransferTask wraps ctx with TransferTaskContext.
func ContextWithTransferTask(ctx context.Context, tc TransferTaskContext) context.Context {
	return context.WithValue(ctx, taskCtxKey{}, tc)
}

// TransferTaskFromContext extracts TransferTaskContext from ctx.
func TransferTaskFromContext(ctx context.Context) (TransferTaskContext, bool) {
	tc, ok := ctx.Value(taskCtxKey{}).(TransferTaskContext)
	return tc, ok
}

// TransferManager manages gotd parallel transport and global RPC rate limits.
type TransferManager struct {
	fileCapacity    int64
	activeFiles     int64
	maxFileThreads  int
	downloader      *downloader.Downloader
	gate            *DataGate
	physicalRetries int64
	userRetry       downloader.RetryHandler
	taskRetry       func(context.Context, downloader.RetryEvent)
}

// Options configures the TransferManager.
type Options struct {
	FileConcurrency  int
	MaxFileThreads   int
	MaxDataInFlight  int64
	RetryHandler     downloader.RetryHandler
	TaskRetryHandler func(context.Context, downloader.RetryEvent)
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
	mgr := &TransferManager{
		fileCapacity:   int64(opts.FileConcurrency),
		maxFileThreads: opts.MaxFileThreads,
		gate:           gate,
		userRetry:      opts.RetryHandler,
		taskRetry:      opts.TaskRetryHandler,
	}

	dl := downloader.NewDownloader().
		WithPartSize(GotdPartSize).
		WithAllowCDN(true)

	userRetry := opts.RetryHandler
	dl = dl.WithRetryHandler(func(event downloader.RetryEvent) {
		atomic.AddInt64(&mgr.physicalRetries, 1)
		if userRetry != nil {
			userRetry(event)
		}
	})
	mgr.downloader = dl

	return mgr
}

// PhysicalRetries returns the total number of transient retries executed by gotd.
func (m *TransferManager) PhysicalRetries() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.physicalRetries)
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

// SetFileConcurrency updates the maximum concurrent active files capacity.
func (m *TransferManager) SetFileConcurrency(n int) error {
	if m == nil {
		return fmt.Errorf("transfer manager is nil")
	}
	if n < 1 || n > 64 {
		return fmt.Errorf("file concurrency must be between 1 and 64, got %d", n)
	}
	atomic.StoreInt64(&m.fileCapacity, int64(n))
	return nil
}

// FileConcurrency returns the maximum concurrent active files capacity.
func (m *TransferManager) FileConcurrency() int {
	if m == nil {
		return 0
	}
	return int(atomic.LoadInt64(&m.fileCapacity))
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

// DownloadResult contains physical transport telemetry for one download execution.
type DownloadResult struct {
	Written         int64 // Unique committed payload bytes
	WireBytes       int64 // Total bytes received across all RPC requests and retries
	ReplayBytes     int64 // Physical replay bytes (WireBytes - Written, >= 0)
	RequestCount    int64 // Total Telegram RPC requests executed
	PhysicalRetries int64 // Physical retry attempts handled inside gotd
	RequestBudget   int64 // Declared bounded request budget
}

// DownloadFileWithResult downloads a Telegram file directly into dest and returns execution telemetry.
func (m *TransferManager) DownloadFileWithResult(
	ctx context.Context,
	client downloader.Client,
	location tg.InputFileLocationClass,
	expectedSize int64,
	dest io.WriterAt,
	onProgress func(downloaded, total int64),
) (DownloadResult, error) {
	atomic.AddInt64(&m.activeFiles, 1)
	defer atomic.AddInt64(&m.activeFiles, -1)

	fileThreads := m.ComputeFileThreads(expectedSize)
	writer := NewCountingWriterAt(dest, expectedSize, onProgress)

	var fileRetries int64
	tc, hasTask := TransferTaskFromContext(ctx)

	maxRetries := DefaultMaxRetryAttempts
	if hasTask && tc.MaxRetries > 0 {
		maxRetries = tc.MaxRetries
	}
	budget := tc.RequestBudget
	if budget <= 0 {
		budget = ComputeRequestBudget(expectedSize, maxRetries)
		if hasTask {
			tc.RequestBudget = budget
			ctx = ContextWithTransferTask(ctx, tc)
		}
	}

	builder := m.downloader.Download(client, location).
		WithThreads(fileThreads).
		WithRetryHandler(func(event downloader.RetryEvent) {
			atomic.AddInt64(&m.physicalRetries, 1)
			atomic.AddInt64(&fileRetries, 1)
			if hasTask && tc.PhysicalRetries != nil {
				atomic.AddInt64(tc.PhysicalRetries, 1)
			}
			if m.userRetry != nil {
				m.userRetry(event)
			}
			if m.taskRetry != nil {
				m.taskRetry(ctx, event)
			}
		})

	_, err := builder.Parallel(ctx, writer)
	downloaded := writer.Downloaded()

	var wireBytes, reqCount int64
	if hasTask {
		if tc.WireBytes != nil {
			wireBytes = atomic.LoadInt64(tc.WireBytes)
		}
		if tc.RequestCount != nil {
			reqCount = atomic.LoadInt64(tc.RequestCount)
		}
	}
	if wireBytes < downloaded {
		wireBytes = downloaded
	}
	var replayBytes int64
	if wireBytes > downloaded {
		replayBytes = wireBytes - downloaded
	}

	res := DownloadResult{
		Written:         downloaded,
		WireBytes:       wireBytes,
		ReplayBytes:     replayBytes,
		RequestCount:    reqCount,
		PhysicalRetries: atomic.LoadInt64(&fileRetries),
		RequestBudget:   budget,
	}
	if err != nil {
		if errors.Is(err, ErrRequestBudgetExhausted) || strings.Contains(err.Error(), ErrRequestBudgetExhausted.Error()) {
			return res, &TransferError{
				Stage:       "transfer",
				Op:          "invoke",
				Class:       "network",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "gotd",
				Cause:       ErrRequestBudgetExhausted,
			}
		}
		if writeErr := writer.WriteErr(); writeErr != nil {
			return res, &TransferError{
				Stage:       "transfer",
				Op:          "write_chunk",
				Class:       "io",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Cause:       writeErr,
			}
		}
		if ctx.Err() != nil {
			return res, &TransferError{
				Stage:       "transfer",
				Op:          "download",
				Class:       "canceled",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Cause:       ctx.Err(),
			}
		}
		if tgerr.Is(err, "FILE_REFERENCE_EXPIRED", "FILEREF_INVALID", "FILE_ID_INVALID", "LOCATION_INVALID") {
			return res, &TransferError{
				Stage:       "transfer",
				Op:          "download",
				Class:       "unavailable",
				Unavailable: true,
				Retryable:   false,
				RetryOwner:  "none",
				Cause:       err,
			}
		}
		return res, &TransferError{
			Stage:       "transfer",
			Op:          "download",
			Class:       "network",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "gotd",
			Cause:       err,
		}
	}

	if expectedSize > 0 && !writer.IsComplete(expectedSize) {
		covered := writer.CoveredBytes()
		covErr := fmt.Errorf("incomplete download coverage: covered %d of %d expected bytes", covered, expectedSize)
		return res, &TransferError{
			Stage:       "transfer",
			Op:          "verify_coverage",
			Class:       "corrupt",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Cause:       covErr,
		}
	}

	return res, nil
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
	res, err := m.DownloadFileWithResult(ctx, client, location, expectedSize, dest, onProgress)
	return res.Written, err
}
