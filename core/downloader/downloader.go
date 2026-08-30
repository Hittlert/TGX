package downloader

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

// MaxPartSize refer to https://core.telegram.org/api/files#downloading-files
const MaxPartSize = 512 * 1024

type Downloader struct {
	opts      Options
	floodGate *gate.FloodGate
}

type Options struct {
	Pool            dcpool.Pool
	Threads         int // Network Chunk Workers (default: 32)
	DiskWorkers     int // Dedicated Disk Writer Workers (default: 6)
	FileConcurrency int // Max Active Concurrently Downloading Files (default: 5)
	Iter            Iter
	Progress        Progress
	FloodGate       *gate.FloodGate
}

func New(opts Options) *Downloader {
	fg := opts.FloodGate
	if fg == nil {
		fg = gate.NewFloodGate(40.0, 10)
	}
	if opts.Threads <= 0 {
		opts.Threads = 32
	}
	if opts.DiskWorkers <= 0 {
		opts.DiskWorkers = 6
	}
	if opts.FileConcurrency <= 0 {
		opts.FileConcurrency = 5
	}
	return &Downloader{
		opts:      opts,
		floodGate: fg,
	}
}

// chunkJob represents an atomic 512KB block task dispatched from Chunk Producer to Network Workers.
type chunkJob struct {
	fileState *activeFileState
	elem      Elem
	dcID      int
	location  tg.InputFileLocationClass
	offset    int64
	limit     int
	totalSize int64
	takeout   bool
}

// writeJob represents downloaded chunk bytes passed from Network Workers to Disk Writers.
type writeJob struct {
	fileState *activeFileState
	data      []byte
	offset    int64
}

// activeFileState binds and tracks the lifecycle of an active file being downloaded.
type activeFileState struct {
	elem            Elem
	writer          io.WriterAt
	totalSize       int64
	totalParts      int32
	remParts        int32
	downloadedBytes int64
	canceled        int32
	firstErr        error
	errOnce         sync.Once
	doneOnce        sync.Once
	doneChan        chan struct{}
}

func (s *activeFileState) fail(err error) {
	atomic.StoreInt32(&s.canceled, 1)
	s.errOnce.Do(func() {
		s.firstErr = err
	})
}

// Download runs the 3-stage Streaming Block Engine:
// Stage 1: Interleaving Chunk Producer (maintains up to FileConcurrency active files, paving chunks in round-robin order)
// Stage 2: 32 Network Chunk Workers (pure chunk pullers with shared DC cooldown gate)
// Stage 3: Dedicated Disk Writer Workers (writes chunk bytes into bound FileCoordinator with zero network blocking)
func (d *Downloader) Download(ctx context.Context, globalConcurrency int) error {
	netWorkers := d.opts.Threads
	if netWorkers <= 0 {
		netWorkers = 32
	}
	if globalConcurrency > 0 && globalConcurrency < netWorkers {
		netWorkers = globalConcurrency
	}

	diskWorkers := d.opts.DiskWorkers
	if diskWorkers <= 0 {
		diskWorkers = 6
	}

	maxActiveFiles := d.opts.FileConcurrency
	if maxActiveFiles <= 0 {
		maxActiveFiles = 5
	}

	jobChan := make(chan *chunkJob, netWorkers*2)
	writeChan := make(chan *writeJob, netWorkers*2)

	g, gctx := errgroup.WithContext(ctx)

	// 1. Launch Disk Writer Workers (Stage 3: 5~8 Dedicated I/O Workers)
	for dw := 0; dw < diskWorkers; dw++ {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case wJob, ok := <-writeChan:
					if !ok {
						return nil
					}
					n, writeErr := wJob.fileState.writer.WriteAt(wJob.data, wJob.offset)
					if writeErr != nil {
						wJob.fileState.fail(writeErr)
					} else {
						curr := atomic.AddInt64(&wJob.fileState.downloadedBytes, int64(n))
						d.opts.Progress.OnDownload(wJob.fileState.elem, ProgressState{
							Downloaded: curr,
							Total:      wJob.fileState.totalSize,
						})
					}

					if atomic.AddInt32(&wJob.fileState.remParts, -1) == 0 {
						wJob.fileState.doneOnce.Do(func() {
							d.opts.Progress.OnDone(wJob.fileState.elem, wJob.fileState.firstErr)
							close(wJob.fileState.doneChan)
						})
					}
				}
			}
		})
	}

	// 2. Launch Network Chunk Workers (Stage 2: Strictly 32 Concurrent Workers)
	var netWg sync.WaitGroup
	for nw := 0; nw < netWorkers; nw++ {
		netWg.Add(1)
		workerID := nw
		g.Go(func() error {
			defer netWg.Done()
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case job, ok := <-jobChan:
					if !ok {
						return nil
					}
					d.fetchChunk(gctx, workerID, job, writeChan)
				}
			}
		})
	}

	// Safely close writeChan only when all network workers have completely returned
	go func() {
		netWg.Wait()
		close(writeChan)
	}()

	// 3. Launch Interleaving Chunk Producer (Stage 1: Maintains Active 5 Files & Paves Chunks Round-Robin)
	g.Go(func() error {
		defer close(jobChan)

		type fileCursor struct {
			state      *activeFileState
			nextPart   int
			totalParts int
			partSize   int64
		}

		activeFiles := make([]*fileCursor, 0, maxActiveFiles)
		iterDone := false

		for {
			// A. Fill active window up to maxActiveFiles
			for len(activeFiles) < maxActiveFiles && !iterDone {
				if !d.opts.Iter.Next(gctx) {
					iterDone = true
					break
				}

				elem := d.opts.Iter.Value()
				if elem == nil || elem.File() == nil {
					continue
				}

				totalSize := elem.File().Size()
				if totalSize <= 0 {
					d.opts.Progress.OnAdd(elem)
					d.opts.Progress.OnDone(elem, errors.New("file size is 0 or negative"))
					continue
				}

				d.opts.Progress.OnAdd(elem)

				partSize := int64(MaxPartSize)
				numParts := int((totalSize + partSize - 1) / partSize)
				if numParts < 1 {
					numParts = 1
				}

				fState := &activeFileState{
					elem:       elem,
					writer:     elem.To(),
					totalSize:  totalSize,
					totalParts: int32(numParts),
					remParts:   int32(numParts),
					doneChan:   make(chan struct{}),
				}

				activeFiles = append(activeFiles, &fileCursor{
					state:      fState,
					nextPart:   0,
					totalParts: numParts,
					partSize:   partSize,
				})
			}

			// If no active files and iter is done -> finished
			if len(activeFiles) == 0 && iterDone {
				break
			}

			// B. Pave 1 chunk from each active file in Round-Robin order!
			chunksEmitted := 0
			newActiveFiles := make([]*fileCursor, 0, len(activeFiles))

			for _, fc := range activeFiles {
				if ce, ok := fc.state.elem.(CancelableElem); ok && ce.IsCanceled() {
					fc.state.fail(context.Canceled)
				}

				select {
				case <-fc.state.doneChan:
					// File completed/errored -> vacate slot to admit next file
					continue
				default:
				}

				if fc.nextPart < fc.totalParts {
					offset := int64(fc.nextPart) * fc.partSize
					limit := int(fc.partSize)

					job := &chunkJob{
						fileState: fc.state,
						elem:      fc.state.elem,
						dcID:      fc.state.elem.File().DC(),
						location:  fc.state.elem.File().Location(),
						offset:    offset,
						limit:     limit,
						totalSize: fc.state.totalSize,
						takeout:   fc.state.elem.AsTakeout(),
					}

					select {
					case <-gctx.Done():
						return gctx.Err()
					case jobChan <- job:
						fc.nextPart++
						chunksEmitted++
					}
				}

				select {
				case <-fc.state.doneChan:
				default:
					newActiveFiles = append(newActiveFiles, fc)
				}
			}

			activeFiles = newActiveFiles

			if chunksEmitted == 0 && len(activeFiles) > 0 {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case <-time.After(15 * time.Millisecond):
				}
			}
		}

		if err := d.opts.Iter.Err(); err != nil {
			return errors.Wrap(err, "iter")
		}
		return nil
	})

	return g.Wait()
}

func (d *Downloader) fetchChunk(ctx context.Context, workerID int, job *chunkJob, writeChan chan<- *writeJob) {
	if ce, ok := job.fileState.elem.(CancelableElem); ok && ce.IsCanceled() {
		job.fileState.fail(context.Canceled)
	}

	// 0. Fail-fast: skip network RPC if file is already cancelled / errored
	if atomic.LoadInt32(&job.fileState.canceled) == 1 {
		if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
			job.fileState.doneOnce.Do(func() {
				d.opts.Progress.OnDone(job.fileState.elem, job.fileState.firstErr)
				close(job.fileState.doneChan)
			})
		}
		return
	}

	expectedBytes := job.limit
	if job.offset+int64(job.limit) > job.totalSize {
		expectedBytes = int(job.totalSize - job.offset)
	}

	client := d.opts.Pool.Client(ctx, job.dcID)
	if job.takeout {
		client = d.opts.Pool.Takeout(ctx, job.dcID)
	}

	req := &tg.UploadGetFileRequest{
		Precise:  true,
		Location: job.location,
		Offset:   job.offset,
		Limit:    job.limit,
	}

	var chunkData []byte
	var fetchErr error

	for attempt := 0; attempt < 5; attempt++ {
		select {
		case <-ctx.Done():
			job.fileState.fail(ctx.Err())
			if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
				job.fileState.doneOnce.Do(func() {
					d.opts.Progress.OnDone(job.fileState.elem, job.fileState.firstErr)
					close(job.fileState.doneChan)
				})
			}
			return
		default:
		}

		if atomic.LoadInt32(&job.fileState.canceled) == 1 {
			if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
				job.fileState.doneOnce.Do(func() {
					d.opts.Progress.OnDone(job.fileState.elem, job.fileState.firstErr)
					close(job.fileState.doneChan)
				})
			}
			return
		}

		elemCtx := ctx
		if ce, ok := job.fileState.elem.(ContextElem); ok && ce.Context() != nil {
			elemCtx = ce.Context()
		}

		chunkCtx, chunkCancel := context.WithTimeout(elemCtx, 60*time.Second)
		var res tg.UploadFileClass
		res, fetchErr = client.UploadGetFile(chunkCtx, req)
		chunkCancel()

		if errors.Is(fetchErr, context.Canceled) || elemCtx.Err() != nil {
			job.fileState.fail(context.Canceled)
			if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
				job.fileState.doneOnce.Do(func() {
					d.opts.Progress.OnDone(job.fileState.elem, context.Canceled)
					close(job.fileState.doneChan)
				})
			}
			return
		}

		if fetchErr == nil {
			if uf, ok := res.(*tg.UploadFile); ok {
				chunkData = uf.Bytes
				if len(chunkData) == expectedBytes {
					break
				}
				fetchErr = fmt.Errorf("short read at offset %d: expected %d bytes, got %d", job.offset, expectedBytes, len(chunkData))
			} else {
				fetchErr = fmt.Errorf("unexpected upload response type: %T", res)
			}
		}

		if tgerr.Is(fetchErr, "FILE_REFERENCE_EXPIRED", "FILEREF_UPGRADE_NEEDED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID") {
			break
		}

		if dWait, isFlood := tgerr.AsFloodWait(fetchErr); isFlood {
			logctx.From(ctx).Warn("UploadGetFile shared flood wait triggered",
				zap.Int("dc", job.dcID),
				zap.Duration("flood_wait", dWait),
			)
			if d.floodGate != nil {
				d.floodGate.TriggerFloodWait(job.dcID, dWait)
			}
			continue
		}

		select {
		case <-ctx.Done():
			job.fileState.fail(ctx.Err())
			if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
				job.fileState.doneOnce.Do(func() {
					d.opts.Progress.OnDone(job.fileState.elem, job.fileState.firstErr)
					close(job.fileState.doneChan)
				})
			}
			return
		case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
		}
	}

	if fetchErr != nil || len(chunkData) != expectedBytes {
		if fetchErr == nil {
			fetchErr = fmt.Errorf("chunk size mismatch at offset %d: expected %d, got %d", job.offset, expectedBytes, len(chunkData))
		}
		job.fileState.fail(fetchErr)
		if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
			job.fileState.doneOnce.Do(func() {
				d.opts.Progress.OnDone(job.fileState.elem, job.fileState.firstErr)
				close(job.fileState.doneChan)
			})
		}
		return
	}

	// Hand off downloaded chunk to Disk Writer Workers!
	select {
	case <-ctx.Done():
		job.fileState.fail(ctx.Err())
		if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
			job.fileState.doneOnce.Do(func() {
				d.opts.Progress.OnDone(job.fileState.elem, job.fileState.firstErr)
				close(job.fileState.doneChan)
			})
		}
	case writeChan <- &writeJob{
		fileState: job.fileState,
		data:      chunkData,
		offset:    job.offset,
	}:
	}
}
