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

// Constants for MTProto Chunking and Dual-Lane Pipeline
const (
	MaxPartSize         = 512 * 1024        // 512 KiB per MTProto chunk
	SmallFileThreshold  = 1024 * 1024       // <= 1 MiB goes to Small File Lane (whole-file in-memory)
	DefaultSmallMemory  = 128 * 1024 * 1024 // 128 MiB dedicated in-memory budget for small files
	MinInflightPerLarge = 4                 // 4 chunk in-flight guarantee per active large file
)

type Downloader struct {
	opts      Options
	floodGate *gate.FloodGate
}

type Options struct {
	Pool            dcpool.Pool
	Threads         int // Network Chunk Workers (default: 32)
	DiskWorkers     int // Dedicated Disk Writer Workers (default: 6: 5 Large + 1 Small)
	FileConcurrency int // Max Active Concurrently Downloading Large Files (default: 5)
	Iter            Iter
	Progress        Progress
	FloodGate       *gate.FloodGate
	SmallMemBudget  int64 // In-memory budget for small files (default: 128 MiB)
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
	if opts.SmallMemBudget < SmallFileThreshold {
		opts.SmallMemBudget = DefaultSmallMemory
	}
	return &Downloader{
		opts:      opts,
		floodGate: fg,
	}
}

// networkJob is the union interface for jobs processed by the 32 Network Workers.
type networkJob interface {
	isNetworkJob()
}

// largeChunkJob represents an atomic 512KB chunk fetch for an active large file (>= 1 MiB).
type largeChunkJob struct {
	fileState *activeLargeFileState
	elem      Elem
	dcID      int
	location  tg.InputFileLocationClass
	offset    int64
	limit     int
	totalSize int64
	takeout   bool
}

func (j *largeChunkJob) isNetworkJob() {}

// smallFetchJob represents an entire small file (<= 1 MiB) fetched into memory by 1 network worker.
type smallFetchJob struct {
	elem      Elem
	dcID      int
	location  tg.InputFileLocationClass
	totalSize int64
	takeout   bool
}

func (j *smallFetchJob) isNetworkJob() {}

// largeWriteJob represents downloaded chunk bytes passed to Large Disk Writers (5 workers).
type largeWriteJob struct {
	fileState *activeLargeFileState
	data      []byte
	offset    int64
}

// smallWriteJob represents an entire downloaded small file passed to Small Disk Writer (1 worker).
type smallWriteJob struct {
	elem      Elem
	data      []byte
	totalSize int64
	err       error
}

// activeLargeFileState binds and tracks the lifecycle of an active large file (>= 1 MiB).
type activeLargeFileState struct {
	elem            Elem
	writer          io.WriterAt
	totalSize       int64
	totalParts      int32
	remParts        int32
	inflight        int32
	downloadedBytes int64
	maxProgress     int64
	progMu          sync.Mutex
	canceled        int32
	firstErr        error
	errOnce         sync.Once
	doneOnce        sync.Once
	doneChan        chan struct{}
}

func (s *activeLargeFileState) fail(err error) {
	atomic.StoreInt32(&s.canceled, 1)
	s.errOnce.Do(func() {
		s.firstErr = err
	})
}

// memoryBudget provides thread-safe, event-driven accounting for small file memory buffer.
type memoryBudget struct {
	mu        sync.Mutex
	available int64
	limit     int64
	wakeCh    chan struct{}
}

func newMemoryBudget(limit int64) *memoryBudget {
	return &memoryBudget{
		available: limit,
		limit:     limit,
		wakeCh:    make(chan struct{}, 1),
	}
}

func (mb *memoryBudget) tryAcquire(bytes int64) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.available >= bytes {
		mb.available -= bytes
		return true
	}
	return false
}

func (mb *memoryBudget) release(bytes int64) {
	mb.mu.Lock()
	mb.available += bytes
	if mb.available > mb.limit {
		mb.available = mb.limit
	}
	select {
	case mb.wakeCh <- struct{}{}:
	default:
	}
	mb.mu.Unlock()
}

type largeFileCursor struct {
	state      *activeLargeFileState
	nextPart   int
	totalParts int
	partSize   int64
}

// Download runs the Dual-Lane Streaming Block Engine:
// 1. Unified 32-worker Network Pool dynamically serving Large and Small lanes.
// 2. Small File Lane: single-worker whole-file fetch into 128 MiB RAM buffer -> 1 serial disk writer.
// 3. Large File Lane: 5 concurrent large files with 4-chunk in-flight guarantee -> 5 chunk disk writers.
func (d *Downloader) Download(ctx context.Context, globalConcurrency int) error {
	netWorkers := d.opts.Threads
	if netWorkers <= 0 {
		netWorkers = 32
	}
	if globalConcurrency > 0 && globalConcurrency < netWorkers {
		netWorkers = globalConcurrency
	}

	largeDiskWorkers := d.opts.DiskWorkers - 1
	if largeDiskWorkers < 1 {
		largeDiskWorkers = 5
	}

	maxActiveLargeFiles := d.opts.FileConcurrency
	if maxActiveLargeFiles <= 0 {
		maxActiveLargeFiles = 5
	}

	jobChan := make(chan networkJob, netWorkers*2)
	elemChan := make(chan Elem, netWorkers*2)
	largeWriteChan := make(chan *largeWriteJob, netWorkers*2)
	smallWriteChan := make(chan *smallWriteJob, netWorkers*2)

	smallBudget := newMemoryBudget(d.opts.SmallMemBudget)

	g, gctx := errgroup.WithContext(ctx)

	// 1. Launch Ingestion Worker (Decouples Iter.Next from chunk dispatch loop)
	g.Go(func() error {
		defer close(elemChan)
		for d.opts.Iter.Next(gctx) {
			elem := d.opts.Iter.Value()
			if elem == nil || elem.File() == nil {
				continue
			}
			select {
			case <-gctx.Done():
				return gctx.Err()
			case elemChan <- elem:
			}
		}
		if err := d.opts.Iter.Err(); err != nil {
			return errors.Wrap(err, "iter")
		}
		return nil
	})

	// 2. Launch Large File Disk Writers (5 Dedicated I/O Workers for Chunk Writes)
	for dw := 0; dw < largeDiskWorkers; dw++ {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case wJob, ok := <-largeWriteChan:
					if !ok {
						return nil
					}
					n, writeErr := wJob.fileState.writer.WriteAt(wJob.data, wJob.offset)
					if writeErr != nil {
						wJob.fileState.fail(writeErr)
					} else {
						curr := atomic.AddInt64(&wJob.fileState.downloadedBytes, int64(n))
						wJob.fileState.progMu.Lock()
						if curr > wJob.fileState.maxProgress {
							wJob.fileState.maxProgress = curr
							d.opts.Progress.OnDownload(wJob.fileState.elem, ProgressState{
								Downloaded: curr,
								Total:      wJob.fileState.totalSize,
							})
						}
						wJob.fileState.progMu.Unlock()
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

	// 3. Launch Small File Serial Disk Writer (1 Dedicated I/O Worker for Serial Flush/Rename)
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case sJob, ok := <-smallWriteChan:
				if !ok {
					return nil
				}
				var finalErr error
				if sJob.err != nil {
					finalErr = sJob.err
				} else {
					w := sJob.elem.To()
					if w != nil {
						_, writeErr := w.WriteAt(sJob.data, 0)
						if writeErr != nil {
							finalErr = writeErr
						}
					}
				}
				d.opts.Progress.OnDone(sJob.elem, finalErr)
				smallBudget.release(sJob.totalSize)
			}
		}
	})

	// 4. Launch Unified 32 Network Workers (Pure Network Pullers with Shared Gate)
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
					switch j := job.(type) {
					case *largeChunkJob:
						d.fetchLargeChunk(gctx, workerID, j, largeWriteChan)
					case *smallFetchJob:
						d.fetchSmallFile(gctx, workerID, j, smallWriteChan, smallBudget)
					}
				}
			}
		})
	}

	// Safely close write channels only when all network workers have completely returned
	go func() {
		netWg.Wait()
		close(largeWriteChan)
		close(smallWriteChan)
	}()

	// 5. Launch Priority Dual-Lane Scheduler (Stage 1: Ingests from elemChan and Balances Large & Small Lanes)
	g.Go(func() error {
		defer close(jobChan)

		activeLargeFiles := make([]*largeFileCursor, 0, maxActiveLargeFiles)
		largeReadyQueue := make([]*largeFileCursor, 0, 64)
		smallReadyQueue := make([]*smallFetchJob, 0, 64)
		elemChanClosed := false

		routeElem := func(elem Elem) {
			totalSize := elem.File().Size()
			if totalSize <= 0 {
				d.opts.Progress.OnAdd(elem)
				d.opts.Progress.OnDone(elem, errors.New("file size is 0 or negative"))
				return
			}

			d.opts.Progress.OnAdd(elem)

			if totalSize <= SmallFileThreshold {
				smallReadyQueue = append(smallReadyQueue, &smallFetchJob{
					elem:      elem,
					dcID:      elem.File().DC(),
					location:  elem.File().Location(),
					totalSize: totalSize,
					takeout:   elem.AsTakeout(),
				})
			} else {
				partSize := int64(MaxPartSize)
				numParts := int((totalSize + partSize - 1) / partSize)
				if numParts < 1 {
					numParts = 1
				}

				fState := &activeLargeFileState{
					elem:       elem,
					writer:     elem.To(),
					totalSize:  totalSize,
					totalParts: int32(numParts),
					remParts:   int32(numParts),
					doneChan:   make(chan struct{}),
				}

				cursor := &largeFileCursor{
					state:      fState,
					nextPart:   0,
					totalParts: numParts,
					partSize:   partSize,
				}

				if len(activeLargeFiles) < maxActiveLargeFiles {
					activeLargeFiles = append(activeLargeFiles, cursor)
				} else {
					largeReadyQueue = append(largeReadyQueue, cursor)
				}
			}
		}

		for {
			// A. Non-blocking Ingestion from elemChan
		IngestLoop:
			for (!elemChanClosed) && (len(activeLargeFiles)+len(largeReadyQueue) < maxActiveLargeFiles*2 || len(smallReadyQueue) < netWorkers*2) {
				select {
				case elem, ok := <-elemChan:
					if !ok {
						elemChanClosed = true
						break IngestLoop
					}
					routeElem(elem)
				default:
					break IngestLoop
				}
			}

			// B. Clean up finished or canceled large files, and promote from largeReadyQueue
			filteredLargeFiles := make([]*largeFileCursor, 0, len(activeLargeFiles))
			for _, fc := range activeLargeFiles {
				if ce, ok := fc.state.elem.(CancelableElem); ok && ce.IsCanceled() {
					fc.state.fail(context.Canceled)
				}
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
					// Drain un-dispatched parts from remParts to avoid deadlock
					undispatched := int32(fc.totalParts - fc.nextPart)
					if undispatched > 0 {
						fc.nextPart = fc.totalParts
						if atomic.AddInt32(&fc.state.remParts, -undispatched) == 0 {
							fc.state.doneOnce.Do(func() {
								d.opts.Progress.OnDone(fc.state.elem, fc.state.firstErr)
								close(fc.state.doneChan)
							})
						}
					}
				}
				select {
				case <-fc.state.doneChan:
					// File completed/errored -> vacate slot
				default:
					filteredLargeFiles = append(filteredLargeFiles, fc)
				}
			}
			activeLargeFiles = filteredLargeFiles

			// Promote from largeReadyQueue up to maxActiveLargeFiles
			for len(activeLargeFiles) < maxActiveLargeFiles && len(largeReadyQueue) > 0 {
				nextLarge := largeReadyQueue[0]
				largeReadyQueue = largeReadyQueue[1:]
				activeLargeFiles = append(activeLargeFiles, nextLarge)
			}

			// Clean up canceled small files
			filteredSmallQueue := make([]*smallFetchJob, 0, len(smallReadyQueue))
			for _, sj := range smallReadyQueue {
				if ce, ok := sj.elem.(CancelableElem); ok && ce.IsCanceled() {
					d.opts.Progress.OnDone(sj.elem, context.Canceled)
					continue
				}
				filteredSmallQueue = append(filteredSmallQueue, sj)
			}
			smallReadyQueue = filteredSmallQueue

			// Check termination condition
			if elemChanClosed && len(activeLargeFiles) == 0 && len(largeReadyQueue) == 0 && len(smallReadyQueue) == 0 {
				break
			}

			dispatchedCount := 0

			// C. Priority 1: Guarantee MinInflightPerLarge (4 chunks) for every active large file
			for _, fc := range activeLargeFiles {
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
					continue
				}
				for fc.nextPart < fc.totalParts && atomic.LoadInt32(&fc.state.inflight) < MinInflightPerLarge {
					offset := int64(fc.nextPart) * fc.partSize
					limit := int(fc.partSize)

					job := &largeChunkJob{
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
						atomic.AddInt32(&fc.state.inflight, 1)
						fc.nextPart++
						dispatchedCount++
					default:
						goto SleepOrYield
					}
				}
			}

			// D. Priority 2: Dispatch Small Files if ready and RAM budget allows
			for len(smallReadyQueue) > 0 {
				nextSmall := smallReadyQueue[0]
				if !smallBudget.tryAcquire(nextSmall.totalSize) {
					// RAM budget full -> pause small lane backpressure
					break
				}

				select {
				case <-gctx.Done():
					return gctx.Err()
				case jobChan <- nextSmall:
					smallReadyQueue = smallReadyQueue[1:]
					dispatchedCount++
				default:
					smallBudget.release(nextSmall.totalSize)
					goto SleepOrYield
				}
			}

			// E. Priority 3: Surplus Worker Capacity -> Round-Robin feed active large files beyond min-inflight
			for _, fc := range activeLargeFiles {
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
					continue
				}
				if fc.nextPart < fc.totalParts {
					offset := int64(fc.nextPart) * fc.partSize
					limit := int(fc.partSize)

					job := &largeChunkJob{
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
						atomic.AddInt32(&fc.state.inflight, 1)
						fc.nextPart++
						dispatchedCount++
					default:
						goto SleepOrYield
					}
				}
			}

		SleepOrYield:
			if dispatchedCount == 0 {
				if elemChanClosed && len(activeLargeFiles) == 0 && len(largeReadyQueue) == 0 && len(smallReadyQueue) == 0 {
					break
				}
				select {
				case <-gctx.Done():
					return gctx.Err()
				case elem, ok := <-elemChan:
					if !ok {
						elemChanClosed = true
					} else {
						routeElem(elem)
					}
				case <-smallBudget.wakeCh:
				case <-time.After(20 * time.Millisecond):
				}
			}
		}

		return nil
	})

	return g.Wait()
}

// fetchLargeChunk downloads a single 512KB chunk for an active large file.
func (d *Downloader) fetchLargeChunk(ctx context.Context, workerID int, job *largeChunkJob, writeChan chan<- *largeWriteJob) {
	defer atomic.AddInt32(&job.fileState.inflight, -1)

	if ce, ok := job.fileState.elem.(CancelableElem); ok && ce.IsCanceled() {
		job.fileState.fail(context.Canceled)
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

	expectedBytes := job.limit
	if job.offset+int64(job.limit) > job.totalSize {
		expectedBytes = int(job.totalSize - job.offset)
	}

	elemCtx := ctx
	if ce, ok := job.fileState.elem.(ContextElem); ok && ce.Context() != nil {
		elemCtx = ce.Context()
	}

	if elemCtx.Err() != nil || atomic.LoadInt32(&job.fileState.canceled) == 1 {
		if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
			job.fileState.doneOnce.Do(func() {
				d.opts.Progress.OnDone(job.fileState.elem, context.Canceled)
				close(job.fileState.doneChan)
			})
		}
		return
	}

	client := d.opts.Pool.Client(elemCtx, job.dcID)
	if job.takeout {
		client = d.opts.Pool.Takeout(elemCtx, job.dcID)
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
		case <-elemCtx.Done():
			job.fileState.fail(context.Canceled)
			if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
				job.fileState.doneOnce.Do(func() {
					d.opts.Progress.OnDone(job.fileState.elem, context.Canceled)
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
		case <-elemCtx.Done():
			job.fileState.fail(context.Canceled)
			if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
				job.fileState.doneOnce.Do(func() {
					d.opts.Progress.OnDone(job.fileState.elem, context.Canceled)
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

	select {
	case <-ctx.Done():
		job.fileState.fail(ctx.Err())
		if atomic.AddInt32(&job.fileState.remParts, -1) == 0 {
			job.fileState.doneOnce.Do(func() {
				d.opts.Progress.OnDone(job.fileState.elem, job.fileState.firstErr)
				close(job.fileState.doneChan)
			})
		}
	case writeChan <- &largeWriteJob{
		fileState: job.fileState,
		data:      chunkData,
		offset:    job.offset,
	}:
	}
}

// fetchSmallFile downloads an entire small file (<= 1 MiB) sequentially in-memory.
func (d *Downloader) fetchSmallFile(ctx context.Context, workerID int, job *smallFetchJob, writeChan chan<- *smallWriteJob, budget *memoryBudget) {
	if ce, ok := job.elem.(CancelableElem); ok && ce.IsCanceled() {
		d.opts.Progress.OnDone(job.elem, context.Canceled)
		budget.release(job.totalSize)
		return
	}

	elemCtx := ctx
	if ce, ok := job.elem.(ContextElem); ok && ce.Context() != nil {
		elemCtx = ce.Context()
	}

	if elemCtx.Err() != nil {
		d.opts.Progress.OnDone(job.elem, context.Canceled)
		budget.release(job.totalSize)
		return
	}

	client := d.opts.Pool.Client(elemCtx, job.dcID)
	if job.takeout {
		client = d.opts.Pool.Takeout(elemCtx, job.dcID)
	}

	fileBuffer := make([]byte, 0, job.totalSize)
	var offset int64 = 0

	for offset < job.totalSize {
		limit := MaxPartSize
		if offset+int64(limit) > job.totalSize {
			limit = int(job.totalSize - offset)
		}

		req := &tg.UploadGetFileRequest{
			Precise:  true,
			Location: job.location,
			Offset:   offset,
			Limit:    limit,
		}

		var chunkData []byte
		var fetchErr error

		for attempt := 0; attempt < 5; attempt++ {
			select {
			case <-ctx.Done():
				d.opts.Progress.OnDone(job.elem, ctx.Err())
				budget.release(job.totalSize)
				return
			case <-elemCtx.Done():
				d.opts.Progress.OnDone(job.elem, context.Canceled)
				budget.release(job.totalSize)
				return
			default:
			}

			chunkCtx, chunkCancel := context.WithTimeout(elemCtx, 60*time.Second)
			var res tg.UploadFileClass
			res, fetchErr = client.UploadGetFile(chunkCtx, req)
			chunkCancel()

			if errors.Is(fetchErr, context.Canceled) || elemCtx.Err() != nil {
				d.opts.Progress.OnDone(job.elem, context.Canceled)
				budget.release(job.totalSize)
				return
			}

			if fetchErr == nil {
				if uf, ok := res.(*tg.UploadFile); ok {
					chunkData = uf.Bytes
					if len(chunkData) == limit {
						break
					}
					fetchErr = fmt.Errorf("short read at offset %d: expected %d bytes, got %d", offset, limit, len(chunkData))
				} else {
					fetchErr = fmt.Errorf("unexpected upload response type: %T", res)
				}
			}

			if tgerr.Is(fetchErr, "FILE_REFERENCE_EXPIRED", "FILEREF_UPGRADE_NEEDED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID") {
				break
			}

			if dWait, isFlood := tgerr.AsFloodWait(fetchErr); isFlood {
				logctx.From(ctx).Warn("UploadGetFile small file flood wait triggered",
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
				d.opts.Progress.OnDone(job.elem, ctx.Err())
				budget.release(job.totalSize)
				return
			case <-elemCtx.Done():
				d.opts.Progress.OnDone(job.elem, context.Canceled)
				budget.release(job.totalSize)
				return
			case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
			}
		}

		if fetchErr != nil || len(chunkData) != limit {
			if fetchErr == nil {
				fetchErr = fmt.Errorf("chunk size mismatch: expected %d, got %d", limit, len(chunkData))
			}
			d.opts.Progress.OnDone(job.elem, fetchErr)
			budget.release(job.totalSize)
			return
		}

		fileBuffer = append(fileBuffer, chunkData...)
		offset += int64(len(chunkData))

		d.opts.Progress.OnDownload(job.elem, ProgressState{
			Downloaded: offset,
			Total:      job.totalSize,
		})
	}

	// Send completed whole-file to the Small Disk Writer
	select {
	case <-ctx.Done():
		d.opts.Progress.OnDone(job.elem, ctx.Err())
		budget.release(job.totalSize)
	case writeChan <- &smallWriteJob{
		elem:      job.elem,
		data:      fileBuffer,
		totalSize: job.totalSize,
	}:
	}
}
