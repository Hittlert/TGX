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
	MaxLargeReadyQueue  = 10                // Bounded capacity for ready large files
	MaxSmallReadyQueue  = 64                // Bounded capacity for ready small files
	MaxWindowPartsAhead = 16                // Max 16 chunks (8 MiB) sliding window ahead of disk writer
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

// largeChunkJob represents an atomic 512KB chunk fetch for an active large file (>= 1 MiB).
type largeChunkJob struct {
	fileState   *activeLargeFileState
	writerIndex int
	elem        Elem
	dcID        int
	location    tg.InputFileLocationClass
	offset      int64
	limit       int
	totalSize   int64
	takeout     bool
}

// smallFetchJob represents an entire small file (<= 1 MiB) fetched into memory by 1 network worker.
type smallFetchJob struct {
	elem      Elem
	dcID      int
	location  tg.InputFileLocationClass
	totalSize int64
	takeout   bool
}

// largeWriteJob represents downloaded chunk bytes or failure tombstone passed to Large Disk Writers.
type largeWriteJob struct {
	fileState *activeLargeFileState
	data      []byte
	offset    int64
	isFailed  bool
	err       error
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
	elem              Elem
	writer            io.WriterAt
	writerIndex       int
	totalSize         int64
	totalParts        int32
	remParts          int32
	inflight          int32
	downloadedBytes   int64
	lastWrittenOffset int64
	maxProgress       int64
	progMu            sync.Mutex
	canceled          int32
	firstErr          error
	errOnce           sync.Once
	doneOnce          sync.Once
	doneChan          chan struct{}
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
// 1. Fair Multiplexed 32-worker Network Pool dynamically serving Large and Small lanes via dual job channels.
// 2. Small File Lane: single-worker whole-file fetch into 128 MiB RAM buffer -> 1 serial disk writer.
// 3. Large File Lane: 5 concurrent large files with 4-chunk in-flight guarantee -> 5 per-file chunk disk writers with sliding-window sequential offset writes.
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

	// Separate typed job channels for true fair worker multiplexing
	largeJobChan := make(chan *largeChunkJob, netWorkers*2)
	smallJobChan := make(chan *smallFetchJob, netWorkers*2)

	// Separate bounded ingestion channels
	largeElemChan := make(chan Elem, MaxLargeReadyQueue)
	smallElemChan := make(chan Elem, MaxSmallReadyQueue)

	// Dedicated per-file write channels for large disk writers
	largeWriteChans := make([]chan *largeWriteJob, largeDiskWorkers)
	for i := 0; i < largeDiskWorkers; i++ {
		largeWriteChans[i] = make(chan *largeWriteJob, netWorkers)
	}
	smallWriteChan := make(chan *smallWriteJob, netWorkers*2)

	smallBudget := newMemoryBudget(d.opts.SmallMemBudget)

	g, gctx := errgroup.WithContext(ctx)

	// 1. Launch Non-Blocking Dual-Lane Ingestion Pipeline
	rawChan := make(chan Elem, 64)
	g.Go(func() error {
		defer close(rawChan)
		for d.opts.Iter.Next(gctx) {
			elem := d.opts.Iter.Value()
			if elem == nil || elem.File() == nil {
				continue
			}
			select {
			case <-gctx.Done():
				return gctx.Err()
			case rawChan <- elem:
			}
		}
		if err := d.opts.Iter.Err(); err != nil {
			return errors.Wrap(err, "iter")
		}
		return nil
	})

	g.Go(func() error {
		defer func() {
			close(largeElemChan)
			close(smallElemChan)
		}()

		var pendingLarge []Elem
		var pendingSmall []Elem
		rawClosed := false

		for {
			if rawClosed && len(pendingLarge) == 0 && len(pendingSmall) == 0 {
				return nil
			}

			var targetLargeChan chan<- Elem
			var nextLarge Elem
			if len(pendingLarge) > 0 {
				targetLargeChan = largeElemChan
				nextLarge = pendingLarge[0]
			}

			var targetSmallChan chan<- Elem
			var nextSmall Elem
			if len(pendingSmall) > 0 {
				targetSmallChan = smallElemChan
				nextSmall = pendingSmall[0]
			}

			var inChan <-chan Elem
			if !rawClosed && len(pendingLarge) < MaxLargeReadyQueue && len(pendingSmall) < MaxSmallReadyQueue {
				inChan = rawChan
			}

			select {
			case <-gctx.Done():
				return gctx.Err()
			case elem, ok := <-inChan:
				if !ok {
					rawClosed = true
					continue
				}
				if elem.File().Size() <= SmallFileThreshold {
					pendingSmall = append(pendingSmall, elem)
				} else {
					pendingLarge = append(pendingLarge, elem)
				}
			case targetLargeChan <- nextLarge:
				pendingLarge = pendingLarge[1:]
			case targetSmallChan <- nextSmall:
				pendingSmall = pendingSmall[1:]
			}
		}
	})

	// 2. Launch Large File Disk Writers (Dedicated 1-to-1 Writer with Reorder Buffer & Tombstone Handling)
	for dw := 0; dw < largeDiskWorkers; dw++ {
		workerIdx := dw
		ch := largeWriteChans[workerIdx]
		g.Go(func() error {
			nextOffset := int64(0)
			pendingChunks := make(map[int64]*largeWriteJob)
			var currentFile *activeLargeFileState

			flushPending := func() {
				if currentFile == nil {
					return
				}

				if atomic.LoadInt32(&currentFile.canceled) == 1 {
					currentFile.doneOnce.Do(func() {
						d.opts.Progress.OnDone(currentFile.elem, currentFile.firstErr)
						close(currentFile.doneChan)
					})
					pendingChunks = make(map[int64]*largeWriteJob)
					return
				}

				for {
					wJob, ok := pendingChunks[nextOffset]
					if !ok {
						break
					}
					delete(pendingChunks, nextOffset)

					if wJob.isFailed {
						currentFile.fail(wJob.err)
						currentFile.doneOnce.Do(func() {
							d.opts.Progress.OnDone(currentFile.elem, currentFile.firstErr)
							close(currentFile.doneChan)
						})
						pendingChunks = make(map[int64]*largeWriteJob)
						return
					}

					n, writeErr := wJob.fileState.writer.WriteAt(wJob.data, wJob.offset)
					if writeErr != nil {
						wJob.fileState.fail(writeErr)
						currentFile.doneOnce.Do(func() {
							d.opts.Progress.OnDone(currentFile.elem, currentFile.firstErr)
							close(currentFile.doneChan)
						})
						pendingChunks = make(map[int64]*largeWriteJob)
						return
					}

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

					nextOffset += int64(len(wJob.data))
					atomic.StoreInt64(&wJob.fileState.lastWrittenOffset, nextOffset)

					if atomic.AddInt32(&wJob.fileState.remParts, -1) == 0 {
						wJob.fileState.doneOnce.Do(func() {
							d.opts.Progress.OnDone(wJob.fileState.elem, wJob.fileState.firstErr)
							close(wJob.fileState.doneChan)
						})
					}
				}
			}

			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case wJob, ok := <-ch:
					if !ok {
						return nil
					}

					if currentFile != wJob.fileState {
						currentFile = wJob.fileState
						nextOffset = 0
						pendingChunks = make(map[int64]*largeWriteJob)
					}

					if wJob.isFailed {
						currentFile.fail(wJob.err)
						currentFile.doneOnce.Do(func() {
							d.opts.Progress.OnDone(currentFile.elem, currentFile.firstErr)
							close(currentFile.doneChan)
						})
						pendingChunks = make(map[int64]*largeWriteJob)
						continue
					}

					pendingChunks[wJob.offset] = wJob
					flushPending()
				}
			}
		})
	}

	// 3. Launch Small File Serial Disk Writer (1 Dedicated I/O Worker)
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

	// 4. Launch 32 Fair-Multiplexed Network Workers
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
				case sJob, ok := <-smallJobChan:
					if !ok {
						// Drain large jobs
						lJob, lOk := <-largeJobChan
						if !lOk {
							return nil
						}
						wIdx := lJob.writerIndex
						if wIdx < 0 || wIdx >= len(largeWriteChans) {
							wIdx = 0
						}
						d.fetchLargeChunk(gctx, workerID, lJob, largeWriteChans[wIdx])
						continue
					}
					d.fetchSmallFile(gctx, workerID, sJob, smallWriteChan, smallBudget)
				case lJob, ok := <-largeJobChan:
					if !ok {
						// Drain small jobs
						sJob, sOk := <-smallJobChan
						if !sOk {
							return nil
						}
						d.fetchSmallFile(gctx, workerID, sJob, smallWriteChan, smallBudget)
						continue
					}
					wIdx := lJob.writerIndex
					if wIdx < 0 || wIdx >= len(largeWriteChans) {
						wIdx = 0
					}
					d.fetchLargeChunk(gctx, workerID, lJob, largeWriteChans[wIdx])
				}
			}
		})
	}

	// Safely close write channels only when all network workers have completely returned
	go func() {
		netWg.Wait()
		for _, ch := range largeWriteChans {
			close(ch)
		}
		close(smallWriteChan)
	}()

	// 5. Launch Priority Dual-Lane Scheduler
	g.Go(func() error {
		defer func() {
			close(largeJobChan)
			close(smallJobChan)
		}()

		activeLargeFiles := make([]*largeFileCursor, 0, maxActiveLargeFiles)
		largeReadyQueue := make([]*largeFileCursor, 0, MaxLargeReadyQueue)
		smallReadyQueue := make([]*smallFetchJob, 0, MaxSmallReadyQueue)

		freeWriterSlots := make([]int, largeDiskWorkers)
		for i := 0; i < largeDiskWorkers; i++ {
			freeWriterSlots[i] = i
		}

		largeChanClosed := false
		smallChanClosed := false

		for {
			// A. Ingest from smallElemChan into smallReadyQueue
			for len(smallReadyQueue) < MaxSmallReadyQueue && !smallChanClosed {
				select {
				case elem, ok := <-smallElemChan:
					if !ok {
						smallChanClosed = true
						break
					}
					totalSize := elem.File().Size()
					if totalSize <= 0 {
						d.opts.Progress.OnAdd(elem)
						d.opts.Progress.OnDone(elem, errors.New("file size is 0 or negative"))
						continue
					}
					d.opts.Progress.OnAdd(elem)
					smallReadyQueue = append(smallReadyQueue, &smallFetchJob{
						elem:      elem,
						dcID:      elem.File().DC(),
						location:  elem.File().Location(),
						totalSize: totalSize,
						takeout:   elem.AsTakeout(),
					})
				default:
					goto DoneSmallIngest
				}
			}
		DoneSmallIngest:

			// B. Ingest from largeElemChan into largeReadyQueue
			for len(largeReadyQueue) < MaxLargeReadyQueue && !largeChanClosed {
				select {
				case elem, ok := <-largeElemChan:
					if !ok {
						largeChanClosed = true
						break
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

					fState := &activeLargeFileState{
						elem:        elem,
						writer:      elem.To(),
						writerIndex: -1,
						totalSize:   totalSize,
						totalParts:  int32(numParts),
						remParts:    int32(numParts),
						doneChan:    make(chan struct{}),
					}

					cursor := &largeFileCursor{
						state:      fState,
						nextPart:   0,
						totalParts: numParts,
						partSize:   partSize,
					}
					largeReadyQueue = append(largeReadyQueue, cursor)
				default:
					goto DoneLargeIngest
				}
			}
		DoneLargeIngest:

			// C. Clean up finished or canceled large files, return leased writer slots, and promote from largeReadyQueue
			filteredLargeFiles := make([]*largeFileCursor, 0, len(activeLargeFiles))
			for _, fc := range activeLargeFiles {
				if ce, ok := fc.state.elem.(CancelableElem); ok && ce.IsCanceled() {
					fc.state.fail(context.Canceled)
				}
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
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
					if fc.state.writerIndex >= 0 {
						freeWriterSlots = append(freeWriterSlots, fc.state.writerIndex)
						fc.state.writerIndex = -1
					}
				default:
					filteredLargeFiles = append(filteredLargeFiles, fc)
				}
			}
			activeLargeFiles = filteredLargeFiles

			// Promote from largeReadyQueue into activeLargeFiles
			for len(activeLargeFiles) < maxActiveLargeFiles && len(largeReadyQueue) > 0 && len(freeWriterSlots) > 0 {
				nextLarge := largeReadyQueue[0]
				largeReadyQueue = largeReadyQueue[1:]

				slot := freeWriterSlots[0]
				freeWriterSlots = freeWriterSlots[1:]
				nextLarge.state.writerIndex = slot

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
			if largeChanClosed && smallChanClosed && len(activeLargeFiles) == 0 && len(largeReadyQueue) == 0 && len(smallReadyQueue) == 0 {
				break
			}

			dispatchedCount := 0

			// D. Priority 1: Guarantee MinInflightPerLarge (4 chunks) within sliding window
			for _, fc := range activeLargeFiles {
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
					continue
				}

				writtenOffset := atomic.LoadInt64(&fc.state.lastWrittenOffset)
				writtenPart := int(writtenOffset / fc.partSize)

				for fc.nextPart < fc.totalParts && atomic.LoadInt32(&fc.state.inflight) < MinInflightPerLarge {
					// Sliding window check: do not dispatch beyond 16 parts ahead of writer
					if fc.nextPart-writtenPart > MaxWindowPartsAhead {
						break
					}

					offset := int64(fc.nextPart) * fc.partSize
					limit := int(fc.partSize)

					job := &largeChunkJob{
						fileState:   fc.state,
						writerIndex: fc.state.writerIndex,
						elem:        fc.state.elem,
						dcID:        fc.state.elem.File().DC(),
						location:    fc.state.elem.File().Location(),
						offset:      offset,
						limit:       limit,
						totalSize:   fc.state.totalSize,
						takeout:     fc.state.elem.AsTakeout(),
					}

					select {
					case <-gctx.Done():
						return gctx.Err()
					case largeJobChan <- job:
						atomic.AddInt32(&fc.state.inflight, 1)
						fc.nextPart++
						dispatchedCount++
					default:
						goto SleepOrYield
					}
				}
			}

			// E. Priority 2: Dispatch Small Files if ready and RAM budget allows
			for len(smallReadyQueue) > 0 {
				nextSmall := smallReadyQueue[0]
				if !smallBudget.tryAcquire(nextSmall.totalSize) {
					break
				}

				select {
				case <-gctx.Done():
					return gctx.Err()
				case smallJobChan <- nextSmall:
					smallReadyQueue = smallReadyQueue[1:]
					dispatchedCount++
				default:
					smallBudget.release(nextSmall.totalSize)
					goto SleepOrYield
				}
			}

			// F. Priority 3: Surplus Worker Capacity -> Feed active large files beyond min-inflight within sliding window
			for _, fc := range activeLargeFiles {
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
					continue
				}

				writtenOffset := atomic.LoadInt64(&fc.state.lastWrittenOffset)
				writtenPart := int(writtenOffset / fc.partSize)

				if fc.nextPart < fc.totalParts && fc.nextPart-writtenPart <= MaxWindowPartsAhead {
					offset := int64(fc.nextPart) * fc.partSize
					limit := int(fc.partSize)

					job := &largeChunkJob{
						fileState:   fc.state,
						writerIndex: fc.state.writerIndex,
						elem:        fc.state.elem,
						dcID:        fc.state.elem.File().DC(),
						location:    fc.state.elem.File().Location(),
						offset:      offset,
						limit:       limit,
						totalSize:   fc.state.totalSize,
						takeout:     fc.state.elem.AsTakeout(),
					}

					select {
					case <-gctx.Done():
						return gctx.Err()
					case largeJobChan <- job:
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
				if largeChanClosed && smallChanClosed && len(activeLargeFiles) == 0 && len(largeReadyQueue) == 0 && len(smallReadyQueue) == 0 {
					break
				}
				select {
				case <-gctx.Done():
					return gctx.Err()
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
		select {
		case <-ctx.Done():
		case writeChan <- &largeWriteJob{
			fileState: job.fileState,
			offset:    job.offset,
			isFailed:  true,
			err:       job.fileState.firstErr,
		}:
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
		job.fileState.fail(context.Canceled)
		select {
		case <-ctx.Done():
		case writeChan <- &largeWriteJob{
			fileState: job.fileState,
			offset:    job.offset,
			isFailed:  true,
			err:       context.Canceled,
		}:
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
			select {
			case writeChan <- &largeWriteJob{fileState: job.fileState, offset: job.offset, isFailed: true, err: ctx.Err()}:
			default:
			}
			return
		case <-elemCtx.Done():
			job.fileState.fail(context.Canceled)
			select {
			case writeChan <- &largeWriteJob{fileState: job.fileState, offset: job.offset, isFailed: true, err: context.Canceled}:
			default:
			}
			return
		default:
		}

		if atomic.LoadInt32(&job.fileState.canceled) == 1 {
			select {
			case writeChan <- &largeWriteJob{fileState: job.fileState, offset: job.offset, isFailed: true, err: job.fileState.firstErr}:
			default:
			}
			return
		}

		chunkCtx, chunkCancel := context.WithTimeout(elemCtx, 60*time.Second)
		var res tg.UploadFileClass
		res, fetchErr = client.UploadGetFile(chunkCtx, req)
		chunkCancel()

		if errors.Is(fetchErr, context.Canceled) || elemCtx.Err() != nil {
			job.fileState.fail(context.Canceled)
			select {
			case writeChan <- &largeWriteJob{fileState: job.fileState, offset: job.offset, isFailed: true, err: context.Canceled}:
			default:
			}
			return
		}

		if fetchErr == nil {
			if uf, ok := res.(*tg.UploadFile); ok {
				chunkData = uf.Bytes
				if len(chunkData) > expectedBytes {
					chunkData = chunkData[:expectedBytes]
				}
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
			select {
			case writeChan <- &largeWriteJob{fileState: job.fileState, offset: job.offset, isFailed: true, err: ctx.Err()}:
			default:
			}
			return
		case <-elemCtx.Done():
			job.fileState.fail(context.Canceled)
			select {
			case writeChan <- &largeWriteJob{fileState: job.fileState, offset: job.offset, isFailed: true, err: context.Canceled}:
			default:
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
		select {
		case <-ctx.Done():
		case writeChan <- &largeWriteJob{
			fileState: job.fileState,
			offset:    job.offset,
			isFailed:  true,
			err:       fetchErr,
		}:
		}
		return
	}

	select {
	case <-ctx.Done():
		job.fileState.fail(ctx.Err())
		select {
		case writeChan <- &largeWriteJob{fileState: job.fileState, offset: job.offset, isFailed: true, err: ctx.Err()}:
		default:
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
		expectedBytes := MaxPartSize
		if offset+int64(expectedBytes) > job.totalSize {
			expectedBytes = int(job.totalSize - offset)
		}

		req := &tg.UploadGetFileRequest{
			Precise:  true,
			Location: job.location,
			Offset:   offset,
			Limit:    MaxPartSize,
		}

		var chunkSuccess bool
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
			res, fetchErr := client.UploadGetFile(chunkCtx, req)
			chunkCancel()

			if errors.Is(fetchErr, context.Canceled) || elemCtx.Err() != nil {
				d.opts.Progress.OnDone(job.elem, context.Canceled)
				budget.release(job.totalSize)
				return
			}

			if fetchErr == nil {
				if uf, ok := res.(*tg.UploadFile); ok {
					chunkBytes := uf.Bytes
					if len(chunkBytes) > expectedBytes {
						chunkBytes = chunkBytes[:expectedBytes]
					}
					if len(chunkBytes) == expectedBytes {
						fileBuffer = append(fileBuffer, chunkBytes...)
						offset += int64(len(chunkBytes))
						chunkSuccess = true
						break
					}
					fetchErr = fmt.Errorf("small file short read at offset %d: expected %d bytes, got %d", offset, expectedBytes, len(chunkBytes))
				} else {
					fetchErr = fmt.Errorf("unexpected upload response type: %T", res)
				}
			}

			if tgerr.Is(fetchErr, "FILE_REFERENCE_EXPIRED", "FILEREF_UPGRADE_NEEDED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID") {
				d.opts.Progress.OnDone(job.elem, fetchErr)
				budget.release(job.totalSize)
				return
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

		if !chunkSuccess {
			d.opts.Progress.OnDone(job.elem, fmt.Errorf("failed to fetch small file chunk at offset %d", offset))
			budget.release(job.totalSize)
			return
		}

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
