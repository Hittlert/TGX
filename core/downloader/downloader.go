package downloader

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
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
	DownloadPartSize    = 1024 * 1024       // 1 MiB per MTProto chunk for high throughput
	MaxPartSize         = DownloadPartSize  // Compatibility alias
	SmallFileThreshold  = 1024 * 1024       // <= 1 MiB goes to Small File Lane (whole-file in-memory)
	DefaultSmallMemory  = 128 * 1024 * 1024 // 128 MiB dedicated in-memory budget for small files
	MinInflightPerLarge = 4                 // 4 chunk in-flight guarantee per active large file
	MaxLargeReadyQueue  = 10                // Bounded capacity for ready large files
	MaxSmallReadyQueue  = 64                // Bounded capacity for ready small files
	MaxLargeWindowBytes = 16 * 1024 * 1024  // Strict 16 MiB sliding window ahead of disk writer
	TargetLargeBPS      = 125 * 1024 * 1024 // 1 Gbps (125 MB/s) target throughput for large files
)

type Downloader struct {
	opts           Options
	floodGate      *gate.FloodGate
	activeLargeRPC int64
	activeSmallRPC int64
	queuedLarge    int64
	queuedSmall    int64
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
		fg = gate.NewFloodGate(gate.InitialStartRate, gate.DefaultBurst)
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

// Stats returns the live broker metrics.
func (d *Downloader) Stats() (activeLargeRPC, activeSmallRPC, queuedLarge, queuedSmall int64) {
	return atomic.LoadInt64(&d.activeLargeRPC), atomic.LoadInt64(&d.activeSmallRPC), atomic.LoadInt64(&d.queuedLarge), atomic.LoadInt64(&d.queuedSmall)
}

// largeChunkJob represents an atomic 1MiB chunk fetch for an active large file (>= 1 MiB).
type largeChunkJob struct {
	fileState   *activeLargeFileState
	leaseGen    uint64
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
	leaseGen  uint64
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
	leaseGen          uint64
	writerIndex       int
	totalSize         int64
	totalParts        int32
	remParts          int32
	inflight          int32
	activeRPC         int32
	downloadedBytes   int64
	lastWrittenOffset int64
	maxProgress       int64
	progMu            sync.Mutex
	canceled          int32
	errMu             sync.Mutex
	firstErr          error
	doneOnce          sync.Once
	doneChan          chan struct{}
}

func (s *activeLargeFileState) fail(err error) {
	if err == nil {
		err = context.Canceled
	}
	s.errMu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.errMu.Unlock()
	atomic.StoreInt32(&s.canceled, 1)
}

func (s *activeLargeFileState) error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.firstErr != nil {
		return s.firstErr
	}
	if atomic.LoadInt32(&s.canceled) == 1 {
		return context.Canceled
	}
	return nil
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
		netWorkers = 48
	}
	if globalConcurrency > 0 {
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
	smallWriteChan := make(chan *smallWriteJob, 4096)

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
		var stagedItem Elem
		rawClosed := false

		for {
			if rawClosed && len(pendingLarge) == 0 && len(pendingSmall) == 0 && stagedItem == nil {
				return nil
			}

			if stagedItem != nil {
				if stagedItem.File().Size() <= SmallFileThreshold {
					if len(pendingSmall) < MaxSmallReadyQueue {
						pendingSmall = append(pendingSmall, stagedItem)
						stagedItem = nil
					}
				} else {
					if len(pendingLarge) < MaxLargeReadyQueue {
						pendingLarge = append(pendingLarge, stagedItem)
						stagedItem = nil
					}
				}
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
			if !rawClosed && stagedItem == nil && (len(pendingLarge) < MaxLargeReadyQueue || len(pendingSmall) < MaxSmallReadyQueue) {
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
					if len(pendingSmall) < MaxSmallReadyQueue {
						pendingSmall = append(pendingSmall, elem)
					} else {
						stagedItem = elem
					}
				} else {
					if len(pendingLarge) < MaxLargeReadyQueue {
						pendingLarge = append(pendingLarge, elem)
					} else {
						stagedItem = elem
					}
				}
			case targetLargeChan <- nextLarge:
				pendingLarge = pendingLarge[1:]
			case targetSmallChan <- nextSmall:
				pendingSmall = pendingSmall[1:]
			}
		}
	})

	// 2. Launch Large File Disk Writers (Dedicated 1-to-1 Writer with Reorder Buffer & Generation Protection)
	for dw := 0; dw < largeDiskWorkers; dw++ {
		workerIdx := dw
		ch := largeWriteChans[workerIdx]
		g.Go(func() error {
			nextOffset := int64(0)
			pendingChunks := make(map[int64]*largeWriteJob)
			var currentFile *activeLargeFileState
			var currentLeaseGen uint64

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
							d.opts.Progress.OnDone(currentFile.elem, currentFile.error())
							close(currentFile.doneChan)
						})
						pendingChunks = make(map[int64]*largeWriteJob)
						return
					}

					_, writeErr := wJob.fileState.writer.WriteAt(wJob.data, wJob.offset)
					if writeErr != nil {
						wJob.fileState.fail(writeErr)
						currentFile.doneOnce.Do(func() {
							d.opts.Progress.OnDone(currentFile.elem, currentFile.error())
							close(currentFile.doneChan)
						})
						pendingChunks = make(map[int64]*largeWriteJob)
						return
					}

					nextOffset += int64(len(wJob.data))
					atomic.StoreInt64(&wJob.fileState.lastWrittenOffset, nextOffset)

					if atomic.AddInt32(&wJob.fileState.remParts, -1) == 0 {
						wJob.fileState.doneOnce.Do(func() {
							d.opts.Progress.OnDone(wJob.fileState.elem, wJob.fileState.error())
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

					// Stale packet check from an older lease generation: ignore without side effects!
					if wJob.leaseGen < currentLeaseGen {
						continue
					}

					if currentFile != wJob.fileState || wJob.leaseGen > currentLeaseGen {
						currentFile = wJob.fileState
						currentLeaseGen = wJob.leaseGen
						nextOffset = 0
						pendingChunks = make(map[int64]*largeWriteJob)
					}

					if wJob.isFailed {
						currentFile.fail(wJob.err)
						currentFile.doneOnce.Do(func() {
							d.opts.Progress.OnDone(currentFile.elem, currentFile.error())
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

	// 3. Launch Dedicated Small File Disk Writers (4 Concurrent I/O Workers)
	const smallDiskWorkers = 4
	for sdw := 0; sdw < smallDiskWorkers; sdw++ {
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
					if ce, ok := sJob.elem.(CancelableElem); ok && ce.IsCanceled() {
						finalErr = context.Canceled
					} else if sJob.err != nil {
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
	}

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
		globalLeaseGen := uint64(0)

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
						atomic.AddInt32(&fc.state.remParts, -undispatched)
					}
					if fc.state.writerIndex >= 0 {
						select {
						case largeWriteChans[fc.state.writerIndex] <- &largeWriteJob{
							fileState: fc.state,
							leaseGen:  fc.state.leaseGen,
							isFailed:  true,
							err:       fc.state.error(),
						}:
						default:
						}
					}
				}
				select {
				case <-fc.state.doneChan:
					// Draining invariant: Only return writer slot when:
					// 1. ALL in-flight network chunks have completed (inflight == 0)
					// 2. AND the disk writer has finished all pending writes and closed doneChan!
					if atomic.LoadInt32(&fc.state.inflight) == 0 {
						if fc.state.writerIndex >= 0 {
							freeWriterSlots = append(freeWriterSlots, fc.state.writerIndex)
							fc.state.writerIndex = -1
						}
					} else {
						// Keep in activeLargeFiles until in-flight chunks drain
						filteredLargeFiles = append(filteredLargeFiles, fc)
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
				globalLeaseGen++
				nextLarge.state.leaseGen = globalLeaseGen
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

			atomic.StoreInt64(&d.queuedLarge, int64(len(largeReadyQueue)))
			atomic.StoreInt64(&d.queuedSmall, int64(len(smallReadyQueue)))

			dispatchedCount := 0

			// D. Priority 1: Guarantee MinInflightPerLarge (4 chunks) per active large file within byte sliding window
		LargeGuaranteedLoop:
			for _, fc := range activeLargeFiles {
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
					continue
				}

				writtenOffset := atomic.LoadInt64(&fc.state.lastWrittenOffset)

				for fc.nextPart < fc.totalParts && atomic.LoadInt32(&fc.state.inflight) < MinInflightPerLarge {
					currentOffset := int64(fc.nextPart) * fc.partSize
					// Strict byte-based sliding window check: do not dispatch beyond 16 MiB ahead of writer
					if (currentOffset+fc.partSize)-writtenOffset > MaxLargeWindowBytes {
						break
					}

					offset := currentOffset
					limit := int(fc.partSize)

					job := &largeChunkJob{
						fileState:   fc.state,
						leaseGen:    fc.state.leaseGen,
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
						break LargeGuaranteedLoop // channel full: exit Priority 1 immediately to prevent busy-spin
					}
				}
			}

			// E. Priority 2: Feed Surplus Capacity to Large Files (Targeting 1 Gbps / 125 MB/s)
			currentCap := int64(gate.MaxDataInFlight)
			if d.floodGate != nil {
				currentCap = d.floodGate.MaxDataCap()
			}

		LargeSurplusLoop:
			for _, fc := range activeLargeFiles {
				if atomic.LoadInt32(&fc.state.canceled) == 1 {
					continue
				}

				writtenOffset := atomic.LoadInt64(&fc.state.lastWrittenOffset)

				for fc.nextPart < fc.totalParts {
					currTotalRPC := atomic.LoadInt64(&d.activeLargeRPC) + atomic.LoadInt64(&d.activeSmallRPC)
					if currTotalRPC >= currentCap {
						break LargeSurplusLoop
					}

					currentOffset := int64(fc.nextPart) * fc.partSize
					if (currentOffset+fc.partSize)-writtenOffset > MaxLargeWindowBytes {
						break
					}

					offset := currentOffset
					limit := int(fc.partSize)

					job := &largeChunkJob{
						fileState:   fc.state,
						leaseGen:    fc.state.leaseGen,
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
						break LargeSurplusLoop // channel full: exit Priority 2 immediately to prevent busy-spin
					}
				}
			}

			// Calculate actual needed dispatchable reservation for active large files
			var guaranteedForLarge int64
			for _, fc := range activeLargeFiles {
				if atomic.LoadInt32(&fc.state.canceled) == 0 && fc.nextPart < fc.totalParts {
					writtenOffset := atomic.LoadInt64(&fc.state.lastWrittenOffset)
					maxOffset := writtenOffset + MaxLargeWindowBytes
					currentOffset := int64(fc.nextPart) * fc.partSize
					if maxOffset > currentOffset {
						windowRemaining := int((maxOffset - currentOffset) / fc.partSize)
						partsRemaining := fc.totalParts - fc.nextPart
						dispatchable := min(partsRemaining, max(1, windowRemaining))
						guaranteedForLarge += int64(min(dispatchable, MinInflightPerLarge))
					}
				}
			}
			if guaranteedForLarge > (currentCap - 4) {
				guaranteedForLarge = max(0, currentCap-4) // Always leave at least 4 slots for small lane
			}

			// F. Priority 3: Dispatch Small Files if ready, RAM budget allows, and without starving large files
		SmallDispatchLoop:
			for len(smallReadyQueue) > 0 {
				currLargeActive := atomic.LoadInt64(&d.activeLargeRPC)
				currSmallActive := atomic.LoadInt64(&d.activeSmallRPC)
				currTotalActive := currLargeActive + currSmallActive

				if currTotalActive >= currentCap {
					break
				}
				// Small lane can dispatch as long as it doesn't starve guaranteed large slots,
				// and is guaranteed up to 4 concurrent files if total capacity permits.
				if currSmallActive >= 4 && currSmallActive >= (currentCap-guaranteedForLarge) {
					break
				}

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
					break SmallDispatchLoop // channel full: exit Priority 3 immediately
				}
			}

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

func (d *Downloader) handleCdnRedirect(ctx context.Context, cdnClient *tg.Client, masterClient *tg.Client, redirect *tg.UploadFileCDNRedirect, originDC int, offset int64, limit int) ([]byte, error) {
	const maxCDNAttempts = 3
	req := &tg.UploadGetCDNFileRequest{
		FileToken: redirect.FileToken,
		Offset:    offset,
		Limit:     limit,
	}

	// Single overall deadline for entire CDN operation including potential reuploads
	cdnTotalCtx, cdnCancel := context.WithTimeout(ctx, 60*time.Second)
	defer cdnCancel()

	for attempt := 0; attempt < maxCDNAttempts; attempt++ {
		// 1. Enforce rate limiting and cooldown specifically for the CDN DC
		if d.floodGate != nil {
			if err := d.floodGate.Wait(cdnTotalCtx, redirect.DCID); err != nil {
				return nil, err
			}
		}

		// Pass single-use admission ticket for redirect.DCID
		cdnReqCtx := gate.WithAdmissionTicket(cdnTotalCtx, redirect.DCID)
		res, err := cdnClient.UploadGetCDNFile(cdnReqCtx, req)
		if err != nil {
			return nil, err
		}

		switch cdnRes := res.(type) {
		case *tg.UploadCDNFile:
			data := cdnRes.Bytes
			chunkStart := offset
			chunkEnd := offset + int64(len(data))

			// 2. Full Continuous Range SHA-256 Hash Verification
			if len(redirect.FileHashes) == 0 {
				return nil, fmt.Errorf("CDN redirect missing file hashes for offset %d", offset)
			}

			type verifiedRange struct {
				start int64
				end   int64
			}
			var ranges []verifiedRange

			for _, h := range redirect.FileHashes {
				hStart := h.Offset
				hEnd := h.Offset + int64(h.Limit)
				if hStart >= chunkStart && hEnd <= chunkEnd && h.Limit > 0 {
					subStart := hStart - chunkStart
					subEnd := subStart + int64(h.Limit)
					subData := data[subStart:subEnd]
					sum := sha256.Sum256(subData)
					if !bytes.Equal(sum[:], h.Hash) {
						return nil, fmt.Errorf("CDN chunk SHA256 mismatch at sub-range [%d, %d)", hStart, hEnd)
					}
					ranges = append(ranges, verifiedRange{start: hStart, end: hEnd})
				}
			}

			if len(ranges) == 0 {
				return nil, fmt.Errorf("no valid CDN file hashes covering chunk [%d, %d)", chunkStart, chunkEnd)
			}

			// Sort ranges by start offset
			sort.Slice(ranges, func(i, j int) bool {
				return ranges[i].start < ranges[j].start
			})

			// Verify contiguous full coverage from chunkStart to chunkEnd without gaps
			currentCovered := chunkStart
			for _, r := range ranges {
				if r.start > currentCovered {
					return nil, fmt.Errorf("gap in CDN file hash coverage at offset %d (next hash starts at %d)", currentCovered, r.start)
				}
				if r.end > currentCovered {
					currentCovered = r.end
				}
			}
			if currentCovered < chunkEnd {
				return nil, fmt.Errorf("CDN file hashes do not fully cover end of chunk (covered to %d, expected %d)", currentCovered, chunkEnd)
			}

			// 3. Decrypt with AES-CTR
			if len(redirect.EncryptionKey) > 0 && len(redirect.EncryptionIv) > 0 {
				block, err := aes.NewCipher(redirect.EncryptionKey)
				if err != nil {
					return nil, err
				}
				iv := make([]byte, len(redirect.EncryptionIv))
				copy(iv, redirect.EncryptionIv)
				if len(iv) >= 16 {
					ivSeq := binary.BigEndian.Uint32(iv[12:16]) + uint32(offset/16)
					binary.BigEndian.PutUint32(iv[12:16], ivSeq)
				}
				stream := cipher.NewCTR(block, iv)
				stream.XORKeyStream(data, data)
			}
			return data, nil

		case *tg.UploadCDNFileReuploadNeeded:
			if masterClient == nil {
				return nil, tgerr.New(400, "CDN_REUPLOAD_NEEDED_NO_MASTER")
			}
			reupReq := &tg.UploadReuploadCDNFileRequest{
				FileToken:    redirect.FileToken,
				RequestToken: cdnRes.RequestToken,
			}
			// Wait on Origin DC Gate for reupload request
			if d.floodGate != nil {
				if err := d.floodGate.Wait(cdnTotalCtx, originDC); err != nil {
					return nil, err
				}
			}
			masterReqCtx := gate.WithAdmissionTicket(cdnTotalCtx, originDC)
			hashes, reupErr := masterClient.UploadReuploadCDNFile(masterReqCtx, reupReq)
			if reupErr != nil {
				return nil, fmt.Errorf("CDN reupload failed on master DC: %w", reupErr)
			}
			if len(hashes) > 0 {
				redirect.FileHashes = append(redirect.FileHashes, hashes...)
			}
			// Continue bounded loop to retry fetching from CDN DC with newly provided hashes
			continue

		default:
			return nil, fmt.Errorf("unexpected cdn file type: %T", res)
		}
	}
	return nil, tgerr.New(400, "CDN_REUPLOAD_ATTEMPTS_EXHAUSTED")
}

// fetchLargeChunk downloads a single 1MiB chunk for an active large file.
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
			leaseGen:  job.leaseGen,
			offset:    job.offset,
			isFailed:  true,
			err:       job.fileState.error(),
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
			leaseGen:  job.leaseGen,
			offset:    job.offset,
			isFailed:  true,
			err:       job.fileState.error(),
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

	for attempt := 0; attempt < 3; attempt++ {
		select {
		case <-ctx.Done():
			job.fileState.fail(ctx.Err())
			select {
			case <-ctx.Done():
			case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: ctx.Err()}:
			}
			return
		case <-elemCtx.Done():
			job.fileState.fail(context.Canceled)
			select {
			case <-ctx.Done():
			case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: context.Canceled}:
			}
			return
		default:
		}

		if atomic.LoadInt32(&job.fileState.canceled) == 1 {
			select {
			case <-ctx.Done():
			case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: job.fileState.error()}:
			}
			return
		}

		// Atomic admission: checks DC cooldown without holding slot, takes rate token, then acquires dynamic slot.
		var releaseSlot func()
		var ticket *gate.AdmissionTicket
		if d.floodGate != nil {
			var err error
			releaseSlot, ticket, err = d.floodGate.AcquireDataPermit(elemCtx, job.dcID)
			if err != nil {
				job.fileState.fail(err)
				select {
				case <-ctx.Done():
				case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: err}:
				}
				return
			}
		} else {
			releaseSlot = func() {}
			ticket = &gate.AdmissionTicket{}
		}

		atomic.AddInt32(&job.fileState.activeRPC, 1)
		atomic.AddInt64(&d.activeLargeRPC, 1)

		chunkCtx, chunkCancel := context.WithTimeout(gate.WithTicket(elemCtx, ticket), 60*time.Second)
		var res tg.UploadFileClass
		res, fetchErr = client.UploadGetFile(chunkCtx, req)
		chunkCancel()

		var chunkSuccess bool
		if fetchErr == nil {
			switch uf := res.(type) {
			case *tg.UploadFile:
				chunkData = uf.Bytes
				if len(chunkData) > expectedBytes {
					chunkData = chunkData[:expectedBytes]
				}
				if len(chunkData) == expectedBytes {
					currBytes := atomic.AddInt64(&job.fileState.downloadedBytes, int64(len(chunkData)))
					job.fileState.progMu.Lock()
					if currBytes > job.fileState.maxProgress {
						job.fileState.maxProgress = currBytes
						d.opts.Progress.OnDownload(job.fileState.elem, ProgressState{
							Downloaded: currBytes,
							Total:      job.fileState.totalSize,
						})
					}
					job.fileState.progMu.Unlock()
					chunkSuccess = true
				} else {
					fetchErr = fmt.Errorf("short read at offset %d: expected %d bytes, got %d", job.offset, expectedBytes, len(chunkData))
				}
			case *tg.UploadFileCDNRedirect:
				cdnClient := d.opts.Pool.Client(elemCtx, uf.DCID)
				chunkData, fetchErr = d.handleCdnRedirect(elemCtx, cdnClient, client, uf, job.dcID, job.offset, job.limit)
				if fetchErr == nil {
					if len(chunkData) > expectedBytes {
						chunkData = chunkData[:expectedBytes]
					}
					if len(chunkData) == expectedBytes {
						currBytes := atomic.AddInt64(&job.fileState.downloadedBytes, int64(len(chunkData)))
						job.fileState.progMu.Lock()
						if currBytes > job.fileState.maxProgress {
							job.fileState.maxProgress = currBytes
							d.opts.Progress.OnDownload(job.fileState.elem, ProgressState{
								Downloaded: currBytes,
								Total:      job.fileState.totalSize,
							})
						}
						job.fileState.progMu.Unlock()
						chunkSuccess = true
					} else {
						fetchErr = fmt.Errorf("cdn short read at offset %d: expected %d bytes, got %d", job.offset, expectedBytes, len(chunkData))
					}
				}
			default:
				fetchErr = fmt.Errorf("unexpected upload response type: %T", res)
			}
		}

		atomic.AddInt32(&job.fileState.activeRPC, -1)
		atomic.AddInt64(&d.activeLargeRPC, -1)
		releaseSlot()

		if errors.Is(fetchErr, context.Canceled) || elemCtx.Err() != nil {
			job.fileState.fail(context.Canceled)
			select {
			case <-ctx.Done():
			case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: context.Canceled}:
			}
			return
		}

		if chunkSuccess {
			break
		}

		if tgerr.Is(fetchErr, "FILE_REFERENCE_EXPIRED", "FILEREF_UPGRADE_NEEDED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID", "LIMIT_INVALID", "OFFSET_INVALID") {
			break
		}

		if dWait, isFlood := tgerr.AsFloodWait(fetchErr); isFlood {
			logctx.From(ctx).Warn("UploadGetFile flood wait triggered",
				zap.Int("dc", job.dcID),
				zap.Duration("flood_wait", dWait),
			)
			if d.floodGate != nil {
				d.floodGate.TriggerFloodWait(job.dcID, dWait)
			}
			continue
		}

		if d.floodGate != nil && fetchErr != nil {
			d.floodGate.TriggerTransportError(fetchErr)
		}

		retryDelay := time.Duration(1<<attempt) * 100 * time.Millisecond

		select {
		case <-ctx.Done():
			job.fileState.fail(ctx.Err())
			select {
			case <-ctx.Done():
			case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: ctx.Err()}:
			}
			return
		case <-elemCtx.Done():
			job.fileState.fail(context.Canceled)
			select {
			case <-ctx.Done():
			case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: context.Canceled}:
			}
			return
		case <-time.After(retryDelay):
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
			leaseGen:  job.leaseGen,
			offset:    job.offset,
			isFailed:  true,
			err:       job.fileState.error(),
		}:
		}
		return
	}

	select {
	case <-ctx.Done():
		job.fileState.fail(ctx.Err())
		select {
		case <-ctx.Done():
		case writeChan <- &largeWriteJob{fileState: job.fileState, leaseGen: job.leaseGen, offset: job.offset, isFailed: true, err: ctx.Err()}:
		}
	case writeChan <- &largeWriteJob{
		fileState: job.fileState,
		leaseGen:  job.leaseGen,
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
		if elemCtx.Err() != nil {
			d.opts.Progress.OnDone(job.elem, context.Canceled)
			budget.release(job.totalSize)
			return
		}

		expectedBytes := int(job.totalSize - offset)
		if expectedBytes > DownloadPartSize {
			expectedBytes = DownloadPartSize
		}

		req := &tg.UploadGetFileRequest{
			Precise:  true,
			Location: job.location,
			Offset:   offset,
			Limit:    DownloadPartSize,
		}

		var chunkSuccess bool
		var lastFetchErr error
		for attempt := 0; attempt < 3; attempt++ {
			if elemCtx.Err() != nil {
				d.opts.Progress.OnDone(job.elem, context.Canceled)
				budget.release(job.totalSize)
				return
			}

			var releaseSlot func()
			var ticket *gate.AdmissionTicket

			if d.floodGate != nil {
				var err error
				releaseSlot, ticket, err = d.floodGate.AcquireDataPermit(elemCtx, job.dcID)
				if err != nil {
					d.opts.Progress.OnDone(job.elem, err)
					budget.release(job.totalSize)
					return
				}
			} else {
				releaseSlot = func() {}
				ticket = &gate.AdmissionTicket{}
			}

			atomic.AddInt64(&d.activeSmallRPC, 1)

			chunkCtx, chunkCancel := context.WithTimeout(gate.WithTicket(elemCtx, ticket), 60*time.Second)
			res, fetchErr := client.UploadGetFile(chunkCtx, req)
			chunkCancel()
			lastFetchErr = fetchErr

			if fetchErr == nil {
				switch uf := res.(type) {
				case *tg.UploadFile:
					chunkBytes := uf.Bytes
					if len(chunkBytes) > 0 {
						if len(chunkBytes) > expectedBytes {
							chunkBytes = chunkBytes[:expectedBytes]
						}
						fileBuffer = append(fileBuffer, chunkBytes...)
						offset += int64(len(chunkBytes))
						chunkSuccess = true
					} else {
						// 0 bytes returned from Telegram means EOF reached
						chunkSuccess = true
					}
				case *tg.UploadFileCDNRedirect:
					cdnClient := d.opts.Pool.Client(elemCtx, uf.DCID)
					var cdnBytes []byte
					cdnBytes, fetchErr = d.handleCdnRedirect(elemCtx, cdnClient, client, uf, job.dcID, offset, DownloadPartSize)
					lastFetchErr = fetchErr
					if fetchErr == nil {
						if len(cdnBytes) > 0 {
							if len(cdnBytes) > expectedBytes {
								cdnBytes = cdnBytes[:expectedBytes]
							}
							fileBuffer = append(fileBuffer, cdnBytes...)
							offset += int64(len(cdnBytes))
							chunkSuccess = true
						} else {
							chunkSuccess = true
						}
					}
				default:
					fetchErr = fmt.Errorf("unexpected upload response type: %T", res)
					lastFetchErr = fetchErr
				}
			}

			atomic.AddInt64(&d.activeSmallRPC, -1)
			releaseSlot()

			if errors.Is(fetchErr, context.Canceled) || elemCtx.Err() != nil {
				d.opts.Progress.OnDone(job.elem, context.Canceled)
				budget.release(job.totalSize)
				return
			}

			if chunkSuccess {
				d.opts.Progress.OnDownload(job.elem, ProgressState{
					Downloaded: offset,
					Total:      offset,
				})
				break
			}

			if tgerr.Is(fetchErr, "FILE_REFERENCE_EXPIRED", "FILEREF_UPGRADE_NEEDED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID", "LIMIT_INVALID", "OFFSET_INVALID") {
				d.opts.Progress.OnDone(job.elem, fetchErr)
				budget.release(job.totalSize)
				return
			}

			if dWait, isFlood := tgerr.AsFloodWait(fetchErr); isFlood {
				logctx.From(ctx).Warn("small file flood wait triggered",
					zap.Int("dc", job.dcID),
					zap.Duration("flood_wait", dWait),
				)
				if d.floodGate != nil {
					d.floodGate.TriggerFloodWait(job.dcID, dWait)
				}
				continue
			}

			if d.floodGate != nil && fetchErr != nil {
				d.floodGate.TriggerTransportError(fetchErr)
			}

			retryDelay := time.Duration(1<<attempt) * 100 * time.Millisecond

			select {
			case <-ctx.Done():
				d.opts.Progress.OnDone(job.elem, ctx.Err())
				budget.release(job.totalSize)
				return
			case <-elemCtx.Done():
				d.opts.Progress.OnDone(job.elem, context.Canceled)
				budget.release(job.totalSize)
				return
			case <-time.After(retryDelay):
			}
		}

		if !chunkSuccess {
			d.opts.Progress.OnDone(job.elem, fmt.Errorf("failed to fetch small file chunk at offset %d: %w", offset, lastFetchErr))
			budget.release(job.totalSize)
			return
		}

		if offset >= job.totalSize || (len(fileBuffer) > 0 && int64(len(fileBuffer)) == offset && offset < job.totalSize) {
			break
		}
	}

	actualSize := int64(len(fileBuffer))
	if actualSize == 0 {
		d.opts.Progress.OnDone(job.elem, errors.New("downloaded empty 0-byte file"))
		budget.release(job.totalSize)
		return
	}

	// Check if canceled before sending to small disk writer
	if ce, ok := job.elem.(CancelableElem); ok && ce.IsCanceled() {
		d.opts.Progress.OnDone(job.elem, context.Canceled)
		budget.release(job.totalSize)
		return
	}

	// Send completed whole-file to the Small Disk Writer
	select {
	case <-ctx.Done():
		d.opts.Progress.OnDone(job.elem, ctx.Err())
		budget.release(job.totalSize)
	case writeChan <- &smallWriteJob{
		elem:      job.elem,
		data:      fileBuffer,
		totalSize: actualSize,
	}:
	}
}
