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
	Pool      dcpool.Pool
	Threads   int
	Iter      Iter
	Progress  Progress
	FloodGate *gate.FloodGate
}

func New(opts Options) *Downloader {
	fg := opts.FloodGate
	if fg == nil {
		fg = gate.NewFloodGate(40.0, 10)
	}
	return &Downloader{
		opts:      opts,
		floodGate: fg,
	}
}

// chunkJob represents a single atomic 512KB block download task dispatched to the global worker pool.
type chunkJob struct {
	elem      Elem
	dcID      int
	location  tg.InputFileLocationClass
	offset    int64
	limit     int
	totalSize int64
	writer    io.WriterAt
	takeout   bool
	onSuccess func(n int)
	onFailure func(err error)
}

type fileState struct {
	elem       Elem
	totalParts int32
	remParts   int32
	firstErr   error
	errOnce    sync.Once
	doneOnce   sync.Once
	doneChan   chan struct{}
}

// Download runs the global streaming block engine with exactly numWorkers concurrent workers.
// The Downloader operates purely at the chunk/block level with zero file-level nested concurrency.
func (d *Downloader) Download(ctx context.Context, globalConcurrency int) error {
	numWorkers := d.opts.Threads
	if numWorkers <= 0 {
		numWorkers = 32
	}
	if globalConcurrency > 0 && globalConcurrency < numWorkers {
		numWorkers = globalConcurrency
	}

	// Buffered channel for global chunk dispatch.
	// When the queue is full, the feeder automatically blocks on submission,
	// providing natural, zero-lag backpressure to the upstream file resolver / Iter.
	jobQueueSize := numWorkers * 2
	jobChan := make(chan *chunkJob, jobQueueSize)

	g, gctx := errgroup.WithContext(ctx)

	// 1. Launch strictly numWorkers global chunk download workers
	for w := 0; w < numWorkers; w++ {
		workerID := w
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case job, ok := <-jobChan:
					if !ok {
						return nil
					}
					d.executeChunk(gctx, workerID, job)
				}
			}
		})
	}

	// 2. Feeder: reads files from Iter and emits individual 512KB chunks into the global jobChan
	g.Go(func() error {
		defer close(jobChan)

		for d.opts.Iter.Next(gctx) {
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

			fState := &fileState{
				elem:       elem,
				totalParts: int32(numParts),
				remParts:   int32(numParts),
				doneChan:   make(chan struct{}),
			}

			var downloadedBytes int64

			for i := 0; i < numParts; i++ {
				offset := int64(i) * partSize
				limit := int(partSize)

				job := &chunkJob{
					elem:      elem,
					dcID:      elem.File().DC(),
					location:  elem.File().Location(),
					offset:    offset,
					limit:     limit,
					totalSize: totalSize,
					writer:    elem.To(),
					takeout:   elem.AsTakeout(),
					onSuccess: func(n int) {
						curr := atomic.AddInt64(&downloadedBytes, int64(n))
						d.opts.Progress.OnDownload(elem, ProgressState{
							Downloaded: curr,
							Total:      totalSize,
						})
						if atomic.AddInt32(&fState.remParts, -1) == 0 {
							fState.doneOnce.Do(func() {
								d.opts.Progress.OnDone(elem, fState.firstErr)
								close(fState.doneChan)
							})
						}
					},
					onFailure: func(err error) {
						fState.errOnce.Do(func() {
							fState.firstErr = err
						})
						if atomic.AddInt32(&fState.remParts, -1) == 0 {
							fState.doneOnce.Do(func() {
								d.opts.Progress.OnDone(elem, fState.firstErr)
								close(fState.doneChan)
							})
						}
					},
				}

				select {
				case <-gctx.Done():
					return gctx.Err()
				case jobChan <- job: // Naturally blocks if queue is full -> Backpressure to upstream Iter
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

// executeChunk pulls a single 512KB chunk over the DC connection pool and writes it into the target WriterAt.
func (d *Downloader) executeChunk(ctx context.Context, workerID int, job *chunkJob) {
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
			job.onFailure(ctx.Err())
			return
		default:
		}

		if d.floodGate != nil {
			if err := d.floodGate.Wait(ctx, job.dcID); err != nil {
				job.onFailure(err)
				return
			}
		}

		chunkCtx, chunkCancel := context.WithTimeout(ctx, 60*time.Second)
		var res tg.UploadFileClass
		res, fetchErr = client.UploadGetFile(chunkCtx, req)
		chunkCancel()

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
			job.onFailure(ctx.Err())
			return
		case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
		}
	}

	if fetchErr != nil || len(chunkData) != expectedBytes {
		if fetchErr != nil {
			job.onFailure(fetchErr)
		} else {
			job.onFailure(fmt.Errorf("chunk size mismatch at offset %d: expected %d, got %d", job.offset, expectedBytes, len(chunkData)))
		}
		return
	}

	n, writeErr := job.writer.WriteAt(chunkData, job.offset)
	if writeErr != nil {
		job.onFailure(writeErr)
		return
	}

	job.onSuccess(n)
}
