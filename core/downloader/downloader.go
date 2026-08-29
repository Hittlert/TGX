package downloader

import (
	"context"
	"fmt"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/util/tutil"
)

// MaxPartSize refer to https://core.telegram.org/api/files#downloading-files
const MaxPartSize = 512 * 1024

type Downloader struct {
	opts Options
}

type Options struct {
	Pool     dcpool.Pool
	Threads  int
	Iter     Iter
	Progress Progress
}

func New(opts Options) *Downloader {
	return &Downloader{
		opts: opts,
	}
}

func (d *Downloader) Download(ctx context.Context, limit int) error {
	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(limit)

	for d.opts.Iter.Next(wgctx) {
		elem := d.opts.Iter.Value()

		wg.Go(func() error {
			var transferErr error
			d.opts.Progress.OnAdd(elem)
			defer func() { d.opts.Progress.OnDone(elem, transferErr) }()

			if err := d.download(wgctx, elem); err != nil {
				transferErr = err
				// canceled by user, so we directly return error to stop all
				if errors.Is(err, context.Canceled) {
					return errors.Wrap(err, "download")
				}

				// don't return error to errgroup to keep other downloads running, just log it
				logctx.
					From(ctx).
					Error("Download error",
						zap.Any("element", elem),
						zap.Error(err),
					)
			}

			return nil
		})
	}

	if err := d.opts.Iter.Err(); err != nil {
		return errors.Wrap(err, "iter")
	}

	return wg.Wait()
}

func (d *Downloader) download(ctx context.Context, elem Elem) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	totalSize := elem.File().Size()
	if totalSize <= 0 {
		return errors.New("file size is 0 or negative")
	}

	timeout := 10*time.Minute + time.Duration(totalSize/(1024*1024))*5*time.Second
	if timeout > 2*time.Hour {
		timeout = 2 * time.Hour
	}
	dlCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	logctx.From(dlCtx).Debug("Start download elem",
		zap.Any("elem", elem))

	partSize := int64(MaxPartSize)
	numParts := int((totalSize + partSize - 1) / partSize)
	threads := tutil.BestThreads(totalSize, d.opts.Threads)
	if threads > numParts {
		threads = numParts
	}
	if threads < 1 {
		threads = 1
	}

	// For single-part files (photos, small audio, small video <= 512KB)
	if numParts == 1 {
		client := d.opts.Pool.Client(dlCtx, elem.File().DC())
		if elem.AsTakeout() {
			client = d.opts.Pool.Takeout(dlCtx, elem.File().DC())
		}
		req := &tg.UploadGetFileRequest{
			Precise:  true,
			Location: elem.File().Location(),
			Offset:   0,
			Limit:    int(MaxPartSize),
		}
		var res tg.UploadFileClass
		var fetchErr error
		for attempt := 0; attempt < 3; attempt++ {
			chunkCtx, chunkCancel := context.WithTimeout(dlCtx, 20*time.Second)
			res, fetchErr = client.UploadGetFile(chunkCtx, req)
			chunkCancel()
			if fetchErr == nil {
				break
			}
			if tgerr.Is(fetchErr, "FILE_REFERENCE_EXPIRED", "FILEREF_UPGRADE_NEEDED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID") {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if fetchErr != nil {
			return errors.Wrap(fetchErr, "upload.getFile single")
		}
		var data []byte
		switch r := res.(type) {
		case *tg.UploadFile:
			data = r.Bytes
		default:
			return errors.Errorf("unexpected file response: %T", res)
		}
		n, err := elem.To().WriteAt(data, 0)
		if err != nil {
			return errors.Wrap(err, "write single chunk")
		}
		d.opts.Progress.OnDownload(elem, ProgressState{
			Downloaded: int64(n),
			Total:      totalSize,
		})
		return nil
	}

	// Multi-part parallel download with deterministic chunk index queue
	type partJob struct {
		index  int
		offset int64
		limit  int
	}

	jobs := make(chan partJob, numParts)
	for i := 0; i < numParts; i++ {
		offset := int64(i) * partSize
		limit := int(partSize)
		jobs <- partJob{index: i, offset: offset, limit: limit}
	}
	close(jobs)

	var downloadedBytes atomic.Int64
	g, gctx := errgroup.WithContext(dlCtx)

	for w := 0; w < threads; w++ {
		g.Go(func() error {
			client := d.opts.Pool.Client(gctx, elem.File().DC())
			if elem.AsTakeout() {
				client = d.opts.Pool.Takeout(gctx, elem.File().DC())
			}
			for job := range jobs {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				var chunkData []byte
				var fetchErr error
				for attempt := 0; attempt < 5; attempt++ {
					req := &tg.UploadGetFileRequest{
						Precise:  true,
						Location: elem.File().Location(),
						Offset:   job.offset,
						Limit:    job.limit,
					}
					chunkCtx, chunkCancel := context.WithTimeout(gctx, 20*time.Second)
					var res tg.UploadFileClass
					res, fetchErr = client.UploadGetFile(chunkCtx, req)
					chunkCancel()
					if fetchErr == nil {
						if uf, ok := res.(*tg.UploadFile); ok {
							chunkData = uf.Bytes
							break
						}
					} else {
						logctx.From(gctx).Warn("UploadGetFile attempt failed",
							zap.Int("part", job.index),
							zap.Int64("offset", job.offset),
							zap.Int("attempt", attempt+1),
							zap.Error(fetchErr))
						if tgerr.Is(fetchErr, "FILE_REFERENCE_EXPIRED", "FILEREF_UPGRADE_NEEDED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID") {
							break
						}
					}
					select {
					case <-gctx.Done():
						return gctx.Err()
					case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
					}
				}

				if fetchErr != nil || len(chunkData) == 0 {
					if fetchErr != nil {
						return errors.Wrapf(fetchErr, "fetch part %d (offset %d)", job.index, job.offset)
					}
					return fmt.Errorf("empty chunk data for part %d", job.index)
				}

				n, err := elem.To().WriteAt(chunkData, job.offset)
				if err != nil {
					return errors.Wrapf(err, "write part %d", job.index)
				}

				curr := downloadedBytes.Add(int64(n))
				d.opts.Progress.OnDownload(elem, ProgressState{
					Downloaded: curr,
					Total:      totalSize,
				})
			}
			return nil
		})
	}

	return g.Wait()
}
