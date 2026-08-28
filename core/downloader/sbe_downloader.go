package downloader

import (
	"context"
	"fmt"
	"sync"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/pkg/sbe"
	"github.com/Hittlert/TGX/pkg/sbe/scheduler"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/sync/singleflight"
)

// FileRefRefresher is a callback to refresh an expired Telegram InputFileLocation.
type FileRefRefresher func(ctx context.Context, fileKey string) (tg.InputFileLocationClass, error)

// SBEDownloader bridges MTProto long-lived DC connection pools with SBE streaming block engine.
type SBEDownloader struct {
	pool      dcpool.Pool
	engine    *sbe.Engine
	refresher FileRefRefresher

	sf singleflight.Group
	mu sync.RWMutex

	locationMap map[string]tg.InputFileLocationClass
	dcMap       map[string]int
}

// NewSBEDownloader creates an SBE-powered Telegram downloader.
func NewSBEDownloader(pool dcpool.Pool, engineCfg sbe.EngineConfig, refresher FileRefRefresher) *SBEDownloader {
	sd := &SBEDownloader{
		pool:        pool,
		refresher:   refresher,
		locationMap: make(map[string]tg.InputFileLocationClass),
		dcMap:       make(map[string]int),
	}

	// Set BlockFetcher callback in SBE Engine
	engineCfg.BlockFetcher = sd.FetchChunk
	sd.engine = sbe.NewEngine(engineCfg)
	return sd
}

// Engine returns the underlying SBE engine.
func (sd *SBEDownloader) Engine() *sbe.Engine {
	return sd.engine
}

// RegisterLocation maps a fileKey to its MTProto location and target DC.
func (sd *SBEDownloader) RegisterLocation(fileKey string, loc tg.InputFileLocationClass, dc int) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	sd.locationMap[fileKey] = loc
	sd.dcMap[fileKey] = dc
}

// UnregisterLocation cleans up location mappings for a completed file.
func (sd *SBEDownloader) UnregisterLocation(fileKey string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	delete(sd.locationMap, fileKey)
	delete(sd.dcMap, fileKey)
}

// FetchChunk executes the dynamic RPC loop to fill a 2MB chunk from Telegram MTProto.
func (sd *SBEDownloader) FetchChunk(ctx context.Context, task scheduler.ChunkTask, buf []byte) (int64, error) {
	sd.mu.RLock()
	loc, hasLoc := sd.locationMap[task.FileKey]
	dc, hasDC := sd.dcMap[task.FileKey]
	sd.mu.RUnlock()

	if !hasLoc || !hasDC {
		return 0, fmt.Errorf("file location not found for fileKey: %s", task.FileKey)
	}

	client := sd.pool.Client(ctx, dc)
	if client == nil {
		return 0, fmt.Errorf("failed to get client for DC %d", dc)
	}

	var filled int64
	targetLen := task.Length

	for filled < targetLen {
		select {
		case <-ctx.Done():
			return filled, ctx.Err()
		default:
		}

		partReqLen := int(targetLen - filled)
		if partReqLen > MaxPartSize {
			partReqLen = MaxPartSize
		}

		reqOffset := task.Offset + filled
		req := &tg.UploadGetFileRequest{
			Location: loc,
			Offset:   reqOffset,
			Limit:    partReqLen,
		}

		res, err := client.UploadGetFile(ctx, req)
		if err != nil {
			// Check for FILE_REFERENCE_EXPIRED
			if tgerr.Is(err, "FILE_REFERENCE_EXPIRED") && sd.refresher != nil {
				// SingleFlight deduplicated refresh
				val, refErr, _ := sd.sf.Do(task.FileKey, func() (interface{}, error) {
					return sd.refresher(ctx, task.FileKey)
				})
				if refErr == nil && val != nil {
					newLoc := val.(tg.InputFileLocationClass)
					sd.mu.Lock()
					sd.locationMap[task.FileKey] = newLoc
					loc = newLoc
					sd.mu.Unlock()

					// Retry with refreshed location
					continue
				}
			}

			// Check for FLOOD_WAIT
			if d, ok := tgerr.AsFloodWait(err); ok {
				task.Coordinator.AbortChunk(task.BlockIndex)
				return filled, fmt.Errorf("flood wait %v: %w", d, err)
			}

			return filled, fmt.Errorf("uploadGetFile error at offset %d: %w", reqOffset, err)
		}

		var chunkBytes []byte
		switch file := res.(type) {
		case *tg.UploadFile:
			chunkBytes = file.Bytes
		case *tg.UploadFileCDNRedirect:
			return filled, fmt.Errorf("CDN redirect not supported in current DC pool")
		default:
			return filled, fmt.Errorf("unknown upload file response type: %T", res)
		}

		if len(chunkBytes) == 0 {
			break
		}

		copy(buf[filled:], chunkBytes)
		filled += int64(len(chunkBytes))
	}

	return filled, nil
}
