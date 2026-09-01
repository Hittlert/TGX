package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/flytam/filenamify"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/downloader"
	atomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/pkg/spool"
	"github.com/Hittlert/TGX/pkg/writeback"
)

type Orchestrator struct {
	db           *Database
	slotPool     *GlobalSlotPool
	proxyManager *ProxyManager
	access       TelegramAccess
	registry     *Registry
	logger       *zap.Logger
	saveDir      string
	bufferDir    string
	spool        spool.Store
	wbSink       *writeback.TargetSink

	runningMu   sync.Mutex
	running     bool
	inFlight    sync.Map
	taskCancels sync.Map
}

func NewOrchestrator(db *Database, slotPool *GlobalSlotPool, proxyManager *ProxyManager, access TelegramAccess, registry *Registry, logger *zap.Logger, saveDir string) *Orchestrator {
	return &Orchestrator{
		db:           db,
		slotPool:     slotPool,
		proxyManager: proxyManager,
		access:       access,
		registry:     registry,
		logger:       logger,
		saveDir:      saveDir,
		running:      true,
	}
}

func (o *Orchestrator) SetSpool(s spool.Store) {
	o.spool = s
}

func (o *Orchestrator) SetTargetSink(sink *writeback.TargetSink) {
	o.wbSink = sink
}

func (o *Orchestrator) SetBufferDir(dir string) {
	o.bufferDir = dir
}

func (o *Orchestrator) Start(ctx context.Context) {
	go o.scanLoop(ctx)
	go o.dispatchLoop(ctx)
	go o.metricsLoop(ctx)
}

func (o *Orchestrator) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentBPS := o.registry.Status().Rolling5sBPS
			if o.proxyManager != nil {
				activeProxy := o.proxyManager.GetActiveProxy()
				if activeProxy != "" {
					o.proxyManager.RecordDownloadSpeed(activeProxy, float64(currentBPS))
				}
			}
		}
	}
}

func (o *Orchestrator) OutputDir() string {
	if o == nil {
		return "."
	}
	return o.saveDir
}

func (o *Orchestrator) IsRunning() bool {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	return o.running
}

func (o *Orchestrator) SetRunning(running bool) {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	o.running = running
}

func (o *Orchestrator) TriggerStreamDispatch(ctx context.Context, record DownloadRecord) {
	if !o.IsRunning() {
		return
	}
	o.dispatchOneRecord(ctx, record)
}

func (o *Orchestrator) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !o.IsRunning() {
				continue
			}

			records, err := o.db.GetPendingDownloads(50)
			if err != nil {
				o.logger.Error("failed to get pending downloads", zap.Error(err))
				continue
			}

			for _, record := range records {
				if !o.IsRunning() {
					break
				}
				o.dispatchOneRecord(ctx, record)
			}
		}
	}
}

func (o *Orchestrator) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if !o.IsRunning() {
			continue
		}

		availableSlots := o.slotPool.Snapshot().AvailableSlots
		if availableSlots <= 0 {
			continue
		}

		limit := availableSlots * 2
		if limit < 10 {
			limit = 10
		}
		if limit > 50 {
			limit = 50
		}

		records, err := o.db.GetPendingDownloads(limit)
		if err != nil {
			o.logger.Error("failed to get pending downloads", zap.Error(err))
			continue
		}

		if len(records) == 0 {
			continue
		}

		for _, record := range records {
			if !o.IsRunning() {
				break
			}
			o.dispatchOneRecord(ctx, record)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (o *Orchestrator) dispatchOneRecord(ctx context.Context, record DownloadRecord) {
	taskID := fmt.Sprintf("%s:%d", record.ChatID, record.MessageID)

	if _, loaded := o.inFlight.LoadOrStore(taskID, struct{}{}); loaded {
		return
	}

	taskCtx, taskCancel := context.WithCancel(ctx)
	o.taskCancels.Store(taskID, taskCancel)

	go func() {
		defer func() {
			o.taskCancels.Delete(taskID)
			taskCancel()
			o.inFlight.Delete(taskID)
		}()

		// 1. Target Disk Guard
		freeSpace, _, err := atomic.GetDiskSpace(o.saveDir)
		if err == nil && (freeSpace < 5*1024*1024*1024 || (record.FileSize > 0 && freeSpace < uint64(record.FileSize)+500*1024*1024)) {
			o.logger.Warn("⚠️ [Disk Guard] Insufficient target disk space, postponing task",
				zap.String("task_id", taskID),
				zap.Uint64("free_bytes", freeSpace),
				zap.Int64("required_bytes", record.FileSize),
			)
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "pending", record.FileName, "", record.MediaType, record.FileSize, "target disk space below safe threshold")
			return
		}

		// 2. Buffer Disk Guard (only requires 500MB safe buffer space, sliding window handles big files)
		if o.bufferDir != "" && o.bufferDir != o.saveDir {
			bufFree, _, bufErr := atomic.GetDiskSpace(o.bufferDir)
			if bufErr == nil && bufFree < 500*1024*1024 {
				o.logger.Warn("⚠️ [Disk Guard] Insufficient buffer disk space, postponing task",
					zap.String("task_id", taskID),
					zap.Uint64("free_bytes", bufFree),
				)
				_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "pending", record.FileName, "", record.MediaType, record.FileSize, "buffer disk space below safe threshold")
				return
			}
		}

		// 3. Acquire slot only for large files (> 1MB / non-photo)
		isLargeFile := record.FileSize > downloader.SmallFileThreshold || (record.FileSize <= 0 && record.MediaType != "photo")
		var slotOnce sync.Once
		releaseSlot := func() {} // no-op for small files
		if isLargeFile {
			_, err = o.slotPool.Acquire(taskCtx, taskID, record.FileSize)
			if err != nil {
				return
			}
			// Slot released via sync.Once: either when task enters moving or on return.
			releaseSlot = func() { slotOnce.Do(func() { o.slotPool.Release(taskID) }) }
			defer releaseSlot()
		}

		folderName := record.TargetTitle
		if folderName == "" {
			folderName = record.ChatID
		}
		safeFolder, _ := filenamify.Filenamify(folderName, filenamify.Options{Replacement: "_"})
		if safeFolder == "" {
			safeFolder = record.ChatID
		}

		msgTime := record.CreatedAt
		if msgTime <= 86400 {
			msgTime = time.Now().Unix()
		}
		yearMonth := time.Unix(msgTime, 0).Format("2006_01")
		rawName := record.FileName
		if rawName == "" || strings.HasSuffix(rawName, ".bin") || strings.HasSuffix(rawName, ".unknown") {
			ext := ".mp4"
			if record.MediaType == "photo" {
				ext = ".jpg"
			} else if record.MediaType == "audio" {
				ext = ".mp3"
			}
			rawName = fmt.Sprintf("%d%s", record.MessageID, ext)
		}
		safeFileName, _ := filenamify.Filenamify(rawName, filenamify.Options{Replacement: "_"})

		finalRelPath := filepath.Join(safeFolder, yearMonth, fmt.Sprintf("%d - %s", record.MessageID, safeFileName))
		finalRelPath = strings.ReplaceAll(finalRelPath, "\\", "/")

		_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "downloading", rawName, finalRelPath, record.MediaType, record.FileSize, "")

		submitReq := TaskRequest{
			ID:           taskID,
			Peer:         record.ChatID,
			MessageID:    record.MessageID,
			FinalPath:    finalRelPath,
			ExpectedSize: record.FileSize,
			Retry:        record.Attempts > 0,
		}

		snapshot, _, err := o.registry.Submit(submitReq)
		if err != nil {
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", record.FileName, finalRelPath, record.MediaType, record.FileSize, err.Error())
			return
		}

		lastProgressTime := time.Now()
		lastDownloaded := int64(0)
		lastNetDownloaded := int64(0)
		recordedMoving := false

		for {
			select {
			case <-taskCtx.Done():
				o.registry.Cancel(taskID, "task context done")
				return
			case <-ctx.Done():
				o.registry.Cancel(taskID, "orchestrator shutdown")
				return
			default:
			}

			if snapshot.Downloaded > lastDownloaded || snapshot.NetDownloaded > lastNetDownloaded {
				if snapshot.Downloaded > lastDownloaded {
					lastDownloaded = snapshot.Downloaded
				}
				if snapshot.NetDownloaded > lastNetDownloaded {
					lastNetDownloaded = snapshot.NetDownloaded
				}
				lastProgressTime = time.Now()
			}

			// Track moving state
			if snapshot.State == StatePublishing && !recordedMoving {
				recordedMoving = true
				_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "moving", record.FileName, finalRelPath, record.MediaType, record.FileSize, "")
				// Release network slot: TargetWriter handles the rest, network is free for other files.
				if isLargeFile {
					releaseSlot()
				}
			}

			if snapshot.State == StateSuccess {
				realFileName := snapshot.FileName
				if realFileName == "" {
					realFileName = record.FileName
				}
				_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "success", realFileName, finalRelPath, record.MediaType, record.FileSize, "")
				return
			}
			if snapshot.State == StateFailed || snapshot.State == StateUnavailable {
				realFileName := snapshot.FileName
				if realFileName == "" {
					realFileName = record.FileName
				}
				_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", realFileName, finalRelPath, record.MediaType, record.FileSize, snapshot.Error)
				return
			}

			// Watchdog: If actively downloading but no byte progress has been made for 5 minutes, abort and mark failed
			if snapshot.State == StateDownloading && time.Since(lastProgressTime) > 5*time.Minute {
				_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", record.FileName, finalRelPath, record.MediaType, record.FileSize, "download stalled / no progress for 5m")
				o.registry.Cancel(taskID, "download stalled / no progress for 5m")
				taskCancel()
				return
			}

			time.Sleep(200 * time.Millisecond)
			if s, ok := o.registry.Task(taskID); ok {
				snapshot = s
			} else {
				break
			}
		}
	}()
}

func (o *Orchestrator) CancelTasksByChatID(chatID string) {
	cleanChatID := strings.TrimPrefix(chatID, "@")

	o.taskCancels.Range(func(key, value any) bool {
		taskID, ok := key.(string)
		if !ok {
			return true
		}
		parts := strings.Split(taskID, ":")
		if len(parts) > 0 {
			taskPeer := strings.TrimPrefix(parts[0], "@")
			if taskPeer == cleanChatID || taskPeer == chatID {
				if cancel, ok := value.(context.CancelFunc); ok && cancel != nil {
					cancel()
				}
				o.slotPool.Release(taskID)
				o.inFlight.Delete(taskID)
			}
		}
		return true
	})

	if o.registry != nil {
		o.registry.CancelTasksByChatID(chatID)
	}
}
