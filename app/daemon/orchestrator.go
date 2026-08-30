package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/flytam/filenamify"
	"go.uber.org/zap"

	atomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/core/downloader"
	"github.com/Hittlert/TGX/pkg/texpr"
)

type TelegramAccess interface {
	GetDialogs(ctx context.Context) ([]DialogDTO, error)
	GetHistory(ctx context.Context, req HistoryRequest) ([]MessageDTO, error)
	ResolvePeerInfo(ctx context.Context, queryStr string) (DialogDTO, error)
}

type Orchestrator struct {
	db           *Database
	slotPool     *GlobalSlotPool
	proxyManager *ProxyManager
	access       TelegramAccess
	registry     *Registry
	logger       *zap.Logger
	saveDir      string

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
	// Fallback gap-recovery loop (low frequency: 60s) since real-time events drive streaming downloads
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
			o.scanAllTargets(ctx)
		}
	}
}

func (o *Orchestrator) scanAllTargets(ctx context.Context) {
	targets, err := o.db.GetListenTargets()
	if err != nil {
		o.logger.Warn("Failed to get listen targets for scanning", zap.Error(err))
		return
	}

	for _, target := range targets {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !target.Enabled {
			continue
		}
		o.scanTarget(ctx, target)
	}
}

func (o *Orchestrator) scanTarget(ctx context.Context, target ListenTarget) {
	cursor, err := o.db.GetScanCursor(target.ChatID)
	if err != nil {
		cursor = 0
	}

	history, err := o.access.GetHistory(ctx, HistoryRequest{
		Peer:     target.ChatID,
		OffsetID: cursor,
		Limit:    100,
		Reverse:  true,
	})
	if err != nil {
		o.logger.Warn("Failed to scan target history", zap.String("chat_id", target.ChatID), zap.Error(err))
		return
	}

	maxID := cursor
	if len(history) > 0 {
		for _, m := range history {
			if m.ID > maxID {
				maxID = m.ID
			}

			cleanFileName := m.FileName
			if cleanFileName == "" && m.HasMedia {
				cleanFileName = fmt.Sprintf("media_%d", m.ID)
			}

			if target.DownloadFilter != "" && m.HasMedia {
				env := texpr.EnvMessage{
					ID:      m.ID,
					Date:    int(m.Date),
					Message: m.Text,
					Media: texpr.EnvMessageMedia{
						Name: cleanFileName,
						Size: m.FileSize,
					},
				}
				prog, err := expr.Compile(target.DownloadFilter, expr.Env(env), expr.AsBool())
				if err == nil {
					result, err := expr.Run(prog, env)
					if err == nil {
						if matched, ok := result.(bool); ok && !matched {
							continue
						}
					}
				}
			}

			err = o.db.IngestMessage(ChatMessage{
				ChatID:           target.ChatID,
				MessageID:        m.ID,
				SenderID:         m.SenderID,
				SenderName:       m.SenderName,
				Text:             m.Text,
				MediaType:        m.MediaType,
				HasMedia:         m.HasMedia,
				FileName:         cleanFileName,
				FileSize:         m.FileSize,
				ReplyToMessageID: m.ReplyToMessageID,
				Date:             m.Date,
			})
			if err != nil {
				o.logger.Warn("Failed to ingest message", zap.String("chat_id", target.ChatID), zap.Int("msg_id", m.ID), zap.Error(err))
			}
		}
	}

	_ = o.db.SaveScanCursor(target.ChatID, maxID)
}

func (o *Orchestrator) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !o.IsRunning() {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		status := o.registry.Status()
		if status.QueueDepth >= 64 {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		records, err := o.db.GetPendingDownloads(32)
		if err != nil || len(records) == 0 {
			time.Sleep(500 * time.Millisecond)
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

		// Disk Guard: ensure at least 5GB or file size + 500MB is free
		freeSpace, _, err := atomic.GetDiskSpace(o.saveDir)
		if err == nil && (freeSpace < 5*1024*1024*1024 || (record.FileSize > 0 && freeSpace < uint64(record.FileSize)+500*1024*1024)) {
			o.logger.Warn("⚠️ [Disk Guard] Insufficient disk space, postponing task",
				zap.String("task_id", taskID),
				zap.Uint64("free_bytes", freeSpace),
				zap.Int64("required_bytes", record.FileSize),
			)
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "pending", record.FileName, "", record.MediaType, record.FileSize, "disk space below safe threshold")
			return
		}

		// Acquire slot only for large files (> 1MB / non-photo)
		if record.FileSize > downloader.SmallFileThreshold || (record.FileSize <= 0 && record.MediaType != "photo") {
			_, err = o.slotPool.Acquire(taskCtx, taskID, record.FileSize)
			if err != nil {
				return
			}
			defer o.slotPool.Release(taskID)
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

		// Wait for completion via registry polling
		lastProgressTime := time.Now()
		lastDownloaded := int64(0)
		lastNetDownloaded := int64(0)

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

			if snapshot.State == StateSuccess {
				realFileName := snapshot.FileName
				if realFileName == "" {
					realFileName = record.FileName
				}
				finalSavedPath := snapshot.FinalPath
				if finalSavedPath == "" {
					finalSavedPath = finalRelPath
				}
				_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "success", realFileName, finalSavedPath, record.MediaType, snapshot.TotalSize, "")
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
	
	// 1. Cancel in-flight worker contexts
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

	// 2. Abort active tasks in registry
	if o.registry != nil {
		o.registry.CancelTasksByChatID(chatID)
	}

	// 3. Reset database records for this chat from 'downloading' back to 'pending'
	if o.db != nil {
		_, _ = o.db.DB().Exec(`UPDATE download_records SET status = 'pending' WHERE chat_id = ? AND status = 'downloading'`, chatID)
	}
}
