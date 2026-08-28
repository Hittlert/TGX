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

	runningMu sync.Mutex
	running   bool
	inFlight  sync.Map
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

		snapshot := o.slotPool.Snapshot()
		if snapshot.ActiveFilesCount >= snapshot.MaxActiveFiles || snapshot.AvailableSlots <= 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		needCount := snapshot.MaxActiveFiles - snapshot.ActiveFilesCount
		if needCount < 1 {
			needCount = 1
		}
		if needCount > 16 {
			needCount = 16
		}

		records, err := o.db.GetPendingDownloads(needCount * 2)
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

	if _, loaded := o.inFlight.LoadOrStore(taskID, true); loaded {
		return
	}

	go func() {
		defer o.inFlight.Delete(taskID)

		// Acquire slot
		_, err := o.slotPool.Acquire(ctx, taskID, record.FileSize)
		if err != nil {
			return
		}
		defer o.slotPool.Release(taskID)

		folderName := record.TargetTitle
		if folderName == "" {
			folderName = record.ChatID
		}
		safeFolder, _ := filenamify.Filenamify(folderName, filenamify.Options{Replacement: "_"})
		if safeFolder == "" {
			safeFolder = record.ChatID
		}
		
		yearMonth := time.Unix(record.CreatedAt, 0).Format("2006_01")
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
		for {
			select {
			case <-ctx.Done():
				return
			default:
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

			time.Sleep(200 * time.Millisecond)
			if s, ok := o.registry.Task(taskID); ok {
				snapshot = s
			} else {
				break
			}
		}
	}()
}
