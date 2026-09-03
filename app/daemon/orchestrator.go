package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/flytam/filenamify"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/transfer"
	"github.com/Hittlert/TGX/internal/fscommit"
)

// Orchestrator coordinates active download admissions, direct SSD writing, and archive handoff.
type Orchestrator struct {
	db            *Database
	transferMgr   *transfer.TransferManager
	ssdAdmission  *fscommit.SSDAdmission
	archiveWorker *ArchiveWorker
	proxyManager  *ProxyManager
	access        TelegramAccess
	registry      *Registry
	logger        *zap.Logger
	saveDir       string

	runningMu   sync.Mutex
	running     bool
	inFlight    sync.Map
	taskCancels sync.Map
}

// NewOrchestrator creates a new direct-SSD download orchestrator.
func NewOrchestrator(
	db *Database,
	transferMgr *transfer.TransferManager,
	ssdAdmission *fscommit.SSDAdmission,
	proxyManager *ProxyManager,
	access TelegramAccess,
	registry *Registry,
	logger *zap.Logger,
	saveDir string,
) *Orchestrator {
	return &Orchestrator{
		db:           db,
		transferMgr:  transferMgr,
		ssdAdmission: ssdAdmission,
		proxyManager: proxyManager,
		access:       access,
		registry:     registry,
		logger:       logger,
		saveDir:      saveDir,
		running:      true,
	}
}

// SetArchiveWorker binds the single asynchronous archive worker.
func (o *Orchestrator) SetArchiveWorker(w *ArchiveWorker) {
	o.archiveWorker = w
}

// Start launches the background orchestrator loops.
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

// OutputDir returns the SSD download directory.
func (o *Orchestrator) OutputDir() string {
	if o == nil {
		return "."
	}
	return o.saveDir
}

// IsRunning returns true if the orchestrator is accepting and dispatching tasks.
func (o *Orchestrator) IsRunning() bool {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	return o.running
}

// SetRunning updates the running state.
func (o *Orchestrator) SetRunning(running bool) {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	o.running = running
}

// CancelTasksByChatID cancels running tasks for the given chat ID.
func (o *Orchestrator) CancelTasksByChatID(chatID string) {
	if o.registry != nil {
		o.registry.CancelTasksByChatID(chatID)
	}
}

// TriggerStreamDispatch processes a new incoming update record immediately.
func (o *Orchestrator) TriggerStreamDispatch(ctx context.Context, record DownloadRecord) {
	if !o.IsRunning() {
		return
	}
	o.dispatchOneRecord(ctx, record)
}

func (o *Orchestrator) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
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
	ticker := time.NewTicker(100 * time.Millisecond)
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

		// Concurrency check based on active file slots
		records, err := o.db.GetPendingDownloads(32)
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

		// 1. SSD Space Admission Check (real free space check & reservation)
		releaseSSD, err := o.ssdAdmission.Reserve(taskID, record.FileSize)
		if err != nil {
			o.logger.Warn("⚠️ [SSD Admission] Insufficient SSD space, postponing task",
				zap.String("task_id", taskID),
				zap.Error(err),
			)
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "pending", record.FileName, "", record.MediaType, record.FileSize, "ssd_space: insufficient disk space")
			return
		}
		defer releaseSSD()

		// 2. Active File Admission Slot
		releaseSlot, err := o.transferMgr.AcquireFileSlot(taskCtx)
		if err != nil {
			return
		}
		defer releaseSlot()

		// 3. Media Ingress / Resolution (done once)
		_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "resolving", record.FileName, "", record.MediaType, record.FileSize, "")
		resolvedMedia, err := o.access.Resolve(taskCtx, record.ChatID, record.MessageID)
		if err != nil {
			o.logger.Warn("failed to resolve telegram media",
				zap.String("task_id", taskID),
				zap.Error(err),
			)
			errStr := strings.ToLower(err.Error())
			status := "failed"
			if strings.Contains(errStr, "deleted") || strings.Contains(errStr, "unavailable") || strings.Contains(errStr, "message_id_invalid") {
				status = "unavailable"
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, status, record.FileName, "", record.MediaType, record.FileSize, err.Error())
			return
		}

		if resolvedMedia.File == nil || resolvedMedia.Size <= 0 {
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "unavailable", record.FileName, "", record.MediaType, record.FileSize, "message has no downloadable media")
			return
		}

		// Update reservation if real resolved size differed from catalog estimate
		if resolvedMedia.Size != record.FileSize {
			releaseSSD()
			releaseSSD, err = o.ssdAdmission.Reserve(taskID, resolvedMedia.Size)
			if err != nil {
				o.logger.Warn("⚠️ [SSD Admission] Insufficient space after resolve", zap.Error(err))
				_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "pending", record.FileName, "", record.MediaType, resolvedMedia.Size, "ssd_space: insufficient disk space")
				return
			}
		}

		// 4. Compute canonical path within SSD download root
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
		rawName := resolvedMedia.Name
		if rawName == "" {
			rawName = record.FileName
		}
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
		finalAbsPath := filepath.Join(o.saveDir, filepath.FromSlash(finalRelPath))
		partAbsPath := finalAbsPath + ".part"

		// 5. Submit to Registry for status reporting
		submitReq := TaskRequest{
			ID:           taskID,
			Peer:         record.ChatID,
			MessageID:    record.MessageID,
			FinalPath:    finalRelPath,
			ExpectedSize: resolvedMedia.Size,
			Retry:        record.Attempts > 0,
		}
		task, _ := o.registry.SubmitActive(submitReq)
		if task != nil {
			task.SetResolved(safeFileName, resolvedMedia.Size, resolvedMedia.DCID)
		}

		// 6. Check existing final file (Idempotent success or typed collision)
		if finInfo, err := os.Stat(finalAbsPath); err == nil {
			if finInfo.Size() == resolvedMedia.Size {
				actualSHA, shaErr := computeFileSHA256(finalAbsPath)
				if shaErr == nil {
					// Existing matches size & SHA: complete idempotently!
					_ = o.db.CompleteDownloadAndQueueArchive(
						record.ChatID, record.MessageID, finalRelPath,
						finInfo.Size(), actualSHA,
						o.archiveWorker != nil && o.archiveWorker.IsEnabled(),
					)
					if o.archiveWorker != nil {
						o.archiveWorker.Wake()
					}
					if task != nil {
						task.SucceedResult(PublishResult{Path: finalRelPath, SHA256: actualSHA, AlreadyExists: true, absolutePath: finalAbsPath})
					}
					return
				}
			}
			// Size or SHA conflict: do not overwrite!
			o.logger.Error("collision: destination exists with conflicting content",
				zap.String("final_path", finalAbsPath),
			)
			if task != nil {
				task.Fail("collision", "destination exists with conflicting content", false)
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, "collision: destination exists with conflicting content")
			return
		}

		// 7. Ensure parent directory exists and create sibling .part
		if err := os.MkdirAll(filepath.Dir(finalAbsPath), 0o755); err != nil {
			o.logger.Error("failed to create target dir", zap.Error(err))
			if task != nil {
				task.Fail("path", err.Error(), false)
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, err.Error())
			return
		}

		partFile, err := os.OpenFile(partAbsPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			o.logger.Error("failed to create part file", zap.Error(err))
			if task != nil {
				task.Fail("io", err.Error(), false)
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, err.Error())
			return
		}
		_ = fscommit.Preallocate(partFile, resolvedMedia.Size)

		// 8. Update DB to 'downloading'
		_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "downloading", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, "")

		// 9. Build gotd client adapter with DataGate protection
		poolClient := o.access.Pool().Client(taskCtx, resolvedMedia.DCID)
		clientAdapter := transfer.NewMasterClientAdapter(poolClient, o.transferMgr.Gate(), resolvedMedia.DCID)

		// 10. Execute parallel chunk download with official gotd downloader
		onProgress := func(downloaded, total int64) {
			if task != nil {
				task.RecordProgress(downloaded)
			}
		}

		written, dlErr := o.transferMgr.DownloadFile(
			taskCtx,
			clientAdapter,
			resolvedMedia.File.Location(),
			resolvedMedia.Size,
			partFile,
			onProgress,
		)

		if dlErr != nil {
			_ = partFile.Close()
			_ = os.Remove(partAbsPath)
			o.logger.Warn("gotd download failed",
				zap.String("task_id", taskID),
				zap.Error(dlErr),
			)
			if task != nil {
				task.Fail("transfer", dlErr.Error(), false)
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, dlErr.Error())
			return
		}

		// 11. Verification: exact size check, fsync, close, SHA calculation
		stat, statErr := partFile.Stat()
		if statErr != nil || (resolvedMedia.Size > 0 && stat.Size() != resolvedMedia.Size) {
			_ = partFile.Close()
			_ = os.Remove(partAbsPath)
			errDesc := fmt.Sprintf("short write: got %d, want %d", written, resolvedMedia.Size)
			if task != nil {
				task.Fail("corrupt", errDesc, false)
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, errDesc)
			return
		}

		_ = partFile.Sync()
		_ = partFile.Close()

		shaHex, shaErr := computeFileSHA256(partAbsPath)
		if shaErr != nil {
			_ = os.Remove(partAbsPath)
			if task != nil {
				task.Fail("hash", shaErr.Error(), false)
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, shaErr.Error())
			return
		}

		// 12. Durable commit intent in DB
		_ = o.db.PrepareDownloadCommit(record.ChatID, record.MessageID, finalRelPath, stat.Size(), shaHex)

		// 13. Preserve timestamp if present
		if resolvedMedia.Date > 0 {
			when := time.Unix(resolvedMedia.Date, 0)
			_ = os.Chtimes(partAbsPath, when, when)
		}

		// 14. Atomic sibling rename .part -> final
		if err := fscommit.CommitSiblingPart(partAbsPath, finalAbsPath); err != nil {
			_ = os.Remove(partAbsPath)
			o.logger.Error("atomic commit failed", zap.Error(err))
			if task != nil {
				task.Fail("commit", err.Error(), false)
			}
			_ = o.db.UpdateDownloadStatus(record.ChatID, record.MessageID, "failed", rawName, finalRelPath, record.MediaType, resolvedMedia.Size, err.Error())
			return
		}

		// 15. Complete download and queue archive in single DB transaction
		queueArchive := o.archiveWorker != nil && o.archiveWorker.IsEnabled()
		if err := o.db.CompleteDownloadAndQueueArchive(record.ChatID, record.MessageID, finalRelPath, stat.Size(), shaHex, queueArchive); err != nil {
			o.logger.Error("failed to complete download in DB", zap.Error(err))
		}

		// 16. Wake archive worker
		if o.archiveWorker != nil {
			o.archiveWorker.Wake()
		}

		// 17. Complete task in Registry
		if task != nil {
			task.SucceedResult(PublishResult{
				Path:         finalRelPath,
				SHA256:       shaHex,
				absolutePath: finalAbsPath,
			})
		}

		o.logger.Info("download completed successfully",
			zap.String("task_id", taskID),
			zap.String("rel_path", finalRelPath),
			zap.Int64("size", stat.Size()),
			zap.String("sha256", shaHex),
		)
	}()
}
