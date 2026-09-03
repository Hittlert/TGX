package daemon

import (
	"context"
	"errors"
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

	runningMu sync.Mutex
	running   bool
	inFlight  sync.Map
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
	// Launch queue workers that pull tasks directly from Registry
	const numWorkers = 32
	for i := 0; i < numWorkers; i++ {
		go o.queueWorker(ctx)
	}

	go o.scanLoop(ctx)
	go o.metricsLoop(ctx)
}

func (o *Orchestrator) queueWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !o.IsRunning() {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		task, err := o.registry.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		go o.executeTask(ctx, task)
	}
}

func (o *Orchestrator) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if o.registry != nil {
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

// TriggerStreamDispatch enqueues a new incoming update record into the registry.
func (o *Orchestrator) TriggerStreamDispatch(ctx context.Context, record DownloadRecord) {
	if !o.IsRunning() {
		return
	}
	taskID := fmt.Sprintf("%s:%d", record.ChatID, record.MessageID)
	req := TaskRequest{
		ID:           taskID,
		Peer:         record.ChatID,
		MessageID:    record.MessageID,
		FinalPath:    record.SavePath,
		ExpectedSize: record.FileSize,
		Retry:        record.Attempts > 0,
	}
	_, _, _ = o.registry.Submit(req)
}

func (o *Orchestrator) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !o.IsRunning() || o.db == nil {
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
				taskID := fmt.Sprintf("%s:%d", record.ChatID, record.MessageID)
				req := TaskRequest{
					ID:           taskID,
					Peer:         record.ChatID,
					MessageID:    record.MessageID,
					FinalPath:    record.SavePath,
					ExpectedSize: record.FileSize,
					Retry:        record.Attempts > 0,
				}
				_, _, _ = o.registry.Submit(req)
			}
		}
	}
}

func (o *Orchestrator) executeTask(ctx context.Context, task *Task) {
	req := task.Request()
	taskID := req.ID
	chatID := req.Peer
	msgID := req.MessageID

	if _, loaded := o.inFlight.LoadOrStore(taskID, struct{}{}); loaded {
		return
	}
	defer o.inFlight.Delete(taskID)

	taskCtx := task.Context()
	if taskCtx == nil {
		taskCtx = ctx
	}

	// 1. SSD Space Admission Check (real free space check & whole-file reservation)
	releaseSSD, err := o.ssdAdmission.Reserve(taskID, req.ExpectedSize)
	if err != nil {
		o.logger.Warn("⚠️ [SSD Admission] Insufficient SSD space, postponing task",
			zap.String("task_id", taskID),
			zap.Error(err),
		)
		task.Fail("ssd_space", "insufficient SSD space", false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "pending", "", req.FinalPath, "", req.ExpectedSize, "ssd_space: insufficient disk space")
		}
		return
	}
	defer releaseSSD()

	// 2. Media Ingress / Resolution (done once)
	if o.db != nil {
		_ = o.db.UpdateDownloadStatus(chatID, msgID, "resolving", "", req.FinalPath, "", req.ExpectedSize, "")
	}

	normalizedPeer := strings.TrimPrefix(strings.TrimSpace(chatID), "@")
	resolvedMedia, err := o.access.Resolve(taskCtx, normalizedPeer, msgID)
	if err != nil {
		o.logger.Warn("failed to resolve telegram media",
			zap.String("task_id", taskID),
			zap.Error(err),
		)
		errStr := strings.ToLower(err.Error())
		isUnavailable := strings.Contains(errStr, "deleted") || strings.Contains(errStr, "unavailable") || strings.Contains(errStr, "message_id_invalid")
		task.Fail("resolve", err.Error(), isUnavailable)
		if o.db != nil {
			st := "failed"
			if isUnavailable {
				st = "unavailable"
			}
			_ = o.db.UpdateDownloadStatus(chatID, msgID, st, "", req.FinalPath, "", req.ExpectedSize, err.Error())
		}
		return
	}

	if resolvedMedia.File == nil || resolvedMedia.Size <= 0 {
		task.Fail("unavailable", "message has no downloadable media", true)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "unavailable", "", req.FinalPath, "", 0, "message has no downloadable media")
		}
		return
	}

	task.SetResolved(resolvedMedia.Name, resolvedMedia.Size, resolvedMedia.DCID)

	// Update reservation if real resolved size differed from catalog estimate
	if resolvedMedia.Size != req.ExpectedSize {
		releaseSSD()
		releaseSSD, err = o.ssdAdmission.Reserve(taskID, resolvedMedia.Size)
		if err != nil {
			o.logger.Warn("⚠️ [SSD Admission] Insufficient space after resolve", zap.Error(err))
			task.Fail("ssd_space", "insufficient SSD space after resolve", false)
			if o.db != nil {
				_ = o.db.UpdateDownloadStatus(chatID, msgID, "pending", resolvedMedia.Name, req.FinalPath, "", resolvedMedia.Size, "ssd_space: insufficient disk space")
			}
			return
		}
	}

	// 4. Compute canonical path within SSD download root
	finalRelPath := req.FinalPath
	if finalRelPath == "" {
		rawName := resolvedMedia.Name
		if rawName == "" || strings.HasSuffix(rawName, ".bin") || strings.HasSuffix(rawName, ".unknown") {
			rawName = fmt.Sprintf("%d.mp4", msgID)
		}
		safeName, _ := filenamify.Filenamify(rawName, filenamify.Options{Replacement: "_"})
		finalRelPath = safeName
	}
	finalRelPath = strings.ReplaceAll(finalRelPath, "\\", "/")
	finalAbsPath := filepath.Join(o.saveDir, filepath.FromSlash(finalRelPath))
	partAbsPath := finalAbsPath + ".part"

	// 5. Check existing final file (Idempotent success or typed collision)
	if finInfo, statErr := os.Stat(finalAbsPath); statErr == nil {
		if finInfo.Size() == resolvedMedia.Size {
			actualSHA, shaErr := computeFileSHA256(finalAbsPath)
			if shaErr == nil {
				// Existing matches size & SHA: complete idempotently!
				if o.db != nil {
					_ = o.db.CompleteDownloadAndQueueArchive(
						chatID, msgID, finalRelPath,
						finInfo.Size(), actualSHA,
						o.archiveWorker != nil && o.archiveWorker.IsEnabled(),
					)
				}
				if o.archiveWorker != nil {
					o.archiveWorker.Wake()
				}
				task.SucceedResult(PublishResult{Path: finalRelPath, SHA256: actualSHA, AlreadyExists: true, absolutePath: finalAbsPath})
				return
			}
		}
		// Size or SHA conflict: do not overwrite!
		o.logger.Error("collision: destination exists with conflicting content",
			zap.String("final_path", finalAbsPath),
		)
		task.Fail("collision", "destination exists with conflicting content", false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "failed", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, "collision: destination exists with conflicting content")
		}
		return
	}

	// 6. Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(finalAbsPath), 0o755); err != nil {
		o.logger.Error("failed to create target dir", zap.Error(err))
		task.Fail("path", err.Error(), false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "failed", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, err.Error())
		}
		return
	}

	// 7. Acquire Active File Concurrency Slot (held strictly during physical byte transfer)
	releaseSlot, err := o.transferMgr.AcquireFileSlot(taskCtx)
	if err != nil {
		task.Fail("canceled", "file slot acquisition canceled", false)
		return
	}
	defer releaseSlot()

	if task.IsTerminal() || taskCtx.Err() != nil {
		return
	}

	partFile, err := os.OpenFile(partAbsPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		o.logger.Error("failed to create part file", zap.Error(err))
		task.Fail("io", err.Error(), false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "failed", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, err.Error())
		}
		return
	}
	_ = fscommit.Preallocate(partFile, resolvedMedia.Size)

	task.SetDownloading()
	if o.db != nil {
		_ = o.db.UpdateDownloadStatus(chatID, msgID, "downloading", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, "")
	}

	// 7. Build gotd client adapter with DataGate protection
	poolClient := o.access.Pool().Client(taskCtx, resolvedMedia.DCID)
	clientAdapter := transfer.NewMasterClientAdapter(poolClient, o.transferMgr.Gate(), resolvedMedia.DCID)

	// 8. Execute parallel chunk download with official gotd downloader
	onProgress := func(downloaded, total int64) {
		task.RecordProgress(downloaded)
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
		task.Fail("transfer", dlErr.Error(), false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "failed", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, dlErr.Error())
		}
		return
	}

	// 9. Verification: exact size check, fsync, close, SHA calculation
	stat, statErr := partFile.Stat()
	if statErr != nil || (resolvedMedia.Size > 0 && stat.Size() != resolvedMedia.Size) {
		_ = partFile.Close()
		_ = os.Remove(partAbsPath)
		errDesc := fmt.Sprintf("short write: got %d, want %d", written, resolvedMedia.Size)
		task.Fail("corrupt", errDesc, false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "failed", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, errDesc)
		}
		return
	}

	_ = partFile.Sync()
	_ = partFile.Close()

	shaHex, shaErr := computeFileSHA256(partAbsPath)
	if shaErr != nil {
		_ = os.Remove(partAbsPath)
		task.Fail("hash", shaErr.Error(), false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "failed", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, shaErr.Error())
		}
		return
	}

	// 10. Durable commit intent in DB
	if o.db != nil {
		_ = o.db.PrepareDownloadCommit(chatID, msgID, finalRelPath, stat.Size(), shaHex)
	}

	// 11. Preserve timestamp if present
	if resolvedMedia.Date > 0 {
		when := time.Unix(resolvedMedia.Date, 0)
		_ = os.Chtimes(partAbsPath, when, when)
	}

	// 12. Atomic sibling rename .part -> final
	task.SetPublishing()
	if err := fscommit.CommitSiblingPart(partAbsPath, finalAbsPath); err != nil {
		_ = os.Remove(partAbsPath)
		o.logger.Error("atomic commit failed", zap.Error(err))
		task.Fail("commit", err.Error(), false)
		if o.db != nil {
			_ = o.db.UpdateDownloadStatus(chatID, msgID, "failed", resolvedMedia.Name, finalRelPath, "", resolvedMedia.Size, err.Error())
		}
		return
	}

	// 13. Complete download and queue archive in single DB transaction
	queueArchive := o.archiveWorker != nil && o.archiveWorker.IsEnabled()
	if o.db != nil {
		if err := o.db.CompleteDownloadAndQueueArchive(chatID, msgID, finalRelPath, stat.Size(), shaHex, queueArchive); err != nil {
			o.logger.Error("failed to complete download in DB", zap.Error(err))
		}
	}

	// 14. Wake archive worker
	if o.archiveWorker != nil {
		o.archiveWorker.Wake()
	}

	// 15. Complete task in Registry
	task.SucceedResult(PublishResult{
		Path:         finalRelPath,
		SHA256:       shaHex,
		absolutePath: finalAbsPath,
	})

	o.logger.Info("download completed successfully",
		zap.String("task_id", taskID),
		zap.String("rel_path", finalRelPath),
		zap.Int64("size", stat.Size()),
		zap.String("sha256", shaHex),
	)
}
