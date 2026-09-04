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

// PathPlanner derives canonical relative file paths within the download root.
type PathPlanner struct{}

// Plan generates a sanitized relative file path: safeFolder/yearMonth/msgID - safeFileName.
func (p PathPlanner) Plan(peer string, channelTitle string, msgID int, rawName string, mediaType string, msgDate int64) string {
	safeFolder := strings.TrimSpace(channelTitle)
	if safeFolder != "" {
		safeFolder, _ = filenamify.Filenamify(safeFolder, filenamify.Options{Replacement: "_"})
	}
	if safeFolder == "" {
		safeFolder, _ = filenamify.Filenamify(peer, filenamify.Options{Replacement: "_"})
	}
	if safeFolder == "" {
		safeFolder = "default"
	}

	if msgDate <= 86400 {
		msgDate = time.Now().Unix()
	}
	yearMonth := time.Unix(msgDate, 0).Format("2006_01")

	if rawName == "" || strings.HasSuffix(rawName, ".bin") || strings.HasSuffix(rawName, ".unknown") {
		ext := ".bin"
		switch mediaType {
		case "photo":
			ext = ".jpg"
		case "audio":
			ext = ".mp3"
		case "video":
			ext = ".mp4"
		}
		rawName = fmt.Sprintf("%d%s", msgID, ext)
	}
	safeFileName, _ := filenamify.Filenamify(rawName, filenamify.Options{Replacement: "_"})
	if safeFileName == "" {
		safeFileName = fmt.Sprintf("%d.bin", msgID)
	}

	finalRelPath := filepath.Join(safeFolder, yearMonth, fmt.Sprintf("%d - %s", msgID, safeFileName))
	return strings.ReplaceAll(finalRelPath, "\\", "/")
}

// Start launches the background orchestrator loops.
func (o *Orchestrator) Start(ctx context.Context) {
	poolSize := 64
	for i := 0; i < poolSize; i++ {
		go o.worker(ctx)
	}

	go o.scanLoop(ctx)
	go o.metricsLoop(ctx)
}

func (o *Orchestrator) worker(ctx context.Context) {
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

		if o.transferMgr != nil && int(o.transferMgr.ActiveFiles()) >= o.transferMgr.FileConcurrency() {
			time.Sleep(25 * time.Millisecond)
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

		o.downloadOne(ctx, task)
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

// SubmitRecord is the unified entrypoint for enqueuing a download record into the registry.
func (o *Orchestrator) SubmitRecord(record DownloadRecord) error {
	if !o.IsRunning() {
		return errors.New("orchestrator is not running")
	}
	taskID := fmt.Sprintf("%s:%d", record.ChatID, record.MessageID)
	req := TaskRequest{
		ID:           taskID,
		Peer:         record.ChatID,
		MessageID:    record.MessageID,
		TargetTitle:  record.TargetTitle,
		MediaType:    record.MediaType,
		FileName:     record.FileName,
		Date:         record.CreatedAt,
		FinalPath:    record.SavePath,
		ExpectedSize: record.FileSize,
		Retry:        record.Attempts > 0,
	}
	_, _, err := o.registry.Submit(req)
	return err
}

// TriggerStreamDispatch enqueues a new incoming update record into the registry.
func (o *Orchestrator) TriggerStreamDispatch(ctx context.Context, record DownloadRecord) {
	if err := o.SubmitRecord(record); err != nil {
		o.logger.Warn("failed to submit stream record to registry",
			zap.String("chat_id", record.ChatID),
			zap.Int("message_id", record.MessageID),
			zap.Error(err),
		)
	}
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
				if err := o.SubmitRecord(record); err != nil {
					o.logger.Warn("failed to submit scanned record to registry",
						zap.String("chat_id", record.ChatID),
						zap.Int("message_id", record.MessageID),
						zap.Error(err),
					)
				}
			}
		}
	}
}

func (o *Orchestrator) executeTask(ctx context.Context, task *Task) {
	o.downloadOne(ctx, task)
}

func (o *Orchestrator) downloadOne(ctx context.Context, task *Task) {
	req := task.Request()
	taskID := req.ID
	chatID := req.Peer
	msgID := req.MessageID
	gen := task.Generation()

	if _, loaded := o.inFlight.LoadOrStore(taskID, struct{}{}); loaded {
		return
	}
	defer o.inFlight.Delete(taskID)

	taskCtx := task.Context()
	if taskCtx == nil {
		taskCtx = ctx
	}

	// 1. Media Ingress / Resolution (done first to obtain authoritative size and metadata)
	if o.db != nil {
		if beginErr := o.db.BeginDownload(chatID, msgID, gen, req.FileName, req.FinalPath, req.MediaType, req.ExpectedSize); errors.Is(beginErr, ErrAlreadySuccess) {
			o.logger.Info("task already completed successfully in DB", zap.String("task_id", taskID))
			task.Succeed(req.FinalPath, true)
			return
		}
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
		taskErr := NewTaskError("resolve", "get_message", "unavailable", isUnavailable, !isUnavailable, err)
		task.Fail("resolve", taskErr.Error(), isUnavailable)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, req.FileName, req.FinalPath, req.MediaType, req.ExpectedSize, taskErr.Error(), isUnavailable)
		}
		return
	}

	if resolvedMedia.File == nil || resolvedMedia.Size <= 0 {
		taskErr := NewTaskError("resolve", "validate_media", "unavailable", true, false, errors.New("message has no downloadable media"))
		task.Fail("unavailable", taskErr.Error(), true)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, req.FileName, req.FinalPath, req.MediaType, 0, taskErr.Error(), true)
		}
		return
	}

	task.SetResolved(resolvedMedia.Name, resolvedMedia.Size, resolvedMedia.DCID)
	authoritativeSize := resolvedMedia.Size

	// 2. Canonical Path Planning
	targetTitle := req.TargetTitle
	if targetTitle == "" && o.db != nil {
		targetTitle = o.db.GetTargetTitle(chatID)
	}
	mediaType := resolvedMedia.MediaType
	if mediaType == "" {
		mediaType = req.MediaType
	}
	fileName := resolvedMedia.Name
	if fileName == "" {
		fileName = req.FileName
	}
	msgDate := resolvedMedia.Date
	if msgDate <= 0 {
		msgDate = req.Date
	}
	finalRelPath := req.FinalPath
	if finalRelPath == "" {
		finalRelPath = PathPlanner{}.Plan(chatID, targetTitle, msgID, fileName, mediaType, msgDate)
	}
	finalRelPath = strings.ReplaceAll(finalRelPath, "\\", "/")
	task.SetFinalPath(finalRelPath)

	finalAbsPath := filepath.Join(o.saveDir, filepath.FromSlash(finalRelPath))
	partAbsPath := finalAbsPath + ".part"

	// 3. Check existing final file (Verified Idempotent success vs Collision)
	if finInfo, statErr := os.Stat(finalAbsPath); statErr == nil {
		isVerified := false
		if o.db != nil {
			if rec, recErr := o.db.GetDownloadRecord(chatID, msgID); recErr == nil && rec != nil {
				if (rec.Status == "committing" || rec.Status == "success" || rec.Status == "archived") &&
					rec.SavePath == finalRelPath && rec.FileSize == finInfo.Size() && rec.SHA256 != "" {
					actualSHA, shaErr := computeFileSHA256(finalAbsPath)
					if shaErr == nil && actualSHA == rec.SHA256 {
						isVerified = true
						_ = o.db.CompleteDownloadAndQueueArchive(
							chatID, msgID, gen, finalRelPath,
							finInfo.Size(), actualSHA,
							o.archiveWorker != nil && o.archiveWorker.IsEnabled(),
						)
						if o.archiveWorker != nil {
							o.archiveWorker.Wake()
						}
						task.SucceedResult(PublishResult{Path: finalRelPath, SHA256: actualSHA, AlreadyExists: true, absolutePath: finalAbsPath})
						return
					}
				}
			}
		}

		if !isVerified {
			o.logger.Error("collision: destination exists without verified task proof",
				zap.String("task_id", taskID),
				zap.String("final_path", finalAbsPath),
			)
			taskErr := NewTaskError("admission", "check_file", "collision", false, false, errors.New("destination exists with conflicting content or without task commit proof"))
			task.Fail("collision", taskErr.Error(), false)
			if o.db != nil {
				_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
			}
			return
		}
	}

	// 4. SSD Space Admission Check (real free space check & reservation with authoritative size)
	releaseSSD, err := o.ssdAdmission.Reserve(taskID, authoritativeSize)
	if err != nil {
		o.logger.Warn("⚠️ [SSD Admission] Insufficient SSD space, requeuing task",
			zap.String("task_id", taskID),
			zap.Error(err),
		)
		time.Sleep(500 * time.Millisecond)
		_ = o.registry.Requeue(task)
		return
	}
	defer releaseSSD()

	// 5. Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(finalAbsPath), 0o755); err != nil {
		o.logger.Error("failed to create target dir", zap.Error(err))
		taskErr := NewTaskError("path", "mkdir", "io", false, false, err)
		task.Fail("path", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}

	if task.IsTerminal() || taskCtx.Err() != nil {
		return
	}

	partFile, err := os.OpenFile(partAbsPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		o.logger.Error("failed to create part file", zap.Error(err))
		taskErr := NewTaskError("download", "open_part", "io", false, false, err)
		task.Fail("io", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}
	_ = fscommit.Preallocate(partFile, authoritativeSize)

	task.SetDownloading()
	if o.db != nil {
		_ = o.db.BeginDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize)
	}

	// 6. Build thin gotd client adapter with DataGate & CDN protection
	pool := o.access.Pool()
	invoker := pool.Invoker(taskCtx, resolvedMedia.DCID)
	client := transfer.NewGatedClient(
		invoker,
		o.transferMgr.Gate(),
		resolvedMedia.DCID,
		pool.CDN,
	)

	// 7. Execute parallel chunk download with official gotd downloader
	onProgress := func(downloaded, total int64) {
		task.RecordProgress(downloaded)
	}

	written, dlErr := o.transferMgr.DownloadFile(
		taskCtx,
		client,
		resolvedMedia.File.Location(),
		authoritativeSize,
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
		taskErr := NewTaskError("transfer", "download_file", "network", false, true, dlErr)
		task.Fail("transfer", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}

	// 9. Verification: exact size check, fsync, close, SHA calculation
	stat, statErr := partFile.Stat()
	if statErr != nil || stat.Size() != authoritativeSize || written != authoritativeSize {
		_ = partFile.Close()
		_ = os.Remove(partAbsPath)
		errDesc := fmt.Sprintf("short write: got %d, want %d", written, authoritativeSize)
		taskErr := NewTaskError("transfer", "verify_size", "corrupt", false, false, errors.New(errDesc))
		task.Fail("corrupt", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}

	if syncErr := partFile.Sync(); syncErr != nil {
		_ = partFile.Close()
		_ = os.Remove(partAbsPath)
		taskErr := NewTaskError("commit", "fsync", "io_sync", false, false, syncErr)
		task.Fail("io_sync", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}

	if closeErr := partFile.Close(); closeErr != nil {
		_ = os.Remove(partAbsPath)
		taskErr := NewTaskError("commit", "close", "io_close", false, false, closeErr)
		task.Fail("io_close", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}

	shaHex, shaErr := computeFileSHA256(partAbsPath)
	if shaErr != nil {
		_ = os.Remove(partAbsPath)
		taskErr := NewTaskError("hash", "sha256", "hash", false, false, shaErr)
		task.Fail("hash", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}

	// 10. Durable commit intent in DB
	if o.db != nil {
		if err := o.db.PrepareDownloadCommit(chatID, msgID, gen, finalRelPath, stat.Size(), shaHex); err != nil {
			_ = os.Remove(partAbsPath)
			taskErr := NewTaskError("commit", "prepare", "db_conflict", false, false, err)
			task.Fail("commit_prepare", taskErr.Error(), false)
			return
		}
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
		taskErr := NewTaskError("commit", "rename", "io", false, false, err)
		task.Fail("commit", taskErr.Error(), false)
		if o.db != nil {
			_ = o.db.FailDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, taskErr.Error(), false)
		}
		return
	}

	// 13. Complete download and queue archive in single DB transaction
	queueArchive := o.archiveWorker != nil && o.archiveWorker.IsEnabled()
	if o.db != nil {
		if err := o.db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, finalRelPath, stat.Size(), shaHex, queueArchive); err != nil {
			o.logger.Error("failed to complete download in DB", zap.Error(err))
			taskErr := NewTaskError("commit", "db_complete", "db_conflict", false, false, err)
			task.Fail("db_complete", taskErr.Error(), false)
			return
		}
	}

	// 14. Wake archive worker
	if o.archiveWorker != nil {
		o.archiveWorker.Wake()
	}

	// 15. Complete task in Registry ONLY after both SSD commit and DB transaction succeed!
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
