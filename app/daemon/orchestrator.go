package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flytam/filenamify"
	"github.com/gotd/td/tgerr"
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

	runningMu     sync.Mutex
	running       bool
	inFlight      sync.Map
	activeTasks   int64
	taskSlotFreed chan struct{}

	testHooks OrchestratorTestHooks
}

// OrchestratorTestHooks provides deterministic interception points during commit/publish lifecycle.
type OrchestratorTestHooks struct {
	BeforePrepareCommit func(taskID, chatID string, msgID int)
	AfterPrepareCommit  func(taskID, chatID string, msgID int)
	AfterSetPublishing  func(taskID, chatID string, msgID int)
	BeforeRename        func(taskID, chatID string, msgID int)
	AfterRename         func(taskID, chatID string, msgID int)
	BeforeCompleteDB    func(taskID, chatID string, msgID int)
}

// SetTestHooks sets deterministic lifecycle interception hooks for testing.
func (o *Orchestrator) SetTestHooks(hooks OrchestratorTestHooks) {
	o.testHooks = hooks
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
		db:            db,
		transferMgr:   transferMgr,
		ssdAdmission:  ssdAdmission,
		proxyManager:  proxyManager,
		access:        access,
		registry:      registry,
		logger:        logger,
		saveDir:       saveDir,
		running:       true,
		taskSlotFreed: make(chan struct{}, 64),
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
	safeFolder = filepath.Clean(strings.TrimSpace(safeFolder))
	if safeFolder == "" || safeFolder == "." || safeFolder == "/" || strings.HasPrefix(safeFolder, "..") {
		safeFolder = "default"
	}

	if msgDate <= 86400 {
		msgDate = time.Now().Unix()
	}
	yearMonth := time.Unix(msgDate, 0).UTC().Format("2006_01")

	cleanRaw := strings.TrimSpace(rawName)
	if cleanRaw == "" || cleanRaw == "." || cleanRaw == fmt.Sprintf("%d.bin", msgID) || cleanRaw == fmt.Sprintf("%d.unknown", msgID) || cleanRaw == "unknown.bin" || strings.HasSuffix(cleanRaw, ".unknown") {
		ext := ".bin"
		switch mediaType {
		case "photo":
			ext = ".jpg"
		case "audio":
			ext = ".mp3"
		case "video":
			ext = ".mp4"
		default:
			ext = ".bin"
		}
		cleanRaw = fmt.Sprintf("%d%s", msgID, ext)
	}
	safeFileName, _ := filenamify.Filenamify(cleanRaw, filenamify.Options{Replacement: "_"})
	safeFileName = strings.TrimSpace(safeFileName)
	if safeFileName == "" || safeFileName == "." || strings.HasPrefix(safeFileName, "..") {
		ext := ".bin"
		switch mediaType {
		case "photo":
			ext = ".jpg"
		case "audio":
			ext = ".mp3"
		case "video":
			ext = ".mp4"
		default:
			ext = ".bin"
		}
		safeFileName = fmt.Sprintf("%d%s", msgID, ext)
	}

	finalRelPath := filepath.Clean(filepath.Join(safeFolder, yearMonth, fmt.Sprintf("%d - %s", msgID, safeFileName)))
	finalRelPath = strings.ReplaceAll(finalRelPath, "\\", "/")
	if strings.HasPrefix(finalRelPath, "..") || strings.HasPrefix(finalRelPath, "/") {
		finalRelPath = fmt.Sprintf("default/%s/%d - %s", yearMonth, msgID, safeFileName)
	}
	return finalRelPath
}

// Start launches the background orchestrator loops.
func (o *Orchestrator) Start(ctx context.Context) {
	if o.taskSlotFreed == nil {
		o.taskSlotFreed = make(chan struct{}, 64)
	}
	go o.dispatchLoop(ctx)
	go o.scanLoop(ctx)
	go o.metricsLoop(ctx)
}

func (o *Orchestrator) dispatchLoop(ctx context.Context) {
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

		maxConc := 5
		if o.transferMgr != nil && o.transferMgr.FileConcurrency() > 0 {
			maxConc = o.transferMgr.FileConcurrency()
		}

		if int(atomic.LoadInt64(&o.activeTasks)) >= maxConc {
			select {
			case <-ctx.Done():
				return
			case <-o.taskSlotFreed:
				continue
			case <-time.After(25 * time.Millisecond):
				continue
			}
		}

		task, err := o.registry.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		atomic.AddInt64(&o.activeTasks, 1)
		go func(t *Task) {
			defer func() {
				atomic.AddInt64(&o.activeTasks, -1)
				if o.taskSlotFreed != nil {
					select {
					case o.taskSlotFreed <- struct{}{}:
					default:
					}
				}
			}()
			o.downloadOne(ctx, t)
		}(task)
	}
}

// ActiveTasks returns the number of tasks currently in-flight in the orchestrator pipeline.
func (o *Orchestrator) ActiveTasks() int64 {
	return atomic.LoadInt64(&o.activeTasks)
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

// CancelTasksByChatID cancels running tasks for the given chat ID, coordinating with durable DB transitions.
func (o *Orchestrator) CancelTasksByChatID(chatID string) {
	if o.registry == nil {
		return
	}
	if o.db != nil {
		o.registry.CancelTasksByChatIDWithDecider(chatID, func(peer string, messageID int, gen string) error {
			return o.db.CancelDownload(peer, messageID, gen, "target disabled by user")
		})
	} else {
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
	if err == nil {
		EmitLifecycle(o.logger, LifecycleEvent{
			Event:     EventItemIngested,
			TaskID:    taskID,
			ChatID:    record.ChatID,
			MessageID: record.MessageID,
			Path:      record.SavePath,
			Size:      record.FileSize,
		})
	}
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

// ReconcileCommitting scans and reconciles all download records in 'committing' status.
// It serves as the continuous online owner of the committing state, converging transient
// database errors to terminal verdicts and driving Registry state without restarting.
func (o *Orchestrator) ReconcileCommitting(ctx context.Context) error {
	if o.db == nil {
		return nil
	}
	archiveEnabled := o.archiveWorker != nil && o.archiveWorker.IsEnabled()

	committingRecs, err := o.db.GetPendingCommittingDownloads()
	if err != nil {
		return fmt.Errorf("get pending committing downloads: %w", err)
	}
	if len(committingRecs) == 0 {
		return nil
	}

	for _, rec := range committingRecs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		finalAbsPath := filepath.Join(o.saveDir, filepath.FromSlash(rec.SavePath))
		partAbsPath := finalAbsPath + ".part"

		// 1. Final SSD file already exists and matches committed size and SHA proof
		if finInfo, statErr := os.Stat(finalAbsPath); statErr == nil && finInfo.Size() == rec.FileSize {
			sha, shaErr := computeFileSHA256(finalAbsPath)
			if shaErr == nil && rec.SHA256 != "" && sha == rec.SHA256 {
				_ = os.Remove(partAbsPath)
				completeErr := o.db.CompleteDownloadAndQueueArchive(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.SavePath, rec.FileSize, sha, archiveEnabled)
				if completeErr == nil || errors.Is(completeErr, ErrArchiveConflict) {
					o.logger.Info("online committing reconciler finalized download to success",
						zap.String("chat_id", rec.ChatID),
						zap.Int("message_id", rec.MessageID),
					)
					if o.registry != nil {
						o.registry.FinishTaskByMessage(rec.ChatID, rec.MessageID, rec.AttemptGeneration, StateSuccess, "", "", rec.SavePath, false, sha)
					}
					continue
				} else {
					o.logger.Warn("online committing reconciler retry failed",
						zap.String("chat_id", rec.ChatID),
						zap.Int("message_id", rec.MessageID),
						zap.Error(completeErr),
					)
					continue
				}
			}
		}

		// 2. .part file exists with matching SHA -> commit sibling part and complete
		if partInfo, statErr := os.Stat(partAbsPath); statErr == nil && partInfo.Size() == rec.FileSize {
			sha, shaErr := computeFileSHA256(partAbsPath)
			if shaErr == nil && rec.SHA256 != "" && sha == rec.SHA256 {
				if commitErr := fscommit.CommitSiblingPart(partAbsPath, finalAbsPath); commitErr == nil {
					completeErr := o.db.CompleteDownloadAndQueueArchive(rec.ChatID, rec.MessageID, rec.AttemptGeneration, rec.SavePath, rec.FileSize, sha, archiveEnabled)
					if completeErr == nil || errors.Is(completeErr, ErrArchiveConflict) {
						o.logger.Info("online committing reconciler committed part and finalized download to success",
							zap.String("chat_id", rec.ChatID),
							zap.Int("message_id", rec.MessageID),
						)
						if o.registry != nil {
							o.registry.FinishTaskByMessage(rec.ChatID, rec.MessageID, rec.AttemptGeneration, StateSuccess, "", "", rec.SavePath, false, sha)
						}
						continue
					} else {
						o.logger.Warn("online committing reconciler retry failed after commit part",
							zap.String("chat_id", rec.ChatID),
							zap.Int("message_id", rec.MessageID),
							zap.Error(completeErr),
						)
						continue
					}
				}
			}
		}

		// 3. Neither valid: reset to pending so it can be re-downloaded
		_ = os.Remove(partAbsPath)
		if updateErr := o.db.UpdateDownloadStatus(rec.ChatID, rec.MessageID, "pending", rec.FileName, rec.SavePath, rec.MediaType, rec.FileSize, ""); updateErr != nil {
			o.logger.Error("online committing reconciler failed to reset invalid committing record",
				zap.String("chat_id", rec.ChatID),
				zap.Int("message_id", rec.MessageID),
				zap.Error(updateErr),
			)
		} else {
			o.logger.Warn("online committing reconciler reset incomplete committing record to pending",
				zap.String("chat_id", rec.ChatID),
				zap.Int("message_id", rec.MessageID),
			)
		}
	}
	return nil
}

func (o *Orchestrator) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !o.IsRunning() || o.db == nil {
				continue
			}

			// 1. Reconcile committing downloads first (online continuous owner for committing state)
			if err := o.ReconcileCommitting(ctx); err != nil {
				o.logger.Error("online committing reconciliation failed", zap.Error(err))
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
	} else if ctx != nil && ctx.Done() != nil {
		var cancelTask context.CancelFunc
		taskCtx, cancelTask = context.WithCancel(taskCtx)
		defer cancelTask()
		stop := context.AfterFunc(ctx, cancelTask)
		defer stop()
	}

	EmitLifecycle(o.logger, LifecycleEvent{
		Event:     EventItemAdmitted,
		TaskID:    taskID,
		AttemptID: gen,
		ChatID:    chatID,
		MessageID: msgID,
		Path:      req.FinalPath,
		Size:      req.ExpectedSize,
	})

	// 1. Media Ingress / Resolution (done first to obtain authoritative size and metadata)
	if o.db != nil {
		beginErr := o.db.BeginDownload(chatID, msgID, gen, req.FileName, req.FinalPath, req.MediaType, req.ExpectedSize)
		if beginErr != nil {
			if errors.Is(beginErr, ErrAlreadySuccess) {
				var alreadyErr *AlreadySuccessError
				successProof := SuccessProof{SavePath: req.FinalPath, FileSize: req.ExpectedSize}
				if errors.As(beginErr, &alreadyErr) {
					successProof = alreadyErr.Proof
				}
				finalAbsPath := filepath.Join(o.OutputDir(), filepath.FromSlash(successProof.SavePath))
				fi, statErr := os.Stat(finalAbsPath)
				if statErr != nil || fi.Size() != successProof.FileSize {
					o.logger.Warn("already success in DB but physical file missing or invalid on disk",
						zap.String("task_id", taskID),
						zap.String("expected_path", finalAbsPath),
					)
					disp := FailureDisposition{
						Stage:       "admission",
						Op:          "db_begin_download",
						Class:       "missing_file",
						Unavailable: false,
						Retryable:   false,
						RetryOwner:  "none",
						Message:     fmt.Sprintf("record is success in DB but file missing or invalid on disk (%s)", successProof.SavePath),
						Cause:       statErr,
					}
					task.FailDisposition(disp)
					return
				}
				actualSHA, shaErr := computeFileSHA256(finalAbsPath)
				if shaErr != nil || actualSHA != successProof.SHA256 {
					o.logger.Warn("already success in DB but physical file hash mismatch on disk",
						zap.String("task_id", taskID),
						zap.String("expected_path", finalAbsPath),
						zap.String("expected_sha", successProof.SHA256),
						zap.String("actual_sha", actualSHA),
					)
					disp := FailureDisposition{
						Stage:       "admission",
						Op:          "db_begin_download",
						Class:       "corrupt",
						Unavailable: false,
						Retryable:   false,
						RetryOwner:  "none",
						Message:     fmt.Sprintf("record is success in DB but file corrupted on disk (%s): sha mismatch", successProof.SavePath),
						Cause:       shaErr,
					}
					task.FailDisposition(disp)
					return
				}
				o.logger.Info("task already completed successfully in DB and verified on disk",
					zap.String("task_id", taskID),
					zap.String("path", successProof.SavePath),
				)
				task.Succeed(successProof.SavePath, true)
				return
			}
			o.logger.Warn("cannot begin download: DB state rejected",
				zap.String("task_id", taskID),
				zap.Error(beginErr),
			)
			disp := FailureDisposition{
				Stage:       "admission",
				Op:          "db_begin_download",
				Class:       "db_conflict",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Message:     beginErr.Error(),
				Cause:       beginErr,
			}
			task.FailDisposition(disp)
			return
		}
	}

	normalizedPeer := strings.TrimPrefix(strings.TrimSpace(chatID), "@")
	var resolvedMedia ResolvedMedia
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		resolvedMedia, err = o.access.Resolve(taskCtx, normalizedPeer, msgID)
		if err == nil {
			break
		}
		if d, isFlood := tgerr.AsFloodWait(err); isFlood {
			select {
			case <-taskCtx.Done():
				break
			case <-time.After(d + time.Second):
				continue
			}
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		o.logger.Warn("failed to resolve telegram media",
			zap.String("task_id", taskID),
			zap.Error(err),
		)
		isUnavailable := IsUnavailable(err)
		errClass := ErrorClass(err)
		if isUnavailable {
			errClass = "unavailable"
		}
		disp := FailureDisposition{
			Stage:       "resolve",
			Op:          "get_message",
			Class:       errClass,
			Unavailable: isUnavailable,
			Retryable:   !isUnavailable,
			RetryOwner:  "none",
			Message:     err.Error(),
			Cause:       err,
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, req.FileName, req.FinalPath, req.MediaType, req.ExpectedSize, disp)
		}
		EmitLifecycle(o.logger, LifecycleEvent{
			Event:      EventItemTerminal,
			TaskID:     taskID,
			AttemptID:  gen,
			ChatID:     chatID,
			MessageID:  msgID,
			Stage:      disp.Stage,
			Op:         disp.Op,
			ErrorClass: disp.Class,
			Error:      disp.Error(),
			Retryable:  disp.Retryable,
			RetryOwner: disp.RetryOwner,
			Status:     "failed",
		})
		return
	}

	if resolvedMedia.File == nil || resolvedMedia.Size <= 0 {
		disp := FailureDisposition{
			Stage:       "resolve",
			Op:          "validate_media",
			Class:       "unavailable",
			Unavailable: true,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     "message has no downloadable media",
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, req.FileName, req.FinalPath, req.MediaType, 0, disp)
		}
		EmitLifecycle(o.logger, LifecycleEvent{
			Event:      EventItemTerminal,
			TaskID:     taskID,
			AttemptID:  gen,
			ChatID:     chatID,
			MessageID:  msgID,
			Stage:      disp.Stage,
			Op:         disp.Op,
			ErrorClass: disp.Class,
			Error:      disp.Error(),
			Retryable:  disp.Retryable,
			RetryOwner: disp.RetryOwner,
			Status:     "unavailable",
		})
		return
	}

	task.SetResolved(resolvedMedia.Name, resolvedMedia.Size, resolvedMedia.DCID)
	authoritativeSize := resolvedMedia.Size

	EmitLifecycle(o.logger, LifecycleEvent{
		Event:     EventItemResolved,
		TaskID:    taskID,
		AttemptID: gen,
		ChatID:    chatID,
		MessageID: msgID,
		DC:        resolvedMedia.DCID,
		Size:      resolvedMedia.Size,
		Path:      resolvedMedia.Name,
	})

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
						if err := o.db.CompleteExistingDownload(
							chatID, msgID, gen, finalRelPath,
							finInfo.Size(), actualSHA,
							o.archiveWorker != nil && o.archiveWorker.IsEnabled(),
						); err != nil {
							o.logger.Error("failed to complete existing download in DB", zap.Error(err))
							disp := FailureDisposition{
								Stage:       "admission",
								Op:          "db_complete_existing",
								Class:       "db_conflict",
								Unavailable: false,
								Retryable:   false,
								RetryOwner:  "none",
								Message:     err.Error(),
								Cause:       err,
							}
							task.FailDisposition(disp)
							return
						}
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
			disp := FailureDisposition{
				Stage:       "admission",
				Op:          "check_file",
				Class:       "collision",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Message:     "destination exists with conflicting content or without task commit proof",
			}
			task.FailDisposition(disp)
			if o.db != nil {
				_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
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

	// 5. Durably accept planned path in DB and transition to downloading BEFORE filesystem mutation!
	if o.db != nil {
		if err := o.db.BeginDownload(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize); err != nil {
			if errors.Is(err, ErrAlreadySuccess) {
				var alreadyErr *AlreadySuccessError
				successProof := SuccessProof{SavePath: finalRelPath, FileSize: authoritativeSize}
				if errors.As(err, &alreadyErr) {
					successProof = alreadyErr.Proof
				}
				finalAbsPath := filepath.Join(o.OutputDir(), filepath.FromSlash(successProof.SavePath))
				fi, statErr := os.Stat(finalAbsPath)
				if statErr != nil || fi.Size() != successProof.FileSize {
					o.logger.Warn("already success in DB but physical file missing or invalid on disk with planned path",
						zap.String("task_id", taskID),
						zap.String("expected_path", finalAbsPath),
					)
					disp := FailureDisposition{
						Stage:       "admission",
						Op:          "db_begin_download",
						Class:       "missing_file",
						Unavailable: false,
						Retryable:   false,
						RetryOwner:  "none",
						Message:     fmt.Sprintf("record is success in DB with planned path but file missing on disk (%s)", successProof.SavePath),
						Cause:       statErr,
					}
					task.FailDisposition(disp)
					return
				}
				actualSHA, shaErr := computeFileSHA256(finalAbsPath)
				if shaErr != nil || actualSHA != successProof.SHA256 {
					o.logger.Warn("already success in DB but physical file hash mismatch on disk with planned path",
						zap.String("task_id", taskID),
						zap.String("expected_path", finalAbsPath),
						zap.String("expected_sha", successProof.SHA256),
						zap.String("actual_sha", actualSHA),
					)
					disp := FailureDisposition{
						Stage:       "admission",
						Op:          "db_begin_download",
						Class:       "corrupt",
						Unavailable: false,
						Retryable:   false,
						RetryOwner:  "none",
						Message:     fmt.Sprintf("record is success in DB with planned path but file corrupted on disk (%s): sha mismatch", successProof.SavePath),
						Cause:       shaErr,
					}
					task.FailDisposition(disp)
					return
				}
				o.logger.Info("task already completed successfully in DB with planned path and verified on disk",
					zap.String("task_id", taskID),
					zap.String("path", successProof.SavePath),
				)
				task.Succeed(successProof.SavePath, true)
				return
			}
			o.logger.Error("failed to begin download in DB with planned path",
				zap.String("task_id", taskID),
				zap.String("planned_path", finalRelPath),
				zap.Error(err),
			)
			disp := FailureDisposition{
				Stage:       "admission",
				Op:          "db_begin_download",
				Class:       "db_conflict",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Message:     err.Error(),
				Cause:       err,
			}
			task.FailDisposition(disp)
			return
		}
	}
	task.SetDownloading()

	// 6. Ensure parent directory exists (ONLY after durable DB acceptance)
	if err := os.MkdirAll(filepath.Dir(finalAbsPath), 0o755); err != nil {
		o.logger.Error("failed to create target dir", zap.Error(err))
		disp := FailureDisposition{
			Stage:       "path",
			Op:          "mkdir",
			Class:       "io",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     err.Error(),
			Cause:       err,
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
		}
		return
	}

	if task.IsTerminal() || taskCtx.Err() != nil {
		return
	}

	partFile, err := os.OpenFile(partAbsPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		o.logger.Error("failed to create part file", zap.Error(err))
		disp := FailureDisposition{
			Stage:       "download",
			Op:          "open_part",
			Class:       "io",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     err.Error(),
			Cause:       err,
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
		}
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = partFile.Close()
			_ = os.Remove(partAbsPath)
		}
	}()
	_ = fscommit.Preallocate(partFile, authoritativeSize)

	// 6. Execute parallel chunk download with official gotd downloader
	onProgress := func(downloaded, total int64) {
		task.RecordProgress(downloaded)
	}

	pool := o.access.Pool()
	invoker := pool.Invoker(taskCtx, resolvedMedia.DCID)
	client := transfer.NewGatedClient(
		invoker,
		o.transferMgr.Gate(),
		resolvedMedia.DCID,
		pool.CDN,
	)

	maxRetries := transfer.DefaultMaxRetryAttempts
	if req.MaxRetries > 0 {
		maxRetries = req.MaxRetries
	}
	budget := transfer.ComputeRequestBudget(authoritativeSize, maxRetries)

	initPhysAttempt := fmt.Sprintf("%s-p0", gen)
	EmitLifecycle(o.logger, LifecycleEvent{
		Event:             EventDownloadStarted,
		TaskID:            taskID,
		AttemptID:         gen,
		PhysicalAttemptID: initPhysAttempt,
		ChatID:            chatID,
		MessageID:         msgID,
		DC:                resolvedMedia.DCID,
		Path:              finalRelPath,
		Size:              authoritativeSize,
		Extra: map[string]any{
			"request_budget":      budget,
			"physical_attempt_id": initPhysAttempt,
		},
	})

	var reqCount int64
	var wireBytes int64
	var physicalRetries int64

	var rangeAttempts sync.Map
	var failedAttempts sync.Map
	var lastPhysID atomic.Pointer[string]

	taskCtx = transfer.ContextWithTransferTask(taskCtx, transfer.TransferTaskContext{
		TaskID:          taskID,
		AttemptID:       gen,
		ChatID:          chatID,
		MessageID:       msgID,
		DCID:            resolvedMedia.DCID,
		MaxRetries:      maxRetries,
		RequestBudget:   budget,
		RequestCount:    &reqCount,
		WireBytes:       &wireBytes,
		PhysicalRetries: &physicalRetries,
		RangeAttempts:   &rangeAttempts,
		FailedAttempts:  &failedAttempts,
		LastPhysicalID:  &lastPhysID,
	})

	dlResult, dlErr := o.transferMgr.DownloadFileWithResult(
		taskCtx,
		client,
		resolvedMedia.File.Location(),
		authoritativeSize,
		partFile,
		onProgress,
	)
	written := dlResult.Written
	retries := dlResult.PhysicalRetries
	wireTotal := dlResult.WireBytes
	replayBytes := dlResult.ReplayBytes
	reqTotal := dlResult.RequestCount
	budget = dlResult.RequestBudget
	physAttemptID := dlResult.PhysicalAttemptID
	if physAttemptID == "" {
		physAttemptID = fmt.Sprintf("%s-p%d", gen, retries)
	}

	task.RecordTransferTelemetry(written, wireTotal, replayBytes, reqTotal, retries, physAttemptID)

	if dlErr != nil {
		_ = partFile.Close()
		_ = os.Remove(partAbsPath)

		if task.IsTerminal() || taskCtx.Err() != nil || errors.Is(dlErr, context.Canceled) {
			if o.db != nil {
				_ = o.db.CancelDownload(chatID, msgID, gen, "task canceled during transfer")
			}
			disp := FailureDisposition{
				Stage:             "transfer",
				Op:                "download_file",
				Class:             "canceled",
				Unavailable:       false,
				Retryable:         false,
				RetryOwner:        "none",
				PhysicalAttemptID: physAttemptID,
				Message:           "task canceled during transfer",
				Cause:             dlErr,
			}
			task.FailDisposition(disp)
			return
		}

		var tErr *transfer.TransferError
		var disp FailureDisposition
		if errors.As(dlErr, &tErr) {
			disp = FailureDisposition{
				Stage:             tErr.Stage,
				Op:                tErr.Op,
				Class:             tErr.Class,
				Unavailable:       tErr.Unavailable,
				Retryable:         tErr.Retryable,
				RetryOwner:        tErr.RetryOwner,
				PhysicalAttemptID: physAttemptID,
				Message:           tErr.Error(),
				Cause:             tErr.Cause,
			}
			o.logger.Warn("download transfer failed",
				zap.String("task_id", taskID),
				zap.String("logical_generation", gen),
				zap.String("physical_attempt_id", physAttemptID),
				zap.Int64("physical_retries", retries),
				zap.Int64("request_count", reqTotal),
				zap.Int64("wire_bytes", wireTotal),
				zap.Int64("replay_bytes", replayBytes),
				zap.Int64("request_budget", budget),
				zap.String("operation", tErr.Op),
				zap.String("typed_cause", tErr.Class),
				zap.String("retry_owner", tErr.RetryOwner),
				zap.Error(tErr.Cause),
			)
		} else {
			disp = FailureDisposition{
				Stage:             "transfer",
				Op:                "download_file",
				Class:             "network",
				Unavailable:       false,
				Retryable:         false,
				RetryOwner:        "gotd",
				PhysicalAttemptID: physAttemptID,
				Message:           dlErr.Error(),
				Cause:             dlErr,
			}
			o.logger.Warn("gotd download failed",
				zap.String("task_id", taskID),
				zap.String("logical_generation", gen),
				zap.String("physical_attempt_id", physAttemptID),
				zap.Int64("physical_retries", retries),
				zap.Int64("request_count", reqTotal),
				zap.Int64("wire_bytes", wireTotal),
				zap.Int64("replay_bytes", replayBytes),
				zap.Int64("request_budget", budget),
				zap.String("operation", "download_file"),
				zap.String("typed_cause", "unknown"),
				zap.String("retry_owner", "gotd"),
				zap.Error(dlErr),
			)
		}

		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
		}
		status := "failed"
		if disp.Unavailable {
			status = "unavailable"
		}
		EmitLifecycle(o.logger, LifecycleEvent{
			Event:             EventItemTerminal,
			TaskID:            taskID,
			AttemptID:         gen,
			PhysicalAttemptID: physAttemptID,
			ChatID:            chatID,
			MessageID:         msgID,
			Stage:             disp.Stage,
			Op:                disp.Op,
			Path:              finalRelPath,
			Size:              authoritativeSize,
			ErrorClass:        disp.Class,
			Error:             disp.Error(),
			Retryable:         disp.Retryable,
			RetryOwner:        disp.RetryOwner,
			Status:            status,
			PhysicalRetries:   retries,
			RequestCount:      reqTotal,
			WireBytes:         wireTotal,
			ReplayBytes:       replayBytes,
			Extra: map[string]any{
				"request_count":       reqTotal,
				"request_budget":      budget,
				"physical_retries":    retries,
				"physical_attempt_id": physAttemptID,
				"wire_bytes":          wireTotal,
				"replay_bytes":        replayBytes,
			},
		})
		return
	}

	// 9. Verification: exact size check, fsync, close, SHA calculation
	stat, statErr := partFile.Stat()
	if statErr != nil || stat.Size() != authoritativeSize || written != authoritativeSize {
		_ = partFile.Close()
		_ = os.Remove(partAbsPath)
		disp := FailureDisposition{
			Stage:       "transfer",
			Op:          "verify_size",
			Class:       "corrupt",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     fmt.Sprintf("short write: got %d, want %d", written, authoritativeSize),
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
		}
		return
	}

	if syncErr := partFile.Sync(); syncErr != nil {
		_ = partFile.Close()
		_ = os.Remove(partAbsPath)
		disp := FailureDisposition{
			Stage:       "commit",
			Op:          "fsync",
			Class:       "io",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     syncErr.Error(),
			Cause:       syncErr,
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
		}
		return
	}

	if closeErr := partFile.Close(); closeErr != nil {
		_ = os.Remove(partAbsPath)
		disp := FailureDisposition{
			Stage:       "commit",
			Op:          "close",
			Class:       "io",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     closeErr.Error(),
			Cause:       closeErr,
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
		}
		return
	}

	shaHex, shaErr := computeFileSHA256(partAbsPath)
	if shaErr != nil {
		_ = os.Remove(partAbsPath)
		disp := FailureDisposition{
			Stage:       "hash",
			Op:          "sha256",
			Class:       "io",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     shaErr.Error(),
			Cause:       shaErr,
		}
		task.FailDisposition(disp)
		if o.db != nil {
			_ = o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp)
		}
		return
	}

	// 10. Durable commit intent in DB
	if o.testHooks.BeforePrepareCommit != nil {
		o.testHooks.BeforePrepareCommit(taskID, chatID, msgID)
	}
	if task.IsTerminal() || taskCtx.Err() != nil {
		_ = os.Remove(partAbsPath)
		if o.db != nil {
			_ = o.db.CancelDownload(chatID, msgID, gen, "canceled before commit")
		}
		if !task.IsTerminal() {
			task.FailDisposition(FailureDisposition{
				Stage:       "commit",
				Op:          "check_cancellation",
				Class:       "canceled",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Message:     "task canceled before commit",
			})
		}
		return
	}

	if o.db != nil {
		if err := o.db.PrepareDownloadCommit(chatID, msgID, gen, finalRelPath, stat.Size(), shaHex); err != nil {
			_ = os.Remove(partAbsPath)
			disp := FailureDisposition{
				Stage:       "commit",
				Op:          "prepare",
				Class:       "db_conflict",
				Unavailable: false,
				Retryable:   false,
				RetryOwner:  "none",
				Message:     err.Error(),
				Cause:       err,
			}
			task.FailDisposition(disp)
			return
		}
		EmitLifecycle(o.logger, LifecycleEvent{
			Event:           EventSSDCommitPrepared,
			TaskID:          taskID,
			AttemptID:       gen,
			ChatID:          chatID,
			MessageID:       msgID,
			Path:            finalRelPath,
			Size:            stat.Size(),
			SHA256:          shaHex,
			PhysicalRetries: retries,
			RequestCount:    reqTotal,
			WireBytes:       wireTotal,
			ReplayBytes:     replayBytes,
			Extra: map[string]any{
				"request_count":    reqTotal,
				"request_budget":   budget,
				"physical_retries": retries,
				"wire_bytes":       wireTotal,
				"committed_bytes":  stat.Size(),
				"replay_bytes":     replayBytes,
			},
		})
	}
	if o.testHooks.AfterPrepareCommit != nil {
		o.testHooks.AfterPrepareCommit(taskID, chatID, msgID)
	}

	// 11. Preserve timestamp if present
	if resolvedMedia.Date > 0 {
		when := time.Unix(resolvedMedia.Date, 0)
		_ = os.Chtimes(partAbsPath, when, when)
	}

	// 12. Atomic sibling rename .part -> final
	// Durable publish intent is authoritative in DB. Worker has single ownership of the publishing window.
	task.SetPublishing()
	if o.testHooks.AfterSetPublishing != nil {
		o.testHooks.AfterSetPublishing(taskID, chatID, msgID)
	}
	if o.testHooks.BeforeRename != nil {
		o.testHooks.BeforeRename(taskID, chatID, msgID)
	}

	if err := fscommit.CommitSiblingPart(partAbsPath, finalAbsPath); err != nil {
		_ = os.Remove(partAbsPath)
		o.logger.Error("atomic commit failed", zap.Error(err))
		disp := FailureDisposition{
			Stage:       "commit",
			Op:          "rename",
			Class:       "io",
			Unavailable: false,
			Retryable:   false,
			RetryOwner:  "none",
			Message:     err.Error(),
			Cause:       err,
		}
		// First acquire DB failure verdict; do not ignore durable transition error
		if o.db != nil {
			if dbErr := o.db.FailDownloadDisposition(chatID, msgID, gen, fileName, finalRelPath, mediaType, authoritativeSize, disp); dbErr != nil {
				o.logger.Error("failed to record failure in DB after rename error; refusing to fake Registry terminal state", zap.Error(dbErr))
				return
			}
		}
		task.FailDisposition(disp)
		return
	}
	committed = true
	if o.testHooks.AfterRename != nil {
		o.testHooks.AfterRename(taskID, chatID, msgID)
	}
	EmitLifecycle(o.logger, LifecycleEvent{
		Event:           EventSSDCommitted,
		TaskID:          taskID,
		AttemptID:       gen,
		ChatID:          chatID,
		MessageID:       msgID,
		Path:            finalRelPath,
		Size:            stat.Size(),
		SHA256:          shaHex,
		PhysicalRetries: retries,
		RequestCount:    reqTotal,
		WireBytes:       wireTotal,
		ReplayBytes:     replayBytes,
		Extra: map[string]any{
			"request_count":    reqTotal,
			"request_budget":   budget,
			"physical_retries": retries,
			"wire_bytes":       wireTotal,
			"committed_bytes":  stat.Size(),
			"replay_bytes":     replayBytes,
		},
	})

	// 13. Complete download and queue archive in single DB transaction
	queueArchive := o.archiveWorker != nil && o.archiveWorker.IsEnabled()
	if o.db != nil {
		if o.testHooks.BeforeCompleteDB != nil {
			o.testHooks.BeforeCompleteDB(taskID, chatID, msgID)
		}
		completeErr := o.db.CompleteDownloadAndQueueArchive(chatID, msgID, gen, finalRelPath, stat.Size(), shaHex, queueArchive)
		if completeErr != nil {
			if errors.Is(completeErr, ErrArchiveConflict) {
				// Authoritative conflict accepted: DB has committed download=success and archive=conflict!
				o.logger.Warn("archive conflict disposition committed for duplicate download completion",
					zap.String("chat_id", chatID),
					zap.Int("message_id", msgID),
					zap.Error(completeErr),
				)
			} else {
				// Transient or DB error: do not fake terminal failed!
				// Keep publishing; trigger online committing reconciler to finalize convergence.
				o.logger.Warn("temporary failure completing download in DB, triggering online committing reconciler",
					zap.String("chat_id", chatID),
					zap.Int("message_id", msgID),
					zap.Error(completeErr),
				)
				go func() {
					_ = o.ReconcileCommitting(context.Background())
				}()
				return
			}
		}

		if queueArchive && !errors.Is(completeErr, ErrArchiveConflict) {
			EmitLifecycle(o.logger, LifecycleEvent{
				Event:     EventArchiveQueued,
				TaskID:    taskID,
				AttemptID: gen,
				ChatID:    chatID,
				MessageID: msgID,
				Path:      finalRelPath,
				Size:      stat.Size(),
				SHA256:    shaHex,
			})
		}
	}

	// 14. Wake archive worker
	if o.archiveWorker != nil {
		o.archiveWorker.Wake()
	}

	// 15. Complete task in Registry ONLY after both SSD commit and DB transaction succeed!
	task.SucceedResult(PublishResult{
		Path:              finalRelPath,
		SHA256:            shaHex,
		WireBytes:         wireTotal,
		ReplayBytes:       replayBytes,
		RequestCount:      reqTotal,
		PhysicalRetries:   retries,
		PhysicalAttemptID: physAttemptID,
		absolutePath:      finalAbsPath,
	})
	EmitLifecycle(o.logger, LifecycleEvent{
		Event:             EventItemTerminal,
		TaskID:            taskID,
		AttemptID:         gen,
		PhysicalAttemptID: physAttemptID,
		ChatID:            chatID,
		MessageID:         msgID,
		Path:              finalRelPath,
		Size:              stat.Size(),
		SHA256:            shaHex,
		Status:            "success",
		PhysicalRetries:   retries,
		RequestCount:      reqTotal,
		WireBytes:         wireTotal,
		ReplayBytes:       replayBytes,
		Extra: map[string]any{
			"request_count":       reqTotal,
			"request_budget":      budget,
			"physical_retries":    retries,
			"physical_attempt_id": physAttemptID,
			"wire_bytes":          wireTotal,
			"committed_bytes":     stat.Size(),
			"replay_bytes":        replayBytes,
		},
	})

	o.logger.Info("download completed successfully",
		zap.String("task_id", taskID),
		zap.String("logical_generation", gen),
		zap.String("physical_attempt_id", physAttemptID),
		zap.String("rel_path", finalRelPath),
		zap.Int64("size", stat.Size()),
		zap.String("sha256", shaHex),
		zap.Int64("physical_retries", retries),
		zap.Int64("request_count", reqTotal),
		zap.Int64("wire_bytes", wireTotal),
		zap.Int64("replay_bytes", replayBytes),
		zap.Int64("request_budget", budget),
	)
}
