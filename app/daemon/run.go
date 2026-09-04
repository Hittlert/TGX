package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/peers"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/storage"
	"github.com/Hittlert/TGX/core/tclient"
	"github.com/Hittlert/TGX/core/transfer"
	"github.com/Hittlert/TGX/internal/fscommit"
)

type Options struct {
	Listen           string
	OutputDir        string // SSD download root
	ArchiveDir       string // HDD archive root (optional)
	MinFreeSpace     uint64 // minimum SSD free space reserve, default 5 GiB
	DBPath           string
	Password         string
	SingboxURL       string
	QueueCapacity    int
	TerminalLimit    int
	FileConcurrency  int // default 32
	Threads          int // max file threads, default 8
	PoolSize         int // max data in-flight, default 40
	StartPaused      bool
	ReconnectTimeout time.Duration
	PeerSyncTimeout  time.Duration
	Namespace        string
}

func DefaultOptions() Options {
	opts := (Options{}).withDefaults()
	opts.StartPaused = true
	return opts
}

func (o Options) withDefaults() Options {
	if o.Namespace == "" {
		o.Namespace = "default"
	}
	if o.Listen == "" {
		o.Listen = "0.0.0.0:18080"
	}
	if o.OutputDir == "" {
		o.OutputDir = "/app/downloads"
	}
	if o.FileConcurrency <= 0 {
		o.FileConcurrency = 5
	}
	if o.Threads <= 0 {
		o.Threads = 48
	}
	if o.PoolSize <= 0 {
		o.PoolSize = 48
	}
	if o.MinFreeSpace == 0 {
		o.MinFreeSpace = fscommit.DefaultMinFreeSpace
	}
	if o.DBPath == "" {
		o.DBPath = "/app/state/download_records.sqlite3"
	}
	if o.QueueCapacity <= 0 {
		o.QueueCapacity = 1000
	}
	if o.TerminalLimit <= 0 {
		o.TerminalLimit = 2000
	}
	if o.PeerSyncTimeout <= 0 {
		o.PeerSyncTimeout = 3 * time.Minute
	}
	return o
}

func (o Options) validate() error {
	if _, _, err := net.SplitHostPort(o.Listen); err != nil {
		return fmt.Errorf("invalid daemon listen address: %w", err)
	}
	if !filepath.IsAbs(o.OutputDir) {
		return errors.New("daemon output directory must be absolute")
	}
	if o.ArchiveDir != "" && !filepath.IsAbs(o.ArchiveDir) {
		return errors.New("daemon archive directory must be absolute")
	}
	if o.FileConcurrency < 1 || o.FileConcurrency > 64 {
		return fmt.Errorf("file concurrency must be between 1 and 64, got %d", o.FileConcurrency)
	}
	if o.Threads < 1 || o.Threads > 64 {
		return fmt.Errorf("download threads must be between 1 and 64, got %d", o.Threads)
	}
	if o.PoolSize < 1 || o.PoolSize > 64 {
		return fmt.Errorf("DC pool size must be between 1 and 64, got %d", o.PoolSize)
	}
	if o.QueueCapacity < 1 || o.TerminalLimit < 1 {
		return errors.New("queue capacity and terminal limit must be positive")
	}
	if o.ReconnectTimeout < 0 {
		return errors.New("reconnect timeout cannot be negative")
	}
	if o.PeerSyncTimeout <= 0 || o.PeerSyncTimeout > 30*time.Minute {
		return errors.New("peer sync timeout must be positive and no more than 30m")
	}
	return nil
}

func Run(ctx context.Context, client *telegram.Client, kvd storage.Storage, opts Options) (resultErr error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return err
	}

	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	listener, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return fmt.Errorf("bind listen address %s: %w", opts.Listen, err)
	}
	_ = listener.Close()

	pool := dcpool.NewPool(client, int64(opts.PoolSize),
		tclient.NewDefaultMiddlewares(ctx, opts.ReconnectTimeout)...)
	defer func() { resultErr = errors.Join(resultErr, pool.Close()) }()
	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(pool.Default(ctx))
	access := newTelegramMediaAccess(ctx, pool, manager, opts.PeerSyncTimeout)

	// 1. Capacity & Admission Owners
	ssdAdmission := fscommit.NewSSDAdmission(opts.OutputDir, opts.MinFreeSpace)
	logger := logctx.From(ctx)
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: opts.FileConcurrency,
		MaxFileThreads:  opts.Threads,
		MaxDataInFlight: int64(opts.PoolSize),
		TaskRetryHandler: func(taskCtx context.Context, event downloader.RetryEvent) {
			tc, _ := transfer.TransferTaskFromContext(taskCtx)
			physAttemptID := fmt.Sprintf("%s-p%d", tc.AttemptID, event.Attempt)
			EmitLifecycle(logger, LifecycleEvent{
				Event:             EventRPCRetry,
				TaskID:            tc.TaskID,
				AttemptID:         tc.AttemptID,
				PhysicalAttemptID: physAttemptID,
				ChatID:            tc.ChatID,
				MessageID:         tc.MessageID,
				DC:                tc.DCID,
				Op:                event.Operation,
				PhysicalRetries:   int64(event.Attempt),
				Error:             fmt.Sprintf("%v", event.Err),
				Extra: map[string]any{
					"operation":           event.Operation,
					"attempt":             event.Attempt,
					"physical_attempt_id": physAttemptID,
				},
			})
		},
	})

	var db *Database
	if opts.DBPath != "" {
		db, err = NewDatabase(opts.DBPath)
		if err != nil {
			logctx.From(ctx).Warn("could not initialize control database", zap.Error(err))
		}
	}
	if db != nil {
		defer db.Close()
	}

	// 2. Single-Worker Whole-File Archive
	var archiveWorker *ArchiveWorker
	if opts.ArchiveDir != "" && db != nil {
		archiveWorker, err = NewArchiveWorker(db, opts.OutputDir, opts.ArchiveDir, logctx.From(ctx))
		if err != nil {
			return fmt.Errorf("init archive worker: %w", err)
		}
	}

	// 3. Restart Crash Recovery Matrix
	if db != nil {
		if recErr := ReconcileOnStartup(ctx, db, opts.OutputDir, opts.ArchiveDir, logctx.From(ctx)); recErr != nil {
			logctx.From(ctx).Warn("startup crash recovery reported warning", zap.Error(recErr))
		}
	}

	registry := NewRegistryWithContext(ctx, opts.QueueCapacity, opts.TerminalLimit, time.Now)
	registry.SetPaused(opts.StartPaused)
	registry.SetPool(PoolSnapshot{Size: opts.PoolSize})

	statsFile := ""
	if opts.DBPath != "" {
		statsFile = filepath.Join(filepath.Dir(opts.DBPath), "proxy_stats.json")
	}
	proxyManager := NewProxyManager(opts.SingboxURL, statsFile)

	group, groupCtx := errgroup.WithContext(ctx)

	var orchestrator *Orchestrator
	if db != nil {
		orchestrator = NewOrchestrator(db, transferMgr, ssdAdmission, proxyManager, access, registry, logctx.From(ctx), opts.OutputDir)
		if archiveWorker != nil {
			orchestrator.SetArchiveWorker(archiveWorker)
		}
		orchestrator.Start(groupCtx)

		// Start MTProto Real-Time Push Updates Streaming Engine
		updatesStream := NewUpdatesStream(db, orchestrator, logctx.From(ctx))
		SetGlobalUpdatesStream(updatesStream)
	}

	if archiveWorker != nil && archiveWorker.IsEnabled() {
		archiveWorker.Start(groupCtx)
	}

	authWizard := NewAuthWizard(db, client, kvd, logctx.From(ctx), opts.Namespace)
	webServer := NewWebServer(db, transferMgr, ssdAdmission, proxyManager, orchestrator, access, registry, logctx.From(ctx), opts.Password)
	webServer.SetAuthWizard(authWizard)

	server := &http.Server{
		Addr: opts.Listen, Handler: webServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second,
	}
	peerSyncResult := startInitialPeerSync(groupCtx, opts.PeerSyncTimeout, access.SyncPeers)
	group.Go(func() error {
		select {
		case err := <-peerSyncResult:
			if err != nil && !errors.Is(err, context.Canceled) {
				registry.SetLastError(err.Error())
				logctx.From(ctx).Warn("initial daemon peer refresh failed", zap.Error(err))
			}
		case <-groupCtx.Done():
		}
		return nil
	})
	group.Go(func() error {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		proxyManager.StartWatchdog(groupCtx)
		return nil
	})
	group.Go(func() error {
		<-groupCtx.Done()
		if orchestrator != nil {
			orchestrator.SetRunning(false)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})

	logctx.From(ctx).Info("TGX direct SSD download daemon started",
		zap.String("listen", opts.Listen),
		zap.String("output_dir", opts.OutputDir),
		zap.String("archive_dir", opts.ArchiveDir),
		zap.Uint64("min_free_space", opts.MinFreeSpace),
		zap.Int("file_concurrency", opts.FileConcurrency),
		zap.Int("threads", opts.Threads),
		zap.Int("max_data_in_flight", opts.PoolSize),
		zap.Duration("peer_sync_timeout", opts.PeerSyncTimeout),
		zap.Bool("paused", opts.StartPaused),
	)
	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func startInitialPeerSync(
	ctx context.Context,
	timeout time.Duration,
	syncPeers func(context.Context) error,
) <-chan error {
	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		syncCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ch <- syncPeers(syncCtx)
	}()
	return ch
}
