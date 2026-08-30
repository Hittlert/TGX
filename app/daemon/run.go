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
	"github.com/gotd/td/telegram/peers"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/downloader"
	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/storage"
	"github.com/Hittlert/TGX/core/tclient"
)

type Options struct {
	Listen           string
	OutputDir        string
	TempDir          string
	DBPath           string
	Password         string
	SingboxURL       string
	QueueCapacity    int
	TerminalLimit    int
	FileConcurrency  int
	Threads          int
	PoolSize         int
	StartPaused      bool
	ReconnectTimeout time.Duration
	PeerSyncTimeout  time.Duration
	Namespace        string
}

func DefaultOptions() Options {
	return (Options{}).withDefaults()
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
	if o.TempDir == "" {
		o.TempDir = "/app/temp/tdl"
	}
	if o.DBPath == "" {
		o.DBPath = "/app/state/download_records.sqlite3"
	}
	if o.SingboxURL == "" {
		o.SingboxURL = "http://127.0.0.1:9090"
	}
	if o.QueueCapacity == 0 {
		o.QueueCapacity = 1000
	}
	if o.TerminalLimit == 0 {
		o.TerminalLimit = 2000
	}
	if o.FileConcurrency == 0 {
		o.FileConcurrency = 5
	}
	if o.Threads == 0 {
		o.Threads = 32
	}
	if o.PoolSize == 0 {
		o.PoolSize = 32
	}
	if o.PeerSyncTimeout == 0 {
		o.PeerSyncTimeout = 3 * time.Minute
	}
	if o == (Options{
		Namespace: "default", Listen: "0.0.0.0:18080", OutputDir: "/app/downloads", TempDir: "/app/temp/tdl",
		DBPath: "/app/state/download_records.sqlite3", SingboxURL: "http://127.0.0.1:9090",
		QueueCapacity: 1000, TerminalLimit: 2000, FileConcurrency: 5, Threads: 32, PoolSize: 32,
		PeerSyncTimeout: 3 * time.Minute,
	}) {
		o.StartPaused = true
	}
	return o
}

func (o Options) validate() error {
	if _, _, err := net.SplitHostPort(o.Listen); err != nil {
		return fmt.Errorf("invalid daemon listen address: %w", err)
	}
	if !filepath.IsAbs(o.OutputDir) || !filepath.IsAbs(o.TempDir) {
		return errors.New("daemon output and temp directories must be absolute")
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
	if err := opts.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(opts.TempDir, 0o755); err != nil {
		return fmt.Errorf("create daemon temp directory: %w", err)
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create daemon output directory: %w", err)
	}

	pool := dcpool.NewPool(client, int64(opts.PoolSize),
		tclient.NewDefaultMiddlewares(ctx, opts.ReconnectTimeout)...)
	defer func() { resultErr = errors.Join(resultErr, pool.Close()) }()
	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(pool.Default(ctx))
	access := newTelegramMediaAccess(pool, manager, opts.PeerSyncTimeout)

	registry := NewRegistry(opts.QueueCapacity, opts.TerminalLimit, time.Now)
	registry.SetPaused(opts.StartPaused)
	registry.SetPool(PoolSnapshot{Size: opts.PoolSize})
	iter := newTaskIter(registry, newTaskResolver(access, opts.TempDir, opts.OutputDir))
	dl := downloader.New(downloader.Options{
		Pool:            pool,
		Threads:         opts.Threads,
		DiskWorkers:     6,
		FileConcurrency: opts.FileConcurrency,
		Iter:            iter,
		Progress:        newTaskProgress(),
	})

	db, err := NewDatabase(opts.DBPath)
	if err != nil {
		logctx.From(ctx).Warn("could not initialize control database", zap.Error(err))
	}
	if db != nil {
		defer db.Close()
		// Execute SBE Startup Crash Recovery Matrix
		reconciler := NewReconciler(db.DB(), opts.OutputDir, opts.TempDir, logctx.From(ctx))
		if recResults, err := reconciler.ReconcileAll(ctx); err != nil {
			logctx.From(ctx).Error("startup crash recovery failed", zap.Error(err))
		} else if len(recResults) > 0 {
			logctx.From(ctx).Info("startup crash recovery completed", zap.Int("recovered_tasks", len(recResults)))
		}
	}

	slotCfg := SlotPoolConfig{
		TotalSlots:      opts.PoolSize,
		MaxActiveFiles:  opts.FileConcurrency,
		SlotUnitMB:      2,
		MaxSlotsPerFile: opts.PoolSize,
	}
	slotPool := NewGlobalSlotPool(slotCfg)
	statsFile := ""
	if opts.DBPath != "" {
		statsFile = filepath.Join(filepath.Dir(opts.DBPath), "proxy_stats.json")
	}
	proxyManager := NewProxyManager(opts.SingboxURL, statsFile)

	group, groupCtx := errgroup.WithContext(ctx)

	var orchestrator *Orchestrator
	if db != nil {
		orchestrator = NewOrchestrator(db, slotPool, proxyManager, access, registry, logctx.From(ctx), opts.OutputDir)
		orchestrator.Start(groupCtx)

		// Start MTProto Real-Time Push Updates Streaming Engine
		updatesStream := NewUpdatesStream(db, orchestrator, logctx.From(ctx))
		SetGlobalUpdatesStream(updatesStream)
	}

	authWizard := NewAuthWizard(db, client, kvd, logctx.From(ctx), opts.Namespace)
	webServer := NewWebServer(db, slotPool, proxyManager, orchestrator, access, registry, logctx.From(ctx), opts.Password)
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
		err := dl.Download(groupCtx, opts.FileConcurrency)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
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
		// Graceful drain: wait 1.5s for active SBE checkpoints to flush
		time.Sleep(1500 * time.Millisecond)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})

	logctx.From(ctx).Info("TGX download daemon started",
		zap.String("listen", opts.Listen),
		zap.Int("file_concurrency", opts.FileConcurrency),
		zap.Int("threads", opts.Threads),
		zap.Int("pool_size", opts.PoolSize),
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
	result := make(chan error, 1)
	go func() {
		syncCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		result <- syncPeers(syncCtx)
		close(result)
	}()
	return result
}
