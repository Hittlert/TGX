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

	"github.com/Hittlert/TGX/core/bucket"
	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/downloader"
	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/storage"
	"github.com/Hittlert/TGX/core/targetwriter"
	"github.com/Hittlert/TGX/core/tclient"
	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

type Options struct {
	Listen           string
	OutputDir        string
	TempDir          string
	BufferType       string // "memory" (default), "ssd", "none"
	BufferDir        string
	BufferSize       int64  // capacity in bytes
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
	if o.BufferType == "" {
		o.BufferType = "memory"
	}
	if o.BufferType == "none" {
		if o.BufferDir == "" {
			o.BufferDir = o.OutputDir
		}
		o.BufferSize = 0
	} else {
		if o.BufferDir == "" {
			o.BufferDir = o.TempDir
		}
		if o.BufferSize == 0 {
			switch o.BufferType {
			case "ssd":
				o.BufferSize = 5 * 1024 * 1024 * 1024 // 5 GiB
			default:
				o.BufferSize = 512 * 1024 * 1024 // 512 MiB
			}
		}
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
		o.Threads = 48
	}
	if o.PoolSize == 0 {
		o.PoolSize = 48
	}
	if o.PeerSyncTimeout == 0 {
		o.PeerSyncTimeout = 3 * time.Minute
	}
	if o == (Options{
		Namespace: "default", Listen: "0.0.0.0:18080", OutputDir: "/app/downloads", TempDir: "/app/temp/tdl",
		BufferType: "memory", BufferDir: "/app/temp/tdl", BufferSize: 512 * 1024 * 1024,
		DBPath: "/app/state/download_records.sqlite3", SingboxURL: "http://127.0.0.1:9090",
		QueueCapacity: 1000, TerminalLimit: 2000, FileConcurrency: 5, Threads: 48, PoolSize: 48,
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
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(opts.TempDir, 0o755); err != nil {
		return fmt.Errorf("create daemon temp directory: %w", err)
	}
	if err := os.MkdirAll(opts.BufferDir, 0o755); err != nil {
		return fmt.Errorf("create daemon buffer directory: %w", err)
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create daemon output directory: %w", err)
	}

	sharedGate := gate.NewFloodGate(gate.InitialStartRate, gate.DefaultBurst)
	effectivePoolSize := opts.PoolSize
	if effectivePoolSize <= 0 {
		effectivePoolSize = 48
	}
	pool := dcpool.NewPoolWithGate(client, int64(effectivePoolSize), sharedGate,
		tclient.NewDefaultMiddlewares(ctx, opts.ReconnectTimeout)...)
	defer func() { resultErr = errors.Join(resultErr, pool.Close()) }()
	manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(pool.Default(ctx))
	access := newTelegramMediaAccess(ctx, pool, manager, opts.PeerSyncTimeout, sharedGate)

	// Automatic disk writer concurrency: SSD buffer/direct is 32, direct HDD is 8
	diskWorkers := 8
	if opts.BufferType == "ssd" || opts.BufferType == "memory" {
		diskWorkers = 32
	}

	// Initialize Object Bucket & Single TargetWriter
	var bkt bucket.Bucket
	var tw *targetwriter.TargetWriter
	if opts.BufferType != "none" {
		bktCfg := bucket.Config{
			Mode:        bucket.Mode(opts.BufferType),
			RootDir:     opts.BufferDir,
			MaxCapacity: opts.BufferSize,
		}
		var err error
		bkt, err = bucket.New(bktCfg)
		if err != nil {
			return fmt.Errorf("init bucket: %w", err)
		}
		if opts.BufferType == "ssd" {
			_ = bkt.Recover(ctx)
		}
		defer func() { _ = bkt.Close() }()

		tw = targetwriter.New(bkt, opts.OutputDir)
		tw.Start(ctx)
		defer func() { _ = tw.Close() }()
	}

	registry := NewRegistryWithContext(ctx, opts.QueueCapacity, opts.TerminalLimit, time.Now)
	registry.SetPaused(opts.StartPaused)
	registry.SetPool(PoolSnapshot{Size: effectivePoolSize})
	iter := newTaskIter(registry, newTaskResolver(access, opts.BufferDir, opts.OutputDir, bkt, tw))
	dl := downloader.New(downloader.Options{
		Pool:            pool,
		Threads:         opts.Threads,
		DiskWorkers:     diskWorkers,
		FileConcurrency: opts.FileConcurrency,
		Iter:            iter,
		Progress:        newTaskProgress(),
		FloodGate:       sharedGate,
	})

	db, err := NewDatabase(opts.DBPath)
	if err != nil {
		logctx.From(ctx).Warn("could not initialize control database", zap.Error(err))
	}
	if db != nil {
		defer db.Close()
		// Execute SBE Startup Crash Recovery Matrix with buffer awareness
		reconciler := NewReconcilerWithBuffer(db.DB(), opts.OutputDir, opts.BufferDir, opts.BufferType, nil, logctx.From(ctx))
		reconciler.SetTargetWriter(tw)
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
		orchestrator.SetBufferDir(opts.BufferDir)
		orchestrator.SetBucket(bkt)
		orchestrator.SetTargetWriter(tw)
		orchestrator.Start(groupCtx)

		// Start MTProto Real-Time Push Updates Streaming Engine
		updatesStream := NewUpdatesStream(db, orchestrator, logctx.From(ctx))
		SetGlobalUpdatesStream(updatesStream)
	}

	authWizard := NewAuthWizard(db, client, kvd, logctx.From(ctx), opts.Namespace)
	webServer := NewWebServer(db, slotPool, proxyManager, orchestrator, access, registry, logctx.From(ctx), opts.Password, sharedGate)
	webServer.SetAuthWizard(authWizard)
	webServer.SetBucket(bkt)
	webServer.SetTargetWriter(tw)

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
		err := dl.Download(groupCtx, opts.Threads)
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
		zap.String("buffer_type", opts.BufferType),
		zap.Int64("buffer_size", opts.BufferSize),
		zap.Int("file_concurrency", opts.FileConcurrency),
		zap.Int("threads", opts.Threads),
		zap.Int("disk_workers", diskWorkers),
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
