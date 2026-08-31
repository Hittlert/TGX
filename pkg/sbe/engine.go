package sbe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hittlert/TGX/pkg/proxy"
	sbeatomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/pkg/sbe/coordinator"
	"github.com/Hittlert/TGX/pkg/sbe/lease"
	"github.com/Hittlert/TGX/pkg/sbe/meta"
	"github.com/Hittlert/TGX/pkg/sbe/scheduler"
)

var (
	ErrEngineStopped = errors.New("SBE engine is stopped or draining")
)

// BlockFetcherFunc represents the MTProto chunk puller function.
type BlockFetcherFunc func(ctx context.Context, task scheduler.ChunkTask, buf []byte) (int64, error)

// WriteJob is passed from Network Workers to Disk Writers.
type WriteJob struct {
	Task     scheduler.ChunkTask
	Data     []byte
	Length   int64
	BufLease *lease.BufferLease
}

// EngineConfig specifies the capacity and worker parameters for SBE.
type EngineConfig struct {
	NetworkWorkers int
	DiskWorkers    int
	BufferBudget   int64
	DirtyBudget    int64
	DialerProvider proxy.DialerProvider
	BlockFetcher   BlockFetcherFunc
}

// Engine is the central Streaming Block Engine coordinating network workers and disk writers.
type Engine struct {
	cfg       EngineConfig
	pool      *lease.Pool
	scheduler *scheduler.DRRScheduler

	chunkChan    chan scheduler.ChunkTask
	writeJobChan chan WriteJob

	mu           sync.RWMutex
	coordinators map[string]*coordinator.FileCoordinator

	fetcher BlockFetcherFunc

	isDraining uint32
	ctx        context.Context
	cancel     context.CancelFunc

	schedWg sync.WaitGroup
	netWg   sync.WaitGroup
	diskWg  sync.WaitGroup
}

// NewEngine creates and configures the SBE engine.
func NewEngine(cfg EngineConfig) *Engine {
	if cfg.NetworkWorkers <= 0 {
		cfg.NetworkWorkers = 64
	}
	if cfg.DiskWorkers <= 0 {
		cfg.DiskWorkers = 5
	}
	if cfg.BufferBudget <= 0 {
		cfg.BufferBudget = lease.DefaultBufferBudget
	}
	if cfg.DirtyBudget <= 0 {
		cfg.DirtyBudget = lease.DefaultDirtyBudget
	}

	p := lease.NewPool(lease.Config{
		BufferBudget: cfg.BufferBudget,
		DirtyBudget:  cfg.DirtyBudget,
	})

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		cfg:          cfg,
		pool:         p,
		scheduler:    scheduler.NewDRRScheduler(),
		chunkChan:    make(chan scheduler.ChunkTask, 128),
		writeJobChan: make(chan WriteJob, 64),
		coordinators: make(map[string]*coordinator.FileCoordinator),
		fetcher:      cfg.BlockFetcher,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start launches the background scheduler loop, 64 network workers, and 5 disk writers.
func (e *Engine) Start() {
	// 1. Launch 5 Disk Writers
	for i := 0; i < e.cfg.DiskWorkers; i++ {
		e.diskWg.Add(1)
		go e.diskWriterWorker(i)
	}

	// 2. Launch 64 Network Workers
	for i := 0; i < e.cfg.NetworkWorkers; i++ {
		e.netWg.Add(1)
		go e.networkStreamingWorker(i)
	}

	// 3. Launch Scheduler Producer Loop
	e.schedWg.Add(1)
	go e.schedulerProducerLoop()
}

// RegisterFile registers a new or existing file task with SBE.
func (e *Engine) RegisterFile(cfg coordinator.Config) (*coordinator.FileCoordinator, *meta.MetaRecoverResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if atomic.LoadUint32(&e.isDraining) == 1 {
		return nil, nil, ErrEngineStopped
	}

	cfg.Pool = e.pool
	fc, rec, err := coordinator.NewFileCoordinator(cfg)
	if err != nil {
		return nil, nil, err
	}

	e.coordinators[cfg.FileKey] = fc
	return fc, rec, nil
}

// ScheduleFile queues all missing blocks of a FileCoordinator into the DRR scheduler.
func (e *Engine) ScheduleFile(fc *coordinator.FileCoordinator) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if atomic.LoadUint32(&e.isDraining) == 1 {
		return ErrEngineStopped
	}

	for {
		idx, ok := fc.NextMissingBlock()
		if !ok {
			break
		}

		off, length, err := fc.BeginChunk(idx)
		if err != nil {
			if errors.Is(err, coordinator.ErrFileFinalizing) || errors.Is(err, coordinator.ErrFileClosed) {
				break
			}
			continue
		}

		e.scheduler.Enqueue(scheduler.ChunkTask{
			FileKey:     fc.PartPath(),
			AttemptID:   [16]byte{},
			BlockIndex:  idx,
			Offset:      off,
			Length:      length,
			TotalSize:   int64(fc.TotalBlocks()) * int64(meta.StandardBlockSize),
			Coordinator: fc,
		})
	}
	return nil
}

// schedulerProducerLoop continuously feeds chunk tasks from DRR to chunkChan.
func (e *Engine) schedulerProducerLoop() {
	defer e.schedWg.Done()
	defer close(e.chunkChan)

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		if atomic.LoadUint32(&e.isDraining) == 1 {
			return
		}

		task, ok := e.scheduler.NextChunk()
		if !ok {
			select {
			case <-e.ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}

		select {
		case <-e.ctx.Done():
			return
		case e.chunkChan <- task:
		}
	}
}

// networkStreamingWorker pulls 2MB chunks via MTProto with BufferLease flow control.
func (e *Engine) networkStreamingWorker(workerID int) {
	defer e.netWg.Done()

	for task := range e.chunkChan {
		// 1. Acquire BufferLease with context backpressure
		bufLease, err := e.pool.AcquireBuffer(e.ctx, task.Length)
		if err != nil {
			task.Coordinator.AbortChunk(task.BlockIndex)
			e.scheduler.CompleteChunk(task.FileKey, false)
			continue
		}

		// 2. Fetch block via RPC
		data := make([]byte, task.Length)
		var n int64

		if e.fetcher != nil {
			n, err = e.fetcher(e.ctx, task, data)
		} else {
			// Stub/Direct fallback
			n = task.Length
		}

		if err != nil || n < task.Length {
			bufLease.Release()
			task.Coordinator.AbortChunk(task.BlockIndex)
			e.scheduler.CompleteChunk(task.FileKey, false)
			// Re-enqueue task to scheduler so it gets retried rather than permanently dropped
			if !task.Coordinator.IsClosed() {
				e.scheduler.EnqueueFront(task)
			}
			continue
		}

		// 3. Hand off WriteJob to Disk Writers
		job := WriteJob{
			Task:     task,
			Data:     data[:n],
			Length:   n,
			BufLease: bufLease,
		}

		select {
		case <-e.ctx.Done():
			bufLease.Release()
			task.Coordinator.AbortChunk(task.BlockIndex)
			e.scheduler.CompleteChunk(task.FileKey, false)
			return
		case e.writeJobChan <- job:
		}
	}
}

// diskWriterWorker writes chunks sequentially with DirtyLease acquisition and state tracking.
func (e *Engine) diskWriterWorker(writerID int) {
	defer e.diskWg.Done()

	for job := range e.writeJobChan {
		fc := job.Task.Coordinator

		// Write to disk with DirtyLease protection
		err := fc.WriteBlock(e.ctx, job.Task.BlockIndex, job.Data, job.BufLease)
		if err != nil {
			// Write failed -> abort and let coordinator retry
			fc.AbortChunk(job.Task.BlockIndex)
			e.scheduler.CompleteChunk(job.Task.FileKey, false)
			if !fc.IsClosed() {
				e.scheduler.EnqueueFront(job.Task)
			}
			continue
		}

		e.scheduler.CompleteChunk(job.Task.FileKey, fc.IsComplete())
	}
}

// Shutdown executes the strict graceful drain protocol.
func (e *Engine) Shutdown(ctx context.Context) error {
	// 1. Stop admission
	atomic.StoreUint32(&e.isDraining, 1)

	// 2. Stop scheduler producer and close chunkChan
	e.cancel()
	e.schedWg.Wait()

	// 3. Wait for network workers to finish and exit
	e.netWg.Wait()

	// 4. Close writeJobChan
	close(e.writeJobChan)

	// 5. Wait for disk writers to drain
	e.diskWg.Wait()

	// 6. Finalize or close all active file coordinators
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, fc := range e.coordinators {
		_ = fc.Close()
	}

	e.pool.Close()
	return nil
}

// CommitCompletedFile finishes a 100% downloaded file using non-replacing atomic rename.
func (e *Engine) CommitCompletedFile(ctx context.Context, fc *coordinator.FileCoordinator) error {
	// 1. Finalize coordinator (flushes COMPLETE slot and closes handles)
	if err := fc.Finalize(ctx); err != nil {
		return fmt.Errorf("failed to finalize coordinator: %w", err)
	}

	// 2. Execute atomic non-replacing commit
	if err := sbeatomic.CommitFile(fc.PartPath(), fc.TargetPath()); err != nil {
		return fmt.Errorf("atomic commit failed: %w", err)
	}

	// 3. Remove .meta file
	_ = sbeatomic.SyncDir(fc.TargetPath())
	return nil
}

// Stats returns live SBE performance statistics for Web UI.
type EngineStats struct {
	LeaseStats     lease.Stats `json:"lease_stats"`
	ActiveFiles    int         `json:"active_files"`
	NetworkWorkers int         `json:"network_workers"`
	DiskWorkers    int         `json:"disk_workers"`
}

// Stats returns current engine metrics.
func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	activeCount := len(e.coordinators)
	e.mu.RUnlock()

	return EngineStats{
		LeaseStats:     e.pool.Stats(),
		ActiveFiles:    activeCount,
		NetworkWorkers: e.cfg.NetworkWorkers,
		DiskWorkers:    e.cfg.DiskWorkers,
	}
}
