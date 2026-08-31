package mover

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	atomicCommit "github.com/Hittlert/TGX/pkg/sbe/atomic"
)

// MoveJob defines a single background file transfer from buffer to target.
type MoveJob struct {
	ID         string
	SrcPath    string // Path on staging disk (for large files)
	SrcData    []byte // In-memory data (for small files)
	DstPath    string // Final destination path
	Size       int64  // Total size in bytes
	OnProgress func(bytesMoved, totalBytes int64)
	OnDone     func(err error)
}

// Mover manages asynchronous sequential streaming of completed files from staging buffer to target storage.
type Mover struct {
	workers     int
	maxCapacity int64
	usedBytes   int64
	queue       chan *MoveJob
	wakeCh      chan struct{}
	mu          sync.Mutex
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	activeCount int64
	closed      int32
}

// New creates a new sequential mover with specified worker count (typically 1-2 for HDD protection) and max buffer capacity.
func New(workers int, maxCapacity int64) *Mover {
	if workers <= 0 {
		workers = 1 // Default to single-threaded sequential write to prevent HDD thrashing
	}
	if maxCapacity <= 0 {
		maxCapacity = 512 * 1024 * 1024 // Default 512MB
	}
	return &Mover{
		workers:     workers,
		maxCapacity: maxCapacity,
		queue:       make(chan *MoveJob, 2048),
		wakeCh:      make(chan struct{}),
	}
}

// Start launches the background mover worker goroutines.
func (m *Mover) Start(ctx context.Context) {
	m.ctx, m.cancel = context.WithCancel(ctx)
	for i := 0; i < m.workers; i++ {
		m.wg.Add(1)
		go m.workerLoop()
	}
}

func (m *Mover) workerLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			// Drain remaining queued jobs with canceled error on shutdown
			m.drainRemaining(m.ctx.Err())
			return
		case job, ok := <-m.queue:
			if !ok {
				return
			}
			atomic.AddInt64(&m.activeCount, 1)
			err := m.processJob(job)
			atomic.AddInt64(&m.activeCount, -1)
			// Always ensure reservation is released
			m.Release(job.Size)
			if job.OnDone != nil {
				job.OnDone(err)
			}
		}
	}
}

func (m *Mover) drainRemaining(err error) {
	for {
		select {
		case job, ok := <-m.queue:
			if !ok {
				return
			}
			m.Release(job.Size)
			if job.OnDone != nil {
				job.OnDone(err)
			}
		default:
			return
		}
	}
}

func (m *Mover) processJob(job *MoveJob) error {
	dst := job.DstPath

	// Ensure destination parent directory exists
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("create target dir %s: %w", dstDir, err)
	}

	// Case 1: In-memory small file payload
	if len(job.SrcData) > 0 {
		if _, err := os.Stat(dst); err == nil {
			return atomicCommit.ErrTargetExists
		}
		tempHash := fmt.Sprintf(".tdl-mover-%s.tmp", job.ID)
		tempPath := filepath.Join(dstDir, tempHash)
		f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("create temp target: %w", err)
		}
		nw, writeErr := f.Write(job.SrcData)
		if writeErr != nil {
			_ = f.Close()
			_ = os.Remove(tempPath)
			return fmt.Errorf("write temp target: %w", writeErr)
		}
		if int64(nw) != int64(len(job.SrcData)) {
			_ = f.Close()
			_ = os.Remove(tempPath)
			return fmt.Errorf("short write: %d of %d", nw, len(job.SrcData))
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(tempPath)
			return fmt.Errorf("sync temp target: %w", err)
		}
		_ = f.Close()

		if err := atomicCommit.CommitFile(tempPath, dst); err != nil {
			_ = os.Remove(tempPath)
			return err
		}
		if job.OnProgress != nil {
			job.OnProgress(job.Size, job.Size)
		}
		return nil
	}

	// Case 2: File on staging disk (.part file)
	src := job.SrcPath
	if src == "" {
		return errors.New("empty src in move job")
	}

	srcStat, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat buffer src %s: %w", src, err)
	}
	if job.Size > 0 && srcStat.Size() != job.Size {
		return fmt.Errorf("buffer file size %d does not match job size %d", srcStat.Size(), job.Size)
	}

	// Use atomicCommit.CommitFile for non-replacing atomic rename or sequential copy
	if err := atomicCommit.CommitFile(src, dst); err != nil {
		return err
	}

	if job.OnProgress != nil {
		job.OnProgress(job.Size, job.Size)
	}
	return nil
}

// Enqueue queues a completed buffer file for sequential transfer to target storage.
func (m *Mover) Enqueue(job *MoveJob) error {
	if m == nil {
		return nil
	}
	if atomic.LoadInt32(&m.closed) == 1 {
		return errors.New("mover is closed")
	}
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.queue <- job:
		return nil
	}
}

// Reserve attempts to allocate buffer space. Returns false if capacity is exceeded.
func (m *Mover) Reserve(bytes int64) bool {
	if m == nil || bytes <= 0 {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.usedBytes+bytes > m.maxCapacity && m.usedBytes > 0 {
		return false
	}
	m.usedBytes += bytes
	return true
}

// Release frees used buffer space and wakes pending downloads.
func (m *Mover) Release(bytes int64) {
	if m == nil || bytes <= 0 {
		return
	}
	m.mu.Lock()
	if m.usedBytes >= bytes {
		m.usedBytes -= bytes
	} else {
		m.usedBytes = 0
	}
	close(m.wakeCh)
	m.wakeCh = make(chan struct{})
	m.mu.Unlock()
}

// WaitBackpressure blocks until enough buffer space is available.
func (m *Mover) WaitBackpressure(ctx context.Context, requiredBytes int64) error {
	if m == nil || requiredBytes <= 0 {
		return nil
	}
	for {
		m.mu.Lock()
		if m.usedBytes+requiredBytes <= m.maxCapacity || m.usedBytes == 0 {
			m.usedBytes += requiredBytes
			m.mu.Unlock()
			return nil
		}
		wake := m.wakeCh
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

// UsedBytes returns current bytes occupying the staging buffer.
func (m *Mover) UsedBytes() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usedBytes
}

// MaxCapacity returns the configured buffer threshold in bytes.
func (m *Mover) MaxCapacity() int64 {
	if m == nil {
		return 0
	}
	return m.maxCapacity
}

// ActiveMoving returns the number of files currently being streamed to target.
func (m *Mover) ActiveMoving() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.activeCount)
}

// Close gracefully stops the mover workers.
func (m *Mover) Close() error {
	if m == nil {
		return nil
	}
	atomic.StoreInt32(&m.closed, 1)
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	return nil
}
