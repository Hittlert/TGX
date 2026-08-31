package mover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// MoveJob defines a single background file transfer from buffer to target.
type MoveJob struct {
	ID         string
	SrcPath    string
	DstPath    string
	Size       int64
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
		queue:       make(chan *MoveJob, 1024),
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
	const bufferSize = 4 * 1024 * 1024 // 4 MiB sequential streaming buffer

	buf := make([]byte, bufferSize)
	for {
		select {
		case <-m.ctx.Done():
			return
		case job, ok := <-m.queue:
			if !ok {
				return
			}
			atomic.AddInt64(&m.activeCount, 1)
			err := m.processJob(job, buf)
			atomic.AddInt64(&m.activeCount, -1)
			if job.OnDone != nil {
				job.OnDone(err)
			}
		}
	}
}

func (m *Mover) processJob(job *MoveJob, buf []byte) (retErr error) {
	src := job.SrcPath
	dst := job.DstPath

	// Ensure destination parent directory exists
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("create target dir %s: %w", dstDir, err)
	}

	// Try atomic rename first (instant if on same filesystem mount)
	if err := os.Rename(src, dst); err == nil {
		m.Release(job.Size)
		if job.OnProgress != nil {
			job.OnProgress(job.Size, job.Size)
		}
		return nil
	}

	// Cross-filesystem move: stream with 4MB sequential buffer to target
	dstTmp := dst + ".moving"
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open buffer src %s: %w", src, err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dstTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("open target dst %s: %w", dstTmp, err)
	}

	var written int64
	defer func() {
		_ = dstFile.Close()
		if retErr != nil {
			_ = os.Remove(dstTmp)
		}
	}()

	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		default:
		}

		nr, readErr := srcFile.Read(buf)
		if nr > 0 {
			nw, writeErr := dstFile.Write(buf[:nr])
			if writeErr != nil {
				return fmt.Errorf("write target file: %w", writeErr)
			}
			written += int64(nw)
			if job.OnProgress != nil {
				job.OnProgress(written, job.Size)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("read buffer file: %w", readErr)
		}
	}

	// Final sync to HDD platters
	if err := dstFile.Sync(); err != nil {
		return fmt.Errorf("sync target file: %w", err)
	}
	if err := dstFile.Close(); err != nil {
		return fmt.Errorf("close target file: %w", err)
	}
	_ = srcFile.Close()

	// Atomic rename to final path
	if err := os.Rename(dstTmp, dst); err != nil {
		return fmt.Errorf("rename target tmp %s to %s: %w", dstTmp, dst, err)
	}

	// Remove source file from buffer
	_ = os.Remove(src)
	m.Release(job.Size)
	return nil
}

// Enqueue queues a completed buffer file for sequential transfer to target storage.
func (m *Mover) Enqueue(job *MoveJob) error {
	if m == nil {
		return nil
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
	if m == nil {
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
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	return nil
}
