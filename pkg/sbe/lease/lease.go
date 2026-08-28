package lease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

var (
	ErrPoolClosed   = errors.New("lease pool is closed")
	ErrLeaseZero    = errors.New("cannot acquire zero or negative lease bytes")
	ErrLeaseTooBig  = errors.New("requested lease exceeds total pool budget")
)

const (
	DefaultBufferBudget = 96 * 1024 * 1024 // 96 MiB
	DefaultDirtyBudget  = 48 * 1024 * 1024 // 48 MiB
	StandardBlockSize   = 2 * 1024 * 1024  // 2 MiB
)

// Pool manages dual-lease memory backpressure:
// 1. BufferLease: limits in-flight network memory buffers
// 2. DirtyLease: limits unflushed disk data pages
type Pool struct {
	bufferBudget int64
	dirtyBudget  int64

	bufSem   *semaphore.Weighted
	dirtySem *semaphore.Weighted

	bufUsed   int64
	dirtyUsed int64

	mu     sync.RWMutex
	closed bool
}

// Config defines the capacity budgets for the lease pool.
type Config struct {
	BufferBudget int64
	DirtyBudget  int64
}

// NewPool creates a new Dual-Lease Pool with given budgets.
func NewPool(cfg Config) *Pool {
	bufB := cfg.BufferBudget
	if bufB <= 0 {
		bufB = DefaultBufferBudget
	}
	dirtyB := cfg.DirtyBudget
	if dirtyB <= 0 {
		dirtyB = DefaultDirtyBudget
	}

	return &Pool{
		bufferBudget: bufB,
		dirtyBudget:  dirtyB,
		bufSem:       semaphore.NewWeighted(bufB),
		dirtySem:     semaphore.NewWeighted(dirtyB),
	}
}

// BufferLease represents a granted quota of memory for network streaming.
type BufferLease struct {
	pool     *Pool
	size     int64
	released uint32
}

// DirtyLease represents a granted quota of unflushed data for disk writers.
type DirtyLease struct {
	pool     *Pool
	size     int64
	released uint32
}

// AcquireBuffer acquires buffer quota for network downloading with context cancellation support.
func (p *Pool) AcquireBuffer(ctx context.Context, size int64) (*BufferLease, error) {
	if size <= 0 {
		return nil, ErrLeaseZero
	}
	if size > p.bufferBudget {
		return nil, ErrLeaseTooBig
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	if err := p.bufSem.Acquire(ctx, size); err != nil {
		return nil, err
	}

	atomic.AddInt64(&p.bufUsed, size)
	return &BufferLease{
		pool: p,
		size: size,
	}, nil
}

// TryAcquireBuffer tries to acquire buffer quota non-blockingly.
func (p *Pool) TryAcquireBuffer(size int64) (*BufferLease, bool) {
	if size <= 0 || size > p.bufferBudget {
		return nil, false
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, false
	}
	p.mu.RUnlock()

	if !p.bufSem.TryAcquire(size) {
		return nil, false
	}

	atomic.AddInt64(&p.bufUsed, size)
	return &BufferLease{
		pool: p,
		size: size,
	}, true
}

// Release returns the buffer quota back to the pool.
func (l *BufferLease) Release() {
	if l == nil || l.pool == nil {
		return
	}
	if atomic.CompareAndSwapUint32(&l.released, 0, 1) {
		l.pool.bufSem.Release(l.size)
		atomic.AddInt64(&l.pool.bufUsed, -l.size)
	}
}

// Size returns the byte size of this lease.
func (l *BufferLease) Size() int64 {
	if l == nil {
		return 0
	}
	return l.size
}

// AcquireDirty acquires dirty disk quota before calling WriteAt.
func (p *Pool) AcquireDirty(ctx context.Context, size int64) (*DirtyLease, error) {
	if size <= 0 {
		return nil, ErrLeaseZero
	}
	if size > p.dirtyBudget {
		return nil, ErrLeaseTooBig
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	if err := p.dirtySem.Acquire(ctx, size); err != nil {
		return nil, err
	}

	atomic.AddInt64(&p.dirtyUsed, size)
	return &DirtyLease{
		pool: p,
		size: size,
	}, nil
}

// Release returns the dirty quota back to the pool after fdatasync and meta checkpoint.
func (l *DirtyLease) Release() {
	if l == nil || l.pool == nil {
		return
	}
	if atomic.CompareAndSwapUint32(&l.released, 0, 1) {
		l.pool.dirtySem.Release(l.size)
		atomic.AddInt64(&l.pool.dirtyUsed, -l.size)
	}
}

// Size returns the byte size of this dirty lease.
func (l *DirtyLease) Size() int64 {
	if l == nil {
		return 0
	}
	return l.size
}

// ReleaseDirtyBytes releases a specific amount of dirty quota (used by batched CheckpointLoop).
func (p *Pool) ReleaseDirtyBytes(size int64) {
	if size <= 0 {
		return
	}
	p.dirtySem.Release(size)
	atomic.AddInt64(&p.dirtyUsed, -size)
}

// Stats returns live usage information for telemetry and Web UI.
type Stats struct {
	BufferBudget int64   `json:"buffer_budget"`
	BufferUsed   int64   `json:"buffer_used"`
	BufferUtil   float64 `json:"buffer_utilization"`
	DirtyBudget  int64   `json:"dirty_budget"`
	DirtyUsed    int64   `json:"dirty_used"`
	DirtyUtil    float64 `json:"dirty_utilization"`
}

// Stats returns current pool metrics.
func (p *Pool) Stats() Stats {
	bufU := atomic.LoadInt64(&p.bufUsed)
	dirtyU := atomic.LoadInt64(&p.dirtyUsed)

	return Stats{
		BufferBudget: p.bufferBudget,
		BufferUsed:   bufU,
		BufferUtil:   float64(bufU) / float64(p.bufferBudget),
		DirtyBudget:  p.dirtyBudget,
		DirtyUsed:    dirtyU,
		DirtyUtil:    float64(dirtyU) / float64(p.dirtyBudget),
	}
}

// Close closes the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}
