package transfer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	DefaultMaxDataInFlight = 40
)

// DataGate limits physical in-flight data RPCs across all files and DCs without an artificial RPS ceiling.
// It tracks per-DC server FloodWait cooldowns.
type DataGate struct {
	sem         *semaphore.Weighted
	maxInFlight int64
	inFlight    int64

	mu          sync.RWMutex
	dcCooldowns map[int]time.Time
}

// NewDataGate creates a new DataGate with maxInFlight permits.
func NewDataGate(maxInFlight int64) *DataGate {
	if maxInFlight <= 0 {
		maxInFlight = DefaultMaxDataInFlight
	}
	return &DataGate{
		sem:         semaphore.NewWeighted(maxInFlight),
		maxInFlight: maxInFlight,
		dcCooldowns: make(map[int]time.Time),
	}
}

// InFlight returns the current number of active physical data RPCs.
func (g *DataGate) InFlight() int64 {
	if g == nil {
		return 0
	}
	return atomic.LoadInt64(&g.inFlight)
}

// MaxInFlight returns the configured capacity limit.
func (g *DataGate) MaxInFlight() int64 {
	if g == nil {
		return DefaultMaxDataInFlight
	}
	return g.maxInFlight
}

// TriggerFloodWait registers a server-issued FloodWait for the specified DC.
func (g *DataGate) TriggerFloodWait(dc int, d time.Duration) {
	if g == nil || d <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	notBefore := time.Now().Add(d)
	if current, exists := g.dcCooldowns[dc]; !exists || notBefore.After(current) {
		g.dcCooldowns[dc] = notBefore
	}
}

// IsDCCooledDown returns true if the DC is currently cooling down from a FloodWait.
func (g *DataGate) IsDCCooledDown(dc int) bool {
	if g == nil {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	nb, exists := g.dcCooldowns[dc]
	return exists && time.Now().Before(nb)
}

// Acquire acquires 1 permit for a physical data RPC to the given DC.
// It checks DC cooldown without holding a semaphore permit.
// Returns an idempotent release function and error.
func (g *DataGate) Acquire(ctx context.Context, dc int) (func(), error) {
	if g == nil {
		return func() {}, nil
	}

	for {
		// 1. Check and wait for DC cooldown without occupying any semaphore permit
		g.mu.Lock()
		now := time.Now()
		for d, nb := range g.dcCooldowns {
			if now.After(nb) {
				delete(g.dcCooldowns, d)
			}
		}
		notBefore, hasCooldown := g.dcCooldowns[dc]
		var waitDuration time.Duration
		if hasCooldown && now.Before(notBefore) {
			waitDuration = notBefore.Sub(now)
		}
		g.mu.Unlock()

		if waitDuration > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitDuration):
			}
			continue
		}

		// 2. Acquire physical data RPC permit
		if err := g.sem.Acquire(ctx, 1); err != nil {
			return nil, err
		}

		// 3. Post-acquire verification: ensure DC didn't get cooled down while waiting for permit
		g.mu.Lock()
		now = time.Now()
		notBefore, hasCooldown = g.dcCooldowns[dc]
		if hasCooldown && now.Before(notBefore) {
			waitDuration = notBefore.Sub(now)
			g.mu.Unlock()
			g.sem.Release(1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitDuration):
			}
			continue
		}
		g.mu.Unlock()

		atomic.AddInt64(&g.inFlight, 1)
		var once sync.Once
		release := func() {
			once.Do(func() {
				atomic.AddInt64(&g.inFlight, -1)
				g.sem.Release(1)
			})
		}
		return release, nil
	}
}
