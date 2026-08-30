package gate

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// FloodGate manages shared per-DC cooldowns and global account request rate limiting.
type FloodGate struct {
	mu          sync.RWMutex
	dcCooldowns map[int]time.Time
	limiter     *rate.Limiter
}

// NewFloodGate creates a FloodGate with a global request rate limiter (e.g. 40 req/s, burst 10).
func NewFloodGate(reqPerSec float64, burst int) *FloodGate {
	if reqPerSec <= 0 {
		reqPerSec = 40.0
	}
	if burst <= 0 {
		burst = 10
	}
	return &FloodGate{
		dcCooldowns: make(map[int]time.Time),
		limiter:     rate.NewLimiter(rate.Limit(reqPerSec), burst),
	}
}

// Wait blocks until any active flood cooldown for the given DC has expired and the token bucket allows the request.
func (g *FloodGate) Wait(ctx context.Context, dc int) error {
	if g == nil {
		return nil
	}

	// 1. Check DC-level cooldown gate
	for {
		g.mu.RLock()
		notBefore, hasCooldown := g.dcCooldowns[dc]
		g.mu.RUnlock()

		if !hasCooldown {
			break
		}

		now := time.Now()
		if now.After(notBefore) {
			g.mu.Lock()
			// Re-check under write lock
			if nb, exists := g.dcCooldowns[dc]; exists && time.Now().After(nb) {
				delete(g.dcCooldowns, dc)
			}
			g.mu.Unlock()
			break
		}

		waitTime := notBefore.Sub(now)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}

	// 2. Token bucket rate limit check to prevent microsecond bursts
	if g.limiter != nil {
		return g.limiter.Wait(ctx)
	}

	return nil
}

// TriggerFloodWait registers a shared cooldown for the given DC across all workers.
func (g *FloodGate) TriggerFloodWait(dc int, duration time.Duration) {
	if g == nil || duration <= 0 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Add jitter (200ms ~ 1000ms) to prevent lockstep thundering herd on wake-up
	jitter := time.Duration(200+rand.Intn(800)) * time.Millisecond
	targetTime := time.Now().Add(duration + jitter)

	if existing, exists := g.dcCooldowns[dc]; !exists || targetTime.After(existing) {
		g.dcCooldowns[dc] = targetTime
	}
}

// IsDCCooledDown returns true if the DC is currently undergoing cooldown.
func (g *FloodGate) IsDCCooledDown(dc int) bool {
	if g == nil {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	notBefore, exists := g.dcCooldowns[dc]
	return exists && time.Now().Before(notBefore)
}
