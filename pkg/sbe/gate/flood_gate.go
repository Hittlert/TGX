package gate

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/time/rate"
)

// FloodGate manages shared per-DC cooldowns and global account request rate limiting.
type FloodGate struct {
	mu           sync.RWMutex
	baseRate     float64
	currentRate  float64
	burst        int
	lastRamp     time.Time
	lastRateDrop time.Time
	dcCooldowns  map[int]time.Time
	limiter      *rate.Limiter
}

// NewFloodGate creates a FloodGate with a global request rate limiter (default 40 req/s, burst 10).
func NewFloodGate(reqPerSec float64, burst int) *FloodGate {
	if reqPerSec <= 0 {
		reqPerSec = 40.0
	}
	if burst <= 0 {
		burst = 10
	}
	now := time.Now()
	return &FloodGate{
		baseRate:     reqPerSec,
		currentRate:  reqPerSec,
		burst:        burst,
		lastRamp:     now,
		lastRateDrop: now.Add(-10 * time.Second),
		dcCooldowns:  make(map[int]time.Time),
		limiter:      rate.NewLimiter(rate.Limit(reqPerSec), burst),
	}
}

// CurrentRate returns current adaptive rate.
func (g *FloodGate) CurrentRate() float64 {
	if g == nil {
		return 0
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.currentRate
}

// BaseRate returns baseline configured rate.
func (g *FloodGate) BaseRate() float64 {
	if g == nil {
		return 0
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.baseRate
}

// Wait blocks until any active flood cooldown for the given DC has expired and the token bucket allows the request.
func (g *FloodGate) Wait(ctx context.Context, dc int) error {
	if g == nil {
		return nil
	}

	for {
		// 1. Check DC cooldown FIRST before taking any global token!
		g.mu.Lock()
		now := time.Now()
		// Clean up expired DC cooldowns
		for d, nb := range g.dcCooldowns {
			if now.After(nb) {
				delete(g.dcCooldowns, d)
			}
		}

		// Smooth Additive Increase: if currentRate < baseRate and no active cooldowns, ramp up +2.0 req/s per second
		if g.currentRate < g.baseRate && len(g.dcCooldowns) == 0 {
			if now.Sub(g.lastRamp) >= time.Second {
				g.currentRate += 2.0
				if g.currentRate > g.baseRate {
					g.currentRate = g.baseRate
				}
				g.limiter.SetLimit(rate.Limit(g.currentRate))
				g.lastRamp = now
			}
		}

		notBefore, hasCooldown := g.dcCooldowns[dc]
		var waitTime time.Duration
		if hasCooldown && now.Before(notBefore) {
			waitTime = notBefore.Sub(now)
		}
		limiter := g.limiter
		g.mu.Unlock()

		if waitTime > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitTime):
			}
			continue
		}

		// 2. DC is healthy: Wait for global token bucket
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return err
			}
		}

		// 3. Quick post-token verification under lock in case another worker triggered cooldown while we waited for token
		g.mu.Lock()
		now = time.Now()
		notBefore, hasCooldown = g.dcCooldowns[dc]
		if hasCooldown && now.Before(notBefore) {
			waitTime = notBefore.Sub(now)
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitTime):
			}
			continue
		}
		g.mu.Unlock()
		return nil
	}
}

// TriggerFloodWait registers a shared cooldown for the given DC across all workers and adaptively lowers rate.
func (g *FloodGate) TriggerFloodWait(dc int, duration time.Duration) {
	if g == nil || duration <= 0 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	// Debounce rate drops: do not multiply decrease more than once per 500ms
	if g.currentRate > 10.0 && now.Sub(g.lastRateDrop) >= 500*time.Millisecond {
		g.currentRate = g.currentRate * 0.75
		if g.currentRate < 10.0 {
			g.currentRate = 10.0
		}
		g.limiter.SetLimit(rate.Limit(g.currentRate))
		g.lastRamp = now
		g.lastRateDrop = now
	}

	// Add jitter (200ms ~ 1000ms) to prevent lockstep thundering herd on wake-up
	jitter := time.Duration(200+rand.Intn(800)) * time.Millisecond
	targetTime := now.Add(duration + jitter)

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

type floodGateMiddleware struct {
	gate *FloodGate
	dc   int
}

func (m *floodGateMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		var lastErr error
		for attempt := 0; attempt < 5; attempt++ {
			if m.gate != nil {
				if err := m.gate.Wait(ctx, m.dc); err != nil {
					return err
				}
			}
			lastErr = next.Invoke(ctx, input, output)
			if d, isFlood := tgerr.AsFloodWait(lastErr); isFlood {
				if m.gate != nil {
					m.gate.TriggerFloodWait(m.dc, d)
				}
				continue
			}
			return lastErr
		}
		return lastErr
	}
}

// Middleware returns a unified telegram.Middleware that enforces shared DC cooldown,
// global account rate limiting, and automatic retry for any MTProto invoker.
func (g *FloodGate) Middleware(dc int) telegram.Middleware {
	return &floodGateMiddleware{gate: g, dc: dc}
}
