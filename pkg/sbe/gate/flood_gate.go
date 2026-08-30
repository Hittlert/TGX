package gate

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

const (
	MaxDataInFlight    = 40
	MaxControlInFlight = 4
	InitialStartRate   = 100.0
	MaxStartRate       = 160.0
	MinRate            = 10.0
	DefaultBurst       = 40
)

// FloodGate manages adaptive request rate limiting, data/control in-flight semaphores, and per-DC cooldowns.
type FloodGate struct {
	mu           sync.RWMutex
	baseRate     float64
	currentRate  float64
	maxRate      float64
	minRate      float64
	burst        int
	lastRamp     time.Time
	lastRateDrop time.Time
	dcCooldowns  map[int]time.Time
	limiter      *rate.Limiter

	// Dynamic Concurrency Control
	dataSem          *semaphore.Weighted
	maxDataCap       int64
	activeDataRPC    int64
	controlSem       *semaphore.Weighted
	activeControlRPC int64
}

// NewFloodGate creates an Adaptive Safety Controller with InitialStartRate=100 req/s, Burst=40, MaxDataInFlight=40, MaxControlInFlight=4.
func NewFloodGate(reqPerSec float64, burst int) *FloodGate {
	if reqPerSec <= 0 {
		reqPerSec = InitialStartRate
	}
	if burst <= 0 {
		burst = DefaultBurst
	}
	now := time.Now()
	return &FloodGate{
		baseRate:       reqPerSec,
		currentRate:    reqPerSec,
		maxRate:        MaxStartRate,
		minRate:        MinRate,
		burst:          burst,
		lastRamp:       now,
		lastRateDrop:   now.Add(-10 * time.Second),
		dcCooldowns:    make(map[int]time.Time),
		limiter:        rate.NewLimiter(rate.Limit(reqPerSec), burst),
		dataSem:        semaphore.NewWeighted(MaxDataInFlight),
		maxDataCap:     MaxDataInFlight,
		controlSem:     semaphore.NewWeighted(MaxControlInFlight),
	}
}

// AcquireDataSlot blocks until a data RPC slot is available within MaxDataInFlight (40).
func (g *FloodGate) AcquireDataSlot(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if err := g.dataSem.Acquire(ctx, 1); err != nil {
		return err
	}
	atomic.AddInt64(&g.activeDataRPC, 1)
	return nil
}

// ReleaseDataSlot releases an acquired data RPC slot.
func (g *FloodGate) ReleaseDataSlot() {
	if g == nil {
		return
	}
	atomic.AddInt64(&g.activeDataRPC, -1)
	g.dataSem.Release(1)
}

// DataInFlight returns the number of actively executing data RPCs.
func (g *FloodGate) DataInFlight() int64 {
	if g == nil {
		return 0
	}
	return atomic.LoadInt64(&g.activeDataRPC)
}

// AcquireControlSlot blocks until a control RPC slot is available within MaxControlInFlight (4).
func (g *FloodGate) AcquireControlSlot(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if err := g.controlSem.Acquire(ctx, 1); err != nil {
		return err
	}
	atomic.AddInt64(&g.activeControlRPC, 1)
	return nil
}

// ReleaseControlSlot releases an acquired control RPC slot.
func (g *FloodGate) ReleaseControlSlot() {
	if g == nil {
		return
	}
	atomic.AddInt64(&g.activeControlRPC, -1)
	g.controlSem.Release(1)
}

// ControlInFlight returns the number of actively executing control RPCs.
func (g *FloodGate) ControlInFlight() int64 {
	if g == nil {
		return 0
	}
	return atomic.LoadInt64(&g.activeControlRPC)
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

		// Smooth Additive Increase: if currentRate < maxRate and no active cooldowns, ramp up +5.0 req/s per second
		if g.currentRate < g.maxRate && len(g.dcCooldowns) == 0 {
			if now.Sub(g.lastRamp) >= time.Second {
				g.currentRate += 5.0
				if g.currentRate > g.maxRate {
					g.currentRate = g.maxRate
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

		// 3. Quick post-token verification under lock in case another worker triggered cooldown while waiting for token
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
	if g == nil {
		return
	}
	// FLOOD_WAIT_0: Telegram signals rate pressure without specifying duration.
	// Enforce a minimum 1s cooldown to back off.
	if duration <= 0 {
		duration = 1 * time.Second
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	// Debounce rate drops: do not multiply decrease more than once per 500ms
	if g.currentRate > g.minRate && now.Sub(g.lastRateDrop) >= 500*time.Millisecond {
		g.currentRate = g.currentRate * 0.75
		if g.currentRate < g.minRate {
			g.currentRate = g.minRate
		}
		g.limiter.SetLimit(rate.Limit(g.currentRate))
		g.lastRamp = now
		g.lastRateDrop = now
	}

	// Add jitter (100ms ~ 500ms) to prevent lockstep thundering herd on wake-up
	jitter := time.Duration(100+rand.Intn(400)) * time.Millisecond
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
		const maxFloodRetries = 5
		for attempt := 0; ; attempt++ {
			if m.gate != nil {
				if err := m.gate.Wait(ctx, m.dc); err != nil {
					return err
				}
			}
			err := next.Invoke(ctx, input, output)
			if d, isFlood := tgerr.AsFloodWait(err); isFlood {
				if attempt >= maxFloodRetries {
					return fmt.Errorf("flood wait retry limit (%d) exceeded on DC %d: %w", maxFloodRetries, m.dc, err)
				}
				if m.gate != nil {
					m.gate.TriggerFloodWait(m.dc, d)
				}
				continue
			}
			return err
		}
	}
}

// Middleware returns a unified telegram.Middleware that enforces shared DC cooldown,
// global account rate limiting, and automatic retry for any MTProto invoker.
func (g *FloodGate) Middleware(dc int) telegram.Middleware {
	return &floodGateMiddleware{gate: g, dc: dc}
}
