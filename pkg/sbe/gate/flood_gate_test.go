package gate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloodGateCoalescing(t *testing.T) {
	gate := NewFloodGate(1000, 100)

	// Initially not cooled down
	if gate.IsDCCooledDown(5) {
		t.Fatal("DC 5 should not be in cooldown initially")
	}

	// Trigger 200ms flood wait
	gate.TriggerFloodWait(5, 200*time.Millisecond)
	if !gate.IsDCCooledDown(5) {
		t.Fatal("DC 5 should be in cooldown after trigger")
	}

	// Wait should block until cooldown expires
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := gate.Wait(ctx, 5)
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond {
		t.Fatalf("Wait returned too early: %v", elapsed)
	}

	if gate.IsDCCooledDown(5) {
		t.Fatal("DC 5 should no longer be in cooldown after wait completed")
	}
}

func TestFloodGateContextCancellation(t *testing.T) {
	gate := NewFloodGate(1000, 100)
	gate.TriggerFloodWait(2, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := gate.Wait(ctx, 2)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got: %v", err)
	}
}

func TestFloodGateAdaptiveRateAIMD(t *testing.T) {
	gate := NewFloodGate(40.0, 10)
	assert.Equal(t, 40.0, gate.CurrentRate())
	assert.Equal(t, 40.0, gate.BaseRate())

	// Multiplicative Decrease on FloodWait
	gate.TriggerFloodWait(5, 50*time.Millisecond)
	assert.Equal(t, 30.0, gate.CurrentRate()) // 40 * 0.75 = 30

	time.Sleep(600 * time.Millisecond)
	gate.TriggerFloodWait(5, 50*time.Millisecond)
	assert.Equal(t, 22.5, gate.CurrentRate()) // 30 * 0.75 = 22.5

	// Wait for cooldown to expire
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := gate.Wait(ctx, 5)
	require.NoError(t, err)

	// Rate remains throttled right after cooldown
	assert.LessOrEqual(t, gate.CurrentRate(), 30.0)
}

func TestFloodGateTokenWaitSnapshotRace(t *testing.T) {
	// Rate limit = 1 req/s, burst = 1
	gate := NewFloodGate(1.0, 1)

	// Consume the initial burst token
	err := gate.Wait(context.Background(), 2)
	require.NoError(t, err)

	doneCh := make(chan error, 1)
	start := time.Now()

	// Launch worker waiting for next token (takes ~1s)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		doneCh <- gate.Wait(ctx, 2)
	}()

	// Sleep 200ms into the token wait, then trigger FloodWait on DC 2 for 500ms
	time.Sleep(200 * time.Millisecond)
	gate.TriggerFloodWait(2, 500*time.Millisecond)

	// Worker must NOT return before the 500ms cooldown + jitter expires!
	err = <-doneCh
	require.NoError(t, err)

	elapsed := time.Since(start)
	// Must have waited for token (~1s) + cooldown guarantee
	assert.GreaterOrEqual(t, elapsed, 700*time.Millisecond)
	assert.False(t, gate.IsDCCooledDown(2))
}

func TestFloodGate_DataAndControlSemaphores(t *testing.T) {
	g := NewFloodGate(100.0, 40)
	ctx := context.Background()

	// 1. Acquire all 40 data slots
	for i := 0; i < MaxDataInFlight; i++ {
		err := g.AcquireDataSlot(ctx)
		require.NoError(t, err)
	}
	assert.Equal(t, int64(MaxDataInFlight), g.DataInFlight())

	// 41st slot must block and time out
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := g.AcquireDataSlot(timeoutCtx)
	assert.Error(t, err)

	// Release 1 slot
	g.ReleaseDataSlot()
	assert.Equal(t, int64(MaxDataInFlight-1), g.DataInFlight())

	// Now can acquire 1 slot again
	err = g.AcquireDataSlot(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(MaxDataInFlight), g.DataInFlight())

	for i := 0; i < MaxDataInFlight; i++ {
		g.ReleaseDataSlot()
	}
	assert.Equal(t, int64(0), g.DataInFlight())

	// 2. Acquire all 4 control slots
	for i := 0; i < MaxControlInFlight; i++ {
		err := g.AcquireControlSlot(ctx)
		require.NoError(t, err)
	}
	assert.Equal(t, int64(MaxControlInFlight), g.ControlInFlight())

	// 5th control slot must block and time out
	timeoutCtx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	err = g.AcquireControlSlot(timeoutCtx2)
	assert.Error(t, err)

	for i := 0; i < MaxControlInFlight; i++ {
		g.ReleaseControlSlot()
	}
	assert.Equal(t, int64(0), g.ControlInFlight())
}

func TestFloodGate_TokenPassedBypass(t *testing.T) {
	g := NewFloodGate(1.0, 1) // rate 1 req/s, burst 1

	// 1. First Wait consumes the token
	err := g.Wait(context.Background(), 2)
	require.NoError(t, err)

	// 2. Normal Wait without token will block
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = g.Wait(ctx, 2)
	assert.Error(t, err) // timed out

	// 3. Middleware with WithTokenPassed skips Wait
	ctxPassed := WithTokenPassed(context.Background())
	mw := g.Middleware(2)
	invoked := false
	invoker := mw.Handle(fakeInvokerFunc(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
		invoked = true
		return nil
	}))

	err = invoker(ctxPassed, nil, nil)
	require.NoError(t, err)
	assert.True(t, invoked)
}

func TestFloodGate_TransportErrorAdaptiveBackoff(t *testing.T) {
	g := NewFloodGate(100.0, 40)
	assert.Equal(t, 100.0, g.CurrentRate())

	// Trigger transport error (e.g. broken pipe)
	g.TriggerTransportError(errors.New("write: broken pipe"))
	assert.Equal(t, 85.0, g.CurrentRate()) // 100 * 0.85 = 85.0

	// Debounce check: another error within 500ms should not drop rate further
	g.TriggerTransportError(errors.New("i/o timeout"))
	assert.Equal(t, 85.0, g.CurrentRate())
}

func TestFloodGate_MiddlewareRetryEnforcesCooldown(t *testing.T) {
	g := NewFloodGate(100.0, 40)
	mw := g.Middleware(3)

	attempts := 0
	invoker := mw.Handle(fakeInvokerFunc(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
		attempts++
		if attempts == 1 {
			// First attempt fails with FLOOD_WAIT_1 (1 second)
			return tgerr.New(420, "FLOOD_WAIT_1")
		}
		return nil
	}))

	// Pass context with WithTokenPassed
	ctxPassed := WithTokenPassed(context.Background())

	// Run invoker with 100ms timeout context - should time out during second attempt waiting for 1s cooldown
	timeoutCtx, cancel := context.WithTimeout(ctxPassed, 100*time.Millisecond)
	defer cancel()

	err := invoker(timeoutCtx, nil, nil)
	assert.Error(t, err) // Context canceled while waiting for DC 3 cooldown
	assert.Equal(t, 1, attempts)
	assert.True(t, g.IsDCCooledDown(3))
}

type fakeInvokerFunc func(ctx context.Context, in bin.Encoder, out bin.Decoder) error

func (f fakeInvokerFunc) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	return f(ctx, in, out)
}
