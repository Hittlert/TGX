package gate

import (
	"context"
	"testing"
	"time"

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
