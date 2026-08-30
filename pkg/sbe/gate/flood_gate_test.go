package gate

import (
	"context"
	"testing"
	"time"
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
