package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Hittlert/TGX/core/util/tutil"
)

func TestOptionsDefaultsAndValidation(t *testing.T) {
	opts := (Options{}).withDefaults()
	if opts.Listen != "0.0.0.0:18080" || opts.OutputDir != "/app/downloads" || opts.TempDir != "/app/temp/tdl" {
		t.Fatalf("unexpected path defaults: %#v", opts)
	}
	if opts.FileConcurrency != 5 || opts.Threads != 48 || opts.PoolSize != 48 {
		t.Fatalf("unexpected transfer defaults: %#v", opts)
	}
	if opts.QueueCapacity != 1000 || opts.TerminalLimit != 2000 || !opts.StartPaused {
		t.Fatalf("unexpected safety defaults: %#v", opts)
	}
	if opts.ReconnectTimeout != 0 {
		t.Fatalf("reconnect should be unbounded, got %s", opts.ReconnectTimeout)
	}
	if opts.PeerSyncTimeout != 3*time.Minute {
		t.Fatalf("peer sync timeout=%s, want 3m", opts.PeerSyncTimeout)
	}
	if err := opts.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestOptionsRejectInvalidRuntimeLimits(t *testing.T) {
	base := (Options{}).withDefaults()
	tests := []Options{
		func() Options { value := base; value.Listen = "bad"; return value }(),
		func() Options { value := base; value.OutputDir = "relative"; return value }(),
		func() Options { value := base; value.TempDir = "relative"; return value }(),
		func() Options { value := base; value.FileConcurrency = 0; return value }(),
		func() Options { value := base; value.Threads = 65; return value }(),
		func() Options { value := base; value.PoolSize = 0; return value }(),
		func() Options { value := base; value.QueueCapacity = 0; return value }(),
		func() Options { value := base; value.TerminalLimit = 0; return value }(),
		func() Options { value := base; value.ReconnectTimeout = -time.Second; return value }(),
		func() Options { value := base; value.PeerSyncTimeout = 0; return value }(),
	}
	for _, opts := range tests {
		if err := opts.validate(); err == nil {
			t.Fatalf("invalid options accepted: %#v", opts)
		}
	}
}

func TestInitialPeerSyncStartsWithoutBlockingCaller(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	result := startInitialPeerSync(context.Background(), time.Minute, func(context.Context) error {
		close(started)
		<-release
		return nil
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background sync did not start")
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestInitialPeerSyncIsBounded(t *testing.T) {
	result := startInitialPeerSync(context.Background(), 10*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("sync error=%v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("background sync ignored its timeout")
	}
}

func TestTelegramErrorClassification(t *testing.T) {
	deleted := errors.Join(errors.New("wrapped"), tutil.ErrMessageDeleted)
	if err := classifyTelegramError(deleted, "resolve message"); !IsUnavailable(err) || ErrorClass(err) != "unavailable" {
		t.Fatalf("deleted message classification=%v", err)
	}
	network := context.DeadlineExceeded
	if err := classifyTelegramError(network, "resolve message"); IsUnavailable(err) || ErrorClass(err) != "telegram" {
		t.Fatalf("network classification=%v", err)
	}
}
