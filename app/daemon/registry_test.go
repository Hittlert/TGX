package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func validRequest(id string, messageID int) TaskRequest {
	return TaskRequest{
		ID: id, Peer: "-1001234567890", MessageID: messageID,
		FinalPath: "Group/2026_07/" + id + ".mp4", ExpectedSize: 20 << 20,
	}
}

func TestSubmitIsIdempotentAndQueueIsBounded(t *testing.T) {
	clock := newFakeClock()
	registry := NewRegistry(2, 100, clock.Now)

	first, created, err := registry.Submit(validRequest("first", 1))
	if err != nil || !created || first.State != StateQueued {
		t.Fatalf("first submit: task=%#v created=%v err=%v", first, created, err)
	}
	duplicate, created, err := registry.Submit(validRequest("first", 1))
	if err != nil || created || duplicate.ID != first.ID {
		t.Fatalf("duplicate submit: task=%#v created=%v err=%v", duplicate, created, err)
	}
	if _, created, err = registry.Submit(validRequest("second", 2)); err != nil || !created {
		t.Fatalf("second submit: created=%v err=%v", created, err)
	}
	if _, _, err = registry.Submit(validRequest("third", 3)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue returned %v", err)
	}
	if got := registry.Status().QueueDepth; got != 2 {
		t.Fatalf("queue depth=%d, want 2", got)
	}
}

func TestExplicitRetryRequeuesOnlyTerminalFailure(t *testing.T) {
	registry := NewRegistry(2, 100, time.Now)
	request := validRequest("stable-id", 1)
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(context.Background())
	task.Fail("network", "proxy reset", false)

	unchanged, created, err := registry.Submit(request)
	if err != nil || created || unchanged.State != StateFailed {
		t.Fatalf("plain duplicate changed failure: task=%#v created=%v err=%v", unchanged, created, err)
	}
	request.Retry = true
	retried, created, err := registry.Submit(request)
	if err != nil || !created || retried.State != StateQueued {
		t.Fatalf("explicit retry was not queued: task=%#v created=%v err=%v", retried, created, err)
	}
	if retried.Downloaded != 0 || retried.Error != "" || retried.Retry {
		t.Fatalf("retry retained stale transfer state: %#v", retried)
	}

	task, _ = registry.Next(context.Background())
	task.Succeed(request.FinalPath, false)
	request.Retry = true
	success, created, err := registry.Submit(request)
	if err != nil || created || success.State != StateSuccess {
		t.Fatalf("retry reopened success: task=%#v created=%v err=%v", success, created, err)
	}
}

func TestPauseBlocksNewStartsWithoutCancelingActiveTask(t *testing.T) {
	clock := newFakeClock()
	registry := NewRegistry(2, 100, clock.Now)
	registry.SetPaused(true)
	_, _, _ = registry.Submit(validRequest("one", 1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan *Task, 1)
	go func() {
		task, _ := registry.Next(ctx)
		result <- task
	}()

	select {
	case <-result:
		t.Fatal("paused registry started a task")
	case <-time.After(50 * time.Millisecond):
	}
	registry.SetPaused(false)
	var task *Task
	select {
	case task = <-result:
	case <-time.After(time.Second):
		t.Fatal("resume did not release queued task")
	}
	if task == nil || task.Snapshot().State != StateResolving {
		t.Fatalf("unexpected dequeued task: %#v", task)
	}

	registry.SetPaused(true)
	task.SetResolved("one.mp4", 20<<20, 5)
	task.SetDownloading()
	if task.Snapshot().State != StateDownloading {
		t.Fatal("pausing canceled an already active task")
	}
}

func TestNextReturnsOnContextCancellation(t *testing.T) {
	registry := NewRegistry(1, 100, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if task, err := registry.Next(ctx); !errors.Is(err, context.Canceled) || task != nil {
		t.Fatalf("Next returned task=%#v err=%v", task, err)
	}
}

func TestTaskLifecycleAndDeterministicStatus(t *testing.T) {
	clock := newFakeClock()
	registry := NewRegistry(3, 100, clock.Now)
	_, _, _ = registry.Submit(validRequest("b", 2))
	_, _, _ = registry.Submit(validRequest("a", 1))

	first, _ := registry.Next(context.Background())
	first.SetResolved("b.mp4", 20<<20, 4)
	first.SetDownloading()
	first.SetPublishing()
	first.Succeed("Group/b.mp4", false)

	second, _ := registry.Next(context.Background())
	second.Fail("unavailable", "message deleted", true)

	status := registry.Status()
	if status.QueueDepth != 0 || status.ActiveFiles != nil {
		t.Fatalf("terminal tasks remained active: %#v", status)
	}
	if status.LastError != "message deleted" {
		t.Fatalf("last error=%q", status.LastError)
	}
	list := registry.Tasks()
	if len(list) != 2 || list[0].ID != "a" || list[1].ID != "b" {
		t.Fatalf("task snapshots are not deterministic: %#v", list)
	}
	if list[0].State != StateUnavailable || list[1].State != StateSuccess {
		t.Fatalf("terminal states wrong: %#v", list)
	}
}

func TestTerminalRetentionIsBounded(t *testing.T) {
	clock := newFakeClock()
	registry := NewRegistry(4, 2, clock.Now)
	for index, id := range []string{"one", "two", "three"} {
		_, _, _ = registry.Submit(validRequest(id, index+1))
		task, _ := registry.Next(context.Background())
		task.Fail("network", id, false)
		clock.Advance(time.Second)
	}
	if _, ok := registry.Task("one"); ok {
		t.Fatal("oldest terminal task was not pruned")
	}
	if got := len(registry.Tasks()); got != 2 {
		t.Fatalf("retained %d terminal tasks, want 2", got)
	}
}

func TestEffectiveRangesAndStaleSpeed(t *testing.T) {
	clock := newFakeClock()
	registry := NewRegistry(1, 100, clock.Now)
	_, _, _ = registry.Submit(validRequest("large", 1))
	task, _ := registry.Next(context.Background())
	task.SetResolved("large.mp4", 20<<20, 5)
	task.SetDownloading()

	if got := task.RecordWrite(0, 5<<20); got != 5<<20 {
		t.Fatalf("first write counted %d", got)
	}
	if got := task.RecordWrite(2<<20, 5<<20); got != 2<<20 {
		t.Fatalf("overlap counted %d new bytes", got)
	}
	if got := task.RecordWrite(0, 5<<20); got != 0 {
		t.Fatalf("retry inflated effective bytes by %d", got)
	}
	if got := task.RecordWrite(10<<20, 5<<20); got != 5<<20 {
		t.Fatalf("out-of-order range counted %d", got)
	}
	clock.Advance(2 * time.Second)

	status := registry.Status()
	if len(status.ActiveFiles) != 1 {
		t.Fatalf("active file missing: %#v", status)
	}
	file := status.ActiveFiles[0]
	if file.Downloaded != 12<<20 || file.Progress != 60 {
		t.Fatalf("unexpected progress: %#v", file)
	}
	if status.Rolling5sBPS != 6<<20 || file.Rolling5sBPS != 6<<20 {
		t.Fatalf("global/file rate mismatch: %#v", status)
	}

	clock.Advance(3 * time.Second)
	status = registry.Status()
	if status.Rolling5sBPS != 0 || status.ActiveFiles[0].Rolling5sBPS != 0 {
		t.Fatalf("stalled task reports fake speed: %#v", status)
	}
}

func TestConcurrentWritesAndSnapshots(t *testing.T) {
	clock := newFakeClock()
	registry := NewRegistry(1, 100, clock.Now)
	request := validRequest("concurrent", 1)
	request.ExpectedSize = 1 << 20
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(context.Background())
	task.SetResolved("concurrent.bin", 1<<20, 2)
	task.SetDownloading()

	var wg sync.WaitGroup
	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			task.RecordWrite(int64(index*(1<<14)), 1<<14)
			_ = registry.Status()
		}(index)
	}
	wg.Wait()
	if got := registry.Status().ActiveFiles[0].Downloaded; got != 1<<20 {
		t.Fatalf("concurrent coverage=%d, want %d", got, 1<<20)
	}
}

func TestSubmitValidation(t *testing.T) {
	registry := NewRegistry(4, 100, time.Now)
	tests := []TaskRequest{
		{},
		{ID: "id", Peer: "peer", MessageID: 1, FinalPath: "../escape.bin", ExpectedSize: 1},
		{ID: "id", Peer: "peer", MessageID: 1, FinalPath: "/absolute.bin", ExpectedSize: 1},
		{ID: "id", Peer: "peer", MessageID: 0, FinalPath: "ok.bin", ExpectedSize: 1},
		{ID: "id", Peer: "", MessageID: 1, FinalPath: "ok.bin", ExpectedSize: 1},
		{ID: "id", Peer: "peer", MessageID: 1, FinalPath: "ok.bin", ExpectedSize: -1},
	}
	for _, request := range tests {
		if _, _, err := registry.Submit(request); err == nil {
			t.Fatalf("invalid request accepted: %#v", request)
		}
	}
}

func TestRecordProgressAndRecordWriteSeparation(t *testing.T) {
	clock := newFakeClock()
	registry := NewRegistry(1, 100, clock.Now)
	_, _, _ = registry.Submit(validRequest("stream-file", 1))
	task, _ := registry.Next(context.Background())
	task.SetResolved("stream-file.mp4", 10<<20, 5)
	task.SetDownloading()

	// 1. Network chunks arrive
	task.RecordProgress(2 << 20) // 2MB over network
	task.RecordProgress(5 << 20) // 5MB over network
	clock.Advance(1 * time.Second)

	// 2. Disk Writer writes chunks
	task.RecordWrite(0, 2<<20)     // write first 2MB
	task.RecordWrite(2<<20, 3<<20) // write next 3MB

	snap := registry.Status().ActiveFiles[0]
	if snap.NetDownloaded != 5<<20 {
		t.Fatalf("NetDownloaded=%d, want %d", snap.NetDownloaded, 5<<20)
	}
	if snap.Downloaded != 5<<20 {
		t.Fatalf("Downloaded=%d, want %d", snap.Downloaded, 5<<20)
	}

	// 5MB in 1s -> 5 MB/s (no double-counting from RecordWrite!)
	if snap.Rolling5sBPS != 5<<20 {
		t.Fatalf("Rolling5sBPS=%d, want %d (double counting detected!)", snap.Rolling5sBPS, 5<<20)
	}
}

func TestNormalizePeer(t *testing.T) {
	for input, want := range map[string]string{
		"-1001234567890": "-1001234567890",
		"-12345":         "-12345",
		"@username":      "username",
		"username":       "username",
		" 123 ":          "123",
	} {
		if got := normalizePeer(input); got != want {
			t.Fatalf("normalizePeer(%q)=%q, want %q", input, got, want)
		}
	}
}

