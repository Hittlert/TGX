package daemon

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/Hittlert/TGX/core/downloader"
)

type fakeDownloadFile struct {
	size int64
	dc   int
}

func (f fakeDownloadFile) Location() tg.InputFileLocationClass {
	return &tg.InputDocumentFileLocation{}
}
func (f fakeDownloadFile) Size() int64 { return f.size }
func (f fakeDownloadFile) DC() int     { return f.dc }

type fakeElement struct {
	task        *Task
	file        downloader.File
	publish     PublishResult
	publishErr  error
	aborted     bool
	alreadyPath string
}

func (e *fakeElement) File() downloader.File { return e.file }
func (e *fakeElement) To() io.WriterAt       { return writerAtDiscard{} }
func (e *fakeElement) AsTakeout() bool       { return false }
func (e *fakeElement) Task() *Task           { return e.task }
func (e *fakeElement) AlreadyComplete() (string, bool) {
	return e.alreadyPath, e.alreadyPath != ""
}
func (e *fakeElement) Publish() (PublishResult, error) { return e.publish, e.publishErr }
func (e *fakeElement) Abort() error {
	e.aborted = true
	return nil
}

type writerAtDiscard struct{}

func (writerAtDiscard) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

type fakeResolver struct {
	mu      sync.Mutex
	results []resolveResult
	byID    map[string]resolveResult
}

type resolveResult struct {
	element taskElement
	err     error
}

func (r *fakeResolver) Resolve(_ context.Context, task *Task) (taskElement, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byID != nil {
		if res, ok := r.byID[task.Request().ID]; ok {
			if res.element != nil {
				res.element.(*fakeElement).task = task
			}
			return res.element, res.err
		}
	}
	if len(r.results) == 0 {
		return nil, errors.New("no more fake results")
	}
	result := r.results[0]
	r.results = r.results[1:]
	if result.element != nil {
		result.element.(*fakeElement).task = task
	}
	return result.element, result.err
}

func TestTaskIteratorSkipsOneResolutionFailureAndContinues(t *testing.T) {
	registry := NewRegistry(4, 100, nil)
	_, _, _ = registry.Submit(validRequest("deleted", 1))
	_, _, _ = registry.Submit(validRequest("healthy", 2))
	healthy := &fakeElement{file: fakeDownloadFile{size: 100, dc: 2}}
	resolver := &fakeResolver{
		byID: map[string]resolveResult{
			"deleted": {err: NewTaskError("unavailable", true, errors.New("message deleted"))},
			"healthy": {element: healthy},
		},
	}
	iter := newTaskIter(registry, resolver)

	if !iter.Next(context.Background()) {
		t.Fatalf("iterator stopped after one bad task: %v", iter.Err())
	}
	if iter.Value() != healthy {
		t.Fatalf("iterator returned wrong element: %#v", iter.Value())
	}
	var deleted TaskSnapshot
	for attempt := 0; attempt < 100; attempt++ {
		deleted, _ = registry.Task("deleted")
		if deleted.State == StateUnavailable && deleted.ErrorClass == "unavailable" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if deleted.State != StateUnavailable || deleted.ErrorClass != "unavailable" {
		t.Fatalf("bad task not isolated: %#v", deleted)
	}
}

func TestTaskIteratorCompletesPreexistingFileWithoutDownloader(t *testing.T) {
	registry := NewRegistry(2, 100, nil)
	_, _, _ = registry.Submit(validRequest("existing", 1))
	_, _, _ = registry.Submit(validRequest("next", 2))
	existing := &fakeElement{file: fakeDownloadFile{size: 100}, alreadyPath: "Group/existing.mp4"}
	next := &fakeElement{file: fakeDownloadFile{size: 100}}
	iter := newTaskIter(registry, &fakeResolver{
		byID: map[string]resolveResult{
			"existing": {element: existing},
			"next":     {element: next},
		},
	})

	if !iter.Next(context.Background()) || iter.Value() != next {
		t.Fatalf("iterator did not advance past existing file: err=%v", iter.Err())
	}
	var snapshot TaskSnapshot
	for attempt := 0; attempt < 100; attempt++ {
		snapshot, _ = registry.Task("existing")
		if snapshot.State == StateSuccess && snapshot.AlreadyExists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if snapshot.State != StateSuccess || !snapshot.AlreadyExists {
		t.Fatalf("existing file not completed: %#v", snapshot)
	}
}

func TestProgressPublishesSuccessAndContainsFailure(t *testing.T) {
	registry := NewRegistry(2, 100, nil)
	_, _, _ = registry.Submit(validRequest("success", 1))
	successTask, _ := registry.Next(context.Background())
	success := &fakeElement{
		task: successTask, file: fakeDownloadFile{size: 100},
		publish: PublishResult{Path: "Group/success.mp4", SHA256: "abc"},
	}
	progress := newTaskProgress()
	progress.OnAdd(success)
	progress.OnDownload(success, downloader.ProgressState{Downloaded: 50, Total: 100})
	progress.OnDone(success, nil)
	snapshot, _ := registry.Task("success")
	if snapshot.State != StateSuccess || snapshot.FinalPath != "Group/success.mp4" || snapshot.SHA256 != "abc" {
		t.Fatalf("success not published: %#v", snapshot)
	}

	_, _, _ = registry.Submit(validRequest("failed", 2))
	failedTask, _ := registry.Next(context.Background())
	failed := &fakeElement{
		task: failedTask, file: fakeDownloadFile{size: 100},
		publishErr: errors.New("temporary file is short"),
	}
	progress.OnAdd(failed)
	progress.OnDone(failed, nil)
	snapshot, _ = registry.Task("failed")
	if snapshot.State != StateFailed || snapshot.ErrorClass != "publish" {
		t.Fatalf("publish failure escaped: %#v", snapshot)
	}
}

func TestProgressAbortsTransportFailure(t *testing.T) {
	registry := NewRegistry(1, 100, nil)
	_, _, _ = registry.Submit(validRequest("failed", 1))
	task, _ := registry.Next(context.Background())
	element := &fakeElement{task: task, file: fakeDownloadFile{size: 100}}
	progress := newTaskProgress()
	progress.OnAdd(element)
	progress.OnDone(element, context.Canceled)

	snapshot, _ := registry.Task("failed")
	if !element.aborted || snapshot.State != StateFailed || snapshot.ErrorClass != "canceled" {
		t.Fatalf("transport failure not contained: aborted=%v task=%#v", element.aborted, snapshot)
	}
}
