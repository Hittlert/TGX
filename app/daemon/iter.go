package daemon

import (
	"context"
	"errors"

	"github.com/Hittlert/TGX/core/downloader"
)

type PublishResult struct {
	Path          string `json:"path"`
	SHA256        string `json:"sha256,omitempty"`
	AlreadyExists bool   `json:"already_exists,omitempty"`
	absolutePath  string
}

type taskElement interface {
	downloader.Elem
	Task() *Task
	AlreadyComplete() (string, bool)
	Publish() (PublishResult, error)
	Abort() error
}

type Resolver interface {
	Resolve(context.Context, *Task) (taskElement, error)
}

type taskIter struct {
	registry *Registry
	resolver Resolver
	current  taskElement
	err      error
}

func newTaskIter(registry *Registry, resolver Resolver) *taskIter {
	return &taskIter{registry: registry, resolver: resolver}
}

func (i *taskIter) Next(ctx context.Context) bool {
	for {
		task, err := i.registry.Next(ctx)
		if err != nil {
			i.err = err
			return false
		}
		element, err := i.resolver.Resolve(ctx, task)
		if err != nil {
			task.Fail(ErrorClass(err), err.Error(), IsUnavailable(err))
			continue
		}
		if path, ok := element.AlreadyComplete(); ok {
			task.Succeed(path, true)
			continue
		}
		i.current = element
		return true
	}
}

func (i *taskIter) Value() downloader.Elem { return i.current }
func (i *taskIter) Err() error             { return i.err }

type taskProgress struct{}

func newTaskProgress() *taskProgress { return &taskProgress{} }

func (p *taskProgress) OnAdd(element downloader.Elem) {
	element.(taskElement).Task().SetDownloading()
}

func (p *taskProgress) OnDownload(_ downloader.Elem, _ downloader.ProgressState) {}

func (p *taskProgress) OnDone(element downloader.Elem, transferErr error) {
	taskElement := element.(taskElement)
	task := taskElement.Task()
	if transferErr != nil {
		_ = taskElement.Abort()
		class := "transport"
		if errors.Is(transferErr, context.Canceled) {
			class = "canceled"
		}
		task.Fail(class, transferErr.Error(), false)
		return
	}
	task.SetPublishing()
	result, err := taskElement.Publish()
	if err != nil {
		task.Fail("publish", err.Error(), false)
		return
	}
	task.SucceedResult(result)
}
