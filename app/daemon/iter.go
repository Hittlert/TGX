package daemon

import (
	"context"
	"errors"
	"strings"
	"sync"

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
	registry  *Registry
	resolver  Resolver
	current   taskElement
	err       error
	readyChan chan taskElement
	errChan   chan error
	once      sync.Once
}

func newTaskIter(registry *Registry, resolver Resolver) *taskIter {
	return &taskIter{
		registry:  registry,
		resolver:  resolver,
		readyChan: make(chan taskElement, 64),
		errChan:   make(chan error, 1),
	}
}

func (i *taskIter) startWorkers(ctx context.Context) {
	numWorkers := 16
	for w := 0; w < numWorkers; w++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				task, err := i.registry.Next(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					select {
					case i.errChan <- err:
					default:
					}
					return
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

				select {
				case <-ctx.Done():
					return
				case i.readyChan <- element:
				}
			}
		}()
	}
}

func (i *taskIter) Next(ctx context.Context) bool {
	i.once.Do(func() {
		i.startWorkers(ctx)
	})

	select {
	case <-ctx.Done():
		i.err = ctx.Err()
		return false
	case err := <-i.errChan:
		i.err = err
		return false
	case elem, ok := <-i.readyChan:
		if !ok {
			return false
		}
		i.current = elem
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
		unavailable := false
		errStr := strings.ToLower(transferErr.Error())
		if errors.Is(transferErr, context.Canceled) {
			class = "canceled"
		} else if strings.Contains(errStr, "connection failed") || strings.Contains(errStr, "file_reference") || strings.Contains(errStr, "fileref") || strings.Contains(errStr, "unavailable") {
			class = "unavailable"
			unavailable = true
		}
		task.Fail(class, transferErr.Error(), unavailable)
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
