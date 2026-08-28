package daemon

import "errors"

type TaskError struct {
	class       string
	unavailable bool
	err         error
}

func NewTaskError(class string, unavailable bool, err error) error {
	if err == nil {
		err = errors.New(class)
	}
	return &TaskError{class: class, unavailable: unavailable, err: err}
}

func (e *TaskError) Error() string { return e.err.Error() }
func (e *TaskError) Unwrap() error { return e.err }

func ErrorClass(err error) string {
	var taskError *TaskError
	if errors.As(err, &taskError) && taskError.class != "" {
		return taskError.class
	}
	return "download"
}

func IsUnavailable(err error) bool {
	var taskError *TaskError
	return errors.As(err, &taskError) && taskError.unavailable
}
