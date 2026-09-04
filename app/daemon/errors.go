package daemon

import (
	"errors"
	"fmt"
)

// TaskError represents an actionable, typed error in the download and archive lifecycle.
type TaskError struct {
	Stage       string `json:"stage"` // "resolve", "admission", "transfer", "commit", "archive"
	Op          string `json:"op"`    // "get_message", "reserve_space", "download", "fsync", "commit_sibling", "copy"
	Class       string `json:"class"` // "unavailable", "network", "io", "conflict", "timeout"
	Unavailable bool   `json:"unavailable"`
	Retryable   bool   `json:"retryable"`
	Cause       error  `json:"-"`
}

func NewTaskError(stage, op, class string, unavailable, retryable bool, cause error) *TaskError {
	if cause == nil {
		cause = errors.New(class)
	}
	return &TaskError{
		Stage:       stage,
		Op:          op,
		Class:       class,
		Unavailable: unavailable,
		Retryable:   retryable,
		Cause:       cause,
	}
}

// NewSimpleTaskError provides backwards compatibility for callers passing class, unavailable, err.
func NewSimpleTaskError(class string, unavailable bool, err error) error {
	return NewTaskError("download", "", class, unavailable, !unavailable, err)
}

func (e *TaskError) Error() string {
	if e == nil {
		return ""
	}
	if e.Op != "" {
		return fmt.Sprintf("[%s:%s] %s: %v", e.Stage, e.Op, e.Class, e.Cause)
	}
	if e.Stage != "" {
		return fmt.Sprintf("[%s] %s: %v", e.Stage, e.Class, e.Cause)
	}
	return fmt.Sprintf("%s: %v", e.Class, e.Cause)
}

func (e *TaskError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *TaskError) ClassName() string {
	if e == nil {
		return ""
	}
	return e.Class
}

func (e *TaskError) IsUnavailable() bool {
	if e == nil {
		return false
	}
	return e.Unavailable
}

func (e *TaskError) IsRetryable() bool {
	if e == nil {
		return false
	}
	return e.Retryable
}

func ErrorClass(err error) string {
	var taskError *TaskError
	if errors.As(err, &taskError) && taskError.Class != "" {
		return taskError.Class
	}
	return "download"
}

func IsUnavailable(err error) bool {
	var taskError *TaskError
	return errors.As(err, &taskError) && taskError.Unavailable
}
