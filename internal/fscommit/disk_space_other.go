//go:build !linux && !darwin && !windows

package fscommit

import (
	"errors"
)

func getDiskSpace(path string) (uint64, uint64, error) {
	return 0, 0, errors.New("getDiskSpace: unsupported platform")
}
