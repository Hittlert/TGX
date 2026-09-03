//go:build !linux && !darwin && !windows

package fscommit

import (
	"errors"
	"os"
)

func commitFile(tempPath, finalPath string) error {
	if _, err := os.Lstat(finalPath); err == nil {
		return ErrTargetExists
	}
	return os.Rename(tempPath, finalPath)
}

func syncDir(dirPath string) error {
	return nil
}

func preallocate(file *os.File, size int64) error {
	return file.Truncate(size)
}
