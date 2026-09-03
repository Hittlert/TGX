//go:build darwin

package fscommit

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func commitFile(tempPath, finalPath string) error {
	// In macOS / APFS, link(temp, final) + unlink(temp) guarantees non-replacing behavior
	err := os.Link(tempPath, finalPath)
	if err != nil {
		if errors.Is(err, syscall.EEXIST) || os.IsExist(err) {
			return ErrTargetExists
		}
		return fmt.Errorf("darwin atomic link: %w", err)
	}

	_ = os.Remove(tempPath)
	return nil
}

func syncDir(dirPath string) error {
	dirFD, err := unix.Open(dirPath, unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(dirFD)
	return unix.Fsync(dirFD)
}

func preallocate(file *os.File, size int64) error {
	return file.Truncate(size)
}
