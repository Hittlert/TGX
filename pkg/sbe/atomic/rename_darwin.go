//go:build darwin

package atomic

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

func commitFile(tempPath, finalPath string) error {
	dir := filepath.Dir(finalPath)

	// In macOS / APFS, link(temp, final) + unlink(temp) guarantees non-replacing behavior
	err := os.Link(tempPath, finalPath)
	if err != nil {
		if errors.Is(err, syscall.EEXIST) || os.IsExist(err) {
			return ErrTargetExists
		}
		return fmt.Errorf("macOS atomic link failed: %w", err)
	}

	_ = os.Remove(tempPath)
	return syncDir(dir)
}

func syncDir(dirPath string) error {
	dirFD, err := unix.Open(dirPath, unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(dirFD)
	return unix.Fsync(dirFD)
}
