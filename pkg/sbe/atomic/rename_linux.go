//go:build linux

package atomic

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

func commitFile(tempPath, finalPath string) error {
	dir := filepath.Dir(finalPath)
	tempBase := filepath.Base(tempPath)
	finalBase := filepath.Base(finalPath)

	dirFD, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("failed to open parent directory %s: %w", dir, err)
	}
	defer unix.Close(dirFD)

	// 1. Primary path: unix.Renameat2 with RENAME_NOREPLACE
	err = unix.Renameat2(dirFD, tempBase, dirFD, finalBase, unix.RENAME_NOREPLACE)
	if err == nil {
		return syncDirFD(dirFD)
	}

	// If target already exists, return explicit error
	if errors.Is(err, syscall.EEXIST) || errors.Is(err, unix.EEXIST) {
		return ErrTargetExists
	}

	// 2. Safe Fallback: linkat + unlinkat if Renameat2 is not supported on older kernel / FS
	if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) {
		linkErr := unix.Linkat(dirFD, tempBase, dirFD, finalBase, 0)
		if linkErr != nil {
			if errors.Is(linkErr, syscall.EEXIST) || errors.Is(linkErr, unix.EEXIST) {
				return ErrTargetExists
			}
			return fmt.Errorf("linkat fallback failed: %w", linkErr)
		}

		_ = unix.Unlinkat(dirFD, tempBase, 0)
		return syncDirFD(dirFD)
	}

	return fmt.Errorf("renameat2 failed: %w", err)
}

func syncDir(dirPath string) error {
	dirFD, err := unix.Open(dirPath, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(dirFD)
	return syncDirFD(dirFD)
}

func syncDirFD(dirFD int) error {
	return unix.Fsync(dirFD)
}
