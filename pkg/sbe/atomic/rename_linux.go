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
	srcDir := filepath.Dir(tempPath)
	dstDir := filepath.Dir(finalPath)
	tempBase := filepath.Base(tempPath)
	finalBase := filepath.Base(finalPath)

	srcDirFD, err := unix.Open(srcDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("failed to open src directory %s: %w", srcDir, err)
	}
	defer unix.Close(srcDirFD)

	dstDirFD, err := unix.Open(dstDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("failed to open dst directory %s: %w", dstDir, err)
	}
	defer unix.Close(dstDirFD)

	// 1. Primary path: unix.Renameat2 across directories with RENAME_NOREPLACE
	err = unix.Renameat2(srcDirFD, tempBase, dstDirFD, finalBase, unix.RENAME_NOREPLACE)
	if err == nil {
		_ = syncDirFD(srcDirFD)
		return syncDirFD(dstDirFD)
	}

	// If target already exists, return explicit error
	if errors.Is(err, syscall.EEXIST) || errors.Is(err, unix.EEXIST) {
		return ErrTargetExists
	}

	// If cross-filesystem, return EXDEV explicitly to trigger streaming fallback
	if errors.Is(err, syscall.EXDEV) || errors.Is(err, unix.EXDEV) {
		return syscall.EXDEV
	}

	// 2. Safe Fallback: linkat + unlinkat if Renameat2 is not supported on older kernel / FS
	if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) {
		linkErr := unix.Linkat(srcDirFD, tempBase, dstDirFD, finalBase, 0)
		if linkErr != nil {
			if errors.Is(linkErr, syscall.EEXIST) || errors.Is(linkErr, unix.EEXIST) {
				return ErrTargetExists
			}
			if errors.Is(linkErr, syscall.EXDEV) || errors.Is(linkErr, unix.EXDEV) {
				return syscall.EXDEV
			}
			return fmt.Errorf("linkat fallback failed: %w", linkErr)
		}

		_ = unix.Unlinkat(srcDirFD, tempBase, 0)
		_ = syncDirFD(srcDirFD)
		return syncDirFD(dstDirFD)
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
