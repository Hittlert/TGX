//go:build linux

package fscommit

import (
	"errors"
	"fmt"
	"os"
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
		return fmt.Errorf("open src dir %s: %w", srcDir, err)
	}
	defer unix.Close(srcDirFD)

	dstDirFD, err := unix.Open(dstDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("open dst dir %s: %w", dstDir, err)
	}
	defer unix.Close(dstDirFD)

	// Primary path: unix.Renameat2 with RENAME_NOREPLACE
	err = unix.Renameat2(srcDirFD, tempBase, dstDirFD, finalBase, unix.RENAME_NOREPLACE)
	if err == nil {
		return nil
	}

	if errors.Is(err, syscall.EEXIST) || errors.Is(err, unix.EEXIST) {
		return ErrTargetExists
	}

	// Fallback for filesystems that do not support renameat2 (e.g. older kernels/NFS/tmpfs): linkat + unlinkat
	if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) {
		linkErr := unix.Linkat(srcDirFD, tempBase, dstDirFD, finalBase, 0)
		if linkErr != nil {
			if errors.Is(linkErr, syscall.EEXIST) || errors.Is(linkErr, unix.EEXIST) {
				return ErrTargetExists
			}
			return fmt.Errorf("linkat fallback: %w", linkErr)
		}
		_ = unix.Unlinkat(srcDirFD, tempBase, 0)
		return nil
	}

	return fmt.Errorf("renameat2 failed: %w", err)
}

func syncDir(dirPath string) error {
	dirFD, err := unix.Open(dirPath, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer unix.Close(dirFD)
	return unix.Fsync(dirFD)
}

func preallocate(file *os.File, size int64) error {
	err := syscall.Fallocate(int(file.Fd()), 0, 0, size)
	if err == nil {
		return nil
	}
	return file.Truncate(size)
}
