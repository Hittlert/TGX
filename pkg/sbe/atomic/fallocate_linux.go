//go:build linux

package atomic

import (
	"os"
	"syscall"
	"golang.org/x/sys/unix"
)

func preallocate(file *os.File, size int64) error {
	// Mode 0 = default allocation (allocates blocks and zeroes them if needed, updates size)
	err := syscall.Fallocate(int(file.Fd()), 0, 0, size)
	if err == nil {
		return nil
	}
	// Fallback to posix_fallocate or ftruncate if fallocate is not supported by filesystem (e.g. ZFS/tmpfs)
	return file.Truncate(size)
}

func getDiskSpace(path string) (uint64, uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	free := stat.Bavail * uint64(stat.Bsize)
	total := stat.Blocks * uint64(stat.Bsize)
	return free, total, nil
}
