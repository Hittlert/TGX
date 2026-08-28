//go:build darwin

package atomic

import (
	"os"
	"golang.org/x/sys/unix"
)

func preallocate(file *os.File, size int64) error {
	// fstore_t structure for F_PREALLOCATE on Darwin
	// If preallocation fails, fallback to standard ftruncate
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
