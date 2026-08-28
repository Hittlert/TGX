package atomic

import (
	"os"
)

// Preallocate physical disk space for the file to prevent file fragmentation.
func Preallocate(file *os.File, size int64) error {
	if size <= 0 || file == nil {
		return nil
	}
	return preallocate(file, size)
}

// GetDiskSpace returns (freeBytes, totalBytes, err) for the given path.
func GetDiskSpace(path string) (uint64, uint64, error) {
	return getDiskSpace(path)
}
