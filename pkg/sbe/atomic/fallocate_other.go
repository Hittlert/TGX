//go:build !linux && !darwin

package atomic

import (
	"os"
)

func preallocate(file *os.File, size int64) error {
	return file.Truncate(size)
}

func getDiskSpace(path string) (uint64, uint64, error) {
	// Fallback for other OSes: return 1TB dummy free space to avoid blocking
	return 1024 * 1024 * 1024 * 1024, 1024 * 1024 * 1024 * 1024, nil
}
