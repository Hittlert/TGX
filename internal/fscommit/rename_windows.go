//go:build windows

package fscommit

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func commitFile(tempPath, finalPath string) error {
	fromPtr, err := windows.UTF16PtrFromString(tempPath)
	if err != nil {
		return err
	}
	toPtr, err := windows.UTF16PtrFromString(finalPath)
	if err != nil {
		return err
	}

	// MoveFileEx with MOVEFILE_WRITE_THROUGH and WITHOUT MOVEFILE_REPLACE_EXISTING
	err = windows.MoveFileEx(fromPtr, toPtr, windows.MOVEFILE_WRITE_THROUGH)
	if err != nil {
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) || errors.Is(err, windows.ERROR_FILE_EXISTS) || os.IsExist(err) {
			return ErrTargetExists
		}
		return fmt.Errorf("windows MoveFileEx: %w", err)
	}
	return nil
}

func syncDir(dirPath string) error {
	// Directory fsync is not required on NTFS / Windows
	return nil
}

func preallocate(file *os.File, size int64) error {
	return file.Truncate(size)
}
