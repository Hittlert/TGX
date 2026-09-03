//go:build windows

package fscommit

import (
	"golang.org/x/sys/windows"
)

func getDiskSpace(path string) (uint64, uint64, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes uint64
	err = windows.GetDiskFreeSpaceEx(pathPtr, &freeBytesAvailable, &totalNumberOfBytes, &totalNumberOfFreeBytes)
	if err != nil {
		return 0, 0, err
	}
	return freeBytesAvailable, totalNumberOfBytes, nil
}
