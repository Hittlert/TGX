package atomic

import (
	"errors"
	"os"
)

var (
	ErrTargetExists     = os.ErrExist
	ErrRenameNotAtomic  = errors.New("atomic non-replacing rename unsupported on target filesystem")
)

// CommitFile atomically moves tempPath to finalPath without replacing any existing finalPath,
// and fsyncs the parent directory.
func CommitFile(tempPath, finalPath string) error {
	return commitFile(tempPath, finalPath)
}

// SyncDir fsyncs the parent directory descriptor to guarantee directory entry persistence.
func SyncDir(dirPath string) error {
	return syncDir(dirPath)
}
