package atomic

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

var (
	ErrTargetExists    = os.ErrExist
	ErrRenameNotAtomic = errors.New("atomic non-replacing rename unsupported on target filesystem")
)

// CommitFile atomically moves tempPath to finalPath without replacing any existing finalPath,
// and fsyncs the parent directory. If crossing filesystem boundaries (EXDEV), it streams
// sequentially with a 4MB buffer and fsyncs the target.
func CommitFile(tempPath, finalPath string) error {
	// If target already exists, do not overwrite
	if _, err := os.Stat(finalPath); err == nil {
		return ErrTargetExists
	}

	err := commitFile(tempPath, finalPath)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrTargetExists) {
		return ErrTargetExists
	}

	// If failed due to cross-device (EXDEV) or different directory not supported by renameat2
	var sysErr syscall.Errno
	if errors.As(err, &sysErr) && sysErr == syscall.EXDEV || errors.Is(err, syscall.EXDEV) || errors.Is(err, os.ErrInvalid) {
		return copyAndRemove(tempPath, finalPath)
	}

	// Fallback attempt with copyAndRemove for cross-filesystem / tmpfs moves
	if fallbackErr := copyAndRemove(tempPath, finalPath); fallbackErr == nil {
		return nil
	}

	return err
}

func copyAndRemove(tempPath, finalPath string) error {
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}

	if _, err := os.Stat(finalPath); err == nil {
		return ErrTargetExists
	}

	src, err := os.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open src %s: %w", tempPath, err)
	}
	defer src.Close()

	srcStat, err := src.Stat()
	if err != nil {
		return fmt.Errorf("stat src: %w", err)
	}

	dstTmp := finalPath + ".moving"
	// Truncate/create temp dst (safe because finalPath has not been created yet)
	dst, err := os.OpenFile(dstTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open dst tmp: %w", err)
	}

	buf := make([]byte, 4*1024*1024) // 4MB sequential buffer
	written, err := io.CopyBuffer(dst, src, buf)
	if err != nil {
		_ = dst.Close()
		_ = os.Remove(dstTmp)
		return fmt.Errorf("copy buffer: %w", err)
	}
	if written != srcStat.Size() {
		_ = dst.Close()
		_ = os.Remove(dstTmp)
		return fmt.Errorf("short copy: wrote %d of %d bytes", written, srcStat.Size())
	}

	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		_ = os.Remove(dstTmp)
		return fmt.Errorf("sync dst: %w", err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(dstTmp)
		return fmt.Errorf("close dst: %w", err)
	}
	_ = src.Close()

	// Atomic non-replacing commit from dstTmp to finalPath (same directory)
	if err := commitFile(dstTmp, finalPath); err != nil {
		_ = os.Remove(dstTmp)
		return err
	}

	_ = os.Remove(tempPath)
	return SyncDir(dir)
}

// SyncDir fsyncs the parent directory descriptor to guarantee directory entry persistence.
func SyncDir(dirPath string) error {
	return syncDir(dirPath)
}
