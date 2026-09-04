package fscommit

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var (
	ErrTargetExists    = os.ErrExist
	ErrNotSibling      = errors.New("partPath and finalPath must be siblings in the same directory")
	ErrRenameNotAtomic = errors.New("atomic non-replacing rename unsupported on target filesystem")
)

// CommitSiblingPart atomically commits a .part file to finalPath within the same parent directory.
// It enforces the sibling constraint, refuses to overwrite an existing finalPath,
// and fsyncs the parent directory.
func CommitSiblingPart(partPath, finalPath string) error {
	partDir := filepath.Clean(filepath.Dir(partPath))
	finalDir := filepath.Clean(filepath.Dir(finalPath))
	if partDir != finalDir {
		return fmt.Errorf("%w: %s vs %s", ErrNotSibling, partDir, finalDir)
	}

	if _, err := os.Lstat(finalPath); err == nil {
		return ErrTargetExists
	}

	if err := commitFile(partPath, finalPath); err != nil {
		return err
	}

	return syncDir(finalDir)
}

// FsyncDir flushes directory metadata to disk where supported by the platform.
func FsyncDir(dirPath string) error {
	return syncDir(dirPath)
}

// Preallocate reserves contiguous disk blocks for a file up to size.
func Preallocate(file *os.File, size int64) error {
	if size <= 0 || file == nil {
		return nil
	}
	return preallocate(file, size)
}

// CopyFileSequential copies src to dst using a sequential 4MB buffer,
// computes SHA-256 simultaneously, fsyncs dst, and returns (writtenBytes, sha256Hex, error).
func CopyFileSequential(srcPath, dstPath string) (int64, string, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return 0, "", fmt.Errorf("open src: %w", err)
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return 0, "", fmt.Errorf("create dst dir: %w", err)
	}

	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, "", fmt.Errorf("open dst: %w", err)
	}

	h := sha256.New()
	mw := io.MultiWriter(dst, h)

	buf := make([]byte, 4*1024*1024) // 4MB buffer for optimal HDD/SSD sequential throughput
	written, copyErr := io.CopyBuffer(mw, src, buf)
	if copyErr != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)
		return 0, "", fmt.Errorf("copy buffer: %w", copyErr)
	}

	if syncErr := dst.Sync(); syncErr != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)
		return 0, "", fmt.Errorf("fsync dst: %w", syncErr)
	}

	if closeErr := dst.Close(); closeErr != nil {
		_ = os.Remove(dstPath)
		return 0, "", fmt.Errorf("close dst: %w", closeErr)
	}

	return written, hex.EncodeToString(h.Sum(nil)), nil
}
