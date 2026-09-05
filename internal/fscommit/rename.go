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

// Syncer is an interface for types that can synchronize in-memory data to persistent storage.
type Syncer interface {
	Sync() error
}

// CopyFileSequential copies src to dst using a sequential 4MB buffer,
// computes SHA-256 simultaneously, fsyncs dst, and returns (writtenBytes, sha256Hex, error).
// On any error during copy/sync/close, writtenBytes retains the actual physical bytes transferred before failure.
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

	written, sha, copyErr := CopyStreamSequential(src, dst)
	if copyErr != nil {
		_ = os.Remove(dstPath)
		return written, "", copyErr
	}
	return written, sha, nil
}

// CopyStreamSequential copies src to dst using a sequential 4MB buffer,
// computes SHA-256 simultaneously, syncs dst if it implements Syncer, and closes dst.
// On any error, written returns the actual physical bytes transferred before failure.
func CopyStreamSequential(src io.Reader, dst io.WriteCloser) (int64, string, error) {
	h := sha256.New()
	mw := io.MultiWriter(dst, h)

	buf := make([]byte, 4*1024*1024) // 4MB buffer for optimal HDD/SSD sequential throughput
	written, copyErr := io.CopyBuffer(mw, src, buf)
	if copyErr != nil {
		_ = dst.Close()
		return written, "", fmt.Errorf("copy buffer: %w", copyErr)
	}

	if s, ok := dst.(Syncer); ok {
		if syncErr := s.Sync(); syncErr != nil {
			_ = dst.Close()
			return written, "", fmt.Errorf("fsync dst: %w", syncErr)
		}
	}

	if closeErr := dst.Close(); closeErr != nil {
		return written, "", fmt.Errorf("close dst: %w", closeErr)
	}

	return written, hex.EncodeToString(h.Sum(nil)), nil
}
