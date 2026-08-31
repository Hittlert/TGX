package bucket

import (
	"fmt"
	"path/filepath"
)

// ObjectKey uniquely identifies an immutable chunk object in the buffer.
type ObjectKey struct {
	TaskID           string // e.g. "chatID:messageID"
	Gen              string // Task generation ID to distinguish retries
	Offset           int64  // Byte offset within target file
	Length           int64  // Object length in bytes
	ExpectedFileSize int64  // Total expected size of the final file
	Checksum         uint32 // CRC32 checksum for fast corruption detection
}

func (k ObjectKey) String() string {
	return fmt.Sprintf("%s/%s/%d-%d-%08x", k.TaskID, k.Gen, k.Offset, k.Length, k.Checksum)
}

// RelPath returns the canonical relative path for this object on disk:
// <taskID-hash-prefix>/<taskID-hash>/<gen>/<offset>-<length>-<checksum>.ready
func (k ObjectKey) RelPath(ext string) string {
	chunkGroup := k.Offset / (64 * 1024 * 1024) // 64MB chunk group folder to prevent large single directory
	return filepath.Join(
		k.TaskID,
		k.Gen,
		fmt.Sprintf("group_%d", chunkGroup),
		fmt.Sprintf("%d-%d-%08x%s", k.Offset, k.Length, k.Checksum, ext),
	)
}

// BufferObject represents an in-memory or on-disk chunk object ready to be consumed by TargetWriter.
type BufferObject struct {
	Key      ObjectKey
	Data     []byte // Populated if in memory
	DiskPath string // Populated if on SSD disk
}
