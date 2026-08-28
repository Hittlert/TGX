package atomic

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreallocate(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "prealloc.bin")

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer file.Close()

	size := int64(1024 * 1024 * 5) // 5MB
	if err := Preallocate(file, size); err != nil {
		t.Fatalf("Preallocate failed: %v", err)
	}

	stat, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if stat.Size() != size {
		t.Fatalf("expected size %d, got %d", size, stat.Size())
	}
}

func TestGetDiskSpace(t *testing.T) {
	tempDir := t.TempDir()
	free, total, err := GetDiskSpace(tempDir)
	if err != nil {
		t.Fatalf("GetDiskSpace failed: %v", err)
	}

	if total == 0 {
		t.Fatalf("expected total > 0, got %d", total)
	}
	if free == 0 {
		t.Fatalf("expected free > 0, got %d", free)
	}
}
