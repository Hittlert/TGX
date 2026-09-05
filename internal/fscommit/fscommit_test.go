package fscommit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskSpace(t *testing.T) {
	tempDir := t.TempDir()
	free, total, err := GetDiskSpace(tempDir)
	if err != nil {
		t.Fatalf("GetDiskSpace failed: %v", err)
	}
	if free == 0 || total == 0 {
		t.Fatalf("expected non-zero disk space, got free=%d, total=%d", free, total)
	}
	if free > total {
		t.Fatalf("free space %d cannot exceed total space %d", free, total)
	}
}

func TestCommitSiblingPart(t *testing.T) {
	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "file.mp4.part")
	finalPath := filepath.Join(tempDir, "file.mp4")

	data := []byte("hello direct ssd commit")
	if err := os.WriteFile(partPath, data, 0o644); err != nil {
		t.Fatalf("failed to write part file: %v", err)
	}

	// 1. Success commit
	if err := CommitSiblingPart(partPath, finalPath); err != nil {
		t.Fatalf("CommitSiblingPart failed: %v", err)
	}

	// Verify final exists with correct content
	readData, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read final file: %v", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("content mismatch: got %q, want %q", readData, data)
	}

	// Verify part no longer exists
	if _, err := os.Lstat(partPath); !os.IsNotExist(err) {
		t.Fatalf("expected part file to be removed, got err: %v", err)
	}

	// 2. Existing final should return ErrTargetExists without overwriting
	newPart := filepath.Join(tempDir, "file.mp4.part")
	if err := os.WriteFile(newPart, []byte("overwrite attempt"), 0o644); err != nil {
		t.Fatalf("failed to write new part: %v", err)
	}
	err = CommitSiblingPart(newPart, finalPath)
	if !errors.Is(err, ErrTargetExists) {
		t.Fatalf("expected ErrTargetExists, got: %v", err)
	}
	// Verify original final content remains unchanged
	readData, _ = os.ReadFile(finalPath)
	if string(readData) != string(data) {
		t.Fatalf("final file was overwritten! got %q, want %q", readData, data)
	}

	// 3. Non-sibling paths must be rejected
	otherDir := t.TempDir()
	nonSiblingFinal := filepath.Join(otherDir, "file.mp4")
	err = CommitSiblingPart(newPart, nonSiblingFinal)
	if !errors.Is(err, ErrNotSibling) {
		t.Fatalf("expected ErrNotSibling, got: %v", err)
	}
}

func TestCopyFileSequential(t *testing.T) {
	tempDir := t.TempDir()
	srcPath := filepath.Join(tempDir, "src.bin")
	dstPath := filepath.Join(tempDir, "dst.bin")

	payload := make([]byte, 5*1024*1024) // 5MB payload
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	h := sha256.Sum256(payload)
	expectedSHA := hex.EncodeToString(h[:])

	if err := os.WriteFile(srcPath, payload, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	written, sha, err := CopyFileSequential(srcPath, dstPath)
	if err != nil {
		t.Fatalf("CopyFileSequential: %v", err)
	}
	if written != int64(len(payload)) {
		t.Fatalf("written bytes: got %d, want %d", written, len(payload))
	}
	if sha != expectedSHA {
		t.Fatalf("sha mismatch: got %s, want %s", sha, expectedSHA)
	}

	readData, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if len(readData) != len(payload) {
		t.Fatalf("dst file size mismatch: got %d, want %d", len(readData), len(payload))
	}
}

func TestSSDAdmission(t *testing.T) {
	tempDir := t.TempDir()
	adm := NewSSDAdmission(tempDir, 1024*1024) // 1MB reserve

	free, _, err := GetDiskSpace(tempDir)
	if err != nil {
		t.Fatalf("get disk space: %v", err)
	}

	// Valid reservation
	release1, err := adm.Reserve("task-1", 1024*1024)
	if err != nil {
		t.Fatalf("reserve task-1: %v", err)
	}

	stats, _ := adm.Stats()
	if stats.ActiveFiles != 1 || stats.ReservedBytes != 1024*1024 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	// Idempotent release
	release1()
	release1()

	stats, _ = adm.Stats()
	if stats.ActiveFiles != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("stats after release: %+v", stats)
	}

	// Oversized reservation must fail
	_, err = adm.Reserve("task-huge", int64(free)+1)
	if !errors.Is(err, ErrInsufficientSSDSpace) {
		t.Fatalf("expected ErrInsufficientSSDSpace, got: %v", err)
	}
}

type faultyReader struct {
	data      []byte
	failAfter int
	readSoFar int
}

func (r *faultyReader) Read(p []byte) (n int, err error) {
	if r.readSoFar >= r.failAfter {
		return 0, errors.New("simulated read failure")
	}
	remaining := r.failAfter - r.readSoFar
	toRead := len(p)
	if toRead > remaining {
		toRead = remaining
	}
	copy(p, r.data[r.readSoFar:r.readSoFar+toRead])
	r.readSoFar += toRead
	if r.readSoFar >= r.failAfter {
		return toRead, errors.New("simulated read failure")
	}
	return toRead, nil
}

type nopWriteCloser struct {
	bytes.Buffer
	syncCalled  bool
	closeCalled bool
}

func (n *nopWriteCloser) Sync() error {
	n.syncCalled = true
	return nil
}

func (n *nopWriteCloser) Close() error {
	n.closeCalled = true
	return nil
}

func TestCopyStreamSequential_FailurePreservesWrittenBytes(t *testing.T) {
	payload := make([]byte, 10240)
	fr := &faultyReader{data: payload, failAfter: 4096}
	dst := &nopWriteCloser{}

	written, _, err := CopyStreamSequential(fr, dst)
	if err == nil {
		t.Fatal("expected copy error, got nil")
	}
	if written != 4096 {
		t.Fatalf("expected written bytes 4096, got %d", written)
	}
	if dst.Len() != 4096 {
		t.Fatalf("expected dst buffer len 4096, got %d", dst.Len())
	}
	if !dst.closeCalled {
		t.Fatal("dst must be closed even on error")
	}
}
