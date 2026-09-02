package spool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStore implements the Store interface using portable filesystem files on SSD or HDD.
type FileStore struct {
	mu      sync.RWMutex
	baseDir string
	capMgr  *CapacityManager
	items   map[string]*SpoolItem // key.String() -> SpoolItem
	files   map[string]*os.File   // key.String() -> open write file handle (closed on MarkReady)
	closed  bool
}

// NewFileStore creates a new FileStore rooted in baseDir with maximum capacity maxBytes.
func NewFileStore(baseDir string, maxBytes int64) (*FileStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create spool base directory: %w", err)
	}

	return &FileStore{
		baseDir: baseDir,
		capMgr:  NewCapacityManager(maxBytes),
		items:   make(map[string]*SpoolItem),
		files:   make(map[string]*os.File),
	}, nil
}

func (s *FileStore) Mode() string {
	return "disk"
}

func (s *FileStore) Reserve(ctx context.Context, bytes int64) error {
	return s.capMgr.Reserve(ctx, bytes)
}

func (s *FileStore) ReleaseReservation(bytes int64) {
	s.capMgr.ReleaseReservation(bytes)
}

func (s *FileStore) segmentPath(key SegmentKey) string {
	hash := sha256.Sum256([]byte(key.TaskID))
	taskDirName := hex.EncodeToString(hash[:8])
	return filepath.Join(s.baseDir, taskDirName, key.Gen, key.SegmentID()+".seg")
}

func (s *FileStore) CreateSegment(key SegmentKey) (*SpoolItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrSpoolClosed
	}

	keyStr := key.String()
	if item, exists := s.items[keyStr]; exists {
		return item, nil
	}

	segPath := s.segmentPath(key)
	if err := os.MkdirAll(filepath.Dir(segPath), 0o755); err != nil {
		return nil, fmt.Errorf("create segment parent dir: %w", err)
	}

	file, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	item := &SpoolItem{
		Key:          key,
		ExpectedSize: key.Length,
		Ranges:       NewRangeSet(),
		State:        StateReceiving,
		Dirty:        true,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	s.items[keyStr] = item
	s.files[keyStr] = file

	return item, nil
}

func (s *FileStore) WriteAt(key SegmentKey, relOffset int64, data []byte) (int, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrSpoolClosed
	}
	keyStr := key.String()
	item, ok := s.items[keyStr]
	file, fileOk := s.files[keyStr]
	s.mu.Unlock()

	if !ok {
		return 0, ErrSegmentNotFound
	}

	dataLen := int64(len(data))
	if relOffset < 0 || relOffset+dataLen > key.Length {
		return 0, ErrOffsetOutOfBounds
	}

	var n int
	var err error
	if fileOk && file != nil {
		n, err = file.WriteAt(data, relOffset)
	} else {
		// Open on-demand if write handle was closed
		f, openErr := os.OpenFile(s.segmentPath(key), os.O_WRONLY, 0o644)
		if openErr != nil {
			return 0, fmt.Errorf("reopen segment for write: %w", openErr)
		}
		defer f.Close()
		n, err = f.WriteAt(data, relOffset)
	}

	if err != nil {
		return n, fmt.Errorf("write segment file at offset %d: %w", relOffset, err)
	}

	// Update range set and capacity conversion
	item.mu.Lock()
	prevCovered := item.Ranges.TotalCovered()
	item.Ranges.Add(relOffset, relOffset+int64(n))
	newCovered := item.Ranges.TotalCovered()
	delta := newCovered - prevCovered
	item.UpdatedAt = time.Now()
	item.mu.Unlock()

	if delta > 0 {
		s.capMgr.ConvertReservationToUsed(delta)
	}

	return n, nil
}

func (s *FileStore) ReadAt(key SegmentKey, relOffset int64, p []byte) (int, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, ErrSpoolClosed
	}
	keyStr := key.String()
	file, fileOk := s.files[keyStr]
	s.mu.RUnlock()

	if fileOk && file != nil {
		return file.ReadAt(p, relOffset)
	}

	segPath := s.segmentPath(key)
	f, err := os.Open(segPath)
	if err != nil {
		return 0, ErrSegmentNotFound
	}
	defer f.Close()
	return f.ReadAt(p, relOffset)
}

func (s *FileStore) Sync(key SegmentKey) error {
	s.mu.RLock()
	keyStr := key.String()
	file, fileOk := s.files[keyStr]
	s.mu.RUnlock()

	if fileOk && file != nil {
		return file.Sync()
	}

	segPath := s.segmentPath(key)
	f, err := os.OpenFile(segPath, os.O_WRONLY, 0o644)
	if err != nil {
		return nil
	}
	defer f.Close()
	return f.Sync()
}

func (s *FileStore) MarkReady(key SegmentKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyStr := key.String()
	item, ok := s.items[keyStr]
	if !ok {
		return ErrSegmentNotFound
	}

	item.mu.Lock()
	defer item.mu.Unlock()

	if item.State == StateReady {
		return nil
	}

	if !item.Ranges.IsComplete(key.Length) {
		return ErrInvalidRange
	}

	// Ensure segment data is flushed to disk before closing handle
	if file, fileOk := s.files[keyStr]; fileOk && file != nil {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync segment file %s: %w", keyStr, err)
		}
		_ = file.Close()
		delete(s.files, keyStr)
	}

	item.State = StateReady
	item.UpdatedAt = time.Now()
	s.capMgr.MarkReady(item.Ranges.TotalCovered())

	return nil
}

func (s *FileStore) Reclaim(key SegmentKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyStr := key.String()
	item, ok := s.items[keyStr]
	if ok {
		item.mu.Lock()
		item.State = StateReclaimed
		item.UpdatedAt = time.Now()
		covered := item.Ranges.TotalCovered()
		item.mu.Unlock()

		s.capMgr.Reclaim(covered)
		delete(s.items, keyStr)
	}

	if file, fileOk := s.files[keyStr]; fileOk && file != nil {
		_ = file.Close()
		delete(s.files, keyStr)
	}

	segPath := s.segmentPath(key)
	if err := os.Remove(segPath); err != nil && !os.IsNotExist(err) {
		// Retain cleanup warning
	}

	// Clean empty parent directory
	parentDir := filepath.Dir(segPath)
	_ = os.Remove(parentDir)

	return nil
}

func (s *FileStore) GetItem(key SegmentKey) (*SpoolItem, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[key.String()]
	return item, ok
}

func (s *FileStore) ListReadySegments() []*SpoolItem {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ready []*SpoolItem
	for _, item := range s.items {
		item.mu.RLock()
		if item.State == StateReady {
			ready = append(ready, item)
		}
		item.mu.RUnlock()
	}
	return ready
}

func (s *FileStore) Recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return filepath.Walk(s.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() || filepath.Ext(path) != ".seg" {
			return nil
		}
		if info.Size() > 0 {
			s.capMgr.ConvertReservationToUsed(info.Size())
		}
		return nil
	})
}

// RestoreSegment restores a segment into the items map during crash recovery.
func (s *FileStore) RestoreSegment(key SegmentKey) (*SpoolItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyStr := key.String()
	segPath := s.segmentPath(key)
	stat, err := os.Stat(segPath)
	if err != nil {
		return nil, err
	}

	item := &SpoolItem{
		Key:          key,
		ExpectedSize: key.Length,
		Ranges:       NewRangeSet(),
		State:        StateReady,
		Dirty:        false,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	item.Ranges.Add(0, stat.Size())

	s.items[keyStr] = item
	s.capMgr.ConvertReservationToUsed(stat.Size())
	s.capMgr.MarkReady(stat.Size())

	return item, nil
}

func (s *FileStore) Metrics() SpoolMetrics {
	max, used, reserved, ready, writing, reclaimed, backpressured := s.capMgr.Snapshot()

	s.mu.RLock()
	activeSegs := len(s.items)
	s.mu.RUnlock()

	return SpoolMetrics{
		Mode:           "disk",
		MaxBytes:       max,
		UsedBytes:      used,
		ReservedBytes:  reserved,
		ReadyBytes:     ready,
		WritingBytes:   writing,
		ReclaimedBytes: reclaimed,
		Backpressured:  backpressured > 0,
		ActiveSegments: activeSegs,
	}
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	s.capMgr.Close()

	for _, file := range s.files {
		if file != nil {
			_ = file.Close()
		}
	}
	s.files = make(map[string]*os.File)
	return nil
}
