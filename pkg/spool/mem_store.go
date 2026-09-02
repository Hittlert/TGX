package spool

import (
	"context"
	"io"
	"sync"
	"time"
)

// MemoryStore implements the Store interface entirely in RAM buffers.
type MemoryStore struct {
	mu      sync.RWMutex
	capMgr  *CapacityManager
	items   map[string]*SpoolItem // key.String() -> SpoolItem
	buffers map[string][]byte     // key.String() -> dynamic in-memory segment byte buffer
	closed  bool
}

// NewMemoryStore creates an in-memory Spool store with maximum memory budget maxBytes.
func NewMemoryStore(maxBytes int64) *MemoryStore {
	return &MemoryStore{
		capMgr:  NewCapacityManager(maxBytes),
		items:   make(map[string]*SpoolItem),
		buffers: make(map[string][]byte),
	}
}

func (s *MemoryStore) Mode() string {
	return "memory"
}

func (s *MemoryStore) Reserve(ctx context.Context, bytes int64) error {
	return s.capMgr.Reserve(ctx, bytes)
}

func (s *MemoryStore) ReleaseReservation(bytes int64) {
	s.capMgr.ReleaseReservation(bytes)
}

func (s *MemoryStore) CreateSegment(key SegmentKey) (*SpoolItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrSpoolClosed
	}

	keyStr := key.String()
	if item, exists := s.items[keyStr]; exists {
		return item, nil
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
	// Dynamically allocated on WriteAt, starting empty to avoid 32MB upfront memory surge
	s.buffers[keyStr] = nil

	return item, nil
}

func (s *MemoryStore) WriteAt(key SegmentKey, relOffset int64, data []byte) (int, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrSpoolClosed
	}
	keyStr := key.String()
	item, ok := s.items[keyStr]
	if !ok {
		s.mu.Unlock()
		return 0, ErrSegmentNotFound
	}

	dataLen := int64(len(data))
	if relOffset < 0 || relOffset+dataLen > key.Length {
		s.mu.Unlock()
		return 0, ErrOffsetOutOfBounds
	}

	buf := s.buffers[keyStr]
	reqLen := relOffset + dataLen
	if int64(len(buf)) < reqLen {
		newBuf := make([]byte, reqLen)
		copy(newBuf, buf)
		buf = newBuf
		s.buffers[keyStr] = buf
	}

	copy(buf[relOffset:relOffset+dataLen], data)
	s.mu.Unlock()

	// Update range set and capacity conversion
	item.mu.Lock()
	prevCovered := item.Ranges.TotalCovered()
	item.Ranges.Add(relOffset, relOffset+dataLen)
	newCovered := item.Ranges.TotalCovered()
	delta := newCovered - prevCovered
	item.UpdatedAt = time.Now()
	item.mu.Unlock()

	if delta > 0 {
		s.capMgr.ConvertReservationToUsed(delta)
	}

	return len(data), nil
}

func (s *MemoryStore) ReadAt(key SegmentKey, relOffset int64, p []byte) (int, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, ErrSpoolClosed
	}
	keyStr := key.String()
	buf, ok := s.buffers[keyStr]
	s.mu.RUnlock()

	if !ok {
		return 0, ErrSegmentNotFound
	}

	if relOffset < 0 || relOffset >= int64(len(buf)) {
		return 0, io.EOF
	}

	n := copy(p, buf[relOffset:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (s *MemoryStore) Sync(key SegmentKey) error {
	return nil
}

func (s *MemoryStore) MarkReady(key SegmentKey) error {
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

	item.State = StateReady
	item.UpdatedAt = time.Now()
	s.capMgr.MarkReady(item.Ranges.TotalCovered())

	return nil
}

func (s *MemoryStore) Reclaim(key SegmentKey) error {
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
	delete(s.buffers, keyStr)
	return nil
}

func (s *MemoryStore) GetItem(key SegmentKey) (*SpoolItem, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[key.String()]
	return item, ok
}

func (s *MemoryStore) ListReadySegments() []*SpoolItem {
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

func (s *MemoryStore) Recover(ctx context.Context) error {
	// Memory store is volatile; nothing to recover from disk
	return nil
}

func (s *MemoryStore) Metrics() SpoolMetrics {
	max, used, reserved, ready, writing, reclaimed, backpressured := s.capMgr.Snapshot()

	s.mu.RLock()
	activeSegs := len(s.items)
	s.mu.RUnlock()

	return SpoolMetrics{
		Mode:           "memory",
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

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.items = make(map[string]*SpoolItem)
	s.buffers = make(map[string][]byte)
	return nil
}
