package spool

import (
	"context"
	"io"
	"sync"
	"time"
)

const memChunkSize int64 = 64 * 1024 // 64 KiB per memory chunk

type memSegmentBuffer struct {
	chunks         map[int][]byte // chunkIndex -> allocated slice
	allocatedBytes int64          // sum of len(slice) for all chunks in this segment
}

// MemoryStore implements the Store interface using chunked in-RAM buffers with strict physical accounting.
type MemoryStore struct {
	mu      sync.RWMutex
	capMgr  *CapacityManager
	items   map[string]*SpoolItem        // key.String() -> SpoolItem
	buffers map[string]*memSegmentBuffer // key.String() -> chunked buffer
	closed  bool
}

// NewMemoryStore creates an in-memory Spool store with maximum memory budget maxBytes.
func NewMemoryStore(maxBytes int64) *MemoryStore {
	return &MemoryStore{
		capMgr:  NewCapacityManager(maxBytes),
		items:   make(map[string]*SpoolItem),
		buffers: make(map[string]*memSegmentBuffer),
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
	s.buffers[keyStr] = &memSegmentBuffer{
		chunks: make(map[int][]byte),
	}

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
	buf, bufOk := s.buffers[keyStr]
	if !ok || !bufOk {
		s.mu.Unlock()
		return 0, ErrSegmentNotFound
	}

	dataLen := int64(len(data))
	if relOffset < 0 || relOffset+dataLen > key.Length {
		s.mu.Unlock()
		return 0, ErrOffsetOutOfBounds
	}

	var newlyAllocatedBytes int64 = 0
	var written int64 = 0

	for written < dataLen {
		currOffset := relOffset + written
		chunkIdx := int(currOffset / memChunkSize)
		chunkOffset := currOffset % memChunkSize

		chunk, exists := buf.chunks[chunkIdx]
		if !exists {
			// Size chunk physically bounded by remaining segment length
			chunkStart := int64(chunkIdx) * memChunkSize
			chunkLen := memChunkSize
			if chunkStart+chunkLen > key.Length {
				chunkLen = key.Length - chunkStart
			}
			chunk = make([]byte, chunkLen)
			buf.chunks[chunkIdx] = chunk
			buf.allocatedBytes += int64(len(chunk))
			newlyAllocatedBytes += int64(len(chunk))
		}

		toCopy := int64(len(chunk)) - chunkOffset
		if toCopy > dataLen-written {
			toCopy = dataLen - written
		}

		copy(chunk[chunkOffset:chunkOffset+toCopy], data[written:written+toCopy])
		written += toCopy
	}
	s.mu.Unlock()

	// Convert exact physical allocated memory in CapacityManager
	if newlyAllocatedBytes > 0 {
		s.capMgr.ConvertReservationToUsed(newlyAllocatedBytes)
	}

	item.mu.Lock()
	item.Ranges.Add(relOffset, relOffset+dataLen)
	item.UpdatedAt = time.Now()
	item.mu.Unlock()

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
	if !ok {
		s.mu.RUnlock()
		return 0, ErrSegmentNotFound
	}

	if relOffset < 0 || relOffset >= key.Length {
		s.mu.RUnlock()
		return 0, io.EOF
	}

	toRead := int64(len(p))
	if relOffset+toRead > key.Length {
		toRead = key.Length - relOffset
	}

	var read int64 = 0
	for read < toRead {
		currOffset := relOffset + read
		chunkIdx := int(currOffset / memChunkSize)
		chunkOffset := currOffset % memChunkSize

		chunk, exists := buf.chunks[chunkIdx]
		chunkToRead := memChunkSize - chunkOffset
		if exists && chunkToRead > int64(len(chunk))-chunkOffset {
			chunkToRead = int64(len(chunk)) - chunkOffset
		}
		if chunkToRead > toRead-read {
			chunkToRead = toRead - read
		}

		if exists {
			copy(p[read:read+chunkToRead], chunk[chunkOffset:chunkOffset+chunkToRead])
		} else {
			for i := int64(0); i < chunkToRead; i++ {
				p[read+i] = 0
			}
		}
		read += chunkToRead
	}
	s.mu.RUnlock()

	if read < int64(len(p)) {
		return int(read), io.EOF
	}
	return int(read), nil
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
	buf, bufOk := s.buffers[keyStr]
	if ok {
		item.mu.Lock()
		item.State = StateReclaimed
		item.UpdatedAt = time.Now()
		item.mu.Unlock()

		var allocated int64 = 0
		if bufOk && buf != nil {
			allocated = buf.allocatedBytes
		} else {
			allocated = item.Ranges.TotalCovered()
		}

		s.capMgr.Reclaim(allocated)
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
	s.capMgr.Close()
	s.items = make(map[string]*SpoolItem)
	s.buffers = make(map[string]*memSegmentBuffer)
	return nil
}
