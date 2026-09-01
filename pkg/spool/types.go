package spool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultSegmentSize is 32 MiB (64 * 512 KiB Telegram chunks).
const DefaultSegmentSize int64 = 32 * 1024 * 1024

// SmallFileThreshold is 16 MiB. Files <= this threshold use a single segment.
const SmallFileThreshold int64 = 16 * 1024 * 1024

var (
	ErrSpoolClosed         = errors.New("spool is closed")
	ErrCapacityExceeded    = errors.New("spool capacity exceeded")
	ErrSegmentNotFound     = errors.New("segment not found")
	ErrSegmentAlreadyReady = errors.New("segment already marked ready")
	ErrOffsetOutOfBounds   = errors.New("write offset out of segment bounds")
	ErrInvalidRange        = errors.New("invalid byte range")
	ErrGenerationMismatch  = errors.New("generation mismatch")
)

// SpoolState represents the strict unidirectional lifecycle of a SpoolItem / Segment.
type SpoolState int

const (
	StateReserved SpoolState = iota
	StateReceiving
	StateReady
	StateQueued
	StateWritingBack
	StateTargetDurable
	StateReclaimed
	StateFailed
	StateCanceled
)

func (s SpoolState) String() string {
	switch s {
	case StateReserved:
		return "RESERVED"
	case StateReceiving:
		return "RECEIVING"
	case StateReady:
		return "READY"
	case StateQueued:
		return "QUEUED"
	case StateWritingBack:
		return "WRITING_BACK"
	case StateTargetDurable:
		return "TARGET_DURABLE"
	case StateReclaimed:
		return "RECLAIMED"
	case StateFailed:
		return "FAILED"
	case StateCanceled:
		return "CANCELED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", s)
	}
}

// SegmentKey uniquely identifies a segment within a specific attempt of a task.
type SegmentKey struct {
	TaskID       string `json:"task_id"`
	Gen          string `json:"gen"`
	SegmentIndex int    `json:"segment_index"`
	StartOffset  int64  `json:"start_offset"`
	Length       int64  `json:"length"`
}

func (k SegmentKey) String() string {
	return fmt.Sprintf("%s:%s:seg%d[%d:%d]", k.TaskID, k.Gen, k.SegmentIndex, k.StartOffset, k.StartOffset+k.Length)
}

// SegmentID produces a compact filesystem-safe and hash-friendly identifier.
func (k SegmentKey) SegmentID() string {
	return fmt.Sprintf("seg_%d_%d_%d", k.SegmentIndex, k.StartOffset, k.Length)
}

// SpoolItem holds in-memory tracking metadata for a segment.
type SpoolItem struct {
	mu           sync.RWMutex
	Key          SegmentKey
	ExpectedSize int64
	Ranges       *RangeSet
	State        SpoolState
	Dirty        bool
	Attempts     int
	NextRetryAt  time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// SpoolMetrics provides real-time observability into Spool utilization.
type SpoolMetrics struct {
	Mode           string `json:"mode"`
	MaxBytes       int64  `json:"max_bytes"`
	UsedBytes      int64  `json:"used_bytes"`
	ReservedBytes  int64  `json:"reserved_bytes"`
	ReadyBytes     int64  `json:"ready_bytes"`
	WritingBytes   int64  `json:"writing_bytes"`
	ReclaimedBytes int64  `json:"reclaimed_bytes"`
	ActiveSegments int    `json:"active_segments"`
	Backpressured  bool   `json:"backpressured"`
}

// Store is the core portable storage abstraction for the write-back spool.
type Store interface {
	// Mode returns "disk" or "memory".
	Mode() string

	// Reserve acquires a byte budget before network transfer. Blocks if capacity is exhausted.
	Reserve(ctx context.Context, bytes int64) error

	// ReleaseReservation releases an unused reservation.
	ReleaseReservation(bytes int64)

	// CreateSegment initializes a new segment item in StateReceiving.
	CreateSegment(key SegmentKey) (*SpoolItem, error)

	// WriteAt writes chunk data into the segment at the specified relative offset within the segment.
	WriteAt(key SegmentKey, relOffset int64, data []byte) (int, error)

	// ReadAt reads data from the segment at the specified relative offset.
	ReadAt(key SegmentKey, relOffset int64, p []byte) (int, error)

	// Sync flushes pending writes for the segment to disk (File.Sync).
	Sync(key SegmentKey) error

	// MarkReady transitions a fully received segment to StateReady.
	MarkReady(key SegmentKey) error

	// Reclaim physically purges the segment from spool storage and releases its capacity.
	Reclaim(key SegmentKey) error

	// GetItem returns the SpoolItem metadata for a key if present.
	GetItem(key SegmentKey) (*SpoolItem, bool)

	// ListReadySegments returns all segments in StateReady waiting for write-back.
	ListReadySegments() []*SpoolItem

	// Recover scans the storage directory on startup and reloads incomplete/ready segments.
	Recover(ctx context.Context) error

	// Metrics returns current spool storage metrics.
	Metrics() SpoolMetrics

	// Close flushes and shuts down the store.
	Close() error
}
