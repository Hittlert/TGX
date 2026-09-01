package spool

import (
	"context"
	"sync"
)

// CapacityManager regulates byte allocations against a configured capacity limit.
type CapacityManager struct {
	mu             sync.Mutex
	cond           *sync.Cond
	maxBytes       int64
	reservedBytes  int64
	usedBytes      int64
	readyBytes     int64
	writingBytes   int64
	reclaimedBytes int64
	closed         bool
}

// NewCapacityManager initializes a CapacityManager with a maximum byte limit.
func NewCapacityManager(maxBytes int64) *CapacityManager {
	cm := &CapacityManager{
		maxBytes: maxBytes,
	}
	cm.cond = sync.NewCond(&cm.mu)
	return cm
}

// Reserve blocks until the requested bytes can be reserved within maxBytes, or context cancels.
func (cm *CapacityManager) Reserve(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// If unlimited capacity (maxBytes <= 0), always allow
	if cm.maxBytes <= 0 {
		cm.reservedBytes += bytes
		return nil
	}

	for {
		if cm.closed {
			return ErrSpoolClosed
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Available = max - (reserved + used)
		allocated := cm.reservedBytes + cm.usedBytes
		if allocated+bytes <= cm.maxBytes {
			cm.reservedBytes += bytes
			return nil
		}

		// Wait for space to be freed
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				cm.mu.Lock()
				cm.cond.Broadcast()
				cm.mu.Unlock()
			case <-done:
			}
		}()

		cm.cond.Wait()
		close(done)
	}
}

// ReleaseReservation releases an unused reservation.
func (cm *CapacityManager) ReleaseReservation(bytes int64) {
	if bytes <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.reservedBytes >= bytes {
		cm.reservedBytes -= bytes
	} else {
		cm.reservedBytes = 0
	}
	cm.cond.Broadcast()
}

// ConvertReservationToUsed converts reserved bytes into actual used bytes on disk/RAM.
func (cm *CapacityManager) ConvertReservationToUsed(bytes int64) {
	if bytes <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.reservedBytes >= bytes {
		cm.reservedBytes -= bytes
	} else {
		cm.reservedBytes = 0
	}
	cm.usedBytes += bytes
}

// MarkReady transitions bytes from active receiving to ready for write-back.
func (cm *CapacityManager) MarkReady(bytes int64) {
	if bytes <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.readyBytes += bytes
}

// MarkWritingBack transitions ready bytes to writing-back state.
func (cm *CapacityManager) MarkWritingBack(bytes int64) {
	if bytes <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.readyBytes >= bytes {
		cm.readyBytes -= bytes
	}
	cm.writingBytes += bytes
}

// Reclaim frees used bytes when a segment is durably written back and deleted.
func (cm *CapacityManager) Reclaim(bytes int64) {
	if bytes <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.usedBytes >= bytes {
		cm.usedBytes -= bytes
	} else {
		cm.usedBytes = 0
	}
	if cm.writingBytes >= bytes {
		cm.writingBytes -= bytes
	}
	if cm.readyBytes >= bytes {
		cm.readyBytes -= bytes
	}
	cm.reclaimedBytes += bytes
	cm.cond.Broadcast()
}

// Close wakes up any waiting goroutines.
func (cm *CapacityManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.closed = true
	cm.cond.Broadcast()
}

// Snapshot returns a point-in-time metrics struct.
func (cm *CapacityManager) Snapshot() (max, used, reserved, ready, writing, reclaimed int64, backpressured bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	max = cm.maxBytes
	used = cm.usedBytes
	reserved = cm.reservedBytes
	ready = cm.readyBytes
	writing = cm.writingBytes
	reclaimed = cm.reclaimedBytes
	if cm.maxBytes > 0 && (used+reserved) >= int64(float64(cm.maxBytes)*0.95) {
		backpressured = true
	}
	return
}
