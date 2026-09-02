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
	backpressured  int64
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

		// P1-2: If requested chunk is larger than maxBytes, allow single in-flight allocation to prevent deadlock
		if bytes > cm.maxBytes && allocated == 0 {
			cm.reservedBytes += bytes
			return nil
		}

		cm.backpressured++

		// Wait for space to be freed using AfterFunc (0 goroutine leak)
		stop := context.AfterFunc(ctx, func() {
			cm.mu.Lock()
			cm.cond.Broadcast()
			cm.mu.Unlock()
		})
		cm.cond.Wait()
		stop()
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

// MarkReady marks used bytes as ready for write-back.
func (cm *CapacityManager) MarkReady(bytes int64) {
	if bytes <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.readyBytes += bytes
}

// MarkWritingBack transitions bytes from ready to writing back.
func (cm *CapacityManager) MarkWritingBack(bytes int64) {
	if bytes <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.readyBytes >= bytes {
		cm.readyBytes -= bytes
	} else {
		cm.readyBytes = 0
	}
	cm.writingBytes += bytes
}

// Reclaim frees bytes back to the capacity pool after target synchronization.
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

// Snapshot returns current byte statistics.
func (cm *CapacityManager) Snapshot() (max, used, reserved, ready, writing, reclaimed, backpressured int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.maxBytes, cm.usedBytes, cm.reservedBytes, cm.readyBytes, cm.writingBytes, cm.reclaimedBytes, cm.backpressured
}

// Close closes the manager and unblocks waiting reservations.
func (cm *CapacityManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.closed = true
	cm.cond.Broadcast()
}
