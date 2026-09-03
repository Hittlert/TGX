package fscommit

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInsufficientSSDSpace = errors.New("insufficient SSD free space for file reservation")
)

const (
	DefaultMinFreeSpace = 5 * 1024 * 1024 * 1024 // 5 GiB default minimum free space reserve
)

// SSDAdmission manages whole-file disk space admission and reservation against real SSD capacity.
type SSDAdmission struct {
	mu           sync.Mutex
	rootPath     string
	minFreeSpace uint64

	reservations  map[string]int64
	totalReserved int64
}

// NewSSDAdmission creates an SSD admission owner for rootPath with minimum free space threshold.
func NewSSDAdmission(rootPath string, minFreeSpace uint64) *SSDAdmission {
	if minFreeSpace == 0 {
		minFreeSpace = DefaultMinFreeSpace
	}
	return &SSDAdmission{
		rootPath:     rootPath,
		minFreeSpace: minFreeSpace,
		reservations: make(map[string]int64),
	}
}

// Stats returns the current SSD capacity snapshot.
type SSDStats struct {
	FreeBytes      uint64 `json:"free_bytes"`
	TotalBytes     uint64 `json:"total_bytes"`
	ReservedBytes  int64  `json:"reserved_bytes"`
	AvailableBytes int64  `json:"available_bytes"`
	ActiveFiles    int    `json:"active_files"`
}

// Stats queries real filesystem free space and active reservations.
func (a *SSDAdmission) Stats() (SSDStats, error) {
	free, total, err := GetDiskSpace(a.rootPath)
	if err != nil {
		return SSDStats{}, fmt.Errorf("statfs %s: %w", a.rootPath, err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	avail := int64(free) - int64(a.minFreeSpace) - a.totalReserved
	if avail < 0 {
		avail = 0
	}

	return SSDStats{
		FreeBytes:      free,
		TotalBytes:     total,
		ReservedBytes:  a.totalReserved,
		AvailableBytes: avail,
		ActiveFiles:    len(a.reservations),
	}, nil
}

// ReservedBytes returns the total currently active reserved bytes.
func (a *SSDAdmission) ReservedBytes() int64 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.totalReserved
}

// Reserve checks real SSD free space and reserves expectedSize bytes for taskID.
// If space is below minFreeSpace + expectedSize, it returns ErrInsufficientSSDSpace.
// It returns an idempotent release function that must be called when the task completes, fails, or cancels.
func (a *SSDAdmission) Reserve(taskID string, expectedSize int64) (func(), error) {
	if a == nil {
		return func() {}, nil
	}

	free, _, err := GetDiskSpace(a.rootPath)
	if err != nil {
		return nil, fmt.Errorf("statfs %s: %w", a.rootPath, err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// If task already has a reservation, release old reservation first
	if oldSize, exists := a.reservations[taskID]; exists {
		a.totalReserved -= oldSize
		delete(a.reservations, taskID)
	}

	// Safe reservation check: free - minFreeSpace - totalReserved >= expectedSize
	// If expectedSize is unknown (<= 0), assume a conservative 100MB buffer reservation
	reqBytes := expectedSize
	if reqBytes <= 0 {
		reqBytes = 100 * 1024 * 1024
	}

	available := int64(free) - int64(a.minFreeSpace) - a.totalReserved
	if available < reqBytes {
		return nil, fmt.Errorf("%w: available %d bytes, requested %d bytes (free %d, minFree %d, reserved %d)",
			ErrInsufficientSSDSpace, available, reqBytes, free, a.minFreeSpace, a.totalReserved)
	}

	a.reservations[taskID] = reqBytes
	a.totalReserved += reqBytes

	var once sync.Once
	release := func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if sz, ok := a.reservations[taskID]; ok {
				a.totalReserved -= sz
				delete(a.reservations, taskID)
			}
		})
	}

	return release, nil
}
