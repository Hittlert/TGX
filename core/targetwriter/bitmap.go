package targetwriter

import (
	"sync"
)

// Range represents a [Start, End) byte range.
type Range struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// MovedBitmap tracks durable written byte ranges for a task being written to target storage.
type MovedBitmap struct {
	mu           sync.RWMutex
	expectedSize int64
	ranges       []Range
}

func NewMovedBitmap(expectedSize int64) *MovedBitmap {
	return &MovedBitmap{
		expectedSize: expectedSize,
		ranges:       make([]Range, 0, 8),
	}
}

func NewMovedBitmapWithRanges(expectedSize int64, ranges []Range) *MovedBitmap {
	bm := &MovedBitmap{
		expectedSize: expectedSize,
		ranges:       make([]Range, 0, len(ranges)),
	}
	for _, r := range ranges {
		bm.AddMark(r.Start, r.End-r.Start)
	}
	return bm
}

// AddMark adds [offset, offset+length) to durable ranges and coalesces adjacent ranges.
func (b *MovedBitmap) AddMark(offset, length int64) {
	if length <= 0 || offset < 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	newStart := offset
	newEnd := offset + length
	if b.expectedSize > 0 && newEnd > b.expectedSize {
		newEnd = b.expectedSize
	}

	var newRanges []Range
	inserted := false

	for _, r := range b.ranges {
		if inserted {
			newRanges = append(newRanges, r)
			continue
		}
		if newEnd < r.Start {
			newRanges = append(newRanges, Range{Start: newStart, End: newEnd}, r)
			inserted = true
		} else if newStart > r.End {
			newRanges = append(newRanges, r)
		} else {
			if r.Start < newStart {
				newStart = r.Start
			}
			if r.End > newEnd {
				newEnd = r.End
			}
		}
	}

	if !inserted {
		newRanges = append(newRanges, Range{Start: newStart, End: newEnd})
	}
	b.ranges = newRanges
}

// Ranges returns a copy of current durable ranges.
func (b *MovedBitmap) Ranges() []Range {
	b.mu.RLock()
	defer b.mu.RUnlock()
	res := make([]Range, len(b.ranges))
	copy(res, b.ranges)
	return res
}

// IsComplete returns true if [0, expectedSize) has been fully and contiguously made durable.
func (b *MovedBitmap) IsComplete() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.expectedSize <= 0 {
		return len(b.ranges) > 0
	}
	if len(b.ranges) != 1 {
		return false
	}
	return b.ranges[0].Start == 0 && b.ranges[0].End >= b.expectedSize
}

// DurableBytes returns total unique durable bytes written.
func (b *MovedBitmap) DurableBytes() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var total int64
	for _, r := range b.ranges {
		total += (r.End - r.Start)
	}
	return total
}

// Contains returns true if [offset, offset+length) is already marked durable.
func (b *MovedBitmap) Contains(offset, length int64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	targetEnd := offset + length
	for _, r := range b.ranges {
		if r.Start <= offset && r.End >= targetEnd {
			return true
		}
	}
	return false
}
