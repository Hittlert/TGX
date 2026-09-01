package spool

import (
	"fmt"
	"strings"
	"sync"
)

// ByteInterval represents a contiguous half-open interval [Start, End).
type ByteInterval struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

func (b ByteInterval) Length() int64 {
	if b.End <= b.Start {
		return 0
	}
	return b.End - b.Start
}

func (b ByteInterval) String() string {
	return fmt.Sprintf("[%d-%d)", b.Start, b.End)
}

// RangeSet manages a sorted set of non-overlapping, non-adjacent contiguous byte ranges.
type RangeSet struct {
	mu     sync.RWMutex
	ranges []ByteInterval
}

// NewRangeSet creates an empty RangeSet.
func NewRangeSet() *RangeSet {
	return &RangeSet{
		ranges: make([]ByteInterval, 0),
	}
}

// NewRangeSetWithIntervals initializes a RangeSet with given intervals.
func NewRangeSetWithIntervals(intervals []ByteInterval) *RangeSet {
	rs := NewRangeSet()
	for _, iv := range intervals {
		rs.Add(iv.Start, iv.End)
	}
	return rs
}

// Add inserts a range [start, end) and merges overlapping or adjacent intervals.
func (rs *RangeSet) Add(start, end int64) {
	if end <= start {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()

	newRange := ByteInterval{Start: start, End: end}
	if len(rs.ranges) == 0 {
		rs.ranges = append(rs.ranges, newRange)
		return
	}

	merged := make([]ByteInterval, 0, len(rs.ranges)+1)
	inserted := false

	for _, curr := range rs.ranges {
		if inserted {
			merged = append(merged, curr)
			continue
		}

		if newRange.End < curr.Start {
			// newRange is strictly before curr
			merged = append(merged, newRange, curr)
			inserted = true
		} else if newRange.Start > curr.End {
			// newRange is strictly after curr
			merged = append(merged, curr)
		} else {
			// Overlap or adjacent: expand newRange
			if curr.Start < newRange.Start {
				newRange.Start = curr.Start
			}
			if curr.End > newRange.End {
				newRange.End = curr.End
			}
		}
	}

	if !inserted {
		merged = append(merged, newRange)
	}

	rs.ranges = merged
}

// Contains checks if the given [start, end) range is completely covered.
func (rs *RangeSet) Contains(start, end int64) bool {
	if end <= start {
		return true
	}
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	for _, curr := range rs.ranges {
		if curr.Start <= start && curr.End >= end {
			return true
		}
		if curr.Start > start {
			break
		}
	}
	return false
}

// TotalCovered returns the total number of distinct bytes covered by all ranges.
func (rs *RangeSet) TotalCovered() int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var total int64
	for _, curr := range rs.ranges {
		total += curr.Length()
	}
	return total
}

// IsComplete checks if the range [0, expectedLength) is 100% covered.
func (rs *RangeSet) IsComplete(expectedLength int64) bool {
	if expectedLength <= 0 {
		return true
	}
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if len(rs.ranges) != 1 {
		return false
	}
	return rs.ranges[0].Start == 0 && rs.ranges[0].End >= expectedLength
}

// Intervals returns a copy of all disjoint intervals.
func (rs *RangeSet) Intervals() []ByteInterval {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	res := make([]ByteInterval, len(rs.ranges))
	copy(res, rs.ranges)
	return res
}

// MissingRanges returns intervals in [0, expectedLength) that have not yet been received.
func (rs *RangeSet) MissingRanges(expectedLength int64) []ByteInterval {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var missing []ByteInterval
	var current int64 = 0

	for _, curr := range rs.ranges {
		if curr.Start > current {
			end := curr.Start
			if end > expectedLength {
				end = expectedLength
			}
			missing = append(missing, ByteInterval{Start: current, End: end})
		}
		if curr.End > current {
			current = curr.End
		}
		if current >= expectedLength {
			break
		}
	}

	if current < expectedLength {
		missing = append(missing, ByteInterval{Start: current, End: expectedLength})
	}
	return missing
}

func (rs *RangeSet) String() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var parts []string
	for _, iv := range rs.ranges {
		parts = append(parts, iv.String())
	}
	return strings.Join(parts, ", ")
}

// Clone creates a deep copy of the RangeSet.
func (rs *RangeSet) Clone() *RangeSet {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	clone := &RangeSet{
		ranges: make([]ByteInterval, len(rs.ranges)),
	}
	copy(clone.ranges, rs.ranges)
	return clone
}
