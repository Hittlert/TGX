package transfer

import (
	"io"
	"sync"
	"sync/atomic"
)

// byteInterval represents a half-open byte range [start, end).
type byteInterval struct {
	start int64
	end   int64
}

// RangeTracker tracks non-overlapping byte coverage intervals in memory for a download attempt.
type RangeTracker struct {
	mu        sync.Mutex
	intervals []byteInterval
}

// NewRangeTracker creates an empty RangeTracker.
func NewRangeTracker() *RangeTracker {
	return &RangeTracker{
		intervals: make([]byteInterval, 0, 16),
	}
}

// AddRange records that bytes in [start, end) were written.
func (rt *RangeTracker) AddRange(start, end int64) {
	if start >= end || start < 0 {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()

	newInterval := byteInterval{start: start, end: end}
	var merged []byteInterval
	inserted := false

	for _, it := range rt.intervals {
		if inserted {
			merged = append(merged, it)
			continue
		}
		if newInterval.end < it.start {
			// newInterval comes strictly before it
			merged = append(merged, newInterval, it)
			inserted = true
		} else if newInterval.start > it.end {
			// newInterval comes strictly after it
			merged = append(merged, it)
		} else {
			// Overlapping or adjacent: merge
			if it.start < newInterval.start {
				newInterval.start = it.start
			}
			if it.end > newInterval.end {
				newInterval.end = it.end
			}
		}
	}
	if !inserted {
		merged = append(merged, newInterval)
	}
	rt.intervals = merged
}

// IsComplete returns true if and only if the intervals contiguously cover [0, expectedSize).
func (rt *RangeTracker) IsComplete(expectedSize int64) bool {
	if expectedSize <= 0 {
		return true
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.intervals) != 1 {
		return false
	}
	return rt.intervals[0].start == 0 && rt.intervals[0].end >= expectedSize
}

// CoveredBytes returns the total unique bytes covered across all intervals.
func (rt *RangeTracker) CoveredBytes() int64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var total int64
	for _, it := range rt.intervals {
		total += (it.end - it.start)
	}
	return total
}

// CountingWriterAt wraps an underlying io.WriterAt and calls onProgress(downloaded, total)
// thread-safely as chunks are written by concurrent gotd workers, while tracking byte coverage.
type CountingWriterAt struct {
	w          io.WriterAt
	totalSize  int64
	onProgress func(downloaded, total int64)
	tracker    *RangeTracker

	mu         sync.Mutex
	maxWritten int64
	downloaded int64
}

// NewCountingWriterAt creates a thread-safe progress reporter and range tracker wrapping w.
func NewCountingWriterAt(w io.WriterAt, totalSize int64, onProgress func(downloaded, total int64)) *CountingWriterAt {
	return &CountingWriterAt{
		w:          w,
		totalSize:  totalSize,
		onProgress: onProgress,
		tracker:    NewRangeTracker(),
	}
}

// WriteAt delegates to the underlying io.WriterAt and records written bytes.
func (c *CountingWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = c.w.WriteAt(p, off)
	if n > 0 {
		c.tracker.AddRange(off, off+int64(n))
		curr := atomic.AddInt64(&c.downloaded, int64(n))
		if c.onProgress != nil {
			c.mu.Lock()
			if curr > c.maxWritten {
				c.maxWritten = curr
				c.onProgress(curr, c.totalSize)
			}
			c.mu.Unlock()
		}
	}
	return n, err
}

// Downloaded returns total bytes written so far.
func (c *CountingWriterAt) Downloaded() int64 {
	return atomic.LoadInt64(&c.downloaded)
}

// IsComplete checks if the range tracker has verified [0, expectedSize) coverage.
func (c *CountingWriterAt) IsComplete(expectedSize int64) bool {
	return c.tracker.IsComplete(expectedSize)
}

// CoveredBytes returns the unique verified bytes covered so far.
func (c *CountingWriterAt) CoveredBytes() int64 {
	return c.tracker.CoveredBytes()
}
