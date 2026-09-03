package transfer

import (
	"io"
	"sync"
	"sync/atomic"
)

// CountingWriterAt wraps an underlying io.WriterAt and calls onProgress(downloaded, total)
// thread-safely as chunks are written by concurrent gotd workers.
type CountingWriterAt struct {
	w          io.WriterAt
	totalSize  int64
	onProgress func(downloaded, total int64)

	mu         sync.Mutex
	maxWritten int64
	downloaded int64
}

// NewCountingWriterAt creates a thread-safe progress reporter wrapping w.
func NewCountingWriterAt(w io.WriterAt, totalSize int64, onProgress func(downloaded, total int64)) *CountingWriterAt {
	return &CountingWriterAt{
		w:          w,
		totalSize:  totalSize,
		onProgress: onProgress,
	}
}

// WriteAt delegates to the underlying io.WriterAt and records written bytes.
func (c *CountingWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = c.w.WriteAt(p, off)
	if n > 0 {
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
