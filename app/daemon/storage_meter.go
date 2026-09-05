package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/Hittlert/TGX/internal/fscommit"
)

// StorageIOMeter provides typed, live physical I/O metrics across distinct storage tiers:
// 1. Direct SSD storage (OutputDir)
// 2. Archive HDD storage (ArchiveDir)
type StorageIOMeter struct {
	ssdWriteBytes    int64 // atomic: cumulative physical bytes written to SSD
	ssdReadBytes     int64 // atomic: cumulative physical bytes read from SSD
	ssdActiveWriters int64 // atomic: current concurrent physical writes to SSD

	archiveWriteBytes    int64 // atomic: cumulative physical bytes written to Archive HDD
	archiveReadBytes     int64 // atomic: cumulative physical bytes read from Archive HDD
	archiveActiveWriters int64 // atomic: current concurrent physical writes to Archive HDD
}

// NewStorageIOMeter creates a new StorageIOMeter instance.
func NewStorageIOMeter() *StorageIOMeter {
	return &StorageIOMeter{}
}

// SSDWriteBytes returns cumulative physical bytes written to Direct SSD storage.
func (m *StorageIOMeter) SSDWriteBytes() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.ssdWriteBytes)
}

// SSDReadBytes returns cumulative physical bytes read from Direct SSD storage.
func (m *StorageIOMeter) SSDReadBytes() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.ssdReadBytes)
}

// SSDActiveWriters returns the count of concurrent physical writes to Direct SSD storage.
func (m *StorageIOMeter) SSDActiveWriters() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.ssdActiveWriters)
}

// ArchiveWriteBytes returns cumulative physical bytes written to Archive HDD storage.
func (m *StorageIOMeter) ArchiveWriteBytes() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.archiveWriteBytes)
}

// ArchiveReadBytes returns cumulative physical bytes read from Archive HDD storage.
func (m *StorageIOMeter) ArchiveReadBytes() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.archiveReadBytes)
}

// ArchiveActiveWriters returns the count of concurrent physical writes to Archive HDD storage.
func (m *StorageIOMeter) ArchiveActiveWriters() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.archiveActiveWriters)
}

// RecordSSDWrite atomically records physical bytes written to SSD storage.
func (m *StorageIOMeter) RecordSSDWrite(n int64) {
	if m != nil && n > 0 {
		atomic.AddInt64(&m.ssdWriteBytes, n)
	}
}

// RecordSSDRead atomically records physical bytes read from SSD storage.
func (m *StorageIOMeter) RecordSSDRead(n int64) {
	if m != nil && n > 0 {
		atomic.AddInt64(&m.ssdReadBytes, n)
	}
}

// RecordArchiveWrite atomically records physical bytes written to Archive HDD storage.
func (m *StorageIOMeter) RecordArchiveWrite(n int64) {
	if m != nil && n > 0 {
		atomic.AddInt64(&m.archiveWriteBytes, n)
	}
}

// RecordArchiveRead atomically records physical bytes read from Archive HDD storage.
func (m *StorageIOMeter) RecordArchiveRead(n int64) {
	if m != nil && n > 0 {
		atomic.AddInt64(&m.archiveReadBytes, n)
	}
}

// WrapSSDWriterAt wraps an io.WriterAt to track live SSD physical writes and concurrency.
func (m *StorageIOMeter) WrapSSDWriterAt(w io.WriterAt) io.WriterAt {
	if m == nil {
		return w
	}
	return &meteredSSDWriterAt{w: w, meter: m}
}

type meteredSSDWriterAt struct {
	w     io.WriterAt
	meter *StorageIOMeter
}

func (mw *meteredSSDWriterAt) WriteAt(p []byte, off int64) (int, error) {
	atomic.AddInt64(&mw.meter.ssdActiveWriters, 1)
	defer atomic.AddInt64(&mw.meter.ssdActiveWriters, -1)

	n, err := mw.w.WriteAt(p, off)
	if n > 0 {
		atomic.AddInt64(&mw.meter.ssdWriteBytes, int64(n))
	}
	return n, err
}

// WrapSSDStreamReader wraps an io.Reader to track live SSD physical reads.
func (m *StorageIOMeter) WrapSSDStreamReader(r io.Reader) io.Reader {
	if m == nil {
		return r
	}
	return &meteredSSDReader{r: r, meter: m}
}

type meteredSSDReader struct {
	r     io.Reader
	meter *StorageIOMeter
}

func (mr *meteredSSDReader) Read(p []byte) (int, error) {
	n, err := mr.r.Read(p)
	if n > 0 {
		atomic.AddInt64(&mr.meter.ssdReadBytes, int64(n))
	}
	return n, err
}

// WrapArchiveStreamWriter wraps an io.WriteCloser to track live Archive HDD physical writes.
func (m *StorageIOMeter) WrapArchiveStreamWriter(w io.WriteCloser) io.WriteCloser {
	if m == nil {
		return w
	}
	return &meteredArchiveWriter{w: w, meter: m}
}

type meteredArchiveWriter struct {
	w     io.WriteCloser
	meter *StorageIOMeter
}

func (mw *meteredArchiveWriter) Write(p []byte) (int, error) {
	n, err := mw.w.Write(p)
	if n > 0 {
		atomic.AddInt64(&mw.meter.archiveWriteBytes, int64(n))
	}
	return n, err
}

func (mw *meteredArchiveWriter) Close() error {
	return mw.w.Close()
}

func (mw *meteredArchiveWriter) Sync() error {
	if s, ok := mw.w.(fscommit.Syncer); ok {
		return s.Sync()
	}
	return nil
}

// WrapArchiveStreamReader wraps an io.Reader to track live Archive HDD physical reads.
func (m *StorageIOMeter) WrapArchiveStreamReader(r io.Reader) io.Reader {
	if m == nil {
		return r
	}
	return &meteredArchiveReader{r: r, meter: m}
}

type meteredArchiveReader struct {
	r     io.Reader
	meter *StorageIOMeter
}

func (mr *meteredArchiveReader) Read(p []byte) (int, error) {
	n, err := mr.r.Read(p)
	if n > 0 {
		atomic.AddInt64(&mr.meter.archiveReadBytes, int64(n))
	}
	return n, err
}

// ComputeSSDFileSHA256 computes SHA-256 for an SSD file, counting live physical read bytes.
func (m *StorageIOMeter) ComputeSSDFileSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, 1024*1024)
	var reader io.Reader = f
	if m != nil {
		reader = m.WrapSSDStreamReader(f)
	}
	if _, err := io.CopyBuffer(h, reader, buf); err != nil {
		return "", fmt.Errorf("read file for sha256: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ComputeArchiveFileSHA256 computes SHA-256 for an Archive HDD file, counting live physical read bytes.
func (m *StorageIOMeter) ComputeArchiveFileSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, 1024*1024)
	var reader io.Reader = f
	if m != nil {
		reader = m.WrapArchiveStreamReader(f)
	}
	if _, err := io.CopyBuffer(h, reader, buf); err != nil {
		return "", fmt.Errorf("read file for sha256: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
