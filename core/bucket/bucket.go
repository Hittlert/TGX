package bucket

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Mode string

const (
	ModeMemory Mode = "memory"
	ModeSSD    Mode = "ssd"
	ModeNone   Mode = "none"
)

// BucketConfig holds buffer capacity and storage parameters.
type Config struct {
	Mode        Mode
	RootDir     string
	MaxCapacity int64
}

// Bucket Metrics snapshot.
type Metrics struct {
	Mode               Mode   `json:"mode"`
	MaxCapacity        int64  `json:"max_capacity"`
	ReservedBytes      int64  `json:"reserved_bytes"`
	ReadyBytes         int64  `json:"ready_bytes"`
	PendingDeleteBytes int64  `json:"pending_delete_bytes"`
	UsedBytes          int64  `json:"used_bytes"`
	ObjectCount        int64  `json:"object_count"`
	Backpressured      bool   `json:"backpressured"`
}

// Bucket interface manages local object storage for Telegram chunk buffers.
type Bucket interface {
	Mode() Mode
	Metrics() Metrics
	Reserve(ctx context.Context, bytes int64) error
	ReleaseReservation(bytes int64)
	PutObject(key ObjectKey, data []byte) error
	ReadObject(key ObjectKey) ([]byte, error)
	TryTakeNext(taskID string, nextOffset int64) (*BufferObject, bool)
	TakeReady() (*BufferObject, bool)
	AckDurable(keys []ObjectKey) error
	DeleteObjects(keys []ObjectKey) error
	Recover(ctx context.Context) error
	Close() error
}

type readyEntry struct {
	obj     BufferObject
	addedAt time.Time
}

type bucketImpl struct {
	cfg   Config
	mu    sync.RWMutex
	cond  *sync.Cond

	reservedBytes      int64
	readyBytes         int64
	pendingDeleteBytes int64
	objectCount        int64

	// Index: taskID -> map[offset]*readyEntry
	readyByTask   map[string]map[int64]*readyEntry
	// FIFO / priority list of tasks
	taskOrder     []string
	memData       map[string][]byte // For ModeMemory: key.String() -> []byte

	closed int32
}

func New(cfg Config) (Bucket, error) {
	if cfg.MaxCapacity <= 0 {
		if cfg.Mode == ModeSSD {
			cfg.MaxCapacity = 5 * 1024 * 1024 * 1024 // 5 GiB
		} else {
			cfg.MaxCapacity = 512 * 1024 * 1024 // 512 MiB
		}
	}
	if cfg.Mode == ModeSSD && cfg.RootDir != "" {
		if err := os.MkdirAll(cfg.RootDir, 0755); err != nil {
			return nil, fmt.Errorf("create bucket root dir %s: %w", cfg.RootDir, err)
		}
	}

	b := &bucketImpl{
		cfg:         cfg,
		readyByTask: make(map[string]map[int64]*readyEntry),
		memData:     make(map[string][]byte),
	}
	b.cond = sync.NewCond(&b.mu)
	return b, nil
}

func (b *bucketImpl) Mode() Mode {
	return b.cfg.Mode
}

func (b *bucketImpl) Metrics() Metrics {
	b.mu.RLock()
	defer b.mu.RUnlock()

	used := b.reservedBytes + b.readyBytes + b.pendingDeleteBytes
	return Metrics{
		Mode:               b.cfg.Mode,
		MaxCapacity:        b.cfg.MaxCapacity,
		ReservedBytes:      b.reservedBytes,
		ReadyBytes:         b.readyBytes,
		PendingDeleteBytes: b.pendingDeleteBytes,
		UsedBytes:          used,
		ObjectCount:        b.objectCount,
		Backpressured:      used >= b.cfg.MaxCapacity,
	}
}

func (b *bucketImpl) Reserve(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		if atomic.LoadInt32(&b.closed) == 1 {
			return errors.New("bucket is closed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		currentUsed := b.reservedBytes + b.readyBytes + b.pendingDeleteBytes
		if currentUsed+bytes <= b.cfg.MaxCapacity || currentUsed == 0 {
			b.reservedBytes += bytes
			return nil
		}

		// Wait for TargetWriter to flush durable objects and release capacity
		b.cond.Wait()
	}
}

func (b *bucketImpl) ReleaseReservation(bytes int64) {
	if bytes <= 0 {
		return
	}
	b.mu.Lock()
	if b.reservedBytes >= bytes {
		b.reservedBytes -= bytes
	} else {
		b.reservedBytes = 0
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *bucketImpl) PutObject(key ObjectKey, data []byte) error {
	if int64(len(data)) != key.Length {
		return fmt.Errorf("data length %d does not match key length %d", len(data), key.Length)
	}

	// Verify checksum
	computedCRC := crc32.ChecksumIEEE(data)
	if key.Checksum == 0 {
		key.Checksum = computedCRC
	} else if key.Checksum != computedCRC {
		return fmt.Errorf("crc32 checksum mismatch: got %08x, expected %08x", computedCRC, key.Checksum)
	}

	obj := BufferObject{
		Key: key,
	}

	if b.cfg.Mode == ModeMemory {
		b.mu.Lock()
		b.memData[key.String()] = data
		obj.Data = data
		b.mu.Unlock()
	} else if b.cfg.Mode == ModeSSD {
		rel := key.RelPath(".ready")
		absPath := filepath.Join(b.cfg.RootDir, rel)
		partPath := filepath.Join(b.cfg.RootDir, key.RelPath(".partial"))

		if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
			return fmt.Errorf("create object dir: %w", err)
		}

		// Write to .partial then atomic rename to .ready
		if err := os.WriteFile(partPath, data, 0644); err != nil {
			return fmt.Errorf("write partial object: %w", err)
		}
		if err := os.Rename(partPath, absPath); err != nil {
			_ = os.Remove(partPath)
			return fmt.Errorf("commit ready object: %w", err)
		}
		obj.DiskPath = absPath
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Move capacity from reservedBytes -> readyBytes
	if b.reservedBytes >= key.Length {
		b.reservedBytes -= key.Length
	} else {
		b.reservedBytes = 0
	}
	b.readyBytes += key.Length
	b.objectCount++

	// Index object
	taskMap, ok := b.readyByTask[key.TaskID]
	if !ok {
		taskMap = make(map[int64]*readyEntry)
		b.readyByTask[key.TaskID] = taskMap
		b.taskOrder = append(b.taskOrder, key.TaskID)
	}
	taskMap[key.Offset] = &readyEntry{
		obj:     obj,
		addedAt: time.Now(),
	}

	b.cond.Broadcast()
	return nil
}

func (b *bucketImpl) ReadObject(key ObjectKey) ([]byte, error) {
	if b.cfg.Mode == ModeMemory {
		b.mu.RLock()
		data, ok := b.memData[key.String()]
		b.mu.RUnlock()
		if !ok {
			return nil, os.ErrNotExist
		}
		res := make([]byte, len(data))
		copy(res, data)
		return res, nil
	}

	absPath := filepath.Join(b.cfg.RootDir, key.RelPath(".ready"))
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != key.Length {
		return nil, fmt.Errorf("corrupt object length %d (expected %d)", len(data), key.Length)
	}
	if crc32.ChecksumIEEE(data) != key.Checksum {
		return nil, errors.New("crc32 checksum verification failed")
	}
	return data, nil
}

func (b *bucketImpl) TryTakeNext(taskID string, nextOffset int64) (*BufferObject, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	taskMap, ok := b.readyByTask[taskID]
	if !ok {
		return nil, false
	}
	entry, ok := taskMap[nextOffset]
	if !ok {
		return nil, false
	}

	delete(taskMap, nextOffset)
	if len(taskMap) == 0 {
		delete(b.readyByTask, taskID)
		b.removeTaskOrderLocked(taskID)
	}

	// Move readyBytes -> pendingDeleteBytes
	if b.readyBytes >= entry.obj.Key.Length {
		b.readyBytes -= entry.obj.Key.Length
	}
	b.pendingDeleteBytes += entry.obj.Key.Length

	return &entry.obj, true
}

func (b *bucketImpl) TakeReady() (*BufferObject, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.readyByTask) == 0 {
		return nil, false
	}

	// Selection Policy:
	// 1. Task that has complete single-chunk or lowest frontier offset
	// 2. FIFO task order
	for _, taskID := range b.taskOrder {
		taskMap, ok := b.readyByTask[taskID]
		if !ok || len(taskMap) == 0 {
			continue
		}

		// Find lowest offset in this task
		var lowestOffset int64 = -1
		for off := range taskMap {
			if lowestOffset == -1 || off < lowestOffset {
				lowestOffset = off
			}
		}

		entry := taskMap[lowestOffset]
		delete(taskMap, lowestOffset)
		if len(taskMap) == 0 {
			delete(b.readyByTask, taskID)
			b.removeTaskOrderLocked(taskID)
		}

		if b.readyBytes >= entry.obj.Key.Length {
			b.readyBytes -= entry.obj.Key.Length
		}
		b.pendingDeleteBytes += entry.obj.Key.Length

		return &entry.obj, true
	}

	return nil, false
}

func (b *bucketImpl) removeTaskOrderLocked(taskID string) {
	for i, id := range b.taskOrder {
		if id == taskID {
			b.taskOrder = append(b.taskOrder[:i], b.taskOrder[i+1:]...)
			return
		}
	}
}

func (b *bucketImpl) AckDurable(keys []ObjectKey) error {
	return b.DeleteObjects(keys)
}

func (b *bucketImpl) DeleteObjects(keys []ObjectKey) error {
	if len(keys) == 0 {
		return nil
	}

	var totalFreed int64
	for _, key := range keys {
		totalFreed += key.Length
		if b.cfg.Mode == ModeMemory {
			b.mu.Lock()
			delete(b.memData, key.String())
			b.mu.Unlock()
		} else if b.cfg.Mode == ModeSSD {
			absPath := filepath.Join(b.cfg.RootDir, key.RelPath(".ready"))
			_ = os.Remove(absPath)
			partPath := filepath.Join(b.cfg.RootDir, key.RelPath(".partial"))
			_ = os.Remove(partPath)
		}
	}

	b.mu.Lock()
	if b.pendingDeleteBytes >= totalFreed {
		b.pendingDeleteBytes -= totalFreed
	} else {
		b.pendingDeleteBytes = 0
	}
	if b.objectCount >= int64(len(keys)) {
		b.objectCount -= int64(len(keys))
	} else {
		b.objectCount = 0
	}
	b.cond.Broadcast()
	b.mu.Unlock()

	return nil
}

func (b *bucketImpl) Recover(ctx context.Context) error {
	if b.cfg.Mode != ModeSSD || b.cfg.RootDir == "" {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Walk root dir and load valid .ready objects into index
	return filepath.Walk(b.cfg.RootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".partial" {
			// Clean stale uncommitted partials
			_ = os.Remove(path)
			return nil
		}
		if filepath.Ext(path) != ".ready" {
			return nil
		}

		// Read and verify object
		data, err := os.ReadFile(path)
		if err != nil {
			_ = os.Remove(path)
			return nil
		}

		var offset, length int64
		var checksum uint32
		base := filepath.Base(path)
		_, scanErr := fmt.Sscanf(base, "%d-%d-%08x.ready", &offset, &length, &checksum)
		if scanErr != nil || int64(len(data)) != length || crc32.ChecksumIEEE(data) != checksum {
			_ = os.Remove(path)
			return nil
		}

		// Parse TaskID and Gen from parent directories
		// Path: RootDir/<TaskID>/<Gen>/group_X/<file>.ready
		rel, _ := filepath.Rel(b.cfg.RootDir, path)
		parts := splitPath(rel)
		if len(parts) < 4 {
			return nil
		}
		taskID := parts[0]
		gen := parts[1]

		key := ObjectKey{
			TaskID:   taskID,
			Gen:      gen,
			Offset:   offset,
			Length:   length,
			Checksum: checksum,
		}

		obj := BufferObject{
			Key:      key,
			DiskPath: path,
		}

		taskMap, ok := b.readyByTask[taskID]
		if !ok {
			taskMap = make(map[int64]*readyEntry)
			b.readyByTask[taskID] = taskMap
			b.taskOrder = append(b.taskOrder, taskID)
		}
		taskMap[offset] = &readyEntry{
			obj:     obj,
			addedAt: info.ModTime(),
		}
		b.readyBytes += length
		b.objectCount++
		return nil
	})
}

func splitPath(path string) []string {
	var parts []string
	clean := filepath.Clean(path)
	for clean != "." && clean != "/" && clean != "" {
		dir, file := filepath.Split(clean)
		if file != "" {
			parts = append([]string{file}, parts...)
		}
		clean = filepath.Clean(dir)
	}
	return parts
}

func (b *bucketImpl) Close() error {
	atomic.StoreInt32(&b.closed, 1)
	b.mu.Lock()
	b.cond.Broadcast()
	b.mu.Unlock()
	return nil
}
