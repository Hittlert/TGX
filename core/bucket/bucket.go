package bucket

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
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
	ReadyTasksCount    int    `json:"ready_tasks_count"`
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
	Requeue(obj *BufferObject)
	AckDurable(keys []ObjectKey) error
	DeleteObjects(keys []ObjectKey) error
	SetTaskGeneration(taskID, gen string)
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

	reservedBytes      int64
	readyBytes         int64
	pendingDeleteBytes int64
	objectCount        int64

	// Authoritative generation per task: taskID -> activeGen
	currentTaskGen map[string]string

	// Index: taskID -> map[offset]*readyEntry
	readyByTask   map[string]map[int64]*readyEntry
	// FIFO / priority list of tasks
	taskOrder     []string
	memData       map[string][]byte // For ModeMemory: key.String() -> []byte
	waiters       []chan struct{}

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
		cfg:            cfg,
		currentTaskGen: make(map[string]string),
		readyByTask:    make(map[string]map[int64]*readyEntry),
		memData:        make(map[string][]byte),
	}
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
		ReadyTasksCount:    len(b.readyByTask),
		Backpressured:      used >= b.cfg.MaxCapacity,
	}
}

func (b *bucketImpl) Reserve(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	b.mu.Lock()

	for {
		if atomic.LoadInt32(&b.closed) == 1 {
			b.mu.Unlock()
			return errors.New("bucket is closed")
		}
		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return err
		}

		currentUsed := b.reservedBytes + b.readyBytes + b.pendingDeleteBytes
		// Strict hard capacity without currentUsed == 0 bypass
		if currentUsed+bytes <= b.cfg.MaxCapacity {
			b.reservedBytes += bytes
			b.mu.Unlock()
			return nil
		}

		waitCh := make(chan struct{}, 1)
		b.waiters = append(b.waiters, waitCh)

		b.mu.Unlock()
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.removeWaiterLocked(waitCh)
			b.mu.Unlock()
			return ctx.Err()
		case <-waitCh:
			b.mu.Lock()
		}
	}
}

func (b *bucketImpl) removeWaiterLocked(ch chan struct{}) {
	for i, w := range b.waiters {
		if w == ch {
			b.waiters = append(b.waiters[:i], b.waiters[i+1:]...)
			return
		}
	}
}

func (b *bucketImpl) notifyWaitersLocked() {
	for _, ch := range b.waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	b.waiters = nil
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
	b.notifyWaitersLocked()
	b.mu.Unlock()
}

// SetTaskGeneration establishes the authoritative active generation for a task.
// Any buffered objects from prior generations for this task are purged to release capacity.
func (b *bucketImpl) SetTaskGeneration(taskID, gen string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.currentTaskGen[taskID] = gen

	taskMap, ok := b.readyByTask[taskID]
	if !ok {
		return
	}

	var toDelete []int64
	for offset, entry := range taskMap {
		if entry.obj.Key.Gen != gen {
			if b.readyBytes >= entry.obj.Key.Length {
				b.readyBytes -= entry.obj.Key.Length
			}
			if b.objectCount > 0 {
				b.objectCount--
			}
			if b.cfg.Mode == ModeSSD && entry.obj.DiskPath != "" {
				_ = os.Remove(entry.obj.DiskPath)
			} else if b.cfg.Mode == ModeMemory {
				delete(b.memData, entry.obj.Key.String())
			}
			toDelete = append(toDelete, offset)
		}
	}
	for _, offset := range toDelete {
		delete(taskMap, offset)
	}
	if len(taskMap) == 0 {
		delete(b.readyByTask, taskID)
		b.removeTaskOrderLocked(taskID)
	}
	b.notifyWaitersLocked()
}

func (b *bucketImpl) PutObject(key ObjectKey, data []byte) error {
	if int64(len(data)) != key.Length {
		return fmt.Errorf("data length %d does not match key length %d", len(data), key.Length)
	}

	computedCRC := crc32.ChecksumIEEE(data)
	if key.Checksum == 0 {
		key.Checksum = computedCRC
	} else if key.Checksum != computedCRC {
		return fmt.Errorf("crc32 checksum mismatch: got %08x, expected %08x", computedCRC, key.Checksum)
	}

	b.mu.Lock()
	// 1. Authoritative generation guard: reject stale generation chunks immediately
	if currentGen, ok := b.currentTaskGen[key.TaskID]; ok && currentGen != "" && key.Gen != currentGen {
		if b.reservedBytes >= key.Length {
			b.reservedBytes -= key.Length
		}
		b.notifyWaitersLocked()
		b.mu.Unlock()
		return nil
	}

	// 2. Idempotency check: if exact duplicate already exists, release reservation and return
	if taskMap, ok := b.readyByTask[key.TaskID]; ok {
		if entry, exists := taskMap[key.Offset]; exists && entry.obj.Key.Gen == key.Gen && entry.obj.Key.Checksum == key.Checksum {
			if b.reservedBytes >= key.Length {
				b.reservedBytes -= key.Length
			}
			b.notifyWaitersLocked()
			b.mu.Unlock()
			return nil
		}
	}
	b.mu.Unlock()

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

	// Double-check authoritative generation to eliminate TOCTOU race:
	if currentGen, ok := b.currentTaskGen[key.TaskID]; ok && currentGen != "" && key.Gen != currentGen {
		if b.cfg.Mode == ModeSSD && obj.DiskPath != "" {
			_ = os.Remove(obj.DiskPath)
		} else if b.cfg.Mode == ModeMemory {
			delete(b.memData, key.String())
		}
		b.notifyWaitersLocked()
		return nil
	}

	if b.reservedBytes >= key.Length {
		b.reservedBytes -= key.Length
	} else {
		b.reservedBytes = 0
	}
	b.readyBytes += key.Length
	b.objectCount++

	taskMap, ok := b.readyByTask[key.TaskID]
	if !ok {
		taskMap = make(map[int64]*readyEntry)
		b.readyByTask[key.TaskID] = taskMap
		b.taskOrder = append(b.taskOrder, key.TaskID)
	}

	// If overwriting an existing entry (different gen/checksum at same offset),
	// reclaim the old entry's capacity and remove orphaned SSD file.
	if oldEntry, exists := taskMap[key.Offset]; exists {
		canRelease := true
		if b.cfg.Mode == ModeSSD && oldEntry.obj.DiskPath != "" {
			if err := os.Remove(oldEntry.obj.DiskPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				// Delete failed: keep capacity accounted to avoid phantom free space
				canRelease = false
			}
		} else if b.cfg.Mode == ModeMemory {
			delete(b.memData, oldEntry.obj.Key.String())
		}
		if canRelease {
			if b.readyBytes >= oldEntry.obj.Key.Length {
				b.readyBytes -= oldEntry.obj.Key.Length
			}
			if b.objectCount > 0 {
				b.objectCount--
			}
		}
	}

	taskMap[key.Offset] = &readyEntry{
		obj:     obj,
		addedAt: time.Now(),
	}

	b.notifyWaitersLocked()
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

	// Priority 1: Check for single-chunk complete small files
	for _, taskID := range b.taskOrder {
		taskMap, ok := b.readyByTask[taskID]
		if !ok || len(taskMap) == 0 {
			continue
		}
		for _, entry := range taskMap {
			if entry.obj.Key.ExpectedFileSize > 0 && entry.obj.Key.Length == entry.obj.Key.ExpectedFileSize {
				delete(taskMap, entry.obj.Key.Offset)
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
		}
	}

	// Priority 2: FIFO task with lowest offset
	for _, taskID := range b.taskOrder {
		taskMap, ok := b.readyByTask[taskID]
		if !ok || len(taskMap) == 0 {
			continue
		}

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

func (b *bucketImpl) Requeue(obj *BufferObject) {
	if obj == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pendingDeleteBytes >= obj.Key.Length {
		b.pendingDeleteBytes -= obj.Key.Length
	}
	b.readyBytes += obj.Key.Length

	taskMap, ok := b.readyByTask[obj.Key.TaskID]
	if !ok {
		taskMap = make(map[int64]*readyEntry)
		b.readyByTask[obj.Key.TaskID] = taskMap
		b.taskOrder = append(b.taskOrder, obj.Key.TaskID)
	}
	taskMap[obj.Key.Offset] = &readyEntry{
		obj:     *obj,
		addedAt: time.Now(),
	}

	b.notifyWaitersLocked()
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

	var freedBytes int64
	var freedCount int64
	var deleteErrs []error

	for _, key := range keys {
		if b.cfg.Mode == ModeMemory {
			b.mu.Lock()
			delete(b.memData, key.String())
			b.mu.Unlock()
			freedBytes += key.Length
			freedCount++
		} else if b.cfg.Mode == ModeSSD {
			absPath := filepath.Join(b.cfg.RootDir, key.RelPath(".ready"))
			err1 := os.Remove(absPath)
			partPath := filepath.Join(b.cfg.RootDir, key.RelPath(".partial"))
			_ = os.Remove(partPath)
			if err1 == nil || errors.Is(err1, os.ErrNotExist) {
				freedBytes += key.Length
				freedCount++
			} else {
				// Collect SSD delete failures to prevent permanent pendingDeleteBytes leak
				deleteErrs = append(deleteErrs, fmt.Errorf("remove %s: %w", absPath, err1))
			}
		}
	}

	b.mu.Lock()
	if b.pendingDeleteBytes >= freedBytes {
		b.pendingDeleteBytes -= freedBytes
	} else {
		b.pendingDeleteBytes = 0
	}
	if b.objectCount >= freedCount {
		b.objectCount -= freedCount
	} else {
		b.objectCount = 0
	}
	b.notifyWaitersLocked()
	b.mu.Unlock()

	if len(deleteErrs) > 0 {
		return fmt.Errorf("failed to delete %d/%d objects: %w", len(deleteErrs), len(keys), errors.Join(deleteErrs...))
	}
	return nil
}

func (b *bucketImpl) Recover(ctx context.Context) error {
	if b.cfg.Mode != ModeSSD || b.cfg.RootDir == "" {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return filepath.Walk(b.cfg.RootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".partial" {
			_ = os.Remove(path)
			return nil
		}
		if filepath.Ext(path) != ".ready" {
			return nil
		}

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

		if existing, exists := taskMap[offset]; exists {
			if isNewerGeneration(gen, existing.obj.Key.Gen) {
				// Scanned file is from a newer generation than previously encountered entry:
				// Delete older file on disk and replace entry in index
				_ = os.Remove(existing.obj.DiskPath)
				b.readyBytes -= existing.obj.Key.Length
				b.readyBytes += length
				taskMap[offset] = &readyEntry{
					obj:     obj,
					addedAt: info.ModTime(),
				}
			} else {
				// Scanned file is older or duplicate: remove orphan file
				_ = os.Remove(path)
			}
			return nil
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

// isNewerGeneration returns true if generation a is newer than generation b.
func isNewerGeneration(a, b string) bool {
	if a == b {
		return false
	}
	if a == "" {
		return false
	}
	if b == "" {
		return true
	}
	if a == "1" && strings.HasPrefix(b, "retry_") {
		return false
	}
	if b == "1" && strings.HasPrefix(a, "retry_") {
		return true
	}
	var tsA, tsB int64
	if n, _ := fmt.Sscanf(a, "retry_%d", &tsA); n == 1 {
		if n2, _ := fmt.Sscanf(b, "retry_%d", &tsB); n2 == 1 {
			return tsA > tsB
		}
	}
	return a > b
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
	b.notifyWaitersLocked()
	b.mu.Unlock()
	return nil
}
