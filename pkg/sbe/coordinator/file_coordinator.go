package coordinator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hittlert/TGX/pkg/sbe/lease"
	"github.com/Hittlert/TGX/pkg/sbe/meta"
	"github.com/bits-and-blooms/bitset"
)

type BlockState uint8

const (
	BlockStateMissing  BlockState = 0
	BlockStateInflight BlockState = 1
	BlockStateWritten  BlockState = 2
	BlockStateDurable  BlockState = 3
)

const (
	CheckpointThresholdBytes = 16 * 1024 * 1024 // 16 MiB
	CheckpointInterval       = 2 * time.Second
)

var (
	ErrFileFinalizing   = errors.New("file is finalizing, cannot begin new chunks")
	ErrFileClosed       = errors.New("file coordinator is closed")
	ErrBlockNotMissing  = errors.New("block is not in missing state")
	ErrBlockNotInflight = errors.New("block is not in inflight state")
	ErrIncompleteBlocks = errors.New("cannot finalize file: not all blocks are durable")
)

// FileCoordinator manages the lifecycle, concurrency, state machine and checkpoint loop for a single file.
type FileCoordinator struct {
	fileKey     string
	attemptID   [16]byte
	targetPath  string
	partPath    string
	totalSize   int64
	blockSize   uint32
	totalBlocks uint32

	dataFile *os.File
	metaFile *meta.MetaFile
	pool     *lease.Pool

	mu            sync.Mutex
	durableBitmap *bitset.BitSet
	writtenBitmap *bitset.BitSet
	blockStates   []BlockState

	activeWorkers int32
	activeWrites  int32
	dirtyBytes    int64

	isFinalizing bool
	isClosed     bool

	checkpointTrigger chan struct{}
	stopLoop          chan struct{}
	loopDone          chan struct{}
}

// Config defines the parameters to initialize a FileCoordinator.
type Config struct {
	FileKey           string
	AttemptID         [16]byte
	TargetDir         string
	FileName          string
	TotalSize         int64
	BlockSize         uint32
	SourceFingerprint uint64
	Pool              *lease.Pool
}

// NewFileCoordinator creates and initializes a FileCoordinator, opening .part and .meta files.
func NewFileCoordinator(cfg Config) (*FileCoordinator, *meta.MetaRecoverResult, error) {
	if cfg.BlockSize == 0 {
		cfg.BlockSize = meta.StandardBlockSize
	}
	totalBlocks := uint32((cfg.TotalSize + int64(cfg.BlockSize) - 1) / int64(cfg.BlockSize))
	if totalBlocks == 0 {
		totalBlocks = 1
	}

	attemptHex := fmt.Sprintf("%x", cfg.AttemptID)
	partName := fmt.Sprintf("%s.part.%s", cfg.FileName, attemptHex)
	partPath := filepath.Join(cfg.TargetDir, partName)
	targetPath := filepath.Join(cfg.TargetDir, cfg.FileName)

	// 1. Open / create .part file
	partFile, err := os.OpenFile(partPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open part file %s: %w", partPath, err)
	}

	// 2. Open / create .meta file
	metaH := &meta.MetaHeader{
		Magic:             meta.MetaMagic,
		Version:           meta.MetaVersion,
		SourceFingerprint: cfg.SourceFingerprint,
		AttemptID:         cfg.AttemptID,
		TotalSize:         cfg.TotalSize,
		BlockSize:         cfg.BlockSize,
		TotalBlocks:       totalBlocks,
	}
	copy(metaH.FileKeyHash[:], []byte(cfg.FileKey)) // Or hashed

	mf, rec, err := meta.CreateOrOpenMetaFile(cfg.TargetDir, cfg.FileName, metaH)
	if err != nil {
		partFile.Close()
		return nil, nil, fmt.Errorf("failed to open meta file: %w", err)
	}

	blockStates := make([]BlockState, totalBlocks)
	writtenBitmap := rec.DurableBitmap.Clone()

	// Populate initial block states from durableBitmap
	for i := uint(0); i < uint(totalBlocks); i++ {
		if rec.DurableBitmap.Test(i) {
			blockStates[i] = BlockStateDurable
		}
	}

	fc := &FileCoordinator{
		fileKey:           cfg.FileKey,
		attemptID:         cfg.AttemptID,
		targetPath:        targetPath,
		partPath:          partPath,
		totalSize:         cfg.TotalSize,
		blockSize:         cfg.BlockSize,
		totalBlocks:       totalBlocks,
		dataFile:          partFile,
		metaFile:          mf,
		pool:              cfg.Pool,
		durableBitmap:     rec.DurableBitmap.Clone(),
		writtenBitmap:     writtenBitmap,
		blockStates:       blockStates,
		checkpointTrigger: make(chan struct{}, 1),
		stopLoop:          make(chan struct{}),
		loopDone:          make(chan struct{}),
	}

	// Start single-writer CheckpointLoop
	go fc.checkpointLoop()

	return fc, rec, nil
}

// TotalBlocks returns the total block count.
func (fc *FileCoordinator) TotalBlocks() uint32 {
	return fc.totalBlocks
}

// DurableCount returns the number of persisted durable blocks.
func (fc *FileCoordinator) DurableCount() uint {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.durableBitmap.Count()
}

// IsComplete returns true if all blocks are durable.
func (fc *FileCoordinator) IsComplete() bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.durableBitmap.Count() == uint(fc.totalBlocks)
}

// NextMissingBlock finds the next unassigned block index, or returns false if none.
func (fc *FileCoordinator) NextMissingBlock() (uint32, bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.isFinalizing || fc.isClosed {
		return 0, false
	}

	for i := uint32(0); i < fc.totalBlocks; i++ {
		if fc.blockStates[i] == BlockStateMissing {
			return i, true
		}
	}
	return 0, false
}

// BeginChunk attempts to claim a block for network downloading.
func (fc *FileCoordinator) BeginChunk(index uint32) (offset int64, length int64, err error) {
	if index >= fc.totalBlocks {
		return 0, 0, fmt.Errorf("block index out of bounds: %d >= %d", index, fc.totalBlocks)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.isFinalizing {
		return 0, 0, ErrFileFinalizing
	}
	if fc.isClosed {
		return 0, 0, ErrFileClosed
	}

	if fc.blockStates[index] != BlockStateMissing {
		return 0, 0, ErrBlockNotMissing
	}

	fc.blockStates[index] = BlockStateInflight
	atomic.AddInt32(&fc.activeWorkers, 1)

	offset = int64(index) * int64(fc.blockSize)
	length = int64(fc.blockSize)
	if offset+length > fc.totalSize {
		length = fc.totalSize - offset
	}

	return offset, length, nil
}

// AbortChunk resets an in-flight block back to missing state (e.g. upon network error or cancellation).
func (fc *FileCoordinator) AbortChunk(index uint32) {
	if index >= fc.totalBlocks {
		return
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.blockStates[index] == BlockStateInflight {
		fc.blockStates[index] = BlockStateMissing
		atomic.AddInt32(&fc.activeWorkers, -1)
	}
}

// PrepareWrite increments activeWrites counter before a writer acquires DirtyLease and performs WriteAt.
func (fc *FileCoordinator) PrepareWrite() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.isClosed {
		return ErrFileClosed
	}
	atomic.AddInt32(&fc.activeWrites, 1)
	return nil
}

// CancelWrite decrements activeWrites counter if write is aborted before WriteAt.
func (fc *FileCoordinator) CancelWrite() {
	atomic.AddInt32(&fc.activeWrites, -1)
}

// MarkWritten is called by DiskWriter after successfully executing WriteAt.
func (fc *FileCoordinator) MarkWritten(index uint32, length int64, bufLease *lease.BufferLease) error {
	if index >= fc.totalBlocks {
		return fmt.Errorf("block index out of bounds: %d >= %d", index, fc.totalBlocks)
	}

	var triggerCheckpoint bool
	func() {
		fc.mu.Lock()
		defer fc.mu.Unlock()

		if fc.blockStates[index] == BlockStateInflight {
			fc.blockStates[index] = BlockStateWritten
			fc.writtenBitmap.Set(uint(index))
			fc.dirtyBytes += length
			atomic.AddInt32(&fc.activeWorkers, -1)
			atomic.AddInt32(&fc.activeWrites, -1)

			if fc.dirtyBytes >= CheckpointThresholdBytes {
				triggerCheckpoint = true
			}
		}
	}()

	// Release BufferLease immediately after WriteAt
	if bufLease != nil {
		bufLease.Release()
	}

	if triggerCheckpoint {
		select {
		case fc.checkpointTrigger <- struct{}{}:
		default:
		}
	}

	return nil
}

// WriteBlock performs the WriteAt operation with DirtyLease acquisition and MarkWritten state update.
func (fc *FileCoordinator) WriteBlock(ctx context.Context, index uint32, data []byte, bufLease *lease.BufferLease) error {
	if err := fc.PrepareWrite(); err != nil {
		return err
	}

	length := int64(len(data))
	offset := int64(index) * int64(fc.blockSize)

	// 1. Must acquire DirtyLease BEFORE WriteAt
	dirtyLease, err := fc.pool.AcquireDirty(ctx, length)
	if err != nil {
		fc.CancelWrite()
		return fmt.Errorf("failed to acquire dirty lease for block %d: %w", index, err)
	}
	_ = dirtyLease // Kept until flushed by CheckpointLoop

	// 2. Perform WriteAt
	if _, err := fc.dataFile.WriteAt(data, offset); err != nil {
		fc.CancelWrite()
		dirtyLease.Release()
		return fmt.Errorf("failed to write data to .part file at offset %d: %w", offset, err)
	}

	// 3. Mark Written and release BufferLease
	return fc.MarkWritten(index, length, bufLease)
}

// checkpointLoop is the sole serial CheckpointLoop goroutine per file.
func (fc *FileCoordinator) checkpointLoop() {
	defer close(fc.loopDone)
	ticker := time.NewTicker(CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fc.stopLoop:
			fc.doCheckpoint()
			return
		case <-ticker.C:
			fc.doCheckpoint()
		case <-fc.checkpointTrigger:
			fc.doCheckpoint()
		}
	}
}

// doCheckpoint executes the durable checkpoint sequence.
func (fc *FileCoordinator) doCheckpoint() {
	var (
		snapshot     *bitset.BitSet
		snapDirty    int64
		hasNewWrites bool
	)

	func() {
		fc.mu.Lock()
		defer fc.mu.Unlock()

		if fc.dirtyBytes > 0 || !fc.writtenBitmap.Equal(fc.durableBitmap) {
			snapshot = fc.writtenBitmap.Clone()
			snapDirty = fc.dirtyBytes
			hasNewWrites = true
		}
	}()

	if !hasNewWrites {
		return
	}

	// 1. Flush data file to disk (fdatasync)
	if err := fc.dataFile.Sync(); err != nil {
		return
	}

	// 2. Calculate next durable bitmap: oldDurable UNION writtenSnapshot
	var nextDurable *bitset.BitSet
	func() {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		nextDurable = fc.durableBitmap.Union(snapshot)
	}()

	// 3. Write next Generation slot to .meta and sync
	if _, err := fc.metaFile.WriteSlot(nextDurable); err != nil {
		return
	}

	// 4. Advance memory DurableBitmap and update block states
	func() {
		fc.mu.Lock()
		defer fc.mu.Unlock()

		fc.durableBitmap = nextDurable
		for i := uint(0); i < uint(fc.totalBlocks); i++ {
			if nextDurable.Test(i) {
				fc.blockStates[i] = BlockStateDurable
			}
		}
		fc.dirtyBytes -= snapDirty
		if fc.dirtyBytes < 0 {
			fc.dirtyBytes = 0
		}
	}()

	// 5. Release DirtyLease quota back to the pool
	if fc.pool != nil && snapDirty > 0 {
		fc.pool.ReleaseDirtyBytes(snapDirty)
	}
}

// Finalize performs the FINALIZING lifecycle gate, writes COMPLETE meta, and closes file handles.
func (fc *FileCoordinator) Finalize(ctx context.Context) error {
	// 1. Enter FINALIZING state
	fc.mu.Lock()
	fc.isFinalizing = true
	fc.mu.Unlock()

	// 2. Wait for all in-flight workers and writes to drain
	for {
		workers := atomic.LoadInt32(&fc.activeWorkers)
		writes := atomic.LoadInt32(&fc.activeWrites)
		if workers == 0 && writes == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}

	// 3. Stop background CheckpointLoop
	select {
	case <-fc.stopLoop:
	default:
		close(fc.stopLoop)
	}
	<-fc.loopDone

	// 4. Force final sync
	fc.doCheckpoint()

	// 5. Verify 100% durable
	fc.mu.Lock()
	isFull := (fc.durableBitmap.Count() == uint(fc.totalBlocks))
	fullBitmap := fc.durableBitmap.Clone()
	fc.mu.Unlock()

	if !isFull {
		return ErrIncompleteBlocks
	}

	// 6. Write COMPLETE slot to .meta
	if err := fc.metaFile.WriteComplete(fullBitmap); err != nil {
		return fmt.Errorf("failed to write complete meta slot: %w", err)
	}

	// 7. Close data and meta file descriptors
	_ = fc.dataFile.Sync()
	_ = fc.dataFile.Close()
	_ = fc.metaFile.Close()

	fc.mu.Lock()
	fc.isClosed = true
	fc.mu.Unlock()

	return nil
}

// Close safely closes the coordinator and stops background tasks.
func (fc *FileCoordinator) Close() error {
	fc.mu.Lock()
	if fc.isClosed {
		fc.mu.Unlock()
		return nil
	}
	fc.isClosed = true
	fc.mu.Unlock()

	select {
	case <-fc.stopLoop:
	default:
		close(fc.stopLoop)
	}
	<-fc.loopDone

	_ = fc.dataFile.Close()
	_ = fc.metaFile.Close()
	return nil
}

// PartPath returns the temporary .part file path.
func (fc *FileCoordinator) PartPath() string {
	return fc.partPath
}

// TargetPath returns the final destination file path.
func (fc *FileCoordinator) TargetPath() string {
	return fc.targetPath
}

// MetaPath returns the .meta sidecar file path.
func (fc *FileCoordinator) MetaPath() string {
	return fc.metaFile.Path()
}
