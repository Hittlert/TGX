package writeback

import (
	"crypto/sha256"
	"errors"
	"hash"
	"sync"
	"time"

	"github.com/Hittlert/TGX/pkg/spool"
)

var (
	ErrTargetConflict  = errors.New("target file exists with mismatched identity or content")
	ErrStaleGeneration = errors.New("writeback item generation is stale")
	ErrIncompleteFile  = errors.New("file is incomplete, cannot finalize")
)

// Item represents a segment or whole file ready for write-back to target storage.
type Item struct {
	Key              spool.SegmentKey
	FinalRelPath     string
	ExpectedFileSize int64
	IsLastSegment    bool
	FileDate         int64
	Item             *spool.SpoolItem
	AddedAt          time.Time
	Attempts         int
	NextRetryAt      time.Time
}

// Callbacks delivers notification hooks when write-back milestones are reached.
type Callbacks struct {
	OnSegmentDurable func(key spool.SegmentKey, durableBytes int64)
	OnTaskFinalized  func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error)
}

// TaskWriteContext tracks streaming writes, open target handles, and sequential incremental SHA256.
type TaskWriteContext struct {
	mu               sync.Mutex
	TaskID           string
	Gen              string
	FinalRelPath     string
	ExpectedSize     int64
	FileDate         int64
	MovingPath       string
	FinalPath        string
	Hasher           hash.Hash
	NextSegmentIndex int
	NextOffset       int64
	WrittenBytes     int64
	DurableRanges    *spool.RangeSet
	PendingSegments  map[int]*Item
	Closed           bool
}

func NewTaskWriteContext(taskID, gen, finalRelPath, movingPath, finalPath string, expectedSize, fileDate int64) *TaskWriteContext {
	return &TaskWriteContext{
		TaskID:          taskID,
		Gen:             gen,
		FinalRelPath:    finalRelPath,
		MovingPath:      movingPath,
		FinalPath:       finalPath,
		ExpectedSize:    expectedSize,
		FileDate:        fileDate,
		Hasher:          sha256.New(),
		DurableRanges:   spool.NewRangeSet(),
		PendingSegments: make(map[int]*Item),
	}
}

// Config defines the tuning knobs for WriteBack queue and TargetSink.
type Config struct {
	OutputDir   string
	Concurrency int // Number of concurrent target writers (default: 5)
	BatchSync   bool
	SyncPeriod  time.Duration
}

func DefaultConfig(outputDir string) Config {
	return Config{
		OutputDir:   outputDir,
		Concurrency: 5,
		BatchSync:   true,
		SyncPeriod:  500 * time.Millisecond,
	}
}
