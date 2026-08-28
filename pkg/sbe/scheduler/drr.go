package scheduler

import (
	"sync"
	"time"

	"github.com/Hittlert/TGX/pkg/sbe/coordinator"
)

const (
	SmallFileThreshold = 10 * 1024 * 1024 // 10 MiB
	SmallQuantum       = 3                // 3 chunk tasks per round
	LargeQuantum       = 1                // 1 chunk task per round
	ActiveLargeFiles   = 12               // Global max active large file sessions
	MaxInflightPerFile = 16               // Peak single-file inflight limit
	MinInflightPerFile = 4                // Floor inflight limit
	TotalWorkerPoolCap = 64               // Global logical network workers
)

// ChunkTask represents an individual block task dispatched to network workers.
type ChunkTask struct {
	FileKey     string
	AttemptID   [16]byte
	BlockIndex  uint32
	Offset      int64
	Length      int64
	TotalSize   int64
	Coordinator *coordinator.FileCoordinator
}

// DRRScheduler implements Work-Conserving Deficit Round Robin dual-lane scheduling.
type DRRScheduler struct {
	mu sync.Mutex

	smallQueue []ChunkTask
	largeQueue []ChunkTask
	timerQueue *TimerQueue

	smallDeficit int
	largeDeficit int

	activeLargeMap map[string]struct{}
	inflightMap    map[string]int // fileKey -> current in-flight chunk count
}

// NewDRRScheduler creates a new dual-lane DRR scheduler.
func NewDRRScheduler() *DRRScheduler {
	return &DRRScheduler{
		smallQueue:     make([]ChunkTask, 0, 128),
		largeQueue:     make([]ChunkTask, 0, 128),
		timerQueue:     NewTimerQueue(),
		smallDeficit:   0,
		largeDeficit:   0,
		activeLargeMap: make(map[string]struct{}),
		inflightMap:    make(map[string]int),
	}
}

// IsSmallFile returns true if total file size <= 10 MiB.
func IsSmallFile(totalSize int64) bool {
	return totalSize <= SmallFileThreshold
}

// CalculateInflightCap computes the dynamic adaptive in-flight chunk limit for large files.
func CalculateInflightCap(activeLargeCount int) int {
	if activeLargeCount <= 0 {
		return MaxInflightPerFile
	}
	val := TotalWorkerPoolCap / activeLargeCount
	if val > MaxInflightPerFile {
		return MaxInflightPerFile
	}
	if val < MinInflightPerFile {
		return MinInflightPerFile
	}
	return val
}

// Enqueue pushes a new chunk task into its corresponding lane.
func (s *DRRScheduler) Enqueue(task ChunkTask) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if IsSmallFile(task.TotalSize) {
		s.smallQueue = append(s.smallQueue, task)
	} else {
		s.largeQueue = append(s.largeQueue, task)
		s.activeLargeMap[task.FileKey] = struct{}{}
	}
}

// EnqueueFront pushes a high-priority / retried chunk task directly to the head of its lane.
func (s *DRRScheduler) EnqueueFront(task ChunkTask) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if IsSmallFile(task.TotalSize) {
		s.smallQueue = append([]ChunkTask{task}, s.smallQueue...)
	} else {
		s.largeQueue = append([]ChunkTask{task}, s.largeQueue...)
		s.activeLargeMap[task.FileKey] = struct{}{}
	}
}

// EnqueueDelay pushes a task into the TimerQueue (e.g. upon FloodWait).
func (s *DRRScheduler) EnqueueDelay(task ChunkTask, notBefore time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timerQueue.Push(task, notBefore)
}

// PopReadyDelayed pops all expired delayed tasks back to the front of their respective queues.
func (s *DRRScheduler) PopReadyDelayed(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	ready := s.timerQueue.PopReady(now)
	for _, task := range ready {
		if IsSmallFile(task.TotalSize) {
			s.smallQueue = append([]ChunkTask{task}, s.smallQueue...)
		} else {
			s.largeQueue = append([]ChunkTask{task}, s.largeQueue...)
			s.activeLargeMap[task.FileKey] = struct{}{}
		}
	}
	return len(ready)
}

// NextChunk selects and pops the next ChunkTask according to DRR (Small:Large = 3:1) and per-file inflight limits.
func (s *DRRScheduler) NextChunk() (ChunkTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Flush ready delayed tasks
	if ready := s.timerQueue.PopReady(time.Now()); len(ready) > 0 {
		for _, task := range ready {
			if IsSmallFile(task.TotalSize) {
				s.smallQueue = append([]ChunkTask{task}, s.smallQueue...)
			} else {
				s.largeQueue = append([]ChunkTask{task}, s.largeQueue...)
				s.activeLargeMap[task.FileKey] = struct{}{}
			}
		}
	}

	hasSmall := len(s.smallQueue) > 0
	hasLarge := len(s.largeQueue) > 0

	if !hasSmall && !hasLarge {
		return ChunkTask{}, false
	}

	// 2. Work-Conserving DRR quantum recharge
	if s.smallDeficit <= 0 && s.largeDeficit <= 0 {
		if hasSmall {
			s.smallDeficit += SmallQuantum
		}
		if hasLarge {
			s.largeDeficit += LargeQuantum
		}
	}

	// 3. Try Small Lane first if deficit available
	if hasSmall && s.smallDeficit > 0 {
		task := s.smallQueue[0]
		s.smallQueue = s.smallQueue[1:]
		s.smallDeficit--
		s.inflightMap[task.FileKey]++
		return task, true
	}

	// 4. Try Large Lane with inflight cap
	if hasLarge {
		capLimit := CalculateInflightCap(len(s.activeLargeMap))
		for i, task := range s.largeQueue {
			curInflight := s.inflightMap[task.FileKey]
			if curInflight < capLimit {
				// Found eligible large task
				s.largeQueue = append(s.largeQueue[:i], s.largeQueue[i+1:]...)
				if s.largeDeficit > 0 {
					s.largeDeficit--
				}
				s.inflightMap[task.FileKey]++
				return task, true
			}
		}
	}

	// 5. If large lane was capped or empty, fallback to small lane if items exist
	if hasSmall {
		task := s.smallQueue[0]
		s.smallQueue = s.smallQueue[1:]
		s.smallDeficit = 0
		s.inflightMap[task.FileKey]++
		return task, true
	}

	return ChunkTask{}, false
}

// CompleteChunk decrements the in-flight counter for a given file.
func (s *DRRScheduler) CompleteChunk(fileKey string, isFileFinished bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.inflightMap[fileKey]--
	if s.inflightMap[fileKey] <= 0 {
		delete(s.inflightMap, fileKey)
	}

	if isFileFinished {
		delete(s.activeLargeMap, fileKey)
	}
}

// Len returns the current total queued chunk count across all lanes.
func (s *DRRScheduler) Len() (small int, large int, delayed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.smallQueue), len(s.largeQueue), s.timerQueue.Len()
}
