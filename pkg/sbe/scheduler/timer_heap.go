package scheduler

import (
	"container/heap"
	"time"
)

// DelayedChunk represents a task that is backed off until notBefore time.
type DelayedChunk struct {
	Chunk     ChunkTask
	NotBefore time.Time
	index     int
}

type delayHeap []*DelayedChunk

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].NotBefore.Before(h[j].NotBefore) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }

func (h *delayHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*DelayedChunk)
	item.index = n
	*h = append(*h, item)
}

func (h *delayHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// TimerQueue wraps delayHeap with safe methods.
type TimerQueue struct {
	h delayHeap
}

// NewTimerQueue creates an empty delay timer queue.
func NewTimerQueue() *TimerQueue {
	tq := &TimerQueue{h: make(delayHeap, 0)}
	heap.Init(&tq.h)
	return tq
}

// Push adds a chunk with its notBefore timestamp.
func (tq *TimerQueue) Push(chunk ChunkTask, notBefore time.Time) {
	item := &DelayedChunk{
		Chunk:     chunk,
		NotBefore: notBefore,
	}
	heap.Push(&tq.h, item)
}

// PeekNotBefore returns the earliest notBefore timestamp if non-empty.
func (tq *TimerQueue) PeekNotBefore() (time.Time, bool) {
	if len(tq.h) == 0 {
		return time.Time{}, false
	}
	return tq.h[0].NotBefore, true
}

// PopReady pops all tasks whose notBefore is <= now.
func (tq *TimerQueue) PopReady(now time.Time) []ChunkTask {
	var ready []ChunkTask
	for len(tq.h) > 0 && !tq.h[0].NotBefore.After(now) {
		item := heap.Pop(&tq.h).(*DelayedChunk)
		ready = append(ready, item.Chunk)
	}
	return ready
}

// Len returns the current length of the delayed queue.
func (tq *TimerQueue) Len() int {
	return len(tq.h)
}
