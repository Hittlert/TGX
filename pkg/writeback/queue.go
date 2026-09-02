package writeback

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/Hittlert/TGX/pkg/spool"
)

// priorityItem wraps an Item with internal index for container/heap.
type priorityItem struct {
	item     *Item
	index    int
	priority int64 // lower score = higher priority
}

type priorityHeap []*priorityItem

func (h priorityHeap) Len() int           { return len(h) }
func (h priorityHeap) Less(i, j int) bool { return h[i].priority < h[j].priority }
func (h priorityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *priorityHeap) Push(x any) {
	n := len(*h)
	item := x.(*priorityItem)
	item.index = n
	*h = append(*h, item)
}
func (h *priorityHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// Queue provides a prioritized write-back work queue.
type Queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	pq     priorityHeap
	lookup map[string]*priorityItem // segmentKey.String() -> priorityItem
	closed bool
}

func NewQueue() *Queue {
	q := &Queue{
		pq:     make(priorityHeap, 0),
		lookup: make(map[string]*priorityItem),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// calculatePriority determines ordering:
// 1. Small files (Length <= SmallFileThreshold) get top priority (0..100)
// 2. Segment index lower is higher priority (e.g. index 0 > index 1)
// 3. FIFO fallback based on AddedAt
func calculatePriority(item *Item) int64 {
	base := item.AddedAt.UnixNano() / 1000000 // ms
	if item.ExpectedFileSize <= spool.SmallFileThreshold {
		return base / 10 // top lane
	}
	// Add segment index weighting to prefer contiguous writing
	return base + int64(item.Key.SegmentIndex)*1000
}

func (q *Queue) Enqueue(item *Item) {
	if item == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	keyStr := item.Key.String()
	if _, exists := q.lookup[keyStr]; exists {
		return
	}

	pItem := &priorityItem{
		item:     item,
		priority: calculatePriority(item),
	}

	heap.Push(&q.pq, pItem)
	q.lookup[keyStr] = pItem
	q.cond.Signal()
}

func (q *Queue) Dequeue(ctx context.Context) (*Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.closed {
			return nil, spool.ErrSpoolClosed
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if len(q.pq) > 0 {
			pItem := heap.Pop(&q.pq).(*priorityItem)
			delete(q.lookup, pItem.item.Key.String())
			return pItem.item, nil
		}

		// Wait for signal or context cancellation using AfterFunc
		stop := context.AfterFunc(ctx, func() {
			q.mu.Lock()
			q.cond.Broadcast()
			q.mu.Unlock()
		})
		q.cond.Wait()
		stop()
	}
}

func (q *Queue) Requeue(item *Item, delay time.Duration) {
	if item == nil {
		return
	}
	item.Attempts++
	item.NextRetryAt = time.Now().Add(delay)

	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		q.Enqueue(item)
	}()
}

func (q *Queue) Cancel(taskID, gen string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	newPQ := make(priorityHeap, 0, len(q.pq))
	for _, pItem := range q.pq {
		if pItem.item.Key.TaskID == taskID && (gen == "" || pItem.item.Key.Gen == gen) {
			delete(q.lookup, pItem.item.Key.String())
			continue
		}
		newPQ = append(newPQ, pItem)
	}
	q.pq = newPQ
	heap.Init(&q.pq)
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pq)
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}
