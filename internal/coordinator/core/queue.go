package core

import (
	"container/heap"
	"errors"
	"sync"
)

// TaskPriority defines task urgency levels (lower value means higher priority).
type TaskPriority int

const (
	TaskPriorityHigh   TaskPriority = 0
	TaskPriorityMedium TaskPriority = 1
	TaskPriorityLow    TaskPriority = 2
)

// ErrQueueEmpty is returned when Pop() or Top() is called on an empty queue.
var ErrQueueEmpty = errors.New("priority queue is empty")

// TaskPriorityQueue is a thread-safe min-heap for tasks, popping highest-priority tasks first.
// Tasks with the same priority are served in FIFO order. Reduce tasks have the lowest priority.
type TaskPriorityQueue interface {
	Push(task *Task, priority TaskPriority) error
	Pop() (*Task, error)
	Top() (*Task, error)
	Len() int
}

type heapTaskPriorityQueue struct {
	pq       priorityQueue
	mu       sync.RWMutex
	sequence uint64
}

func NewTaskPriorityQueue() TaskPriorityQueue {
	pq := make(priorityQueue, 0)
	heap.Init(&pq)
	return &heapTaskPriorityQueue{pq: pq}
}

func (tpq *heapTaskPriorityQueue) Push(task *Task, priority TaskPriority) error {
	if task == nil {
		return errors.New("cannot push nil task")
	}

	tpq.mu.Lock()
	defer tpq.mu.Unlock()

	heap.Push(&tpq.pq, &item{
		task:     task,
		priority: priority,
		sequence: tpq.sequence,
	})
	tpq.sequence++
	return nil
}

func (tpq *heapTaskPriorityQueue) Pop() (*Task, error) {
	tpq.mu.Lock()
	defer tpq.mu.Unlock()

	if tpq.pq.Len() == 0 {
		return nil, ErrQueueEmpty
	}
	it := heap.Pop(&tpq.pq).(*item)
	return it.task, nil
}

func (tpq *heapTaskPriorityQueue) Top() (*Task, error) {
	tpq.mu.RLock()
	defer tpq.mu.RUnlock()
	if tpq.pq.Len() == 0 {
		return nil, ErrQueueEmpty
	}
	return tpq.pq[0].task, nil
}

func (tpq *heapTaskPriorityQueue) Len() int {
	tpq.mu.RLock()
	defer tpq.mu.RUnlock()
	return tpq.pq.Len()
}

// item wraps a Task with its priority, sequence number, and index in the heap.
type item struct {
	task     *Task
	priority TaskPriority
	sequence uint64 // Insertion order for FIFO within same priority
	index    int    // Required by heap.Interface
}

// priorityQueue satisfies heap.Interface.
type priorityQueue []*item

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	// Min-heap based on priority (lower value = higher priority)
	if pq[i].priority != pq[j].priority {
		return pq[i].priority < pq[j].priority
	}
	// If priorities are equal, maintain FIFO order (lower sequence = earlier)
	return pq[i].sequence < pq[j].sequence
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x any) {
	n := len(*pq)
	it := x.(*item)
	it.index = n
	*pq = append(*pq, it)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*pq = old[0 : n-1]
	return it
}
