package queue

import "github.com/jeraldoalday/batch-jobs/internal/job"

// readyHeap is a container/heap of jobs that are eligible to run now,
// ordered so that higher-priority jobs are dequeued first and jobs of equal
// priority are dequeued in the order they were created (FIFO).
type readyHeap []*job.Job

func (h readyHeap) Len() int { return len(h) }

func (h readyHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *readyHeap) Push(x any) {
	*h = append(*h, x.(*job.Job))
}

func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// delayedHeap is a container/heap of jobs scheduled to run in the future,
// ordered by their RunAt time so the soonest-due job is always at the root.
type delayedHeap []*job.Job

func (h delayedHeap) Len() int { return len(h) }

func (h delayedHeap) Less(i, j int) bool {
	return h[i].RunAt.Before(h[j].RunAt)
}

func (h delayedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *delayedHeap) Push(x any) {
	*h = append(*h, x.(*job.Job))
}

func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}
