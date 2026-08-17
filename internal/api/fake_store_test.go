package api

import (
	"errors"
	"sync"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

var _ Store = (*fakeStore)(nil)

// errBoom is a sentinel error injected via fakeStore's *Err fields to
// exercise handlers' 500 paths.
var errBoom = errors.New("boom")

// fakeStore is a minimal in-memory Store for handler-level unit tests. It
// doesn't replicate sqlite's dequeue/priority ordering -- only what
// handlers need: keyed lookup, status-filtered listing, and
// pending/retrying-gated cancellation.
type fakeStore struct {
	mu   sync.Mutex
	jobs map[string]*job.Job

	// enqueueErr/listErr/statsErr, when set, make the corresponding method
	// return that error instead of its normal result, so tests can
	// exercise the handlers' 500 paths.
	enqueueErr error
	listErr    error
	statsErr   error
}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: make(map[string]*job.Job)}
}

func (f *fakeStore) Enqueue(j *job.Job) error {
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if j.IdempotencyKey != "" {
		for _, existing := range f.jobs {
			if existing.Queue == j.Queue && existing.IdempotencyKey == j.IdempotencyKey {
				*j = *existing
				return nil
			}
		}
	}

	f.jobs[j.ID] = j
	return nil
}

func (f *fakeStore) Get(id string) (*job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return nil, queue.ErrNotFound
	}
	cp := *j
	return &cp, nil
}

func (f *fakeStore) List(status job.Status, limit int) ([]*job.Job, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []*job.Job
	for _, j := range f.jobs {
		if j.Status != status {
			continue
		}
		cp := *j
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeStore) Cancel(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	j, ok := f.jobs[id]
	if !ok {
		return queue.ErrNotFound
	}
	if j.Status != job.StatusPending && j.Status != job.StatusRetrying {
		return queue.ErrNotCancelable
	}
	j.Status = job.StatusCancelled
	return nil
}

func (f *fakeStore) Retry(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	j, ok := f.jobs[id]
	if !ok {
		return queue.ErrNotFound
	}
	if j.Status != job.StatusDead && j.Status != job.StatusFailed {
		return queue.ErrNotRetryable
	}
	j.Status = job.StatusPending
	j.RetryCount = 0
	j.LastError = ""
	return nil
}

func (f *fakeStore) Stats() (map[job.Status]int, error) {
	if f.statsErr != nil {
		return nil, f.statsErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	stats := make(map[job.Status]int)
	for _, j := range f.jobs {
		stats[j.Status]++
	}
	return stats, nil
}
