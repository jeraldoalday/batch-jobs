package sqlite

import (
	"fmt"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// seedBacklog fills q with ready and delayed jobs, spread across three
// priorities so the claim query's ORDER BY has real work to do.
func seedBacklog(tb testing.TB, q *Queue, ready, delayed int) {
	tb.Helper()

	batch := make([]*job.Job, 0, 1000)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := q.EnqueueBatch(batch); err != nil {
			tb.Fatalf("EnqueueBatch: %v", err)
		}
		batch = batch[:0]
	}

	for i := 0; i < ready+delayed; i++ {
		j := job.New("bench", nil)
		j.Priority = job.Priority(i % 3)
		if i >= ready {
			j.RunAt = time.Now().Add(time.Hour)
		}
		batch = append(batch, j)
		if len(batch) == cap(batch) {
			flush()
		}
	}
	flush()

	if err := q.Optimize(); err != nil {
		tb.Fatalf("Optimize: %v", err)
	}
}

// BenchmarkClaimUnderBacklog measures the cost of one claim against
// backlogs of different sizes and shapes. It is the benchmark that matters
// most in this package: every worker runs this query, and it used to be
// proportional to the whole backlog because the ordering could not come
// from an index and SQLite sorted every ready job to take one of them.
//
// The delayed-heavy variants exist because they are the case the two
// dequeue indexes disagree about, and so the case where the query planner
// has to be given statistics good enough to choose between them.
func BenchmarkClaimUnderBacklog(b *testing.B) {
	shapes := []struct {
		name           string
		ready, delayed int
	}{
		{"Ready/1000", 1000, 0},
		{"Ready/10000", 10000, 0},
		{"Ready/50000", 50000, 0},
		{"MostlyDelayed/50000", 500, 50000},
	}

	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			q := benchQueue(b)
			seedBacklog(b, q, shape.ready, shape.delayed)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j, err := q.TryDequeue()
				if err != nil {
					b.Fatalf("TryDequeue: %v", err)
				}
				// Put it straight back, so the backlog stays the size the
				// benchmark is measuring rather than draining away.
				b.StopTimer()
				j.Status = job.StatusPending
				if err := q.Enqueue(j); err != nil {
					b.Fatalf("Enqueue: %v", err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkClaimBoundQueue measures the same claim restricted to one named
// queue, the path a queue.Router's routes take.
func BenchmarkClaimBoundQueue(b *testing.B) {
	q := benchQueue(b)

	// Several named queues, so the filter is actually excluding rows
	// rather than matching everything.
	batch := make([]*job.Job, 0, 1000)
	for i := 0; i < 20000; i++ {
		j := job.New(fmt.Sprintf("q%d", i%4), nil)
		j.Priority = job.Priority(i % 3)
		batch = append(batch, j)
		if len(batch) == cap(batch) {
			if err := q.EnqueueBatch(batch); err != nil {
				b.Fatalf("EnqueueBatch: %v", err)
			}
			batch = batch[:0]
		}
	}
	if err := q.EnqueueBatch(batch); err != nil {
		b.Fatalf("EnqueueBatch: %v", err)
	}
	if err := q.Optimize(); err != nil {
		b.Fatalf("Optimize: %v", err)
	}

	bound := q.Bind("q2")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j, err := bound.TryDequeue()
		if err != nil {
			b.Fatalf("TryDequeue: %v", err)
		}
		b.StopTimer()
		j.Status = job.StatusPending
		if err := q.Enqueue(j); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
		b.StartTimer()
	}
}
