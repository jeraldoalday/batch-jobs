package sqlite

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// openBenchQueue opens a file-backed queue, which is what a deployment
// actually runs: an in-memory database would flatter every number here by
// removing the disk entirely.
func openBenchQueue(b *testing.B, opts Options) *Queue {
	b.Helper()

	q, err := Open(filepath.Join(b.TempDir(), "bench.db"), opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { q.Close() })
	return q
}

// BenchmarkHeartbeat measures the per-renewal cost, which sets the floor on
// how short a lease TTL can sensibly be: every running job pays this once
// per heartbeat interval.
func BenchmarkHeartbeat(b *testing.B) {
	q := openBenchQueue(b, Options{LeaseTTL: time.Hour})

	j := job.New("q", []byte("payload"), job.WithMaxRetries(3))
	if err := q.Enqueue(j); err != nil {
		b.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		b.Fatalf("TryDequeue: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Heartbeat(j.ID); err != nil {
			b.Fatalf("Heartbeat: %v", err)
		}
	}
}

// BenchmarkRecoverNoOrphans measures the sweep a healthy system runs over
// and over: the reaper fires on every interval whether or not anything
// died, so the empty case is the one that has to be cheap.
func BenchmarkRecoverNoOrphans(b *testing.B) {
	q := openBenchQueue(b, Options{LeaseTTL: time.Hour})

	for i := 0; i < 100; i++ {
		if err := q.Enqueue(job.New("q", []byte("payload"))); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}
	for i := 0; i < 10; i++ {
		if _, err := q.TryDequeue(); err != nil {
			b.Fatalf("TryDequeue: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, err := q.Recover()
		if err != nil {
			b.Fatalf("Recover: %v", err)
		}
		if rec.Total() != 0 {
			b.Fatalf("Recover reclaimed %d live claims", rec.Total())
		}
	}
}

// BenchmarkRecoverOrphans measures a sweep that finds work, at a few
// different orphan counts: the interesting question is whether the cost
// tracks the number of orphans (set-based UPDATEs) or the size of the
// table.
func BenchmarkRecoverOrphans(b *testing.B) {
	for _, orphans := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("orphans=%d", orphans), func(b *testing.B) {
			q := openBenchQueue(b, Options{LeaseTTL: time.Nanosecond})

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := 0; j < orphans; j++ {
					if err := q.Enqueue(job.New("q", []byte("payload"), job.WithMaxRetries(1))); err != nil {
						b.Fatalf("Enqueue: %v", err)
					}
				}
				for j := 0; j < orphans; j++ {
					if _, err := q.TryDequeue(); err != nil {
						b.Fatalf("TryDequeue: %v", err)
					}
				}
				b.StartTimer()

				rec, err := q.Recover()
				if err != nil {
					b.Fatalf("Recover: %v", err)
				}
				if rec.Total() != orphans {
					b.Fatalf("Recover reclaimed %d jobs, want %d", rec.Total(), orphans)
				}
			}
		})
	}
}

// BenchmarkCompleteJob measures the write-back every finished job now pays,
// against the claim it pairs with.
func BenchmarkCompleteJob(b *testing.B) {
	q := openBenchQueue(b, Options{LeaseTTL: time.Hour})

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		j := job.New("q", []byte("payload"))
		if err := q.Enqueue(j); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
		claimed, err := q.TryDequeue()
		if err != nil {
			b.Fatalf("TryDequeue: %v", err)
		}
		claimed.MarkSucceeded(time.Now().UTC())
		b.StartTimer()

		if err := q.Complete(claimed); err != nil {
			b.Fatalf("Complete: %v", err)
		}
	}
}
