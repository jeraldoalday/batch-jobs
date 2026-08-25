package sqlite

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// BenchmarkEnqueueBatch measures the per-job cost of admitting jobs in one
// transaction against admitting them one at a time. The gap is the point
// of the API: on a durable backend the transaction around a write costs
// far more than the row does, so amortizing one commit over a batch is
// most of what there is to win.
//
// Both variants are reported per job, so the numbers are directly
// comparable.
func BenchmarkEnqueueBatch(b *testing.B) {
	sizes := []int{1, 10, 100, 1000}

	b.Run("OneAtATime", func(b *testing.B) {
		q := benchQueue(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := q.Enqueue(job.New("bench", nil)); err != nil {
				b.Fatalf("Enqueue: %v", err)
			}
		}
	})

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Batched/%d", size), func(b *testing.B) {
			q := benchQueue(b)
			batch := make([]*job.Job, size)

			b.ResetTimer()
			for i := 0; i < b.N; i += size {
				n := size
				if remaining := b.N - i; remaining < n {
					n = remaining
				}
				for k := 0; k < n; k++ {
					batch[k] = job.New("bench", nil)
				}
				if err := q.EnqueueBatch(batch[:n]); err != nil {
					b.Fatalf("EnqueueBatch: %v", err)
				}
			}
		})
	}
}

// BenchmarkEnqueueBatchIdempotent measures what the idempotency-key
// lookup adds per job inside a batch, since that path prepares and runs a
// second statement for every job that carries a key.
func BenchmarkEnqueueBatchIdempotent(b *testing.B) {
	const size = 100

	b.Run("NoKeys", func(b *testing.B) {
		benchBatchWithKeys(b, size, false)
	})
	b.Run("UniqueKeys", func(b *testing.B) {
		benchBatchWithKeys(b, size, true)
	})
}

func benchBatchWithKeys(b *testing.B, size int, keys bool) {
	q := benchQueue(b)
	batch := make([]*job.Job, size)

	b.ResetTimer()
	for i := 0; i < b.N; i += size {
		n := size
		if remaining := b.N - i; remaining < n {
			n = remaining
		}
		for k := 0; k < n; k++ {
			j := job.New("bench", nil)
			if keys {
				j.IdempotencyKey = fmt.Sprintf("key-%d-%d", i, k)
			}
			batch[k] = j
		}
		if err := q.EnqueueBatch(batch[:n]); err != nil {
			b.Fatalf("EnqueueBatch: %v", err)
		}
	}
}

// benchQueue opens a file-backed queue in a temporary directory. Batching
// is about commit costs, so an in-memory database would measure the wrong
// thing.
func benchQueue(b *testing.B) *Queue {
	b.Helper()
	q, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { q.Close() })
	return q
}
