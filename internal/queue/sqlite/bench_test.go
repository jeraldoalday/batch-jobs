package sqlite

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// BenchmarkSQLiteEnqueueDequeue measures the cost of the round trip this
// package adds over queue.Memory: a durable, file-backed database instead
// of an in-process heap. See BenchmarkMemoryEnqueueDequeue in the queue
// package for the baseline this should be compared against.
func BenchmarkSQLiteEnqueueDequeue(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.db")
	q, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer q.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Enqueue(job.New("bench", nil)); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
		if _, err := q.TryDequeue(); err != nil {
			b.Fatalf("TryDequeue: %v", err)
		}
	}
}

// BenchmarkSQLiteEnqueueDequeueInMemory isolates disk I/O out of the
// comparison by using an in-memory SQLite database, showing how much of
// BenchmarkSQLiteEnqueueDequeue's overhead is the query/serialization path
// itself versus fsync/WAL checkpointing.
func BenchmarkSQLiteEnqueueDequeueInMemory(b *testing.B) {
	q, err := Open("file:" + b.Name() + "?mode=memory&cache=shared")
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer q.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Enqueue(job.New("bench", nil)); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
		if _, err := q.TryDequeue(); err != nil {
			b.Fatalf("TryDequeue: %v", err)
		}
	}
}

// BenchmarkSQLiteEnqueueIdempotencyKey measures the overhead the
// idempotency-key fast path (a SELECT before the INSERT) adds over a plain
// Enqueue, both for the common case where every key is new and for the
// dedupe case where every Enqueue after the first is a no-op.
func BenchmarkSQLiteEnqueueIdempotencyKey(b *testing.B) {
	b.Run("UniqueKeys", func(b *testing.B) {
		q, err := Open("file:" + b.Name() + "?mode=memory&cache=shared")
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		defer q.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i)
			if err := q.Enqueue(job.New("bench", nil, job.WithIdempotencyKey(key))); err != nil {
				b.Fatalf("Enqueue: %v", err)
			}
		}
	})

	b.Run("SameKeyDeduped", func(b *testing.B) {
		q, err := Open("file:" + b.Name() + "?mode=memory&cache=shared")
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		defer q.Close()

		if err := q.Enqueue(job.New("bench", nil, job.WithIdempotencyKey("shared"))); err != nil {
			b.Fatalf("seed Enqueue: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := q.Enqueue(job.New("bench", nil, job.WithIdempotencyKey("shared"))); err != nil {
				b.Fatalf("Enqueue: %v", err)
			}
		}
	})
}

// BenchmarkSQLiteConcurrentEnqueueDequeue measures throughput under
// concurrent access, the scenario the WAL + busy_timeout configuration in
// Open exists for.
func BenchmarkSQLiteConcurrentEnqueueDequeue(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.db")
	q, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer q.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := q.Enqueue(job.New("bench", nil)); err != nil {
				b.Fatalf("Enqueue: %v", err)
			}
			if _, err := q.TryDequeue(); err != nil && err != queue.ErrEmpty {
				b.Fatalf("TryDequeue: %v", err)
			}
		}
	})
}
