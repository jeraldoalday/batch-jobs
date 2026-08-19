package queue

import (
	"strconv"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// benchRouter builds a router over n in-memory routes named "q0".."qN".
func benchRouter(b *testing.B, n int) (*Router, []string) {
	b.Helper()

	names := make([]string, n)
	routes := make([]Route, n)
	for i := range routes {
		names[i] = "q" + strconv.Itoa(i)
		routes[i] = Route{Name: names[i], Queue: NewMemory()}
	}

	r, err := NewRouter(routes)
	if err != nil {
		b.Fatalf("NewRouter: %v", err)
	}
	return r, names
}

// BenchmarkMemoryEnqueue is the baseline the router's fan-out is measured
// against: the same work with no name lookup in front of it.
func BenchmarkMemoryEnqueue(b *testing.B) {
	q := NewMemory()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := q.Enqueue(job.New("q0", nil)); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}
}

func BenchmarkRouterEnqueue(b *testing.B) {
	for _, routes := range []int{1, 4, 16} {
		b.Run(strconv.Itoa(routes)+"-routes", func(b *testing.B) {
			r, names := benchRouter(b, routes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := r.Enqueue(job.New(names[i%len(names)], nil)); err != nil {
					b.Fatalf("Enqueue: %v", err)
				}
			}
		})
	}
}

// The weighted sweep is the router's one real cost on the hot path: a
// dequeue may probe several routes before finding work. This measures the
// happy case, where every route has jobs waiting.
func BenchmarkRouterTryDequeueAllRoutesBusy(b *testing.B) {
	for _, routes := range []int{1, 4, 16} {
		b.Run(strconv.Itoa(routes)+"-routes", func(b *testing.B) {
			r, names := benchRouter(b, routes)
			for i := 0; i < b.N; i++ {
				if err := r.Enqueue(job.New(names[i%len(names)], nil)); err != nil {
					b.Fatalf("Enqueue: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := r.TryDequeue(); err != nil {
					b.Fatalf("TryDequeue: %v", err)
				}
			}
		})
	}
}

// The worst case for the sweep: only the last route has work, so every
// dequeue walks the whole roster before finding it.
func BenchmarkRouterTryDequeueOneRouteBusy(b *testing.B) {
	const routes = 16

	r, names := benchRouter(b, routes)
	busy := names[routes-1]
	for i := 0; i < b.N; i++ {
		if err := r.Enqueue(job.New(busy, nil)); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := r.TryDequeue(); err != nil {
			b.Fatalf("TryDequeue: %v", err)
		}
	}
}

// An empty router is what a worker pool polls between bursts, so the cost
// of finding nothing matters as much as the cost of finding something.
func BenchmarkRouterTryDequeueEmpty(b *testing.B) {
	r, _ := benchRouter(b, 16)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := r.TryDequeue(); err == nil {
			b.Fatal("TryDequeue: err = nil, want ErrEmpty")
		}
	}
}
