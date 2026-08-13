package worker

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestPoolTracksJobsInFlight(t *testing.T) {
	q := queue.NewMemory()
	m := metrics.New(prometheus.NewRegistry())
	started := make(chan struct{})
	release := make(chan struct{})

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			close(started)
			<-release
			return nil
		},
		Concurrency: 1,
		Metrics:     m,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("emails", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	<-started
	waitForScrapeContains(t, m, `batchjobs_jobs_in_flight{queue="emails"} 1`)

	close(release)
	waitForScrapeContains(t, m, `batchjobs_jobs_in_flight{queue="emails"} 0`)
}
