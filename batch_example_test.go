package batchjobs_test

import (
	"context"
	"fmt"
	"log"

	batchjobs "github.com/jeraldoalday/batch-jobs"
)

// A fan-out -- one event turning into many jobs -- should be submitted in
// one call rather than one call per job. The queue is durable, so most of
// what a single enqueue costs is the transaction around it, and a batch
// pays for one of those instead of a thousand.
func ExampleEngine_EnqueueBatch() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	recipients := []string{"a@example.com", "b@example.com", "c@example.com"}

	subs := make([]batchjobs.Submission, 0, len(recipients))
	for _, to := range recipients {
		sub, err := batchjobs.JSON("emails", map[string]string{"to": to},
			batchjobs.WithMaxRetries(3),
			// Keying on the recipient makes the whole batch safe to
			// resubmit: a retry after a timeout adopts the original jobs
			// rather than sending everyone a second copy.
			batchjobs.WithIdempotencyKey("welcome:"+to),
		)
		if err != nil {
			log.Fatal(err)
		}
		subs = append(subs, sub)
	}

	jobs, err := engine.EnqueueBatch(context.Background(), subs)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("enqueued:", len(jobs))

	// Submitting the same batch again finds every key already on file, so
	// nothing new is created.
	if _, err := engine.EnqueueBatch(context.Background(), subs); err != nil {
		log.Fatal(err)
	}
	fmt.Println("waiting:", engine.Len())

	// Output:
	// enqueued: 3
	// waiting: 3
}

// EnqueueBatchJSON is the shorthand when every job goes on the same queue
// with the same options.
func ExampleEngine_EnqueueBatchJSON() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	type resize struct {
		ImageID string `json:"image_id"`
		Width   int    `json:"width"`
	}

	values := []any{
		resize{ImageID: "img-1", Width: 320},
		resize{ImageID: "img-2", Width: 320},
	}

	jobs, err := engine.EnqueueBatchJSON(context.Background(), "thumbnails", values,
		batchjobs.WithPriority(batchjobs.PriorityLow))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("enqueued:", len(jobs))
	fmt.Println("priority:", jobs[0].Priority)

	// Output:
	// enqueued: 2
	// priority: low
}
