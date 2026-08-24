package batchjobs_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jeraldoalday/batch-jobs"
)

// The usual shape of an embedded queue: open it, register a handler per
// queue, start the workers, and enqueue work from anywhere in the process.
func Example() {
	engine, err := batchjobs.Open(batchjobs.Config{
		DB:      "jobs.db",
		Workers: 8,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	engine.Handle("emails", func(ctx context.Context, j *batchjobs.Job) error {
		var msg struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
		}
		if err := j.Decode(&msg); err != nil {
			// A payload that will not parse now will not parse on the
			// next attempt either, so do not spend the job's retries on
			// it: fail it permanently and let a human read it.
			return batchjobs.Permanent(err)
		}
		return sendEmail(ctx, msg.To, msg.Subject)
	})

	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer engine.Stop()

	if _, err := engine.EnqueueJSON(ctx, "emails", map[string]string{
		"to":      "someone@example.com",
		"subject": "Welcome",
	}, batchjobs.WithMaxRetries(3), batchjobs.WithTimeout(30*time.Second)); err != nil {
		log.Fatal(err)
	}
}

// A job's retry policy is set per job, not per queue, so work that is
// expensive to repeat and work that is cheap can share a handler.
func ExampleEngine_Enqueue() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Run ahead of everything else already waiting.
	urgent, err := engine.Enqueue(ctx, "reports", []byte("incident"),
		batchjobs.WithPriority(batchjobs.PriorityHigh))
	if err != nil {
		log.Fatal(err)
	}

	// Run in an hour, and give up after two failures.
	nightly, err := engine.Enqueue(ctx, "reports", []byte("nightly"),
		batchjobs.WithDelay(time.Hour),
		batchjobs.WithMaxRetries(2))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(urgent.Priority, nightly.AttemptsLeft())
	// Output: high 2
}

// An idempotency key makes an enqueue safe to repeat, which matters
// whenever the caller might retry after a failure it cannot interpret --
// a timeout that may or may not have been delivered.
func ExampleEngine_EnqueueResult() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	key := batchjobs.WithIdempotencyKey("order-42")

	first, created, err := engine.EnqueueResult(ctx, "payments", []byte("charge"), key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("first:", created)

	// The same call again -- a retry of a request whose outcome the caller
	// never saw -- returns the original job instead of charging twice.
	second, created, err := engine.EnqueueResult(ctx, "payments", []byte("charge"), key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("second:", created, "same job:", first.ID == second.ID)

	// Output:
	// first: true
	// second: false same job: true
}

// Permanent separates a job that is wrong from a job that could not get
// through, which is what makes a dead-letter queue worth reading.
func ExamplePermanent() {
	handler := func(ctx context.Context, j *batchjobs.Job) error {
		var order struct {
			ID string `json:"id"`
		}
		if err := j.Decode(&order); err != nil {
			// Wrong: no attempt will fix it. Lands in StatusFailed.
			return batchjobs.Permanent(err)
		}
		if err := charge(ctx, order.ID); err != nil {
			// Could not get through: worth another attempt. Lands in
			// StatusRetrying, then StatusDead once retries are spent.
			return err
		}
		return nil
	}

	err := handler(context.Background(), &batchjobs.Job{ID: "job-1", Payload: []byte("not json")})
	fmt.Println(batchjobs.IsPermanent(err))
	// Output: true
}

// A failed job's fate is readable after the fact: dead letters are the
// ones worth retrying once whatever broke has been fixed.
func ExampleEngine_List() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	dead, err := engine.List(ctx, batchjobs.StatusDead, 100)
	if err != nil {
		log.Fatal(err)
	}

	for _, j := range dead {
		log.Printf("job %s on %s failed %d times: %s", j.ID, j.Queue, j.Attempt(), j.LastError)
		if err := engine.RetryJob(ctx, j.ID); err != nil && !errors.Is(err, batchjobs.ErrNotRetryable) {
			log.Print(err)
		}
	}
}

// Stats answers the operational questions directly, rather than leaving
// callers to add statuses up themselves.
func ExampleStats() {
	stats := batchjobs.Stats{
		batchjobs.StatusPending:   4,
		batchjobs.StatusRunning:   2,
		batchjobs.StatusSucceeded: 91,
		batchjobs.StatusDead:      3,
	}

	fmt.Println("anything left to do:", stats.Active())
	fmt.Println("waiting for a worker:", stats.Backlog())
	fmt.Println("needs attention:", stats.Failing())
	// Output:
	// anything left to do: 6
	// waiting for a worker: 4
	// needs attention: 3
}

// The queue can be exposed over HTTP on a listener the application already
// has, so producers in other processes -- and the dashboard -- reach the
// same jobs the embedded workers are running.
func ExampleEngine_APIHandler() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: "jobs.db", Workers: 4})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	mux := http.NewServeMux()
	mux.Handle("/jobs/", http.StripPrefix("/jobs", engine.APIHandler(batchjobs.APIOptions{
		AuthToken: "an-api-key",
		Dashboard: true,
	})))
	mux.Handle("/metrics", engine.MetricsHandler())

	log.Fatal(http.ListenAndServe(":8080", mux))
}

// Stubs standing in for whatever the handlers actually do.
func sendEmail(ctx context.Context, to, subject string) error { return nil }
func charge(ctx context.Context, orderID string) error        { return nil }
