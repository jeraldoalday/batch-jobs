package client_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jeraldoalday/batch-jobs/client"
)

// Talking to a batch-jobs server from another process: submit work, then
// check on it by ID.
func Example() {
	c, err := client.New("http://localhost:8080", client.WithAPIKey("an-api-key"))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	j, err := c.EnqueueJSON(ctx, "emails", map[string]string{
		"to":      "someone@example.com",
		"subject": "Welcome",
	}, client.WithMaxRetries(3), client.WithTimeout(30*time.Second))
	if err != nil {
		log.Fatal(err)
	}

	status, err := c.Get(ctx, j.ID)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("job %s is %s", status.ID, status.Status)
}

// Backpressure is not a failure: a 503 means the request was fine and the
// server is full, so the right response is to wait as long as it asked and
// try the same request again.
func ExampleTemporary() {
	c, err := client.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for attempt := range 5 {
		_, err := c.Enqueue(ctx, "emails", []byte("hello"))
		if err == nil {
			break
		}
		if !client.Temporary(err) {
			// A malformed request or an unknown job will not become
			// correct by being sent again.
			log.Fatal(err)
		}

		delay := client.RetryAfter(err, time.Duration(attempt+1)*time.Second)
		log.Printf("server is busy, retrying in %s", delay)
		time.Sleep(delay)
	}
}

// Sentinel errors cover the outcomes worth branching on; *Error carries the
// detail behind them.
func ExampleError() {
	c, err := client.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	switch err := c.Cancel(context.Background(), "job-1"); {
	case err == nil:
		fmt.Println("cancelled")
	case errors.Is(err, client.ErrNotFound):
		fmt.Println("no such job")
	case errors.Is(err, client.ErrConflict):
		// A worker got to it first, which is a race the caller can lose.
		fmt.Println("already started or finished")
	default:
		var apiErr *client.Error
		if errors.As(err, &apiErr) {
			fmt.Printf("server said %d: %s\n", apiErr.StatusCode, apiErr.Message)
		}
	}
}

// Dead letters are the jobs worth reviving once whatever they were failing
// against has been fixed.
func ExampleClient_List() {
	c, err := client.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	dead, err := c.List(ctx, client.StatusDead, 100)
	if err != nil {
		log.Fatal(err)
	}

	for _, j := range dead {
		log.Printf("job %s on %s failed after %d attempts: %s", j.ID, j.Queue, j.Attempt(), j.LastError)
		if err := c.Retry(ctx, j.ID); err != nil {
			log.Print(err)
		}
	}
}

// Watch follows the server's live stats stream, the same one the dashboard
// is driven by, so a monitor does not have to poll.
func ExampleClient_Watch() {
	c, err := client.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	err = c.Watch(ctx, func(stats client.Stats) error {
		log.Printf("%s (backlog %d)", stats, stats.Backlog())
		if stats.Failing() > 100 {
			return errors.New("too many failed jobs, giving up on watching")
		}
		return nil
	})
	if err != nil {
		log.Print(err)
	}
}

// Stats exposes the derived numbers directly, so callers do not add
// statuses up by hand.
func ExampleStats() {
	stats := client.Stats{
		client.StatusPending:   4,
		client.StatusRunning:   2,
		client.StatusSucceeded: 91,
		client.StatusDead:      3,
	}

	fmt.Println(stats)
	fmt.Println("backlog:", stats.Backlog(), "failing:", stats.Failing())
	// Output:
	// pending=4 running=2 succeeded=91 dead=3
	// backlog: 4 failing: 3
}
