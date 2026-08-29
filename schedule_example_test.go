package batchjobs_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jeraldoalday/batch-jobs"
)

// Recurring work: turn scheduling on, describe when the job runs, and let
// the engine enqueue it. The handler is an ordinary one -- a scheduled job
// is an ordinary job by the time a worker sees it.
func Example_schedules() {
	engine, err := batchjobs.Open(batchjobs.Config{
		DB:        "jobs.db",
		Workers:   4,
		Schedules: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	engine.Handle("reports", func(ctx context.Context, j *batchjobs.Job) error {
		var req struct {
			Window string `json:"window"`
		}
		if err := j.Decode(&req); err != nil {
			return batchjobs.Permanent(err)
		}
		return buildReport(ctx, req.Window)
	})

	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer engine.Stop()

	// 3am on weekdays.
	_, err = engine.AddScheduleJSON(ctx, "reports", "0 3 * * 1-5",
		map[string]string{"window": "daily"},
		batchjobs.WithMaxRetries(3),
		batchjobs.WithTimeout(10*time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
}

// Before saving an expression someone typed, check that it parses and that
// it means what they meant. "0 3 * * *" and "3 0 * * *" are both valid and
// only one of them is 3am.
func ExampleValidateCron() {
	for _, expr := range []string{"0 3 * * 1-5", "*/15 * * * *", "3am daily"} {
		if err := batchjobs.ValidateCron(expr); err != nil {
			fmt.Printf("%-14s rejected\n", expr)
			continue
		}
		fmt.Printf("%-14s ok\n", expr)
	}
	// Output:
	// 0 3 * * 1-5    ok
	// */15 * * * *   ok
	// 3am daily      rejected
}

// Upcoming turns an expression into the times it actually fires, which is
// the only way to be sure a schedule says what its author meant.
func ExampleSchedule_Upcoming() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB, Schedules: true})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	s, err := engine.AddSchedule(ctx, "reports", "0 */6 * * *", nil)
	if err != nil {
		log.Fatal(err)
	}

	times, err := s.Upcoming(4)
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i < len(times); i++ {
		fmt.Println(times[i].Sub(times[i-1]))
	}
	// Output:
	// 6h0m0s
	// 6h0m0s
	// 6h0m0s
}

// Editing a schedule is a read-modify-write: the engine hands you a copy,
// you change what you meant to change, and it stores the result. Changing
// the expression recomputes the next fire time; changing anything else
// leaves tonight's run where it was.
func ExampleEngine_UpdateSchedule() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB, Schedules: true})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	s, err := engine.AddSchedule(ctx, "reports", "0 3 * * *", nil)
	if err != nil {
		log.Fatal(err)
	}

	updated, err := engine.UpdateSchedule(ctx, s.ID, func(s *batchjobs.Schedule) {
		s.Cron = "0 4 * * *"
		s.MaxRetries = 5
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(updated.Cron, updated.MaxRetries)

	// An expression that will not parse is refused, and nothing is
	// written: the stored schedule is never left half edited.
	if _, err := engine.UpdateSchedule(ctx, s.ID, func(s *batchjobs.Schedule) {
		s.Cron = "every four hours"
	}); err != nil {
		fmt.Println("rejected")
	}
	// Output:
	// 0 4 * * * 5
	// rejected
}

// Pausing stops the timetable, not the operator: a paused schedule can
// still be run on demand, and resuming it does not fire off everything it
// missed.
func ExampleEngine_TriggerSchedule() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB, Schedules: true})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	s, err := engine.AddSchedule(ctx, "reports", "0 3 * * *", []byte("daily"))
	if err != nil {
		log.Fatal(err)
	}
	if err := engine.PauseSchedule(ctx, s.ID); err != nil {
		log.Fatal(err)
	}

	j, err := engine.TriggerSchedule(ctx, s.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(j.Queue, string(j.Payload), j.Status)

	// The manual run is not recorded as a firing, so the schedule's own
	// timetable is exactly where it was.
	after, err := engine.GetSchedule(ctx, s.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("fired:", !after.LastRunAt.IsZero())
	// Output:
	// reports daily pending
	// fired: false
}

// A schedule method on an engine without Config.Schedules reports the
// wiring mistake rather than quietly creating a database nobody asked for.
func ExampleErrSchedulingDisabled() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	_, err = engine.AddSchedule(context.Background(), "reports", "0 3 * * *", nil)
	fmt.Println(errors.Is(err, batchjobs.ErrSchedulingDisabled))
	// Output:
	// true
}

// buildReport stands in for the application's own work.
func buildReport(ctx context.Context, window string) error { return nil }
