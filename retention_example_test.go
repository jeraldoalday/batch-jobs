package batchjobs_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jeraldoalday/batch-jobs"
)

// A queue's database keeps every job it has ever run. Retention is how
// long each kind of finished job is worth keeping; the engine deletes the
// rest on its own once started.
func Example_retention() {
	engine, err := batchjobs.Open(batchjobs.Config{
		DB:      "jobs.db",
		Workers: 4,
		Retention: batchjobs.Retention{
			// A success is an audit trail whose value decays quickly.
			Succeeded: 48 * time.Hour,
			Cancelled: 48 * time.Hour,

			// A permanent failure says something was wrong with the job
			// itself, and the evidence is in the row. Keep it longer.
			Failed: 30 * 24 * time.Hour,

			// Dead is left at zero -- forever. The dead-letter queue is
			// the list of work that still needs doing, and deleting a
			// row discards the only record it was ever meant to happen.
		},
		PruneInterval: time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	engine.Handle("emails", func(ctx context.Context, j *batchjobs.Job) error {
		return nil
	})
	if err := engine.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer engine.Stop()
}

// The zero value keeps everything, which is the default. DefaultRetention
// is a starting point for a queue whose job rows are a debugging
// convenience rather than a record something else depends on.
func ExampleDefaultRetention() {
	var keepEverything batchjobs.Retention
	fmt.Println("zero value:", keepEverything)
	fmt.Println("default:   ", batchjobs.DefaultRetention())
	// Output:
	// zero value: keep everything
	// default:    succeeded=24h0m0s cancelled=24h0m0s failed=168h0m0s
}

// Pruning on a timer suits a single process. A fleet sharing one database
// usually wants the opposite: keep the policy, but run the passes from one
// place -- a maintenance command, a cron entry, an admin endpoint -- since
// one pass already deletes everything eligible and the rest only duplicate
// the write-lock pressure.
func ExampleEngine_Prune() {
	engine, err := batchjobs.Open(batchjobs.Config{
		DB:            "jobs.db",
		Retention:     batchjobs.DefaultRetention(),
		PruneInterval: -1, // no janitor; prune when asked
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	pruned, err := engine.Prune(context.Background())
	if err != nil {
		// An engine with no Config.Retention has not been told what may
		// be deleted, and will not guess.
		if errors.Is(err, batchjobs.ErrNoRetention) {
			log.Fatal("no retention policy configured")
		}
		log.Fatal(err)
	}

	fmt.Printf("removed %d job records (%s)\n", pruned.Total(), pruned)
}

// Whether retention is keeping up takes two numbers, not one. The count
// can sit still while the history quietly ages past its policy -- pruning
// that stopped on a queue that has since gone quiet.
func ExampleEngine_Retained() {
	engine, err := batchjobs.Open(batchjobs.Config{DB: "jobs.db"})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	retained, err := engine.Retained()
	if err != nil {
		log.Fatal(err)
	}

	oldest, err := engine.OldestRetained()
	if err != nil {
		log.Fatal(err)
	}
	if oldest.IsZero() {
		fmt.Println("no finished jobs are being kept")
		return
	}

	fmt.Printf("%d finished jobs kept, oldest finished %s ago\n",
		retained, time.Since(oldest).Round(time.Hour))
}
