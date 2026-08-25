package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCallCount(t *testing.T) {
	tests := []struct {
		jobs, batch, want int
	}{
		{0, 1, 0},     // unbounded by job count
		{0, 100, 0},   //
		{10, 1, 10},   // one call per job
		{100, 10, 10}, // exact multiple
		{105, 10, 11}, // the remainder still needs a call
		{5, 10, 1},    // fewer jobs than one batch
	}
	for _, tc := range tests {
		if got := callCount(tc.jobs, tc.batch); got != tc.want {
			t.Errorf("callCount(%d, %d) = %d, want %d", tc.jobs, tc.batch, got, tc.want)
		}
	}
}

func TestRunRejectsAnUnboundedRun(t *testing.T) {
	err := run(config{producers: 1, batch: 1}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("a run with neither -duration nor -jobs succeeded; it would never return")
	}
}

func TestRunRejectsInvalidCounts(t *testing.T) {
	tests := map[string]config{
		"batch below one":     {jobs: 10, batch: 0, producers: 1},
		"producers below one": {jobs: 10, batch: 1, producers: 0},
	}
	for name, cfg := range tests {
		if err := run(cfg, &bytes.Buffer{}); err == nil {
			t.Errorf("%s: run succeeded, want an error", name)
		}
	}
}

// TestRunEnqueueOnly exercises the whole command with no workers, which is
// the configuration that measures the producer side's ceiling.
func TestRunEnqueueOnly(t *testing.T) {
	var out bytes.Buffer
	cfg := config{
		db:        filepath.Join(t.TempDir(), "bench.db"),
		jobs:      200,
		producers: 2,
		batch:     1,
		workers:   0,
		payload:   32,
	}
	if err := run(cfg, &out); err != nil {
		t.Fatalf("run: %v", err)
	}

	report := out.String()
	for _, want := range []string{"enqueue", "throughput", "200 ok", "p99"} {
		if !strings.Contains(report, want) {
			t.Errorf("report is missing %q:\n%s", want, report)
		}
	}
	if strings.Contains(report, "end-to-end") {
		t.Errorf("a run with no workers reported end-to-end latency:\n%s", report)
	}
}

// TestRunWithWorkersReportsEndToEnd covers the consuming half: jobs are
// enqueued, run, and their whole lifetime is measured.
func TestRunWithWorkersReportsEndToEnd(t *testing.T) {
	var out bytes.Buffer
	cfg := config{
		db:        filepath.Join(t.TempDir(), "bench.db"),
		jobs:      100,
		producers: 2,
		batch:     1,
		workers:   4,
		payload:   16,
		drainWait: 20 * time.Second,
	}
	if err := run(cfg, &out); err != nil {
		t.Fatalf("run: %v", err)
	}

	report := out.String()
	if !strings.Contains(report, "end-to-end") {
		t.Errorf("report is missing the end-to-end block:\n%s", report)
	}
	if strings.Contains(report, "still waiting when the run ended") {
		t.Errorf("a run of 100 trivial jobs through 4 workers did not drain:\n%s", report)
	}
}

func TestRunBatched(t *testing.T) {
	var out bytes.Buffer
	cfg := config{
		db:        filepath.Join(t.TempDir(), "bench.db"),
		jobs:      200,
		producers: 2,
		batch:     20,
		workers:   0,
		payload:   16,
	}
	if err := run(cfg, &out); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Ten calls of twenty jobs each: the report counts calls, since that
	// is the operation whose latency it measured.
	if got := out.String(); !strings.Contains(got, "10 ok") {
		t.Errorf("report does not show 10 batched calls:\n%s", got)
	}
}

func TestDatabasePathUsesTheGivenPath(t *testing.T) {
	want := filepath.Join(t.TempDir(), "given.db")

	got, cleanup, err := databasePath(config{db: want})
	if err != nil {
		t.Fatalf("databasePath: %v", err)
	}
	defer cleanup()

	if got != want {
		t.Errorf("databasePath = %q, want %q", got, want)
	}
}

func TestDatabasePathRemovesItsTemporaryDirectory(t *testing.T) {
	path, cleanup, err := databasePath(config{})
	if err != nil {
		t.Fatalf("databasePath: %v", err)
	}

	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("temporary directory was not created: %v", err)
	}

	cleanup()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("temporary directory survived cleanup: %v", err)
	}
}

func TestDescribeNamesTheOfferedLoad(t *testing.T) {
	unpaced := describe(config{producers: 4, batch: 1, workers: 2}, "x.db")
	if !strings.Contains(unpaced, "unpaced") {
		t.Errorf("describe of a rate-less run = %q, want it to say unpaced", unpaced)
	}

	paced := describe(config{rate: 250, producers: 4, batch: 1, workers: 2}, "x.db")
	if !strings.Contains(paced, "250 jobs/s") {
		t.Errorf("describe of a paced run = %q, want it to state the rate", paced)
	}
}
