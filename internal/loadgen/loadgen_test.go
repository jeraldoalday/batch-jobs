package loadgen

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunRequiresSubmit(t *testing.T) {
	if _, err := Run(context.Background(), Config{Jobs: 1}, nil); err == nil {
		t.Fatal("Run with a nil SubmitFunc succeeded, want an error")
	}
}

func TestRunRequiresABound(t *testing.T) {
	_, err := Run(context.Background(), Config{}, func(context.Context, int) error { return nil })
	if err == nil {
		t.Fatal("Run with neither Duration nor Jobs succeeded; it would never return")
	}
}

func TestRunClosedLoopSubmitsExactlyJobs(t *testing.T) {
	var calls atomic.Int64
	res, err := Run(context.Background(), Config{Jobs: 500, Concurrency: 4},
		func(context.Context, int) error {
			calls.Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := calls.Load(); got != 500 {
		t.Errorf("SubmitFunc called %d times, want exactly 500", got)
	}
	if res.Submitted != 500 {
		t.Errorf("Submitted = %d, want 500", res.Submitted)
	}
	if res.Failed != 0 {
		t.Errorf("Failed = %d, want 0", res.Failed)
	}
	if res.Latency.Count() != 500 {
		t.Errorf("Latency.Count() = %d, want 500", res.Latency.Count())
	}
	if res.Schedule != nil {
		t.Error("Schedule is non-nil for a closed-loop run, which has no schedule to fall behind")
	}
}

// TestRunSequenceNumbersAreUniqueAndDense matters because callers derive
// idempotency keys and payloads from seq: a repeat would silently dedupe
// half the run away.
func TestRunSequenceNumbersAreUniqueAndDense(t *testing.T) {
	const n = 1000
	seen := make([]atomic.Bool, n)

	_, err := Run(context.Background(), Config{Jobs: n, Concurrency: 8},
		func(_ context.Context, seq int) error {
			if seq < 0 || seq >= n {
				t.Errorf("sequence number %d outside [0, %d)", seq, n)
				return nil
			}
			if seen[seq].Swap(true) {
				t.Errorf("sequence number %d handed out twice", seq)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for i := range seen {
		if !seen[i].Load() {
			t.Fatalf("sequence number %d was never handed out", i)
		}
	}
}

func TestRunCountsAndGroupsErrors(t *testing.T) {
	boom := errors.New("boom")
	res, err := Run(context.Background(), Config{Jobs: 100, Concurrency: 4},
		func(_ context.Context, seq int) error {
			if seq%4 == 0 {
				return boom
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Failed != 25 {
		t.Errorf("Failed = %d, want 25", res.Failed)
	}
	if res.Submitted != 75 {
		t.Errorf("Submitted = %d, want 75", res.Submitted)
	}
	if res.Total() != 100 {
		t.Errorf("Total() = %d, want 100", res.Total())
	}
	if got := res.ErrorRate(); got != 0.25 {
		t.Errorf("ErrorRate() = %v, want 0.25", got)
	}
	if got := res.Errors["boom"]; got != 25 {
		t.Errorf("Errors[%q] = %d, want 25", "boom", got)
	}
}

// TestRunBoundsDistinctErrors guards the tally against an error string
// that carries a unique ID, which would otherwise grow the map for the
// whole run.
func TestRunBoundsDistinctErrors(t *testing.T) {
	res, err := Run(context.Background(), Config{Jobs: 500, Concurrency: 1},
		func(_ context.Context, seq int) error {
			return errors.New("job " + string(rune('a'+seq%26)) + string(rune('a'+seq/26%26)) + " failed")
		})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(res.Errors) > maxDistinctErrors {
		t.Errorf("Errors has %d keys, want at most %d", len(res.Errors), maxDistinctErrors)
	}
	if res.Failed != 500 {
		t.Errorf("Failed = %d, want 500: capping the map must not lose the count", res.Failed)
	}
}

func TestRunOpenLoopKeepsToItsRate(t *testing.T) {
	const rate = 400.0
	res, err := Run(context.Background(), Config{
		Rate:        rate,
		Duration:    300 * time.Millisecond,
		Concurrency: 8,
	}, func(context.Context, int) error { return nil })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// A no-op submit is easily fast enough to hold this rate, so the
	// achieved throughput should be close to the target. The bounds are
	// loose because timer granularity on a loaded CI machine is not.
	if got := res.Rate(); got < rate*0.5 || got > rate*1.6 {
		t.Errorf("Rate() = %.1f ops/s, want roughly the %v target", got, rate)
	}
	if res.Schedule == nil {
		t.Fatal("Schedule is nil for an open-loop run")
	}
	if !res.KeptSchedule(0.5) {
		t.Errorf("KeptSchedule(0.5) = false at %.1f ops/s against a %v target", res.Rate(), rate)
	}
}

// TestRunOpenLoopMeasuresFromIntendedSendTime is the coordinated-omission
// correction: when the system cannot keep up, the reported latency must
// grow to include the time an operation spent waiting to be issued, not
// just its own service time.
func TestRunOpenLoopMeasuresFromIntendedSendTime(t *testing.T) {
	const service = 20 * time.Millisecond

	// One producer offered 200 ops/s but able to serve only 50: every
	// operation after the first waits behind its predecessors.
	res, err := Run(context.Background(), Config{
		Rate:        200,
		Duration:    400 * time.Millisecond,
		Concurrency: 1,
	}, func(context.Context, int) error {
		time.Sleep(service)
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Latency.Count() < 5 {
		t.Skipf("only %d samples on a loaded machine, not enough to judge", res.Latency.Count())
	}

	// Timing from the actual send would report roughly the 20ms service
	// time at every percentile. Timing from the intended send time must
	// report far more at the tail, because the backlog is real.
	if p99 := res.Latency.Percentile(99); p99 < 3*service {
		t.Errorf("p99 latency = %v; an overloaded run must report queueing time, not just the %v service time", p99, service)
	}
	if !res.KeptSchedule(0.1) {
		return // Expected: the generator could not keep up, which is the point.
	}
	t.Errorf("KeptSchedule reported true at %.1f ops/s against a 200 target", res.Rate())
}

func TestRunWarmupExcludesEarlyOperations(t *testing.T) {
	var calls atomic.Int64
	res, err := Run(context.Background(), Config{
		Duration:    150 * time.Millisecond,
		Warmup:      100 * time.Millisecond,
		Concurrency: 2,
	}, func(context.Context, int) error {
		calls.Add(1)
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Total() >= uint64(calls.Load()) {
		t.Errorf("Total() = %d with %d actual calls; warmup operations must not be counted", res.Total(), calls.Load())
	}
	if res.Total() == 0 {
		t.Error("Total() = 0; the measured window recorded nothing at all")
	}
	// Duration is the measured window and Warmup sits on top of it, so
	// the run takes 250ms in total and reports on the last 150ms.
	if res.Elapsed < 120*time.Millisecond || res.Elapsed > 200*time.Millisecond {
		t.Errorf("Elapsed = %v, want roughly the configured 150ms measured window", res.Elapsed)
	}
}

func TestRunReturnsPartialResultsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var calls atomic.Int64
	go func() {
		for calls.Load() < 20 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	res, err := Run(ctx, Config{Jobs: 1_000_000, Concurrency: 2},
		func(context.Context, int) error {
			calls.Add(1)
			time.Sleep(200 * time.Microsecond)
			return nil
		})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled", err)
	}
	if res.Total() == 0 {
		t.Error("a cancelled run returned no observations; the work it did do still counts")
	}
	if res.Total() >= 1_000_000 {
		t.Error("a cancelled run ran to completion")
	}
}

func TestResultRateAndKeptScheduleEdges(t *testing.T) {
	var zero Result
	if got := zero.Rate(); got != 0 {
		t.Errorf("Rate() on a zero Result = %v, want 0", got)
	}
	if got := zero.ErrorRate(); got != 0 {
		t.Errorf("ErrorRate() on a zero Result = %v, want 0", got)
	}
	if !zero.KeptSchedule(0) {
		t.Error("KeptSchedule on a closed-loop Result = false; there is no target to miss")
	}

	behind := Result{
		Config:    Config{Rate: 1000},
		Elapsed:   time.Second,
		Submitted: 500,
	}
	if behind.KeptSchedule(0.05) {
		t.Error("KeptSchedule(0.05) = true at half the target rate")
	}
	if !behind.KeptSchedule(0.9) {
		t.Error("KeptSchedule(0.9) = false at half the target rate")
	}
}

func TestRecorderIsConcurrencySafe(t *testing.T) {
	r := NewRecorder()

	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(base int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 1000; j++ {
				r.Record(time.Duration(base+j) * time.Microsecond)
			}
		}(i * 1000)
	}
	for i := 0; i < 8; i++ {
		<-done
	}

	snap := r.Snapshot()
	if got := snap.Count(); got != 8000 {
		t.Errorf("Snapshot().Count() = %d, want 8000", got)
	}

	// A snapshot must be independent of continued recording.
	r.Record(time.Hour)
	if got := snap.Count(); got != 8000 {
		t.Errorf("Snapshot().Count() changed to %d after a later Record", got)
	}
}

func TestResultString(t *testing.T) {
	res := Result{
		Elapsed:   2 * time.Second,
		Submitted: 1000,
		Failed:    10,
		Latency:   NewHistogram(),
	}
	res.Latency.Record(5 * time.Millisecond)

	got := res.String()
	for _, want := range []string{"1010 ops", "2s", "500/s", "0.99%"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, want it to contain %q", got, want)
		}
	}
}
