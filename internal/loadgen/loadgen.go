package loadgen

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultConcurrency is the number of producer goroutines a run uses when
// Config.Concurrency is unset.
const DefaultConcurrency = 8

// maxDistinctErrors bounds how many distinct error strings a Result keeps.
// A run that is failing usually fails the same handful of ways, and a
// pathological one -- an error string carrying a job ID, say -- must not
// be able to grow the map without limit.
const maxDistinctErrors = 16

// SubmitFunc performs one unit of work against the system under test. seq
// is the zero-based sequence number of the operation within the run, so a
// caller can vary the payload, the queue name, or the idempotency key
// without keeping its own counter.
//
// A SubmitFunc is called from several goroutines at once and must be safe
// for concurrent use.
type SubmitFunc func(ctx context.Context, seq int) error

// Config describes a load run.
type Config struct {
	// Rate is the target arrival rate in operations per second. Zero or
	// negative runs closed-loop instead: each producer submits again as
	// soon as its last operation returned, which measures the system's
	// maximum throughput rather than its behaviour at a chosen load.
	Rate float64

	// Duration bounds the *measured* window: Warmup is spent on top of
	// it, so a run configured with both takes Warmup+Duration of
	// wall-clock time and reports on the last Duration of it. Zero means
	// unbounded, in which case Jobs or context cancellation must stop
	// the run.
	Duration time.Duration

	// Jobs bounds the run's total operation count. Zero means unbounded.
	// When both Duration and Jobs are set the run stops at whichever
	// comes first.
	Jobs int

	// Concurrency is the number of producer goroutines, defaulting to
	// DefaultConcurrency. Under an open-loop Rate it is a ceiling on how
	// many operations can be in flight at once, so it must be high
	// enough that the generator can keep to its schedule; closed-loop it
	// is the offered concurrency itself.
	Concurrency int

	// Warmup discards everything observed in this much wall-clock time at
	// the start of the run. It exists because the first operations against
	// a fresh process pay for cold caches, an unwarmed connection pool,
	// and an empty SQLite page cache, none of which say anything about
	// steady-state behaviour.
	Warmup time.Duration
}

// concurrency returns the producer count to use, applying the default.
func (c Config) concurrency() int {
	if c.Concurrency > 0 {
		return c.Concurrency
	}
	return DefaultConcurrency
}

// openLoop reports whether the run paces itself to a target rate rather
// than submitting as fast as it can.
func (c Config) openLoop() bool { return c.Rate > 0 }

// Result is what a run observed.
type Result struct {
	// Config is the configuration the run was given, so a report can
	// state what was asked for alongside what happened.
	Config Config

	// Elapsed is the measured window: wall-clock time from the end of
	// warmup to the end of the run.
	Elapsed time.Duration

	// Submitted counts operations that returned without error, and
	// Failed counts those that did not. Both exclude anything observed
	// during warmup.
	Submitted uint64
	Failed    uint64

	// Latency is the distribution of per-operation latency. Under an
	// open-loop Rate it is measured from the moment the operation was
	// *scheduled* to be submitted, not from when a producer actually got
	// to it -- see Schedule below.
	Latency *Histogram

	// Schedule is how far behind its intended send time each operation
	// was actually submitted. It separates the two ways a run can look
	// slow: a system that takes a long time to answer (Latency high,
	// Schedule low) and a generator that could not offer the load it was
	// asked for (both high). It is nil for a closed-loop run, which has
	// no schedule to fall behind.
	Schedule *Histogram

	// Errors counts the distinct error strings seen, capped at a fixed
	// number of keys so a run that fails in unboundedly many ways cannot
	// grow it without limit.
	Errors map[string]uint64
}

// Rate returns the achieved throughput in successful operations per
// second over the measured window.
func (r Result) Rate() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Submitted) / r.Elapsed.Seconds()
}

// Total returns every operation observed, successful or not.
func (r Result) Total() uint64 { return r.Submitted + r.Failed }

// ErrorRate returns the fraction of observed operations that failed, in
// [0, 1].
func (r Result) ErrorRate() float64 {
	total := r.Total()
	if total == 0 {
		return 0
	}
	return float64(r.Failed) / float64(total)
}

// KeptSchedule reports whether the generator managed to offer the load it
// was configured for, within tolerance (a fraction in [0, 1]). A run that
// did not is not evidence about the system under test: it is evidence
// about the generator, and its latency numbers describe a lighter load
// than the one requested. Closed-loop runs have no target to miss and
// always report true.
func (r Result) KeptSchedule(tolerance float64) bool {
	if !r.Config.openLoop() {
		return true
	}
	return r.Rate() >= r.Config.Rate*(1-tolerance)
}

// Run drives submit according to cfg and returns what it observed. It
// returns early, with the results gathered so far, if ctx is cancelled.
func Run(ctx context.Context, cfg Config, submit SubmitFunc) (Result, error) {
	if submit == nil {
		return Result{}, errors.New("loadgen: Config needs a SubmitFunc")
	}
	if cfg.Duration <= 0 && cfg.Jobs <= 0 {
		return Result{}, errors.New("loadgen: a run needs a Duration, a Jobs count, or both")
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		// Duration is the measured window, so the deadline is warmup on
		// top of it: asking for thirty seconds of measurement after a
		// five second warmup should give thirty seconds of measurement,
		// not twenty-five.
		runCtx, cancel = context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
		defer cancel()
	}

	r := &run{
		cfg:    cfg,
		submit: submit,
		shards: make([]*shard, cfg.concurrency()),
	}
	for i := range r.shards {
		r.shards[i] = newShard(cfg.openLoop())
	}

	start := time.Now()
	r.measureFrom = start.Add(cfg.Warmup)

	var wg sync.WaitGroup
	for i := range r.shards {
		wg.Add(1)
		go func(s *shard) {
			defer wg.Done()
			r.produce(runCtx, s, start)
		}(r.shards[i])
	}
	wg.Wait()

	elapsed := time.Since(r.measureFrom)
	if elapsed < 0 {
		// The run ended inside its own warmup, so nothing was measured.
		elapsed = 0
	}

	res := Result{
		Config:  cfg,
		Elapsed: elapsed,
		Latency: NewHistogram(),
		Errors:  make(map[string]uint64),
	}
	if cfg.openLoop() {
		res.Schedule = NewHistogram()
	}
	for _, s := range r.shards {
		res.Submitted += s.submitted
		res.Failed += s.failed
		res.Latency.Merge(s.latency)
		if res.Schedule != nil {
			res.Schedule.Merge(s.schedule)
		}
		for msg, n := range s.errors {
			if _, ok := res.Errors[msg]; !ok && len(res.Errors) >= maxDistinctErrors {
				continue
			}
			res.Errors[msg] += n
		}
	}

	// A cancelled parent context is the caller stopping the run, not a
	// failure: the results gathered so far still stand, so they are
	// returned alongside the error rather than discarded.
	if err := ctx.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// run is the mutable state of one Run call.
type run struct {
	cfg    Config
	submit SubmitFunc
	shards []*shard

	// seq hands out operation sequence numbers across producers. It is
	// also what enforces Config.Jobs and, under an open-loop rate, what
	// pins each operation to its place in the schedule.
	seq atomic.Int64

	// measureFrom is when warmup ends and observations start counting.
	measureFrom time.Time
}

// shard is one producer's private tally, merged into the Result once the
// run is over. Per-producer state is what lets the recording path stay
// lock-free at rates where a shared mutex would show up in the numbers.
type shard struct {
	submitted uint64
	failed    uint64
	latency   *Histogram
	schedule  *Histogram
	errors    map[string]uint64
}

func newShard(openLoop bool) *shard {
	s := &shard{
		latency: NewHistogram(),
		errors:  make(map[string]uint64),
	}
	if openLoop {
		s.schedule = NewHistogram()
	}
	return s
}

// record folds one completed operation into the shard.
func (s *shard) record(latency, behind time.Duration, err error) {
	s.latency.Record(latency)
	if s.schedule != nil {
		s.schedule.Record(behind)
	}
	if err == nil {
		s.submitted++
		return
	}
	s.failed++
	msg := err.Error()
	if _, ok := s.errors[msg]; ok || len(s.errors) < maxDistinctErrors {
		s.errors[msg]++
	}
}

// produce is one producer goroutine's loop.
func (r *run) produce(ctx context.Context, s *shard, start time.Time) {
	for {
		seq := int(r.seq.Add(1) - 1)
		if r.cfg.Jobs > 0 && seq >= r.cfg.Jobs {
			return
		}
		if ctx.Err() != nil {
			return
		}

		// due is when this operation was supposed to be submitted. Under
		// a closed loop there is no schedule, so it is simply now.
		due := time.Now()
		if r.cfg.openLoop() {
			due = start.Add(time.Duration(float64(seq) / r.cfg.Rate * float64(time.Second)))
			if !sleepUntil(ctx, due) {
				return
			}
		}

		sent := time.Now()
		err := r.submit(ctx, seq)
		done := time.Now()

		if done.Before(r.measureFrom) {
			continue
		}
		// Latency is measured from the intended send time, not from when
		// a producer got around to it. Measuring from `sent` would hide
		// exactly the failure that matters most: once the system cannot
		// keep up, operations queue behind the producers, and timing
		// only the part after they were finally issued reports a
		// comfortable latency for a system that is in fact falling
		// further behind every second.
		r.record(s, done.Sub(due), sent.Sub(due), err)
	}
}

// record forwards to the shard, keeping the coordinated-omission
// correction in one place.
func (r *run) record(s *shard, latency, behind time.Duration, err error) {
	if behind < 0 {
		behind = 0
	}
	s.record(latency, behind, err)
}

// sleepUntil blocks until t, reporting false if ctx was cancelled first.
// A deadline already in the past returns true immediately, which is the
// common case once a run has fallen behind its schedule.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Recorder is a concurrency-safe Histogram, for latencies observed
// somewhere other than a producer goroutine -- most usefully end-to-end
// job latency, which only the handler running the job can see.
type Recorder struct {
	mu sync.Mutex
	h  *Histogram
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{h: NewHistogram()} }

// Record adds one observation.
func (r *Recorder) Record(d time.Duration) {
	r.mu.Lock()
	r.h.Record(d)
	r.mu.Unlock()
}

// Snapshot returns a copy of the observations so far, safe to query while
// recording continues.
func (r *Recorder) Snapshot() *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := NewHistogram()
	out.Merge(r.h)
	return out
}

// String summarises the run in one line, for progress output.
func (r Result) String() string {
	return fmt.Sprintf("%d ops in %s (%.0f/s, %.2f%% errors, p99 %s)",
		r.Total(), r.Elapsed.Round(time.Millisecond), r.Rate(),
		r.ErrorRate()*100, r.Latency.Percentile(99).Round(time.Microsecond))
}
