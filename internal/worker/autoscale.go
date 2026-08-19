package worker

import "time"

// DefaultScaleInterval is how often an autoscaling Pool samples its
// workload and decides whether to resize.
const DefaultScaleInterval = time.Second

// DefaultScaleDownAfter is how many consecutive samples must call for a
// smaller pool before a worker is actually retired.
const DefaultScaleDownAfter = 3

// Sample is one observation of a pool's workload, taken by the autoscaler
// on each tick.
type Sample struct {
	// Workers is how many worker goroutines are currently running.
	Workers int

	// Busy is how many of them are currently inside a handler. The
	// remainder are blocked in Dequeue waiting for work.
	Busy int

	// Backlog is how many jobs are waiting in the queue, as reported by
	// Queue.Len.
	Backlog int
}

// Policy describes how a pool's worker count should track its workload. The
// zero value is not usable: Max must be positive for autoscaling to be
// enabled at all, which is what keeps a Pool that never sets it running at
// a fixed size exactly as before.
type Policy struct {
	// Min is the floor on worker count. Values below 1 are treated as 1:
	// a pool with no workers would never notice work arriving, since
	// there would be nobody blocked in Dequeue.
	Min int

	// Max is the ceiling on worker count, and the switch that enables
	// autoscaling. It is the operator's real control: the pool's
	// concurrency is what determines how hard the downstream systems
	// handlers talk to get hit, so it must never be inferred from load.
	Max int

	// JobsPerWorker is how much backlog one worker is expected to absorb
	// when sizing the pool. Values below 1 are treated as 1, which asks
	// for a worker per waiting job (up to Max). Raising it makes the
	// pool grow more conservatively: at 10, a backlog of 50 asks for 5
	// additional workers rather than 50.
	JobsPerWorker int

	// ScaleDownAfter is how many consecutive samples must call for a
	// smaller pool before one worker is retired. It is the hysteresis
	// that keeps a bursty workload from thrashing goroutines up and down
	// every tick. Values below 1 are treated as DefaultScaleDownAfter.
	ScaleDownAfter int
}

// normalized returns p with its defaults filled in, so the decision logic
// never has to repeat the "unset means" rules.
func (p Policy) normalized() Policy {
	if p.Min < 1 {
		p.Min = 1
	}
	if p.JobsPerWorker < 1 {
		p.JobsPerWorker = 1
	}
	if p.ScaleDownAfter < 1 {
		p.ScaleDownAfter = DefaultScaleDownAfter
	}
	if p.Max < p.Min {
		p.Max = p.Min
	}
	return p
}

// Target returns the worker count the policy considers correct for s,
// ignoring hysteresis: every busy worker is needed as-is, plus enough
// additional workers to cover the backlog at JobsPerWorker each, clamped to
// [Min, Max].
//
// Busy is counted rather than Workers so idle workers do not keep
// themselves alive: a pool of 20 with 2 busy and an empty queue targets 2,
// and the 18 idle ones become candidates for retirement.
func (p Policy) Target(s Sample) int {
	p = p.normalized()

	needed := s.Backlog / p.JobsPerWorker
	if s.Backlog%p.JobsPerWorker != 0 {
		needed++
	}
	target := s.Busy + needed

	if target < p.Min {
		return p.Min
	}
	if target > p.Max {
		return p.Max
	}
	return target
}

// Scaler turns a stream of Samples into worker-count decisions, holding the
// hysteresis state that a single Sample cannot express. A Scaler is not safe
// for concurrent use; a Pool drives one from its single autoscaler
// goroutine.
type Scaler struct {
	// Policy is the sizing rule to apply.
	Policy Policy

	// shrinkStreak counts consecutive samples whose target was below the
	// current worker count.
	shrinkStreak int
}

// Next returns the number of workers the pool should have after observing
// s. Growth is immediate — a backlog is a queue of customers already
// waiting, so there is nothing to gain by easing into it — while shrinking
// is deliberately slow: one worker per decision, and only after
// ScaleDownAfter consecutive samples agree the pool is oversized. Retiring
// a worker that a burst two seconds later would need back is pure churn.
func (s *Scaler) Next(sample Sample) int {
	target := s.Policy.Target(sample)

	switch {
	case target > sample.Workers:
		s.shrinkStreak = 0
		return target
	case target < sample.Workers:
		s.shrinkStreak++
		if s.shrinkStreak < s.Policy.normalized().ScaleDownAfter {
			return sample.Workers
		}
		s.shrinkStreak = 0
		return sample.Workers - 1
	default:
		s.shrinkStreak = 0
		return sample.Workers
	}
}
