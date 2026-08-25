// Package loadgen drives synthetic load at a job queue and reports what
// came back: throughput, latency distribution, and errors.
//
// It exists because the microbenchmarks in this module answer a different
// question than the one an operator asks. A benchmark measures one
// operation in isolation, on an otherwise idle machine, with nothing
// competing for the database. A load test measures the whole system --
// producers, workers, SQLite, the lease sweep -- at a sustained arrival
// rate, which is where queueing effects, lock contention, and the tail of
// the latency distribution actually show up.
package loadgen

import (
	"math/bits"
	"sort"
	"time"
)

// Histogram records a distribution of latencies and answers percentile
// queries about it in constant space.
//
// Keeping every sample would be simpler, but a run at a few thousand jobs
// per second for a few minutes is millions of samples, and the recording
// then costs an allocation and a growing slice in the hot path -- the load
// generator would be measuring itself. Instead samples are counted into
// log-linear buckets: the value range is split at each power of two, and
// each of those octaves is divided into subCount equal parts. That bounds
// the relative error of any reported percentile at roughly 1/subCount
// (about 1.6%), independent of the magnitude of the value, so a 40µs
// enqueue and a 40s job are recorded with the same proportional accuracy
// from the same fixed-size table.
type Histogram struct {
	buckets []uint64
	count   uint64
	sum     int64
	min     int64
	max     int64
}

// subBits is the base-two log of the number of sub-buckets each octave is
// divided into. Six gives 64 sub-buckets, so the widest a bucket is
// relative to the values it holds is 1/32, and the whole int64 nanosecond
// range fits in fewer than 1900 counters.
const subBits = 6

// subCount is the number of sub-buckets per octave, and also the value
// below which every integer gets a bucket of its own.
const subCount = 1 << subBits

// bucketCount is the size of the counter table: subCount exact buckets for
// small values, then subCount/2 buckets for each of the 57 octaves an
// int64 nanosecond duration can span.
const bucketCount = subCount + 57*(subCount/2)

// NewHistogram returns an empty Histogram ready to record into.
func NewHistogram() *Histogram {
	return &Histogram{
		buckets: make([]uint64, bucketCount),
		min:     -1,
	}
}

// Record adds one observation. Negative durations -- which a caller can
// produce by subtracting timestamps taken from different clocks -- are
// clamped to zero rather than dropped, so the sample count stays an honest
// count of operations.
func (h *Histogram) Record(d time.Duration) {
	v := int64(d)
	if v < 0 {
		v = 0
	}

	h.buckets[index(v)]++
	h.count++
	h.sum += v
	if h.min < 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
}

// Count returns how many observations have been recorded.
func (h *Histogram) Count() uint64 { return h.count }

// Min returns the smallest observation exactly, or 0 if none were
// recorded. Unlike Percentile it is not bucket-rounded: the extremes of a
// run are worth reporting precisely.
func (h *Histogram) Min() time.Duration {
	if h.min < 0 {
		return 0
	}
	return time.Duration(h.min)
}

// Max returns the largest observation exactly, or 0 if none were recorded.
func (h *Histogram) Max() time.Duration { return time.Duration(h.max) }

// Mean returns the arithmetic mean of the observations, or 0 if none were
// recorded. It is computed from the running sum rather than the buckets,
// so it carries no bucketing error.
func (h *Histogram) Mean() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.sum / int64(h.count))
}

// Percentile returns the smallest recorded value at or below which at
// least p percent of observations fall, for p in [0, 100]. It reports the
// upper edge of the bucket the answer landed in, so the true value is
// guaranteed to be no larger than what is returned -- a tail latency is
// more useful rounded up than rounded down. Percentile of an empty
// Histogram is 0.
func (h *Histogram) Percentile(p float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	switch {
	case p <= 0:
		return h.Min()
	case p >= 100:
		return h.Max()
	}

	// The rank of the sample being sought, counting from one. Rounding up
	// keeps p=50 on a two-sample run pointing at the larger of the two,
	// matching the "at least p percent fall at or below" definition.
	rank := uint64(p/100*float64(h.count) + 0.5)
	if rank == 0 {
		rank = 1
	}

	var seen uint64
	for i, n := range h.buckets {
		if n == 0 {
			continue
		}
		seen += n
		if seen >= rank {
			return clampToRange(bucketHigh(i), h.min, h.max)
		}
	}
	return h.Max()
}

// Merge folds other's observations into h, so per-goroutine histograms
// recorded without a lock can be combined once the run is over.
func (h *Histogram) Merge(other *Histogram) {
	if other == nil || other.count == 0 {
		return
	}
	for i, n := range other.buckets {
		h.buckets[i] += n
	}
	h.count += other.count
	h.sum += other.sum
	if h.min < 0 || (other.min >= 0 && other.min < h.min) {
		h.min = other.min
	}
	if other.max > h.max {
		h.max = other.max
	}
}

// Percentiles returns the requested percentiles in one pass over the
// buckets, in the order given. Reporting a summary line usually needs
// several of them, and walking the table once is both faster and, more
// importantly, guaranteed self-consistent.
func (h *Histogram) Percentiles(ps ...float64) []time.Duration {
	out := make([]time.Duration, len(ps))
	if h.count == 0 || len(ps) == 0 {
		return out
	}

	// The endpoints are answered exactly rather than from the buckets,
	// matching Percentile, and are then left out of the walk.
	order := make([]int, 0, len(ps))
	for i, p := range ps {
		switch {
		case p <= 0:
			out[i] = h.Min()
		case p >= 100:
			out[i] = h.Max()
		default:
			order = append(order, i)
		}
	}

	// Sort the remaining requests by percentile so one forward walk can
	// answer them all, then scatter the answers back to the caller's
	// order.
	sort.SliceStable(order, func(a, b int) bool { return ps[order[a]] < ps[order[b]] })

	next := 0
	var seen uint64
	for i, n := range h.buckets {
		if n == 0 {
			continue
		}
		seen += n
		for next < len(order) {
			at := order[next]
			rank := uint64(ps[at]/100*float64(h.count) + 0.5)
			if rank == 0 {
				rank = 1
			}
			if seen < rank {
				break
			}
			out[at] = clampToRange(bucketHigh(i), h.min, h.max)
			next++
		}
		if next == len(order) {
			break
		}
	}
	for ; next < len(order); next++ {
		out[order[next]] = h.Max()
	}
	return out
}

// index maps a non-negative nanosecond value to its bucket.
//
// Values below subCount are small enough that a bucket each costs nothing
// and removes all error from the sub-microsecond range. Above that, the
// position of the value's highest set bit picks the octave and the next
// subBits bits pick the sub-bucket within it, which is what makes the
// error relative rather than absolute.
func index(v int64) int {
	if v < subCount {
		return int(v)
	}
	// shift is the octave, numbered from one: how far the value must be
	// shifted right for its top subBits+1 bits to be all that is left.
	shift := bits.Len64(uint64(v)) - subBits
	// The shifted value always lands in [subCount/2, subCount), because
	// its top bit is now the highest bit of a subBits-wide field.
	sub := int(v>>uint(shift)) - subCount/2
	return subCount + (shift-1)*(subCount/2) + sub
}

// bucketHigh returns the largest value bucket i can hold, which is what
// Percentile reports.
func bucketHigh(i int) int64 {
	if i < subCount {
		return int64(i)
	}
	k := i - subCount
	shift := k/(subCount/2) + 1
	sub := int64(k%(subCount/2)) + subCount/2
	return (sub+1)<<uint(shift) - 1
}

// clampToRange keeps a bucket-rounded answer inside the range actually
// observed, so a percentile can never claim a latency larger than the
// run's own maximum.
func clampToRange(v, min, max int64) time.Duration {
	if min >= 0 && v < min {
		v = min
	}
	if v > max {
		v = max
	}
	return time.Duration(v)
}
