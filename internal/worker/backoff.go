package worker

import (
	"math/rand"
	"time"
)

// BackoffFunc computes how long to wait before a job's next attempt, given
// the attempt number that just failed (1 for the first failure, 2 for the
// second, and so on).
type BackoffFunc func(attempt int) time.Duration

// ExponentialBackoff returns a BackoffFunc that starts at base and doubles
// with each attempt, never exceeding max.
func ExponentialBackoff(base, max time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := base
		for i := 1; i < attempt; i++ {
			if d >= max {
				return max
			}
			d *= 2
		}
		if d > max {
			return max
		}
		return d
	}
}

// ConstantBackoff returns a BackoffFunc that always waits d, regardless of
// attempt number.
func ConstantBackoff(d time.Duration) BackoffFunc {
	return func(int) time.Duration { return d }
}

// WithJitter wraps base so each computed delay is randomly scaled by a
// factor in [1-fraction, 1+fraction]. Without jitter, every job that fails
// at the same moment (e.g. because a shared dependency went down) retries
// in lockstep and can re-overwhelm that dependency the instant it
// recovers; randomizing each delay spreads retries out instead. fraction
// is clamped to [0, 1]; 0 disables jitter and returns base's delay
// unchanged.
func WithJitter(base BackoffFunc, fraction float64) BackoffFunc {
	switch {
	case fraction < 0:
		fraction = 0
	case fraction > 1:
		fraction = 1
	}

	return func(attempt int) time.Duration {
		d := base(attempt)
		if fraction == 0 || d <= 0 {
			return d
		}
		scale := 1 + fraction*(2*rand.Float64()-1)
		return time.Duration(float64(d) * scale)
	}
}
