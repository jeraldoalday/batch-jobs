package worker

import "time"

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
