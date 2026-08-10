package worker

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	b := ExponentialBackoff(time.Second, 10*time.Second)

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, // clamped up to attempt 1
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // capped
		{6, 10 * time.Second}, // still capped
	}

	for _, tc := range cases {
		if got := b(tc.attempt); got != tc.want {
			t.Errorf("ExponentialBackoff(1s, 10s)(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestConstantBackoff(t *testing.T) {
	b := ConstantBackoff(5 * time.Second)
	for _, attempt := range []int{1, 2, 10} {
		if got := b(attempt); got != 5*time.Second {
			t.Errorf("ConstantBackoff(5s)(%d) = %v, want 5s", attempt, got)
		}
	}
}
