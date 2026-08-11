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

func TestWithJitterZeroFractionIsExact(t *testing.T) {
	b := WithJitter(ConstantBackoff(10*time.Second), 0)
	for i := 0; i < 20; i++ {
		if got := b(1); got != 10*time.Second {
			t.Fatalf("WithJitter(10s, 0)(1) = %v, want exactly 10s", got)
		}
	}
}

func TestWithJitterStaysWithinBounds(t *testing.T) {
	const base = 10 * time.Second
	const fraction = 0.3
	lo := time.Duration(float64(base) * (1 - fraction))
	hi := time.Duration(float64(base) * (1 + fraction))

	b := WithJitter(ConstantBackoff(base), fraction)
	for i := 0; i < 200; i++ {
		got := b(1)
		if got < lo || got > hi {
			t.Fatalf("WithJitter(10s, 0.3)(1) = %v, want in [%v, %v]", got, lo, hi)
		}
	}
}

func TestWithJitterVaries(t *testing.T) {
	b := WithJitter(ConstantBackoff(10*time.Second), 0.5)

	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		seen[b(1)] = true
	}
	if len(seen) < 2 {
		t.Fatal("WithJitter produced the same delay on every call, want randomized delays")
	}
}

func TestWithJitterClampsOutOfRangeFraction(t *testing.T) {
	const base = 10 * time.Second

	tooLow := WithJitter(ConstantBackoff(base), -1)
	if got := tooLow(1); got < 0 {
		t.Fatalf("WithJitter(10s, -1)(1) = %v, want a non-negative duration (fraction should clamp to 0)", got)
	}

	tooHigh := WithJitter(ConstantBackoff(base), 5)
	if got := tooHigh(1); got < 0 || got > 2*base {
		t.Fatalf("WithJitter(10s, 5)(1) = %v, want in [0, %v] (fraction should clamp to 1)", got, 2*base)
	}
}

func TestWithJitterPassesThroughZeroDelay(t *testing.T) {
	b := WithJitter(ConstantBackoff(0), 0.5)
	if got := b(1); got != 0 {
		t.Fatalf("WithJitter(0, 0.5)(1) = %v, want 0", got)
	}
}
