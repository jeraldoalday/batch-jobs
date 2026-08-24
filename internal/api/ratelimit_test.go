package api

import (
	"fmt"
	"testing"
	"time"
)

func TestLimiterIdleTTLMatchesRefillTime(t *testing.T) {
	tests := []struct {
		cfg  RateLimit
		want time.Duration
	}{
		// A bucket refills at RPS tokens per second, so Burst/RPS seconds
		// is exactly when it is full again.
		{RateLimit{RPS: 10, Burst: 20}, 2 * time.Second},
		{RateLimit{RPS: 1, Burst: 1}, time.Second},
		{RateLimit{RPS: 0.5, Burst: 5}, 10 * time.Second},
	}

	for _, tt := range tests {
		if got := tt.cfg.idleTTL(); got != tt.want {
			t.Errorf("RateLimit{RPS: %v, Burst: %d}.idleTTL() = %v, want %v",
				tt.cfg.RPS, tt.cfg.Burst, got, tt.want)
		}
	}
}

func TestLimiterEvictsIdleBuckets(t *testing.T) {
	lim := newLimiter(RateLimit{RPS: 10, Burst: 10})

	for i := range 100 {
		lim.allow(fmt.Sprintf("10.0.0.%d", i))
	}
	if got := len(lim.buckets); got != 100 {
		t.Fatalf("limiter holds %d buckets, want 100", got)
	}

	// Move every bucket past its refill time, then trip the sweep. Without
	// eviction this map only ever grows, one entry per client IP the
	// process has ever seen.
	stale := time.Now().Add(-2 * lim.idleTTL)
	for _, b := range lim.buckets {
		b.lastFill = stale
	}
	lim.evictIdleLocked(time.Now())

	if got := len(lim.buckets); got != 0 {
		t.Errorf("limiter holds %d buckets after eviction, want 0", got)
	}
}

func TestLimiterKeepsActiveBuckets(t *testing.T) {
	lim := newLimiter(RateLimit{RPS: 1, Burst: 5})

	lim.allow("10.0.0.1")
	lim.allow("10.0.0.2")

	// One client has gone quiet long enough to have refilled; the other
	// asked a moment ago and is still spending its burst.
	lim.buckets["10.0.0.1"].lastFill = time.Now().Add(-2 * lim.idleTTL)
	lim.evictIdleLocked(time.Now())

	if _, ok := lim.buckets["10.0.0.1"]; ok {
		t.Error("an idle bucket survived eviction")
	}
	if _, ok := lim.buckets["10.0.0.2"]; !ok {
		t.Error("an active bucket was evicted, which would refund its spent tokens")
	}
}

func TestLimiterEvictionDoesNotRefundTokens(t *testing.T) {
	// One token per second, one at a time: the second request in the same
	// instant is denied.
	lim := newLimiter(RateLimit{RPS: 1, Burst: 1})

	if !lim.allow("10.0.0.1") {
		t.Fatal("first request denied, want allowed")
	}
	if lim.allow("10.0.0.1") {
		t.Fatal("second immediate request allowed, want denied")
	}

	// A sweep right now must not drop the throttled client's bucket: doing
	// so would hand it a full one and let it straight through.
	lim.evictIdleLocked(time.Now())
	if lim.allow("10.0.0.1") {
		t.Error("a sweep refilled a throttled client's bucket, want it still denied")
	}
}

func TestLimiterSweepsOnItsOwnSchedule(t *testing.T) {
	// A fast limit refills in 100ms, but sweeping that often on every
	// request would cost more than the memory it reclaims.
	lim := newLimiter(RateLimit{RPS: 100, Burst: 10})
	if lim.sweepInterval != minSweepInterval {
		t.Errorf("sweepInterval = %v, want it floored at %v", lim.sweepInterval, minSweepInterval)
	}

	// A slow limit refills slowly, and its buckets must live at least that
	// long, so the sweep interval follows the TTL up.
	slow := newLimiter(RateLimit{RPS: 0.1, Burst: 60})
	if slow.sweepInterval != slow.idleTTL {
		t.Errorf("sweepInterval = %v, want the idle TTL %v", slow.sweepInterval, slow.idleTTL)
	}

	// The sweep is driven by allow rather than by a goroutine, so a
	// limiter that is never called costs nothing.
	lim.lastSweep = time.Now().Add(-2 * lim.sweepInterval)
	lim.allow("10.0.0.1")
	if time.Since(lim.lastSweep) > time.Second {
		t.Error("allow did not run the overdue sweep")
	}
}

func TestLimiterBucketsAreIndependent(t *testing.T) {
	lim := newLimiter(RateLimit{RPS: 1, Burst: 2})

	// Spending one client's burst leaves every other client untouched.
	for range 2 {
		if !lim.allow("10.0.0.1") {
			t.Fatal("request within the burst denied")
		}
	}
	if lim.allow("10.0.0.1") {
		t.Fatal("request past the burst allowed")
	}
	if !lim.allow("10.0.0.2") {
		t.Error("a second client was denied on the first client's spent budget")
	}
}
