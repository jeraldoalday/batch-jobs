package loadgen

import (
	"math/rand"
	"testing"
	"time"
)

func TestHistogramEmpty(t *testing.T) {
	h := NewHistogram()

	if got := h.Count(); got != 0 {
		t.Errorf("Count() = %d, want 0", got)
	}
	for _, tc := range []struct {
		name string
		got  time.Duration
	}{
		{"Min", h.Min()},
		{"Max", h.Max()},
		{"Mean", h.Mean()},
		{"P50", h.Percentile(50)},
		{"P99", h.Percentile(99)},
	} {
		if tc.got != 0 {
			t.Errorf("%s() on empty histogram = %v, want 0", tc.name, tc.got)
		}
	}
}

func TestHistogramSingleSample(t *testing.T) {
	h := NewHistogram()
	h.Record(5 * time.Millisecond)

	if got := h.Count(); got != 1 {
		t.Errorf("Count() = %d, want 1", got)
	}
	if got := h.Min(); got != 5*time.Millisecond {
		t.Errorf("Min() = %v, want 5ms", got)
	}
	if got := h.Max(); got != 5*time.Millisecond {
		t.Errorf("Max() = %v, want 5ms", got)
	}
	if got := h.Mean(); got != 5*time.Millisecond {
		t.Errorf("Mean() = %v, want 5ms", got)
	}
	// Every percentile of a one-sample distribution is that sample; the
	// bucket clamp is what keeps p50 from reporting the bucket's upper
	// edge instead.
	for _, p := range []float64{0, 1, 50, 99, 100} {
		if got := h.Percentile(p); got != 5*time.Millisecond {
			t.Errorf("Percentile(%v) = %v, want 5ms", p, got)
		}
	}
}

// TestHistogramPercentileAccuracy checks the central promise of the
// log-linear bucketing: a reported percentile is never below the true one,
// and never more than one bucket width (about 1/32) above it.
func TestHistogramPercentileAccuracy(t *testing.T) {
	h := NewHistogram()
	// A uniform 1..10000 microsecond distribution: the true p-th
	// percentile is p*100 microseconds.
	for i := 1; i <= 10000; i++ {
		h.Record(time.Duration(i) * time.Microsecond)
	}

	for _, p := range []float64{1, 10, 25, 50, 75, 90, 95, 99, 99.9} {
		want := time.Duration(p*100) * time.Microsecond
		got := h.Percentile(p)
		if got < want {
			t.Errorf("Percentile(%v) = %v, below true value %v", p, got, want)
		}
		if float64(got) > float64(want)*1.05 {
			t.Errorf("Percentile(%v) = %v, more than 5%% above true value %v", p, got, want)
		}
	}
}

// TestHistogramRelativeErrorAcrossMagnitudes records a single value at
// wildly different scales and checks the reported percentile stays within
// the bucketing's relative-error bound at every one of them. This is the
// property a linear histogram would not have.
func TestHistogramRelativeErrorAcrossMagnitudes(t *testing.T) {
	values := []time.Duration{
		37 * time.Nanosecond,
		1500 * time.Nanosecond,
		250 * time.Microsecond,
		9 * time.Millisecond,
		1200 * time.Millisecond,
		45 * time.Second,
		20 * time.Minute,
	}

	for _, want := range values {
		h := NewHistogram()
		for i := 0; i < 100; i++ {
			h.Record(want)
		}
		got := h.Percentile(50)
		if got != want {
			t.Errorf("Percentile(50) of 100x%v = %v; a single repeated value must round-trip exactly", want, got)
		}
	}
}

// TestHistogramIndexIsMonotonic is the invariant the whole scheme rests
// on: a larger value never lands in an earlier bucket. If it did,
// Percentile's forward walk would report nonsense.
func TestHistogramIndexIsMonotonic(t *testing.T) {
	prev := index(0)
	for v := int64(1); v < 1<<20; v += 1 + v/97 {
		got := index(v)
		if got < prev {
			t.Fatalf("index(%d) = %d, less than the index of a smaller value (%d)", v, got, prev)
		}
		prev = got
	}

	// Spot-check the top of the range too, where the octaves are widest.
	for e := 20; e < 62; e++ {
		v := int64(1) << uint(e)
		got := index(v)
		if got < prev {
			t.Fatalf("index(2^%d) = %d, less than the index of a smaller value (%d)", e, got, prev)
		}
		if got >= bucketCount {
			t.Fatalf("index(2^%d) = %d, outside the %d-bucket table", e, got, bucketCount)
		}
		prev = got
	}
}

// TestHistogramBucketHighContainsValue checks index and bucketHigh agree:
// a value must never exceed the upper edge of the bucket it was counted
// in, or Percentile could report a latency smaller than one that was
// actually observed.
func TestHistogramBucketHighContainsValue(t *testing.T) {
	for v := int64(0); v < 1<<22; v += 1 + v/31 {
		i := index(v)
		if high := bucketHigh(i); high < v {
			t.Fatalf("value %d landed in bucket %d whose upper edge is %d", v, i, high)
		}
		if i > 0 {
			if prevHigh := bucketHigh(i - 1); prevHigh >= v {
				t.Fatalf("value %d landed in bucket %d, but bucket %d already covers up to %d", v, i, i-1, prevHigh)
			}
		}
	}
}

func TestHistogramNegativeDurationsClampToZero(t *testing.T) {
	h := NewHistogram()
	h.Record(-5 * time.Second)
	h.Record(10 * time.Millisecond)

	if got := h.Count(); got != 2 {
		t.Errorf("Count() = %d, want 2: a negative sample is clamped, not dropped", got)
	}
	if got := h.Min(); got != 0 {
		t.Errorf("Min() = %v, want 0", got)
	}
}

func TestHistogramMerge(t *testing.T) {
	a, b := NewHistogram(), NewHistogram()
	for i := 1; i <= 500; i++ {
		a.Record(time.Duration(i) * time.Microsecond)
	}
	for i := 501; i <= 1000; i++ {
		b.Record(time.Duration(i) * time.Microsecond)
	}

	whole := NewHistogram()
	for i := 1; i <= 1000; i++ {
		whole.Record(time.Duration(i) * time.Microsecond)
	}

	a.Merge(b)

	if a.Count() != whole.Count() {
		t.Errorf("merged Count() = %d, want %d", a.Count(), whole.Count())
	}
	if a.Min() != whole.Min() {
		t.Errorf("merged Min() = %v, want %v", a.Min(), whole.Min())
	}
	if a.Max() != whole.Max() {
		t.Errorf("merged Max() = %v, want %v", a.Max(), whole.Max())
	}
	if a.Mean() != whole.Mean() {
		t.Errorf("merged Mean() = %v, want %v", a.Mean(), whole.Mean())
	}
	for _, p := range []float64{50, 90, 99} {
		if got, want := a.Percentile(p), whole.Percentile(p); got != want {
			t.Errorf("merged Percentile(%v) = %v, want %v", p, got, want)
		}
	}
}

func TestHistogramMergeNilAndEmpty(t *testing.T) {
	h := NewHistogram()
	h.Record(3 * time.Millisecond)

	h.Merge(nil)
	h.Merge(NewHistogram())

	if got := h.Count(); got != 1 {
		t.Errorf("Count() = %d, want 1: merging nothing must change nothing", got)
	}
	if got := h.Min(); got != 3*time.Millisecond {
		t.Errorf("Min() = %v, want 3ms: an empty histogram's unset min must not win", got)
	}
}

// TestHistogramPercentilesMatchesPercentile checks the batched query
// against the single one, since the batched walk is the subtler of the
// two and is what the report actually uses.
func TestHistogramPercentilesMatchesPercentile(t *testing.T) {
	h := NewHistogram()
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		h.Record(time.Duration(rng.Intn(5_000_000)))
	}

	// Deliberately out of order, to exercise the sort-and-scatter.
	ps := []float64{99, 50, 100, 90, 0, 99.9, 75}
	got := h.Percentiles(ps...)

	for i, p := range ps {
		if want := h.Percentile(p); got[i] != want {
			t.Errorf("Percentiles()[%d] for p=%v = %v, want %v", i, p, got[i], want)
		}
	}
}

func TestHistogramPercentilesEmpty(t *testing.T) {
	h := NewHistogram()
	got := h.Percentiles(50, 99)
	if len(got) != 2 {
		t.Fatalf("Percentiles() returned %d values, want 2", len(got))
	}
	for i, d := range got {
		if d != 0 {
			t.Errorf("Percentiles()[%d] = %v on an empty histogram, want 0", i, d)
		}
	}
	if got := h.Percentiles(); len(got) != 0 {
		t.Errorf("Percentiles() with no arguments returned %d values, want 0", len(got))
	}
}

// TestHistogramPercentileIsMonotonic guards against the batched and
// single-shot walks ever reporting a lower latency for a higher
// percentile, which would make a report self-contradictory.
func TestHistogramPercentileIsMonotonic(t *testing.T) {
	h := NewHistogram()
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 5000; i++ {
		// A long-tailed distribution, which is where percentile bugs hide.
		h.Record(time.Duration(rng.ExpFloat64() * float64(time.Millisecond)))
	}

	var prev time.Duration
	for p := 0.0; p <= 100; p += 0.5 {
		got := h.Percentile(p)
		if got < prev {
			t.Fatalf("Percentile(%v) = %v, below Percentile(%v) = %v", p, got, p-0.5, prev)
		}
		prev = got
	}
}

func BenchmarkHistogramRecord(b *testing.B) {
	h := NewHistogram()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Record(time.Duration(i%1_000_000) * time.Nanosecond)
	}
}
