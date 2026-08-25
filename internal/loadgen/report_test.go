package loadgen

import (
	"strings"
	"testing"
	"time"
)

// reportOf renders a Result and returns the text, failing the test if
// rendering itself errors.
func reportOf(t *testing.T, r Result, title string) string {
	t.Helper()
	var b strings.Builder
	if err := r.Report(&b, title); err != nil {
		t.Fatalf("Report: %v", err)
	}
	return b.String()
}

func TestReportClosedLoop(t *testing.T) {
	res := Result{
		Config:    Config{Concurrency: 4},
		Elapsed:   10 * time.Second,
		Submitted: 20000,
		Latency:   NewHistogram(),
	}
	for i := 1; i <= 1000; i++ {
		res.Latency.Record(time.Duration(i) * time.Microsecond)
	}

	out := reportOf(t, res, "enqueue")

	for _, want := range []string{
		"enqueue\n=======",
		"closed loop (unpaced)",
		"producers    4",
		"throughput   2000.0 ops/s",
		"20000 ok, 0 failed (0.00%)",
		"p50", "p90", "p95", "p99", "p99.9",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "send lag") {
		t.Errorf("closed-loop report mentions send lag, which it does not measure:\n%s", out)
	}
}

func TestReportOpenLoopWarnsWhenBehindSchedule(t *testing.T) {
	res := Result{
		Config:    Config{Rate: 5000, Concurrency: 8, Warmup: 2 * time.Second},
		Elapsed:   10 * time.Second,
		Submitted: 10000, // 1000 ops/s against a 5000 target.
		Latency:   NewHistogram(),
		Schedule:  NewHistogram(),
	}
	res.Latency.Record(time.Millisecond)
	res.Schedule.Record(3 * time.Second)

	out := reportOf(t, res, "")

	if !strings.Contains(out, "5000 ops/s (open loop)") {
		t.Errorf("report does not state the offered rate:\n%s", out)
	}
	if !strings.Contains(out, "after 2s warmup") {
		t.Errorf("report does not mention the warmup:\n%s", out)
	}
	if !strings.Contains(out, "send lag") {
		t.Errorf("open-loop report omits the send-lag block:\n%s", out)
	}
	if !strings.Contains(out, "WARNING") {
		t.Errorf("report at a fifth of the target rate carries no warning:\n%s", out)
	}
}

func TestReportOmitsWarningWhenOnSchedule(t *testing.T) {
	res := Result{
		Config:    Config{Rate: 1000, Concurrency: 8},
		Elapsed:   10 * time.Second,
		Submitted: 9900,
		Latency:   NewHistogram(),
		Schedule:  NewHistogram(),
	}
	res.Latency.Record(time.Millisecond)
	res.Schedule.Record(time.Microsecond)

	if out := reportOf(t, res, ""); strings.Contains(out, "WARNING") {
		t.Errorf("report within 1%% of its target rate carries a warning:\n%s", out)
	}
}

func TestReportListsErrorsByDescendingCount(t *testing.T) {
	res := Result{
		Config:  Config{Concurrency: 1},
		Elapsed: time.Second,
		Failed:  6,
		Latency: NewHistogram(),
		Errors:  map[string]uint64{"rare": 1, "common": 5},
	}
	res.Latency.Record(time.Millisecond)

	out := reportOf(t, res, "")
	common, rare := strings.Index(out, "common"), strings.Index(out, "rare")
	if common < 0 || rare < 0 {
		t.Fatalf("report is missing an error line:\n%s", out)
	}
	if common > rare {
		t.Errorf("errors are not ordered by descending count:\n%s", out)
	}
}

func TestReportEmptyHistogram(t *testing.T) {
	res := Result{Config: Config{Concurrency: 2}, Latency: NewHistogram()}

	out := reportOf(t, res, "")
	if !strings.Contains(out, "(no samples)") {
		t.Errorf("report of a run that observed nothing does not say so:\n%s", out)
	}
}

func TestReportIsDeterministic(t *testing.T) {
	res := Result{
		Config:  Config{Concurrency: 2},
		Elapsed: time.Second,
		Failed:  9,
		Latency: NewHistogram(),
		Errors:  map[string]uint64{"a": 3, "b": 3, "c": 3},
	}
	res.Latency.Record(time.Millisecond)

	first := reportOf(t, res, "run")
	for i := 0; i < 20; i++ {
		if got := reportOf(t, res, "run"); got != first {
			t.Fatalf("report differs between renderings of the same Result:\n%s\n---\n%s", first, got)
		}
	}
}

func TestReportHistogramStandalone(t *testing.T) {
	h := NewHistogram()
	for i := 1; i <= 100; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}

	var b strings.Builder
	if err := ReportHistogram(&b, "end-to-end", h); err != nil {
		t.Fatalf("ReportHistogram: %v", err)
	}
	out := b.String()
	if !strings.Contains(out, "end-to-end") {
		t.Errorf("standalone report is missing its label:\n%s", out)
	}
	if !strings.Contains(out, "p99") {
		t.Errorf("standalone report is missing its percentiles:\n%s", out)
	}
}

func TestRoundKeepsDurationsReadable(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want time.Duration
	}{
		{0, 0},
		{437 * time.Nanosecond, 437 * time.Nanosecond},
		{123456 * time.Nanosecond, 123460 * time.Nanosecond},
		{1234567 * time.Nanosecond, 1230 * time.Microsecond},
		{1234567890 * time.Nanosecond, 1230 * time.Millisecond},
		{95*time.Second + 400*time.Millisecond, 95 * time.Second},
	}
	for _, tc := range tests {
		if got := round(tc.in); got != tc.want {
			t.Errorf("round(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestTrimPercentile(t *testing.T) {
	tests := map[float64]string{50: "50", 99: "99", 99.9: "99.9", 99.5: "99.5"}
	for p, want := range tests {
		if got := trimPercentile(p); got != want {
			t.Errorf("trimPercentile(%v) = %q, want %q", p, got, want)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("truncate(%q, 10) = %q, want it unchanged", "short", got)
	}
	got := truncate("abcdefghij", 5)
	if len([]rune(got)) != 5 {
		t.Errorf("truncate(..., 5) = %q, %d runes, want 5", got, len([]rune(got)))
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncate(..., 5) = %q, want it marked as shortened", got)
	}
}
