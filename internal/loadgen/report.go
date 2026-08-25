package loadgen

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// ReportPercentiles is the set of percentiles a report prints. It stops at
// p99.9 because a run of a few tens of thousands of operations does not
// have the sample count to say anything trustworthy about p99.99: at
// 20,000 samples that percentile is decided by two observations.
var ReportPercentiles = []float64{50, 90, 95, 99, 99.9}

// Report renders a human-readable summary of a run to w.
//
// The layout is deliberately plain text rather than a table library: the
// output is read in a terminal, pasted into commit messages, and diffed
// between runs, and all three want stable, greppable lines.
func (r Result) Report(w io.Writer, title string) error {
	var b strings.Builder

	if title != "" {
		b.WriteString(title)
		b.WriteString("\n")
		b.WriteString(strings.Repeat("=", len(title)))
		b.WriteString("\n\n")
	}

	offered := "closed loop (unpaced)"
	if r.Config.openLoop() {
		offered = fmt.Sprintf("%.0f ops/s (open loop)", r.Config.Rate)
	}
	fmt.Fprintf(&b, "  offered      %s\n", offered)
	fmt.Fprintf(&b, "  producers    %d\n", r.Config.concurrency())
	fmt.Fprintf(&b, "  window       %s", r.Elapsed.Round(time.Millisecond))
	if r.Config.Warmup > 0 {
		fmt.Fprintf(&b, " (after %s warmup)", r.Config.Warmup)
	}
	b.WriteString("\n\n")

	fmt.Fprintf(&b, "  throughput   %.1f ops/s\n", r.Rate())
	fmt.Fprintf(&b, "  operations   %d ok, %d failed (%.2f%%)\n", r.Submitted, r.Failed, r.ErrorRate()*100)
	b.WriteString("\n")

	writeDistribution(&b, "latency", r.Latency)

	if r.Schedule != nil {
		b.WriteString("\n")
		writeDistribution(&b, "send lag", r.Schedule)
		if !r.KeptSchedule(0.05) {
			// Said plainly, because it invalidates the latency numbers
			// above: they describe a lighter load than the one asked for.
			fmt.Fprintf(&b, "\n  WARNING: the generator offered %.1f ops/s against a target of %.0f.\n", r.Rate(), r.Config.Rate)
			b.WriteString("  Latencies below describe that lower load. Raise Concurrency or lower Rate.\n")
		}
	}

	if len(r.Errors) > 0 {
		b.WriteString("\n  errors\n")
		for _, e := range sortedErrors(r.Errors) {
			fmt.Fprintf(&b, "    %8d  %s\n", e.count, truncate(e.msg, 96))
		}
	}

	_, err := io.WriteString(w, b.String())
	return err
}

// writeDistribution prints one histogram's summary block under a label.
func writeDistribution(b *strings.Builder, label string, h *Histogram) {
	if h == nil || h.Count() == 0 {
		fmt.Fprintf(b, "  %-12s (no samples)\n", label)
		return
	}

	ps := h.Percentiles(ReportPercentiles...)
	fmt.Fprintf(b, "  %-12s min %s  mean %s  max %s\n", label,
		round(h.Min()), round(h.Mean()), round(h.Max()))
	for i, p := range ReportPercentiles {
		fmt.Fprintf(b, "  %-12s p%-6s %s\n", "", trimPercentile(p), round(ps[i]))
	}
}

// round trims a duration to three significant figures' worth of units, so
// a column of them lines up and stays readable: 1.23ms rather than
// 1.234567ms, 45.6s rather than 45.612345678s.
func round(d time.Duration) time.Duration {
	switch {
	case d == 0:
		return 0
	case d < time.Microsecond:
		return d
	case d < time.Millisecond:
		return d.Round(10 * time.Nanosecond)
	case d < time.Second:
		return d.Round(10 * time.Microsecond)
	case d < time.Minute:
		return d.Round(10 * time.Millisecond)
	default:
		return d.Round(time.Second)
	}
}

// trimPercentile formats a percentile without a trailing ".0", so the
// common whole-number cases read as "p99" rather than "p99.0".
func trimPercentile(p float64) string {
	s := fmt.Sprintf("%.1f", p)
	return strings.TrimSuffix(s, ".0")
}

// errorCount pairs an error string with how often it was seen.
type errorCount struct {
	msg   string
	count uint64
}

// sortedErrors orders the error tally by descending count, breaking ties
// on the message so the output of two identical runs is identical.
func sortedErrors(errs map[string]uint64) []errorCount {
	out := make([]errorCount, 0, len(errs))
	for msg, n := range errs {
		out = append(out, errorCount{msg: msg, count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].count != out[j].count {
			return out[i].count > out[j].count
		}
		return out[i].msg < out[j].msg
	})
	return out
}

// truncate shortens s to at most n runes, marking that it did so.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// ReportHistogram renders a standalone distribution -- one gathered
// outside a Run, such as end-to-end job latency observed by a handler --
// in the same shape as the blocks inside Report.
func ReportHistogram(w io.Writer, label string, h *Histogram) error {
	var b strings.Builder
	writeDistribution(&b, label, h)
	_, err := io.WriteString(w, b.String())
	return err
}
