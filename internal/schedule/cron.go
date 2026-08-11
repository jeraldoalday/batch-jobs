// Package schedule adds recurring, cron-driven jobs on top of the queue
// package: a Schedule is a job template plus a cron expression, and a Store
// persists Schedules so a scheduler loop (see internal/scheduler) can find
// the ones due to fire.
package schedule

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// bitset64 tracks which values in a small integer range (0-63) are present
// in a parsed cron field.
type bitset64 uint64

func (b *bitset64) set(n int) { *b |= 1 << uint(n) }

func (b bitset64) has(n int) bool { return b&(1<<uint(n)) != 0 }

// Cron is a parsed 5-field cron expression: minute, hour, day-of-month,
// month, and day-of-week. A Cron is safe for concurrent use; it holds no
// mutable state after parsing.
type Cron struct {
	expr string

	minutes bitset64 // 0-59
	hours   bitset64 // 0-23
	doms    bitset64 // 1-31
	months  bitset64 // 1-12
	dows    bitset64 // 0-6, Sunday = 0

	// domWildcard and dowWildcard record whether the day-of-month and
	// day-of-week fields were literally "*", which changes how the two
	// combine: per standard cron semantics, if only one of the two
	// fields is restricted it alone determines whether a day matches; if
	// both are restricted, a day matches if either one does.
	domWildcard bool
	dowWildcard bool
}

// String returns the expression Cron was parsed from.
func (c *Cron) String() string { return c.expr }

// ParseCron parses a standard 5-field cron expression (minute hour
// day-of-month month day-of-week). Each field accepts "*", a single value,
// a comma-separated list, a "lo-hi" range, or any of those with a "/step"
// suffix (e.g. "*/15", "10-20/5", "5/10").
func ParseCron(expr string) (*Cron, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("schedule: cron expression %q must have 5 fields (minute hour dom month dow), got %d", expr, len(fields))
	}

	minutes, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("schedule: minute: %w", err)
	}
	hours, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("schedule: hour: %w", err)
	}
	doms, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("schedule: day-of-month: %w", err)
	}
	months, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("schedule: month: %w", err)
	}
	dows, err := parseField(fields[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("schedule: day-of-week: %w", err)
	}

	return &Cron{
		expr:        expr,
		minutes:     minutes,
		hours:       hours,
		doms:        doms,
		months:      months,
		dows:        dows,
		domWildcard: fields[2] == "*",
		dowWildcard: fields[4] == "*",
	}, nil
}

// parseField parses one comma-separated cron field into a bitset of the
// values it selects, each of which must fall within [min, max].
func parseField(field string, min, max int) (bitset64, error) {
	var bits bitset64
	for _, item := range strings.Split(field, ",") {
		lo, hi, step, err := parseItem(item, min, max)
		if err != nil {
			return 0, fmt.Errorf("invalid field %q: %w", field, err)
		}
		for v := lo; v <= hi; v += step {
			bits.set(v)
		}
	}
	return bits, nil
}

// parseItem parses one comma-separated element of a cron field, e.g. "*",
// "5", "1-5", "*/15", or "10-20/5", into an inclusive [lo, hi] range and a
// step.
func parseItem(item string, min, max int) (lo, hi, step int, err error) {
	rangePart, stepPart, hasStep := strings.Cut(item, "/")
	step = 1
	if hasStep {
		step, err = strconv.Atoi(stepPart)
		if err != nil || step <= 0 {
			return 0, 0, 0, fmt.Errorf("invalid step %q", stepPart)
		}
	}

	switch {
	case rangePart == "*":
		lo, hi = min, max
	case strings.Contains(rangePart, "-"):
		loStr, hiStr, _ := strings.Cut(rangePart, "-")
		if lo, err = strconv.Atoi(loStr); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid range start %q", loStr)
		}
		if hi, err = strconv.Atoi(hiStr); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid range end %q", hiStr)
		}
	default:
		if lo, err = strconv.Atoi(rangePart); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid value %q", rangePart)
		}
		hi = lo
		if hasStep {
			// "a/n" means "every n-th value starting at a, up to max",
			// the same shorthand cron uses for "a-max/n".
			hi = max
		}
	}

	if lo < min || hi > max || lo > hi {
		return 0, 0, 0, fmt.Errorf("value out of range [%d,%d]: %q", min, max, item)
	}
	return lo, hi, step, nil
}

// maxSearchHorizon bounds how far into the future Next will look before
// giving up on an expression that never matches (e.g. "0 0 30 2 *", since
// February never has 30 days).
const maxSearchHorizon = 5 * 366 * 24 * time.Hour

// Next returns the earliest time strictly after from at which c matches,
// and true. If no matching time exists within the next five years, it
// returns the zero time and false.
func (c *Cron) Next(from time.Time) (time.Time, bool) {
	loc := from.Location()
	limit := from.Add(maxSearchHorizon)

	// Start at the next whole minute after from: cron fires on minute
	// boundaries, and Next must never return from itself even when from
	// already matches.
	t := from.Truncate(time.Minute).Add(time.Minute)

	for t.Before(limit) {
		if !c.months.has(int(t.Month())) {
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc).AddDate(0, 1, 0)
			continue
		}
		if !c.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1)
			continue
		}
		if !c.hours.has(t.Hour()) {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc).Add(time.Hour)
			continue
		}
		if !c.minutes.has(t.Minute()) {
			t = t.Add(time.Minute)
			continue
		}
		return t, true
	}
	return time.Time{}, false
}

// dayMatches reports whether t's calendar day satisfies the expression's
// day-of-month and day-of-week fields, applying cron's OR-when-both-
// restricted rule.
func (c *Cron) dayMatches(t time.Time) bool {
	switch {
	case c.domWildcard && c.dowWildcard:
		return true
	case c.domWildcard:
		return c.dows.has(int(t.Weekday()))
	case c.dowWildcard:
		return c.doms.has(t.Day())
	default:
		return c.doms.has(t.Day()) || c.dows.has(int(t.Weekday()))
	}
}
