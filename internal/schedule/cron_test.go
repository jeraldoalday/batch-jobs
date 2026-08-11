package schedule

import (
	"testing"
	"time"
)

func mustParseCron(t *testing.T, expr string) *Cron {
	t.Helper()
	c, err := ParseCron(expr)
	if err != nil {
		t.Fatalf("ParseCron(%q): %v", expr, err)
	}
	return c
}

func mustParseTime(t *testing.T, layout, value string) time.Time {
	t.Helper()
	tm, err := time.Parse(layout, value)
	if err != nil {
		t.Fatalf("time.Parse(%q, %q): %v", layout, value, err)
	}
	return tm.UTC()
}

const timeLayout = "2006-01-02 15:04"

func TestParseCronFieldCount(t *testing.T) {
	tests := []string{
		"* * * *",
		"* * * * * *",
		"",
	}
	for _, expr := range tests {
		if _, err := ParseCron(expr); err == nil {
			t.Errorf("ParseCron(%q): want error for wrong field count", expr)
		}
	}
}

func TestParseCronInvalidFields(t *testing.T) {
	tests := []string{
		"60 * * * *",   // minute out of range
		"* 24 * * *",   // hour out of range
		"* * 0 * *",    // day-of-month out of range (min is 1)
		"* * * 13 *",   // month out of range
		"* * * * 7",    // day-of-week out of range (max is 6)
		"a * * * *",    // non-numeric
		"5-3 * * * *",  // inverted range
		"*/0 * * * *",  // zero step
		"1,,2 * * * *", // empty list item
	}
	for _, expr := range tests {
		if _, err := ParseCron(expr); err == nil {
			t.Errorf("ParseCron(%q): want error, got none", expr)
		}
	}
}

func TestParseCronValid(t *testing.T) {
	tests := []string{
		"* * * * *",
		"0 0 * * *",
		"*/15 * * * *",
		"0 9-17 * * 1-5",
		"0,30 * * * *",
		"0 0 1,15 * *",
		"10-20/5 * * * *",
		"5/10 * * * *",
	}
	for _, expr := range tests {
		if _, err := ParseCron(expr); err != nil {
			t.Errorf("ParseCron(%q): unexpected error: %v", expr, err)
		}
	}
}

func TestCronNextEveryMinute(t *testing.T) {
	c := mustParseCron(t, "* * * * *")
	from := mustParseTime(t, timeLayout, "2026-01-01 10:00")
	got, ok := c.Next(from)
	if !ok {
		t.Fatal("Next: want ok=true")
	}
	want := mustParseTime(t, timeLayout, "2026-01-01 10:01")
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want %v", from, got, want)
	}
}

func TestCronNextTopOfHour(t *testing.T) {
	c := mustParseCron(t, "0 * * * *")
	from := mustParseTime(t, timeLayout, "2026-01-01 10:15")
	got, ok := c.Next(from)
	if !ok {
		t.Fatal("Next: want ok=true")
	}
	want := mustParseTime(t, timeLayout, "2026-01-01 11:00")
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want %v", from, got, want)
	}
}

func TestCronNextDaily(t *testing.T) {
	c := mustParseCron(t, "30 6 * * *")
	from := mustParseTime(t, timeLayout, "2026-01-01 06:30")
	got, ok := c.Next(from)
	if !ok {
		t.Fatal("Next: want ok=true")
	}
	// from already matches exactly; Next must return the following day's
	// occurrence, never from itself.
	want := mustParseTime(t, timeLayout, "2026-01-02 06:30")
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want %v", from, got, want)
	}
}

func TestCronNextEveryFifteenMinutes(t *testing.T) {
	c := mustParseCron(t, "*/15 * * * *")
	from := mustParseTime(t, timeLayout, "2026-01-01 10:16")
	got, ok := c.Next(from)
	if !ok {
		t.Fatal("Next: want ok=true")
	}
	want := mustParseTime(t, timeLayout, "2026-01-01 10:30")
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want %v", from, got, want)
	}
}

func TestCronNextWeekdaysAcrossWeekend(t *testing.T) {
	c := mustParseCron(t, "0 9 * * 1-5")
	// 2026-01-03 is a Saturday.
	from := mustParseTime(t, timeLayout, "2026-01-03 09:00")
	got, ok := c.Next(from)
	if !ok {
		t.Fatal("Next: want ok=true")
	}
	// Next weekday match is Monday 2026-01-05.
	want := mustParseTime(t, timeLayout, "2026-01-05 09:00")
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want %v", from, got, want)
	}
}

func TestCronNextMonthlyAcrossYearBoundary(t *testing.T) {
	c := mustParseCron(t, "0 0 1 * *")
	from := mustParseTime(t, timeLayout, "2026-12-01 00:00")
	got, ok := c.Next(from)
	if !ok {
		t.Fatal("Next: want ok=true")
	}
	want := mustParseTime(t, timeLayout, "2027-01-01 00:00")
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want %v", from, got, want)
	}
}

func TestCronNextDomDowOR(t *testing.T) {
	// Fires on the 1st or 15th of the month, OR every Wednesday, since
	// both day-of-month and day-of-week are restricted.
	c := mustParseCron(t, "0 0 1,15 * 3")
	// 2026-01-02 is a Friday; the next Wednesday is 2026-01-07, which
	// comes before the 15th.
	from := mustParseTime(t, timeLayout, "2026-01-02 00:00")
	got, ok := c.Next(from)
	if !ok {
		t.Fatal("Next: want ok=true")
	}
	want := mustParseTime(t, timeLayout, "2026-01-07 00:00")
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want %v", from, got, want)
	}
}

func TestCronNextImpossibleDateGivesUp(t *testing.T) {
	// February never has 30 days, so this expression never fires.
	c := mustParseCron(t, "0 0 30 2 *")
	from := mustParseTime(t, timeLayout, "2026-01-01 00:00")
	if _, ok := c.Next(from); ok {
		t.Fatal("Next: want ok=false for an expression that never matches")
	}
}

func TestCronString(t *testing.T) {
	const expr = "*/5 9-17 * * 1-5"
	c := mustParseCron(t, expr)
	if got := c.String(); got != expr {
		t.Errorf("String() = %q, want %q", got, expr)
	}
}
