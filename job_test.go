package batchjobs

import "testing"

func TestStatusString(t *testing.T) {
	if got := StatusRetrying.String(); got != "retrying" {
		t.Errorf("String() = %q, want %q", got, "retrying")
	}
}

func TestPriorityString(t *testing.T) {
	cases := map[Priority]string{
		PriorityLow:    "low",
		PriorityNormal: "normal",
		PriorityHigh:   "high",
		Priority(99):   "priority(99)",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("Priority(%d).String() = %q, want %q", int(p), got, want)
		}
	}
}
