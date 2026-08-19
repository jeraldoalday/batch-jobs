package worker

import "testing"

func TestPolicyTarget(t *testing.T) {
	tests := []struct {
		name   string
		policy Policy
		sample Sample
		want   int
	}{
		{
			name:   "idle pool falls to the floor",
			policy: Policy{Min: 2, Max: 10},
			sample: Sample{Workers: 8, Busy: 0, Backlog: 0},
			want:   2,
		},
		{
			name:   "busy workers are always kept",
			policy: Policy{Min: 1, Max: 10},
			sample: Sample{Workers: 5, Busy: 5, Backlog: 0},
			want:   5,
		},
		{
			name:   "backlog adds a worker each by default",
			policy: Policy{Min: 1, Max: 10},
			sample: Sample{Workers: 1, Busy: 1, Backlog: 4},
			want:   5,
		},
		{
			name:   "JobsPerWorker divides the backlog",
			policy: Policy{Min: 1, Max: 100, JobsPerWorker: 10},
			sample: Sample{Workers: 1, Busy: 0, Backlog: 50},
			want:   5,
		},
		{
			name:   "a partial batch still gets a worker",
			policy: Policy{Min: 1, Max: 100, JobsPerWorker: 10},
			sample: Sample{Workers: 1, Busy: 0, Backlog: 51},
			want:   6,
		},
		{
			name:   "target is capped at Max",
			policy: Policy{Min: 1, Max: 4},
			sample: Sample{Workers: 4, Busy: 4, Backlog: 1000},
			want:   4,
		},
		{
			name:   "target is floored at Min",
			policy: Policy{Min: 3, Max: 10},
			sample: Sample{Workers: 3, Busy: 0, Backlog: 0},
			want:   3,
		},
		{
			name:   "zero Min is treated as one",
			policy: Policy{Max: 10},
			sample: Sample{Workers: 1, Busy: 0, Backlog: 0},
			want:   1,
		},
		{
			name:   "Max below Min is raised to Min",
			policy: Policy{Min: 5, Max: 2},
			sample: Sample{Workers: 5, Busy: 5, Backlog: 10},
			want:   5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.policy.Target(tt.sample); got != tt.want {
				t.Errorf("Target(%+v) = %d, want %d", tt.sample, got, tt.want)
			}
		})
	}
}

// Target must not mutate the Policy it normalizes, since a Pool holds one
// and reuses it on every tick.
func TestPolicyTargetDoesNotMutate(t *testing.T) {
	p := Policy{Max: 10}
	p.Target(Sample{Workers: 1, Backlog: 5})

	if p.Min != 0 || p.JobsPerWorker != 0 || p.ScaleDownAfter != 0 {
		t.Errorf("Target mutated its receiver: %+v", p)
	}
}

func TestScalerGrowsImmediately(t *testing.T) {
	s := &Scaler{Policy: Policy{Min: 1, Max: 10}}

	if got := s.Next(Sample{Workers: 1, Busy: 1, Backlog: 5}); got != 6 {
		t.Errorf("Next = %d, want 6 on the first oversubscribed sample", got)
	}
}

// Shrinking waits for ScaleDownAfter consecutive samples to agree, then
// retires exactly one worker at a time.
func TestScalerShrinksSlowly(t *testing.T) {
	s := &Scaler{Policy: Policy{Min: 1, Max: 10, ScaleDownAfter: 3}}
	idle := Sample{Workers: 5, Busy: 0, Backlog: 0}

	for i := 1; i <= 2; i++ {
		if got := s.Next(idle); got != 5 {
			t.Fatalf("sample %d: Next = %d, want 5 (still within hysteresis)", i, got)
		}
	}
	if got := s.Next(idle); got != 4 {
		t.Fatalf("sample 3: Next = %d, want 4 (one worker retired)", got)
	}

	// The streak resets after acting, so the next retirement takes
	// another full ScaleDownAfter samples.
	idle.Workers = 4
	for i := 1; i <= 2; i++ {
		if got := s.Next(idle); got != 4 {
			t.Fatalf("post-shrink sample %d: Next = %d, want 4", i, got)
		}
	}
	if got := s.Next(idle); got != 3 {
		t.Errorf("post-shrink sample 3: Next = %d, want 3", got)
	}
}

// A single busy sample in the middle of an idle streak must cancel the
// pending shrink: the pool is in use again.
func TestScalerBusySampleResetsShrinkStreak(t *testing.T) {
	s := &Scaler{Policy: Policy{Min: 1, Max: 10, ScaleDownAfter: 3}}
	idle := Sample{Workers: 5, Busy: 0, Backlog: 0}

	s.Next(idle)
	s.Next(idle)
	if got := s.Next(Sample{Workers: 5, Busy: 5, Backlog: 0}); got != 5 {
		t.Fatalf("busy sample: Next = %d, want 5 (steady)", got)
	}

	if got := s.Next(idle); got != 5 {
		t.Errorf("first idle sample after the reset: Next = %d, want 5", got)
	}
}

func TestScalerNeverCrossesBounds(t *testing.T) {
	s := &Scaler{Policy: Policy{Min: 2, Max: 4, ScaleDownAfter: 1}}

	if got := s.Next(Sample{Workers: 4, Busy: 4, Backlog: 999}); got != 4 {
		t.Errorf("saturated pool: Next = %d, want 4 (Max)", got)
	}

	workers := 4
	for i := 0; i < 10; i++ {
		workers = s.Next(Sample{Workers: workers, Busy: 0, Backlog: 0})
	}
	if workers != 2 {
		t.Errorf("fully idle pool settled at %d workers, want 2 (Min)", workers)
	}
}

// An oversized pool (e.g. after Max was lowered) drains down to the new
// ceiling one worker at a time rather than dumping workers all at once.
func TestScalerDrainsOversizedPoolGradually(t *testing.T) {
	s := &Scaler{Policy: Policy{Min: 1, Max: 3, ScaleDownAfter: 1}}

	if got := s.Next(Sample{Workers: 10, Busy: 10, Backlog: 0}); got != 9 {
		t.Errorf("Next = %d, want 9 (one at a time, not straight to Max)", got)
	}
}
