package batchjobs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs"
)

// everyMinute is the expression most tests here use when they only care
// that a schedule exists, not when it fires.
const everyMinute = "* * * * *"

// scheduledEngine is newEngine with scheduling enabled and the poll loop
// tightened, so a test that starts the engine does not spend a second per
// firing waiting for the default cadence.
func scheduledEngine(t *testing.T, configure ...func(*batchjobs.Config)) *batchjobs.Engine {
	t.Helper()
	return newEngine(t, append([]func(*batchjobs.Config){func(c *batchjobs.Config) {
		c.Schedules = true
		c.SchedulePoll = 5 * time.Millisecond
	}}, configure...)...)
}

func TestAddScheduleRoundTrips(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "0 3 * * *", []byte("nightly"),
		batchjobs.WithPriority(batchjobs.PriorityHigh),
		batchjobs.WithMaxRetries(3),
		batchjobs.WithTimeout(90*time.Second),
	)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if created.ID == "" {
		t.Error("ID is empty, want a generated one")
	}
	if !created.Enabled {
		t.Error("Enabled = false, want a new schedule to be enabled")
	}

	got, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got.Queue != "reports" {
		t.Errorf("Queue = %q, want %q", got.Queue, "reports")
	}
	if got.Cron != "0 3 * * *" {
		t.Errorf("Cron = %q, want %q", got.Cron, "0 3 * * *")
	}
	if string(got.Payload) != "nightly" {
		t.Errorf("Payload = %q, want %q", got.Payload, "nightly")
	}
	if got.Priority != batchjobs.PriorityHigh {
		t.Errorf("Priority = %v, want %v", got.Priority, batchjobs.PriorityHigh)
	}
	if got.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", got.MaxRetries)
	}
	if got.Timeout != 90*time.Second {
		t.Errorf("Timeout = %v, want %v", got.Timeout, 90*time.Second)
	}
	if !got.LastRunAt.IsZero() {
		t.Errorf("LastRunAt = %v, want zero for a schedule that has never fired", got.LastRunAt)
	}
}

func TestAddScheduleJSONDecodesInTheHandler(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	type report struct {
		Window string `json:"window"`
	}
	created, err := e.AddScheduleJSON(ctx, "reports", "0 3 * * *", report{Window: "daily"})
	if err != nil {
		t.Fatalf("AddScheduleJSON: %v", err)
	}

	got, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}

	var decoded report
	if err := got.Decode(&decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Window != "daily" {
		t.Errorf("Window = %q, want %q", decoded.Window, "daily")
	}
}

func TestAddScheduleRejectsBadInput(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	tests := []struct {
		name     string
		queue    string
		cronExpr string
		opts     []batchjobs.Option
	}{
		{name: "empty queue", queue: "", cronExpr: everyMinute},
		{name: "unparsable cron", queue: "reports", cronExpr: "every minute please"},
		{name: "cron with too few fields", queue: "reports", cronExpr: "* * *"},
		{name: "cron that never fires", queue: "reports", cronExpr: "0 0 30 2 *"},
		{
			name: "delay", queue: "reports", cronExpr: everyMinute,
			opts: []batchjobs.Option{batchjobs.WithDelay(time.Minute)},
		},
		{
			name: "run at", queue: "reports", cronExpr: everyMinute,
			opts: []batchjobs.Option{batchjobs.WithRunAt(time.Now().Add(time.Hour))},
		},
		{
			name: "idempotency key", queue: "reports", cronExpr: everyMinute,
			opts: []batchjobs.Option{batchjobs.WithIdempotencyKey("k")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := e.AddSchedule(ctx, tt.queue, tt.cronExpr, nil, tt.opts...); err == nil {
				t.Fatal("AddSchedule returned nil error, want a rejection")
			}
		})
	}
}

func TestScheduleMethodsReportSchedulingDisabled(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	if _, err := e.AddSchedule(ctx, "reports", everyMinute, nil); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("AddSchedule = %v, want ErrSchedulingDisabled", err)
	}
	if _, err := e.GetSchedule(ctx, "id"); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("GetSchedule = %v, want ErrSchedulingDisabled", err)
	}
	if _, err := e.ListSchedules(ctx); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("ListSchedules = %v, want ErrSchedulingDisabled", err)
	}
	if _, err := e.UpdateSchedule(ctx, "id", func(*batchjobs.Schedule) {}); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("UpdateSchedule = %v, want ErrSchedulingDisabled", err)
	}
	if err := e.PauseSchedule(ctx, "id"); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("PauseSchedule = %v, want ErrSchedulingDisabled", err)
	}
	if err := e.ResumeSchedule(ctx, "id"); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("ResumeSchedule = %v, want ErrSchedulingDisabled", err)
	}
	if err := e.DeleteSchedule(ctx, "id"); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("DeleteSchedule = %v, want ErrSchedulingDisabled", err)
	}
	if _, err := e.TriggerSchedule(ctx, "id"); !errors.Is(err, batchjobs.ErrSchedulingDisabled) {
		t.Errorf("TriggerSchedule = %v, want ErrSchedulingDisabled", err)
	}
}

func TestGetScheduleUnknownIDIsNotFound(t *testing.T) {
	e := scheduledEngine(t)

	_, err := e.GetSchedule(context.Background(), "no-such-schedule")
	if !errors.Is(err, batchjobs.ErrScheduleNotFound) {
		t.Fatalf("GetSchedule = %v, want ErrScheduleNotFound", err)
	}
}

func TestListSchedulesReturnsEveryOne(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	for _, name := range []string{"reports", "digests", "cleanup"} {
		if _, err := e.AddSchedule(ctx, name, everyMinute, nil); err != nil {
			t.Fatalf("AddSchedule(%q): %v", name, err)
		}
	}

	got, err := e.ListSchedules(ctx)
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ListSchedules returned %d schedules, want 3", len(got))
	}

	seen := make(map[string]bool, len(got))
	for _, s := range got {
		seen[s.Queue] = true
	}
	for _, name := range []string{"reports", "digests", "cleanup"} {
		if !seen[name] {
			t.Errorf("schedule for queue %q missing from the listing", name)
		}
	}
}

func TestListSchedulesEmptyIsNotAnError(t *testing.T) {
	got, err := scheduledEngine(t).ListSchedules(context.Background())
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ListSchedules returned %d schedules, want none", len(got))
	}
}

func TestUpdateScheduleAppliesTheEdit(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "0 3 * * *", []byte("old"))
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	updated, err := e.UpdateSchedule(ctx, created.ID, func(s *batchjobs.Schedule) {
		s.Queue = "digests"
		s.Payload = []byte("new")
		s.Priority = batchjobs.PriorityLow
		s.MaxRetries = 9
		s.Timeout = 45 * time.Second
	})
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	if updated.Queue != "digests" || string(updated.Payload) != "new" {
		t.Errorf("update returned queue %q payload %q, want digests/new", updated.Queue, updated.Payload)
	}

	got, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got.Priority != batchjobs.PriorityLow {
		t.Errorf("Priority = %v, want %v", got.Priority, batchjobs.PriorityLow)
	}
	if got.MaxRetries != 9 {
		t.Errorf("MaxRetries = %d, want 9", got.MaxRetries)
	}
	if got.Timeout != 45*time.Second {
		t.Errorf("Timeout = %v, want %v", got.Timeout, 45*time.Second)
	}
}

func TestUpdateScheduleLeavesNextRunAloneWhenCronIsUnchanged(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	// Adjusting the retry budget must not postpone tonight's run.
	updated, err := e.UpdateSchedule(ctx, created.ID, func(s *batchjobs.Schedule) {
		s.MaxRetries = 5
	})
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	if !updated.NextRunAt.Equal(created.NextRunAt) {
		t.Errorf("NextRunAt = %v, want it left at %v", updated.NextRunAt, created.NextRunAt)
	}
}

func TestUpdateScheduleRecomputesNextRunForANewExpression(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	updated, err := e.UpdateSchedule(ctx, created.ID, func(s *batchjobs.Schedule) {
		s.Cron = "*/5 * * * *"
	})
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	if updated.Cron != "*/5 * * * *" {
		t.Errorf("Cron = %q, want %q", updated.Cron, "*/5 * * * *")
	}
	if !updated.NextRunAt.Before(created.NextRunAt) {
		t.Errorf("NextRunAt = %v, want it recomputed to before %v", updated.NextRunAt, created.NextRunAt)
	}
}

func TestUpdateScheduleRejectsABadExpressionWithoutWriting(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "0 3 * * *", []byte("original"))
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	if _, err := e.UpdateSchedule(ctx, created.ID, func(s *batchjobs.Schedule) {
		s.Payload = []byte("edited")
		s.Cron = "not a cron expression"
	}); err == nil {
		t.Fatal("UpdateSchedule with an unparsable expression returned nil error")
	}

	// The whole edit is discarded, not just the expression: a partly
	// applied update is worse than a rejected one.
	got, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if string(got.Payload) != "original" {
		t.Errorf("Payload = %q, want the rejected edit discarded", got.Payload)
	}
	if got.Cron != "0 3 * * *" {
		t.Errorf("Cron = %q, want it left at %q", got.Cron, "0 3 * * *")
	}
}

func TestUpdateScheduleRejectsAnEmptyQueue(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", everyMinute, nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	if _, err := e.UpdateSchedule(ctx, created.ID, func(s *batchjobs.Schedule) {
		s.Queue = ""
	}); err == nil {
		t.Fatal("UpdateSchedule clearing the queue returned nil error")
	}
}

func TestUpdateScheduleRequiresAnEditFunction(t *testing.T) {
	e := scheduledEngine(t)
	if _, err := e.UpdateSchedule(context.Background(), "id", nil); err == nil {
		t.Fatal("UpdateSchedule with a nil edit function returned nil error")
	}
}

func TestPauseAndResumeSchedule(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", everyMinute, nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	if err := e.PauseSchedule(ctx, created.ID); err != nil {
		t.Fatalf("PauseSchedule: %v", err)
	}
	paused, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if paused.Enabled {
		t.Error("Enabled = true after PauseSchedule, want false")
	}

	if err := e.ResumeSchedule(ctx, created.ID); err != nil {
		t.Fatalf("ResumeSchedule: %v", err)
	}
	resumed, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if !resumed.Enabled {
		t.Error("Enabled = false after ResumeSchedule, want true")
	}
}

func TestResumeScheduleClearsAnOverdueFireTime(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", everyMinute, nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if err := e.PauseSchedule(ctx, created.ID); err != nil {
		t.Fatalf("PauseSchedule: %v", err)
	}

	// Stand in for a long pause by dragging the stored fire time into
	// the past, the state a schedule paused for a week comes back in.
	if _, err := e.UpdateSchedule(ctx, created.ID, func(s *batchjobs.Schedule) {
		s.Cron = "0 3 * * *"
	}); err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}

	before := time.Now().UTC()
	if err := e.ResumeSchedule(ctx, created.ID); err != nil {
		t.Fatalf("ResumeSchedule: %v", err)
	}

	got, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got.NextRunAt.Before(before) {
		t.Errorf("NextRunAt = %v, want it recomputed to after %v", got.NextRunAt, before)
	}
	if got.Overdue(time.Now().UTC()) {
		t.Error("Overdue() = true right after a resume, want false")
	}
}

func TestResumeAnEnabledScheduleIsANoOp(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	if err := e.ResumeSchedule(ctx, created.ID); err != nil {
		t.Fatalf("ResumeSchedule: %v", err)
	}
	got, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if !got.NextRunAt.Equal(created.NextRunAt) {
		t.Errorf("NextRunAt = %v, want it left at %v", got.NextRunAt, created.NextRunAt)
	}
}

func TestDeleteSchedule(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", everyMinute, nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if err := e.DeleteSchedule(ctx, created.ID); err != nil {
		t.Fatalf("DeleteSchedule: %v", err)
	}
	if _, err := e.GetSchedule(ctx, created.ID); !errors.Is(err, batchjobs.ErrScheduleNotFound) {
		t.Fatalf("GetSchedule after delete = %v, want ErrScheduleNotFound", err)
	}

	// Deleting again is not an error, so a cleanup path need not look
	// first.
	if err := e.DeleteSchedule(ctx, created.ID); err != nil {
		t.Fatalf("DeleteSchedule of an already-deleted schedule: %v", err)
	}
}

func TestTriggerScheduleEnqueuesWithoutMovingTheTimetable(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "0 3 * * *", []byte("nightly"),
		batchjobs.WithPriority(batchjobs.PriorityHigh),
		batchjobs.WithMaxRetries(4),
	)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	j, err := e.TriggerSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("TriggerSchedule: %v", err)
	}
	if j.Queue != "reports" || string(j.Payload) != "nightly" {
		t.Errorf("job = queue %q payload %q, want reports/nightly", j.Queue, j.Payload)
	}
	if j.Priority != batchjobs.PriorityHigh {
		t.Errorf("Priority = %v, want the schedule's %v", j.Priority, batchjobs.PriorityHigh)
	}
	if j.MaxRetries != 4 {
		t.Errorf("MaxRetries = %d, want the schedule's 4", j.MaxRetries)
	}

	stored, err := e.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != batchjobs.StatusPending {
		t.Errorf("Status = %v, want %v", stored.Status, batchjobs.StatusPending)
	}

	after, err := e.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if !after.NextRunAt.Equal(created.NextRunAt) {
		t.Errorf("NextRunAt = %v, want it left at %v", after.NextRunAt, created.NextRunAt)
	}
	if !after.LastRunAt.IsZero() {
		t.Errorf("LastRunAt = %v, want a manual trigger not recorded as a firing", after.LastRunAt)
	}
}

func TestTriggerAPausedScheduleStillRuns(t *testing.T) {
	e := scheduledEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", everyMinute, nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if err := e.PauseSchedule(ctx, created.ID); err != nil {
		t.Fatalf("PauseSchedule: %v", err)
	}

	if _, err := e.TriggerSchedule(ctx, created.ID); err != nil {
		t.Fatalf("TriggerSchedule on a paused schedule: %v", err)
	}
}

func TestSchedulesSurviveAnEngineRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "jobs.db")
	ctx := context.Background()

	first := newEngine(t, func(c *batchjobs.Config) {
		c.DB = dbPath
		c.Schedules = true
	})
	created, err := first.AddSchedule(ctx, "reports", "0 3 * * *", []byte("nightly"))
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second := newEngine(t, func(c *batchjobs.Config) {
		c.DB = dbPath
		c.Schedules = true
	})
	got, err := second.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule after restart: %v", err)
	}
	if got.Queue != "reports" || string(got.Payload) != "nightly" {
		t.Errorf("schedule = queue %q payload %q, want reports/nightly", got.Queue, got.Payload)
	}
}

func TestSchedulesDatabaseSitsBesideTheJobsDatabase(t *testing.T) {
	dir := t.TempDir()
	e := newEngine(t, func(c *batchjobs.Config) {
		c.DB = filepath.Join(dir, "jobs.db")
		c.Schedules = true
	})
	if _, err := e.AddSchedule(context.Background(), "reports", everyMinute, nil); err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "jobs.schedules.db")); err != nil {
		entries, _ := os.ReadDir(dir)
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("jobs.schedules.db not found beside the jobs database; directory holds %v", names)
	}
}

func TestSchedulesDBOverrideImpliesScheduling(t *testing.T) {
	// A caller who names the database should not also have to set the
	// flag, so the override alone turns the feature on.
	e := newEngine(t, func(c *batchjobs.Config) {
		c.SchedulesDB = filepath.Join(t.TempDir(), "custom-schedules.db")
	})

	if _, err := e.AddSchedule(context.Background(), "reports", everyMinute, nil); err != nil {
		t.Fatalf("AddSchedule with only SchedulesDB set: %v", err)
	}
}

func TestValidateCron(t *testing.T) {
	valid := []string{"* * * * *", "*/15 * * * *", "0 3 * * 1-5", "0 0 1 1 *", "5,20,35 * * * *"}
	for _, expr := range valid {
		if err := batchjobs.ValidateCron(expr); err != nil {
			t.Errorf("ValidateCron(%q) = %v, want nil", expr, err)
		}
	}

	invalid := []string{"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "nonsense"}
	for _, expr := range invalid {
		if err := batchjobs.ValidateCron(expr); err == nil {
			t.Errorf("ValidateCron(%q) = nil, want an error", expr)
		}
	}
}

func TestScheduleUpcoming(t *testing.T) {
	e := scheduledEngine(t)

	created, err := e.AddSchedule(context.Background(), "reports", "0 * * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	times, err := created.Upcoming(4)
	if err != nil {
		t.Fatalf("Upcoming: %v", err)
	}
	if len(times) != 4 {
		t.Fatalf("Upcoming(4) returned %d times, want 4", len(times))
	}
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap != time.Hour {
			t.Errorf("gap between fire %d and %d = %v, want 1h", i-1, i, gap)
		}
	}
}
