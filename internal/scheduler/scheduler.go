// Package scheduler runs the loop that turns internal/schedule.Schedules
// into jobs: it polls a schedule.Store for schedules that are due, enqueues
// the job each one produces onto a queue.Queue, and advances the schedule
// to its next fire time.
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// DefaultPollInterval is how often Scheduler checks the Store for due
// schedules when PollInterval is unset.
const DefaultPollInterval = time.Second

// Scheduler periodically enqueues jobs for due schedules. A Scheduler must
// not be copied after Start has been called.
//
// Firing a schedule is not transactional: Scheduler enqueues the job and
// then advances the schedule's next fire time as two separate steps. If
// the process crashes between them, the schedule will be picked up again
// on the next poll and its job enqueued a second time. Handlers for
// schedule-produced jobs should be idempotent, the same requirement any
// at-least-once job queue places on its handlers.
type Scheduler struct {
	// Store is where due schedules are read from and advanced.
	// Required.
	Store schedule.Store

	// Queue is where each due schedule's job is enqueued. Required.
	Queue queue.Queue

	// PollInterval is how often the Store is checked for due schedules.
	// Defaults to DefaultPollInterval if unset.
	PollInterval time.Duration

	// Logger is used for operational logging. Defaults to
	// slog.Default() if unset.
	Logger *slog.Logger

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// Start launches the polling loop, which runs until ctx is cancelled or
// Stop is called. Start returns immediately; use Stop or wait on ctx to
// know when the loop has actually exited.
func (s *Scheduler) Start(ctx context.Context) {
	if s.PollInterval <= 0 {
		s.PollInterval = DefaultPollInterval
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.loop(ctx)
	}()
}

// Stop signals the polling loop to stop and blocks until it exits, letting
// any tick already in progress finish first.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *Scheduler) loop(ctx context.Context) {
	ticker := time.NewTicker(s.PollInterval)
	defer ticker.Stop()

	// Check immediately on start rather than waiting a full
	// PollInterval for the first tick.
	s.tick()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

// tick queries the Store for due schedules and fires each one.
func (s *Scheduler) tick() {
	due, err := s.Store.Due(time.Now().UTC())
	if err != nil {
		s.Logger.Error("scheduler: failed to query due schedules", "error", err)
		return
	}
	for _, sch := range due {
		s.fire(sch)
	}
}

// fire enqueues the job produced by sch and advances it to its next fire
// time. If sch's cron expression can no longer be advanced (e.g. it was
// corrupted after being written), sch is disabled instead of being
// retried forever.
func (s *Scheduler) fire(sch *schedule.Schedule) {
	now := time.Now().UTC()

	next, err := sch.Advance(now)
	if err != nil {
		s.Logger.Error("scheduler: failed to compute next fire time; disabling schedule", "schedule_id", sch.ID, "error", err)
		if disErr := s.Store.SetEnabled(sch.ID, false); disErr != nil {
			s.Logger.Error("scheduler: failed to disable schedule", "schedule_id", sch.ID, "error", disErr)
		}
		return
	}

	if err := s.Queue.Enqueue(sch.NewJob()); err != nil {
		s.Logger.Error("scheduler: failed to enqueue job for schedule", "schedule_id", sch.ID, "error", err)
		return
	}

	if err := s.Store.MarkFired(sch.ID, now, next); err != nil {
		s.Logger.Error("scheduler: failed to mark schedule fired", "schedule_id", sch.ID, "error", err)
	}
}
