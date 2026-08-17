package sqlite

import (
	"database/sql"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// scanner is satisfied by both *sql.Row and *sql.Rows, letting scanSchedule
// be used from single-row and multi-row queries alike.
type scanner interface {
	Scan(dest ...any) error
}

// scheduleColumns is the column list, in order, used by every SELECT that
// produces a full schedule row. Keeping it in one place keeps scanSchedule
// in sync with the queries that feed it.
const scheduleColumns = "id, queue, payload, priority, max_retries, cron_expr, enabled, next_run_at, last_run_at, created_at, updated_at, timeout_ns"

// scanSchedule reads one row in the scheduleColumns order into a
// *schedule.Schedule.
func scanSchedule(s scanner) (*schedule.Schedule, error) {
	var (
		sch       schedule.Schedule
		priority  int
		enabled   int
		nextRunAt int64
		lastRunAt sql.NullInt64
		createdAt int64
		updatedAt int64
		timeoutNs int64
	)

	if err := s.Scan(
		&sch.ID,
		&sch.Queue,
		&sch.Payload,
		&priority,
		&sch.MaxRetries,
		&sch.CronExpr,
		&enabled,
		&nextRunAt,
		&lastRunAt,
		&createdAt,
		&updatedAt,
		&timeoutNs,
	); err != nil {
		return nil, err
	}

	sch.Priority = job.Priority(priority)
	sch.Enabled = enabled != 0
	sch.NextRunAt = time.Unix(0, nextRunAt).UTC()
	if lastRunAt.Valid {
		sch.LastRunAt = time.Unix(0, lastRunAt.Int64).UTC()
	}
	sch.CreatedAt = time.Unix(0, createdAt).UTC()
	sch.UpdatedAt = time.Unix(0, updatedAt).UTC()
	sch.Timeout = time.Duration(timeoutNs)
	return &sch, nil
}

// insertArgs returns s's fields in scheduleColumns order, ready to be
// passed as the argument list for an INSERT statement.
func insertArgs(s *schedule.Schedule) []any {
	var lastRunAt any
	if !s.LastRunAt.IsZero() {
		lastRunAt = s.LastRunAt.UnixNano()
	}

	return []any{
		s.ID,
		s.Queue,
		s.Payload,
		int(s.Priority),
		s.MaxRetries,
		s.CronExpr,
		boolToInt(s.Enabled),
		s.NextRunAt.UnixNano(),
		lastRunAt,
		s.CreatedAt.UnixNano(),
		s.UpdatedAt.UnixNano(),
		int64(s.Timeout),
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
