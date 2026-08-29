package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

var _ schedule.Store = (*Store)(nil)

// Create implements schedule.Store.
func (s *Store) Create(sch *schedule.Schedule) error {
	const insertQuery = `INSERT INTO schedules (` + scheduleColumns + `) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	if _, err := s.db.Exec(insertQuery, insertArgs(sch)...); err != nil {
		return fmt.Errorf("schedule/sqlite: create %s: %w", sch.ID, err)
	}
	return nil
}

// updateQuery rewrites every field an edit may change. It deliberately
// leaves enabled and last_run_at alone: those belong to SetEnabled and
// MarkFired, and folding them in here would let an edit racing with a
// firing roll the schedule's history backwards.
const updateQuery = `
UPDATE schedules
SET queue = ?, payload = ?, priority = ?, max_retries = ?, cron_expr = ?,
    next_run_at = ?, updated_at = ?, timeout_ns = ?
WHERE id = ?;
`

// Update implements schedule.Store.
func (s *Store) Update(sch *schedule.Schedule) error {
	res, err := s.db.Exec(updateQuery,
		sch.Queue,
		sch.Payload,
		int(sch.Priority),
		sch.MaxRetries,
		sch.CronExpr,
		sch.NextRunAt.UnixNano(),
		time.Now().UTC().UnixNano(),
		int64(sch.Timeout),
		sch.ID,
	)
	if err != nil {
		return fmt.Errorf("schedule/sqlite: update %s: %w", sch.ID, err)
	}
	return checkRowFound(res, "update", sch.ID)
}

// Get implements schedule.Store.
func (s *Store) Get(id string) (*schedule.Schedule, error) {
	row := s.db.QueryRow(`SELECT `+scheduleColumns+` FROM schedules WHERE id = ?`, id)

	sch, err := scanSchedule(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, schedule.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("schedule/sqlite: get %s: %w", id, err)
	}
	return sch, nil
}

// List implements schedule.Store.
func (s *Store) List() ([]*schedule.Schedule, error) {
	rows, err := s.db.Query(`SELECT ` + scheduleColumns + ` FROM schedules ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("schedule/sqlite: list: %w", err)
	}
	defer rows.Close()

	schedules, err := scanSchedules(rows)
	if err != nil {
		return nil, fmt.Errorf("schedule/sqlite: list: %w", err)
	}
	return schedules, nil
}

// dueQuery selects enabled schedules whose next_run_at has passed, ordered
// soonest first, so a scheduler loop processes the most overdue schedules
// ahead of ones that just became eligible.
const dueQuery = `
SELECT ` + scheduleColumns + `
FROM schedules
WHERE enabled = 1 AND next_run_at <= ?
ORDER BY next_run_at ASC;
`

// Due implements schedule.Store.
func (s *Store) Due(now time.Time) ([]*schedule.Schedule, error) {
	rows, err := s.db.Query(dueQuery, now.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("schedule/sqlite: due: %w", err)
	}
	defer rows.Close()

	schedules, err := scanSchedules(rows)
	if err != nil {
		return nil, fmt.Errorf("schedule/sqlite: due: %w", err)
	}
	return schedules, nil
}

// scanSchedules scans every remaining row of rows into a slice, using
// scanSchedule for each individual row.
func scanSchedules(rows *sql.Rows) ([]*schedule.Schedule, error) {
	var schedules []*schedule.Schedule
	for rows.Next() {
		sch, err := scanSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		schedules = append(schedules, sch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schedules, nil
}

// MarkFired implements schedule.Store.
func (s *Store) MarkFired(id string, firedAt, next time.Time) error {
	const query = `
UPDATE schedules
SET last_run_at = ?, next_run_at = ?, updated_at = ?
WHERE id = ?;
`
	res, err := s.db.Exec(query, firedAt.UnixNano(), next.UnixNano(), firedAt.UnixNano(), id)
	if err != nil {
		return fmt.Errorf("schedule/sqlite: mark fired %s: %w", id, err)
	}
	return checkRowFound(res, "mark fired", id)
}

// SetEnabled implements schedule.Store.
func (s *Store) SetEnabled(id string, enabled bool) error {
	const query = `UPDATE schedules SET enabled = ?, updated_at = ? WHERE id = ?;`

	res, err := s.db.Exec(query, boolToInt(enabled), time.Now().UTC().UnixNano(), id)
	if err != nil {
		return fmt.Errorf("schedule/sqlite: set enabled %s: %w", id, err)
	}
	return checkRowFound(res, "set enabled", id)
}

// Delete implements schedule.Store.
func (s *Store) Delete(id string) error {
	if _, err := s.db.Exec(`DELETE FROM schedules WHERE id = ?`, id); err != nil {
		return fmt.Errorf("schedule/sqlite: delete %s: %w", id, err)
	}
	return nil
}

// checkRowFound returns schedule.ErrNotFound if res reports that op
// touched no rows, which for an UPDATE keyed by id means no schedule with
// that id exists.
func checkRowFound(res sql.Result, op, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("schedule/sqlite: %s %s: %w", op, id, err)
	}
	if n == 0 {
		return schedule.ErrNotFound
	}
	return nil
}
