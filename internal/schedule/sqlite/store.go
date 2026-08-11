package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// Create implements schedule.Store.
func (s *Store) Create(sch *schedule.Schedule) error {
	const insertQuery = `INSERT INTO schedules (` + scheduleColumns + `) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	if _, err := s.db.Exec(insertQuery, insertArgs(sch)...); err != nil {
		return fmt.Errorf("schedule/sqlite: create %s: %w", sch.ID, err)
	}
	return nil
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
