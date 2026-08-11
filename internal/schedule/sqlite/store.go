package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

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
