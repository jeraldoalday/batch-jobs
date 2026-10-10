package main

import (
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
)

func TestParseLevelAcceptsEveryNamedLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for s, want := range cases {
		if got := parseLevel(s); got != want {
			t.Errorf("parseLevel(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestParseLevelDefaultsToInfoOnGarbage(t *testing.T) {
	if got := parseLevel("not-a-level"); got != slog.LevelInfo {
		t.Errorf("parseLevel(garbage) = %v, want %v", got, slog.LevelInfo)
	}
}

func TestRequireMigratedSchemaOnFreshDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	// A path that does not exist yet has nothing applied, so every
	// migration this binary knows about is pending.
	if err := requireMigratedSchema(path); err == nil {
		t.Fatal("requireMigratedSchema on an unmigrated database = nil error, want one naming pending migrations")
	}
}

func TestRequireMigratedSchemaAfterOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	// sqlite.Open applies every pending migration itself, so a database
	// it just created should report nothing left to do.
	q, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer q.Close()

	if err := requireMigratedSchema(path); err != nil {
		t.Errorf("requireMigratedSchema on a freshly migrated database: %v", err)
	}
}
