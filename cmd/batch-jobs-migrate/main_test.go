package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	queuesqlite "github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitemigrate"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitex"
)

// jobsTarget opens a fresh SQLite database at path within t's temp dir and
// returns the target that exercises it against the real jobs-store
// migrations, the same ones the server applies.
func jobsTarget(t *testing.T) target {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jobs.db")
	return target{name: "jobs", path: path, fsys: queuesqlite.Migrations(), dir: queuesqlite.MigrationsDir}
}

// captureStdout runs fn with os.Stdout redirected to a pipe and returns
// everything written to it. status/up/verify/version print straight to
// os.Stdout rather than taking a writer, so this is the only way to assert
// on what an operator running the command would actually see.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	original := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = original }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("closing pipe writer: %v", err)
	}
	var buf [64 << 10]byte
	n, _ := r.Read(buf[:])
	if err := r.Close(); err != nil {
		t.Fatalf("closing pipe reader: %v", err)
	}
	return string(buf[:n])
}

func TestStatusReportsEveryPendingMigration(t *testing.T) {
	tg := jobsTarget(t)
	db, err := sqlitex.Open(tg.path)
	if err != nil {
		t.Fatalf("sqlitex.Open: %v", err)
	}
	defer db.Close()

	records, err := sqlitemigrate.Status(db, tg.fsys, tg.dir)
	if err != nil {
		t.Fatalf("sqlitemigrate.Status: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("the jobs store ships no migrations; this test needs at least one to be meaningful")
	}

	out := captureStdout(t, func() {
		if err := status(db, tg); err != nil {
			t.Errorf("status: %v", err)
		}
	})

	if !strings.Contains(out, tg.path) {
		t.Errorf("status output %q does not name the database path %q", out, tg.path)
	}
	want := len(records)
	if !strings.Contains(out, "of "+strconv.Itoa(want)+" migration(s) pending") {
		t.Errorf("status output %q does not report %d pending migration(s) (none applied yet)", out, want)
	}
}

func TestUpAppliesEveryPendingMigrationAndStatusAgreesAfterward(t *testing.T) {
	tg := jobsTarget(t)
	db, err := sqlitex.Open(tg.path)
	if err != nil {
		t.Fatalf("sqlitex.Open: %v", err)
	}
	defer db.Close()

	before, err := pendingCount(db, tg)
	if err != nil {
		t.Fatalf("pendingCount: %v", err)
	}
	if before == 0 {
		t.Fatal("nothing pending before up; this test needs a fresh database")
	}

	out := captureStdout(t, func() {
		if err := up(db, tg); err != nil {
			t.Errorf("up: %v", err)
		}
	})

	if !strings.Contains(out, "applied "+strconv.Itoa(before)+" migration(s)") {
		t.Errorf("up output %q does not report applying the %d migration(s) that were pending", out, before)
	}

	after, err := pendingCount(db, tg)
	if err != nil {
		t.Fatalf("pendingCount after up: %v", err)
	}
	if after != 0 {
		t.Errorf("pendingCount() after up = %d, want 0", after)
	}

	// A second up has nothing left to do, and must say so rather than
	// silently repeating the first run's report.
	out = captureStdout(t, func() {
		if err := up(db, tg); err != nil {
			t.Errorf("second up: %v", err)
		}
	})
	if !strings.Contains(out, "applied 0 migration(s)") {
		t.Errorf("second up output %q does not report that nothing was pending", out)
	}
}

func TestVerifyPassesOnlyOnceEverythingIsApplied(t *testing.T) {
	tg := jobsTarget(t)
	db, err := sqlitex.Open(tg.path)
	if err != nil {
		t.Fatalf("sqlitex.Open: %v", err)
	}
	defer db.Close()

	// Pending migrations are not drift -- verify should pass on a database
	// nothing has been applied to yet, the same way sqlitemigrate.Verify
	// documents it should.
	if err := verify(db, tg); err != nil {
		t.Errorf("verify on a fresh database: %v", err)
	}

	if err := up(db, tg); err != nil {
		t.Fatalf("up: %v", err)
	}

	out := captureStdout(t, func() {
		if err := verify(db, tg); err != nil {
			t.Errorf("verify after up: %v", err)
		}
	})
	if !strings.Contains(out, tg.name+": ok") {
		t.Errorf("verify output %q does not report ok for target %q", out, tg.name)
	}
}

func TestVersionMatchesSqlitemigrateVersion(t *testing.T) {
	tg := jobsTarget(t)
	db, err := sqlitex.Open(tg.path)
	if err != nil {
		t.Fatalf("sqlitex.Open: %v", err)
	}
	defer db.Close()

	if err := up(db, tg); err != nil {
		t.Fatalf("up: %v", err)
	}

	want, err := sqlitemigrate.Version(db)
	if err != nil {
		t.Fatalf("sqlitemigrate.Version: %v", err)
	}

	out := captureStdout(t, func() {
		if err := version(db, tg); err != nil {
			t.Errorf("version: %v", err)
		}
	})
	if !strings.Contains(out, tg.name+": "+strconv.Itoa(want)) {
		t.Errorf("version output %q does not report version %d", out, want)
	}
}

func TestAppliedAtRendersADashForAPendingMigration(t *testing.T) {
	tg := jobsTarget(t)
	db, err := sqlitex.Open(tg.path)
	if err != nil {
		t.Fatalf("sqlitex.Open: %v", err)
	}
	defer db.Close()

	records, err := sqlitemigrate.Status(db, tg.fsys, tg.dir)
	if err != nil {
		t.Fatalf("sqlitemigrate.Status: %v", err)
	}
	if len(records) == 0 || !records[0].Pending() {
		t.Fatal("expected at least one pending record on a fresh database")
	}
	if got := appliedAt(records[0]); got != "-" {
		t.Errorf("appliedAt(pending) = %q, want %q", got, "-")
	}

	if err := up(db, tg); err != nil {
		t.Fatalf("up: %v", err)
	}
	records, err = sqlitemigrate.Status(db, tg.fsys, tg.dir)
	if err != nil {
		t.Fatalf("sqlitemigrate.Status after up: %v", err)
	}
	if got := appliedAt(records[0]); got == "-" {
		t.Errorf("appliedAt(applied) = %q, want a timestamp", got)
	}
}

func TestRunDispatchesAcrossEveryTargetInOrder(t *testing.T) {
	jobs := jobsTarget(t)
	// A second, independent database under the same command: run must
	// apply to both rather than stopping after the first, the way
	// -schedules-db wires a second target alongside -db.
	schedules := jobsTarget(t)
	schedules.name = "schedules"

	if err := run("up", []target{jobs, schedules}); err != nil {
		t.Fatalf("run(up): %v", err)
	}

	for _, tg := range []target{jobs, schedules} {
		db, err := sqlitex.Open(tg.path)
		if err != nil {
			t.Fatalf("sqlitex.Open(%s): %v", tg.name, err)
		}
		n, err := pendingCount(db, tg)
		db.Close()
		if err != nil {
			t.Fatalf("pendingCount(%s): %v", tg.name, err)
		}
		if n != 0 {
			t.Errorf("target %q still has %d pending migration(s) after run(up)", tg.name, n)
		}
	}
}

func TestRunWrapsAFailureWithTheOffendingTargetsNameAndPath(t *testing.T) {
	tg := jobsTarget(t)

	err := run("not-a-real-command", []target{tg})
	if err == nil {
		t.Fatal("run with an unknown command succeeded, want an error")
	}
	if !strings.Contains(err.Error(), tg.name) || !strings.Contains(err.Error(), tg.path) {
		t.Errorf("run error %q does not name the failing target (%s, %s)", err, tg.name, tg.path)
	}
}

func TestRunOneRejectsAnUnknownCommand(t *testing.T) {
	tg := jobsTarget(t)
	err := run("bogus", []target{tg})
	if err == nil {
		t.Fatal("expected an error for an unknown command")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("error %q does not mention an unknown command", err)
	}
}

func TestBuildTargetsOmitsSchedulesWhenTheFlagIsEmpty(t *testing.T) {
	targets, err := buildTargets("jobs.db", "")
	if err != nil {
		t.Fatalf("buildTargets: %v", err)
	}
	if len(targets) != 1 || targets[0].name != "jobs" || targets[0].path != "jobs.db" {
		t.Errorf("targets = %+v, want a single jobs target for jobs.db", targets)
	}
}

func TestBuildTargetsIncludesSchedulesWhenTheFlagIsSet(t *testing.T) {
	targets, err := buildTargets("jobs.db", "schedules.db")
	if err != nil {
		t.Fatalf("buildTargets: %v", err)
	}
	if len(targets) != 2 || targets[1].name != "schedules" || targets[1].path != "schedules.db" {
		t.Errorf("targets = %+v, want jobs then schedules targets", targets)
	}
}

func TestBuildTargetsRejectsTheSamePathForBothStores(t *testing.T) {
	_, err := buildTargets("shared.db", "shared.db")
	if err == nil {
		t.Fatal("buildTargets with -schedules-db equal to -db succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "must differ") {
		t.Errorf("error %q does not explain that the two paths must differ", err)
	}
}
