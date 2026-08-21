package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
)

// recoveringStore is a fakeStore that also leases jobs, so the recover
// endpoint can be exercised without a database.
type recoveringStore struct {
	*fakeStore

	result     queue.Recovery
	remaining  int
	recoverErr error
	staleErr   error
	calls      int
}

func newRecoveringStore() *recoveringStore {
	return &recoveringStore{fakeStore: newFakeStore()}
}

func (s *recoveringStore) Recover() (queue.Recovery, error) {
	s.calls++
	if s.recoverErr != nil {
		return queue.Recovery{}, s.recoverErr
	}
	return s.result, nil
}

func (s *recoveringStore) ExpiredLeases() (int, error) {
	if s.staleErr != nil {
		return 0, s.staleErr
	}
	return s.remaining, nil
}

// quietServer discards logs, since the recovery paths under test log at
// warn and error on purpose.
func quietServer(store Store) http.Handler {
	return (&Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
}

func TestHandleRecoverReportsWhatItReclaimed(t *testing.T) {
	store := newRecoveringStore()
	store.result = queue.Recovery{Requeued: 3, DeadLettered: 1}

	rec := doJSON(t, quietServer(store), http.MethodPost, "/v1/recover", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var got RecoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Requeued != 3 || got.DeadLettered != 1 || got.Remaining != 0 {
		t.Fatalf("response = %+v, want 3 requeued, 1 dead-lettered, 0 remaining", got)
	}
	if store.calls != 1 {
		t.Fatalf("Recover called %d times, want 1", store.calls)
	}
}

func TestHandleRecoverReportsOutstandingOrphans(t *testing.T) {
	store := newRecoveringStore()
	store.result = queue.Recovery{Requeued: 1}
	store.remaining = 2

	rec := doJSON(t, quietServer(store), http.MethodPost, "/v1/recover", nil)

	var got RecoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Orphans left after a pass mean they are appearing faster than one
	// sweep clears them, which a caller cannot infer from the counts of
	// what was reclaimed.
	if got.Remaining != 2 {
		t.Fatalf("Remaining = %d, want 2", got.Remaining)
	}
}

func TestHandleRecoverUnsupportedStore(t *testing.T) {
	// The plain fakeStore does not lease.
	rec := doJSON(t, quietServer(newFakeStore()), http.MethodPost, "/v1/recover", nil)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNotImplemented, rec.Body)
	}
}

func TestHandleRecoverStoreError(t *testing.T) {
	store := newRecoveringStore()
	store.recoverErr = errBoom

	rec := doJSON(t, quietServer(store), http.MethodPost, "/v1/recover", nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestHandleRecoverSurvivesStaleCountError(t *testing.T) {
	store := newRecoveringStore()
	store.result = queue.Recovery{Requeued: 2}
	store.staleErr = errors.New("count failed")

	rec := doJSON(t, quietServer(store), http.MethodPost, "/v1/recover", nil)

	// The recovery itself succeeded; failing the whole request over the
	// follow-up count would hide work that actually happened.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var got RecoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Requeued != 2 {
		t.Fatalf("Requeued = %d, want 2", got.Requeued)
	}
}

func TestHandleRecoverRejectsGet(t *testing.T) {
	rec := doJSON(t, quietServer(newRecoveringStore()), http.MethodGet, "/v1/recover", nil)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestIntegrationRecoverReclaimsOrphanedJob(t *testing.T) {
	// A lease that lapses the instant it is granted: claiming a job and
	// not finishing it is all it takes to produce an orphan.
	q, err := sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared",
		sqlite.Options{LeaseTTL: time.Nanosecond})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	ts := httptest.NewServer((&Server{
		Store:  q,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}).Handler())
	t.Cleanup(ts.Close)

	// Claim a job and never report an outcome, as a worker that died
	// would.
	j := job.New("reports", []byte("work"), job.WithMaxRetries(2))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	resp, err := ts.Client().Post(ts.URL+"/v1/recover", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /v1/recover: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var got RecoverResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Requeued != 1 {
		t.Fatalf("Requeued = %d, want 1", got.Requeued)
	}

	recovered, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if recovered.Status != job.StatusPending {
		t.Fatalf("recovered job status = %q, want pending", recovered.Status)
	}
}
