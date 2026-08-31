package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
)

// pruningStore is a fakeStore that also keeps finished jobs, so the prune
// endpoint can be exercised without a database.
type pruningStore struct {
	*fakeStore

	result      queue.Pruned
	retained    int
	applied     queue.Retention
	calls       int
	pruneErr    error
	retainedErr error
}

func newPruningStore() *pruningStore {
	return &pruningStore{fakeStore: newFakeStore()}
}

func (s *pruningStore) Prune(r queue.Retention) (queue.Pruned, error) {
	s.calls++
	s.applied = r
	if s.pruneErr != nil {
		return s.result, s.pruneErr
	}
	return s.result, nil
}

func (s *pruningStore) Retained() (int, error) {
	if s.retainedErr != nil {
		return 0, s.retainedErr
	}
	return s.retained, nil
}

// pruningServer wires a store and a policy behind the routed handler,
// discarding logs since the failure paths under test log on purpose.
func pruningServer(store Store, r queue.Retention) http.Handler {
	return (&Server{
		Store:     store,
		Retention: r,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}).Handler()
}

func TestHandlePruneReportsWhatItDeleted(t *testing.T) {
	store := newPruningStore()
	store.result = queue.Pruned{Succeeded: 12, Cancelled: 3}
	store.retained = 40

	rec := doJSON(t, pruningServer(store, queue.Retention{Succeeded: time.Hour}), http.MethodPost, "/v1/prune", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
	}

	var got PruneResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := PruneResponse{Succeeded: 12, Cancelled: 3, Total: 15, Retained: 40}
	if got != want {
		t.Errorf("response = %+v, want %+v", got, want)
	}
}

// The server's own policy is what runs, never one the caller supplies.
func TestHandlePruneAppliesTheServersPolicy(t *testing.T) {
	store := newPruningStore()
	policy := queue.Retention{Succeeded: 2 * time.Hour, Dead: 48 * time.Hour, BatchSize: 250}

	body := map[string]any{"succeeded_seconds": 0, "dead_seconds": 0}
	rec := doJSON(t, pruningServer(store, policy), http.MethodPost, "/v1/prune", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
	}

	if store.applied != policy {
		t.Errorf("applied policy = %+v, want the server's own %+v", store.applied, policy)
	}
}

// A store that keeps no finished jobs has nothing to prune. That is a
// property of the server, not a bad request, so it is 501.
func TestHandlePruneUnsupportedStore(t *testing.T) {
	rec := doJSON(t, pruningServer(newFakeStore(), queue.Retention{Succeeded: time.Hour}), http.MethodPost, "/v1/prune", nil)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "does not keep finished jobs") {
		t.Errorf("body = %s, want it to say why", rec.Body)
	}
}

// A server with no policy answers 501 rather than 404, so a caller can
// tell "this server does not prune" from a mistyped path -- and rather
// than 200 with nothing deleted, which would look like success.
func TestHandlePruneWithoutAPolicy(t *testing.T) {
	store := newPruningStore()

	rec := doJSON(t, pruningServer(store, queue.Retention{}), http.MethodPost, "/v1/prune", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "no retention policy") {
		t.Errorf("body = %s, want it to name the missing policy", rec.Body)
	}
	if store.calls != 0 {
		t.Errorf("the store was asked to prune %d times with no policy configured", store.calls)
	}
}

func TestHandlePruneStoreError(t *testing.T) {
	store := newPruningStore()
	store.pruneErr = errBoom

	rec := doJSON(t, pruningServer(store, queue.Retention{Succeeded: time.Hour}), http.MethodPost, "/v1/prune", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body: %s", rec.Code, rec.Body)
	}
}

// A failure counting what remains must not lose the pass's own numbers:
// the deletions really happened, and reporting zero would be a lie.
func TestHandlePruneSurvivesARetainedCountFailure(t *testing.T) {
	store := newPruningStore()
	store.result = queue.Pruned{Succeeded: 5}
	store.retainedErr = errBoom

	rec := doJSON(t, pruningServer(store, queue.Retention{Succeeded: time.Hour}), http.MethodPost, "/v1/prune", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
	}

	var got PruneResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Succeeded != 5 || got.Total != 5 {
		t.Errorf("response = %+v, want the pass's counts preserved", got)
	}
	if got.Retained != 0 {
		t.Errorf("Retained = %d, want 0 when the count could not be taken", got.Retained)
	}
}

func TestHandlePruneRejectsGet(t *testing.T) {
	rec := doJSON(t, pruningServer(newPruningStore(), queue.Retention{Succeeded: time.Hour}), http.MethodGet, "/v1/prune", nil)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/prune status = %d, want 405", rec.Code)
	}
}

// End to end against the real backend: a sqlite queue holding a finished
// job, pruned through the HTTP handler.
func TestHandlePruneAgainstSQLite(t *testing.T) {
	q, err := sqlite.Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()

	j := job.New("q", []byte("work"))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	claimed.MarkSucceeded(time.Now().UTC())
	if err := q.Complete(claimed); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	h := pruningServer(q, queue.Retention{Succeeded: time.Nanosecond})
	rec := doJSON(t, h, http.MethodPost, "/v1/prune", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
	}

	var got PruneResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Succeeded != 1 || got.Retained != 0 {
		t.Errorf("response = %+v, want one job pruned and none retained", got)
	}
	if _, err := q.Get(j.ID); err == nil {
		t.Error("the job is still in the queue after a successful prune")
	}
}
