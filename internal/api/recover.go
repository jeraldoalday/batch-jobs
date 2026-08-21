package api

import (
	"net/http"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// handleRecover handles POST /v1/recover, reclaiming jobs whose worker
// died holding them and reporting what it did.
//
// A reaper normally does this on its own schedule; the endpoint exists for
// the times waiting for the next sweep is not acceptable -- an operator who
// has just confirmed a host is gone and wants its jobs running again now,
// or a deploy script that reclaims a retired instance's work as its last
// step. It is deliberately explicit rather than something the API does on
// a timer of its own, so the queue has exactly one recovery policy.
//
// Stores that do not lease jobs (and so cannot tell an orphan from a
// long-running job) return 501: the request is well-formed, the backend
// simply has no such concept.
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	rec, ok := s.Store.(queue.Recoverer)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this store does not lease jobs, so it has nothing to recover")
		return
	}

	result, err := rec.Recover()
	if err != nil {
		s.Logger.Error("api: recovery failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to recover orphaned jobs")
		return
	}

	if result.Total() > 0 {
		s.Logger.Warn("api: recovered orphaned jobs",
			"requeued", result.Requeued, "dead_lettered", result.DeadLettered)
	}

	// The remaining count is reported alongside the pass's own numbers so
	// a caller can tell "nothing was orphaned" from "orphans are being
	// created faster than this pass could clear them".
	remaining, err := rec.ExpiredLeases()
	if err != nil {
		s.Logger.Error("api: expired lease count failed", "error", err)
	}

	writeJSON(w, http.StatusOK, RecoverResponse{
		Requeued:     result.Requeued,
		DeadLettered: result.DeadLettered,
		Remaining:    remaining,
	})
}
