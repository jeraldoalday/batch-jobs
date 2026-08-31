package api

import (
	"net/http"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// handlePrune handles POST /v1/prune, deleting finished jobs the server's
// retention policy has expired and reporting what it removed.
//
// The policy lives on the server rather than in the request body, which is
// the whole design of the endpoint. A caller who could name their own
// retention could delete any job in the queue by asking for a retention of
// zero, which makes an operational convenience into a destructive API that
// every client holds. What the endpoint offers instead is the timing: run
// the configured policy now, rather than at the janitor's next interval.
//
// Two different absences both answer 501, because both mean "this server
// does not do that" rather than "you asked wrongly": a store that does not
// keep finished jobs to begin with, and a server with no policy
// configured. The message distinguishes them for whoever is reading it.
func (s *Server) handlePrune(w http.ResponseWriter, r *http.Request) {
	pruner, ok := s.Store.(queue.Pruner)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this store does not keep finished jobs, so it has nothing to prune")
		return
	}
	if !s.Retention.Enabled() {
		writeError(w, http.StatusNotImplemented, "this server has no retention policy configured")
		return
	}

	pruned, err := pruner.Prune(s.Retention)
	if err != nil {
		s.Logger.Error("api: prune failed", "error", err, "pruned", pruned.String())
		writeError(w, http.StatusInternalServerError, "failed to prune finished jobs")
		return
	}

	if pruned.Total() > 0 {
		s.Logger.Info("api: pruned finished jobs", "total", pruned.Total(), "detail", pruned.String())
	}

	// The remaining count is reported beside the pass's own numbers for
	// the same reason recovery reports its outstanding leases: it is what
	// distinguishes "the history is now within policy" from "this pass
	// deleted a lot and there is still a lot there".
	retained, err := pruner.Retained()
	if err != nil {
		s.Logger.Error("api: retained job count failed", "error", err)
	}

	writeJSON(w, http.StatusOK, PruneResponse{
		Succeeded: pruned.Succeeded,
		Failed:    pruned.Failed,
		Dead:      pruned.Dead,
		Cancelled: pruned.Cancelled,
		Total:     pruned.Total(),
		Retained:  retained,
	})
}
