package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"
)

// writeJSON encodes v as the JSON response body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a JSON {"error": msg} body with the given status code.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// writeRetryable writes an error response that invites the client to try
// again, setting Retry-After to after (rounded up to whole seconds, as the
// header's delay-seconds form requires, and never below 1). It is the
// response shape for backpressure: the request was not wrong, the server is
// just not able to take it right now.
func writeRetryable(w http.ResponseWriter, status int, msg string, after time.Duration) {
	seconds := int(math.Ceil(after.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	writeError(w, status, msg)
}
