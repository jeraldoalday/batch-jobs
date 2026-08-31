package client

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestPrune(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"succeeded":12,"failed":2,"dead":0,"cancelled":3,"total":17,"retained":40}`)

	pruned, err := c.Prune(context.Background())
	if err != nil {
		t.Fatalf("Prune = %v", err)
	}
	if pruned.Succeeded != 12 || pruned.Failed != 2 || pruned.Cancelled != 3 || pruned.Dead != 0 {
		t.Errorf("Prune = %+v, want succeeded=12 failed=2 cancelled=3 dead=0", pruned)
	}
	if got := pruned.Total(); got != 17 {
		t.Errorf("Total() = %d, want 17", got)
	}
	if pruned.Retained != 40 {
		t.Errorf("Retained = %d, want 40", pruned.Retained)
	}
	method, rawURL, _, _ := s.lastRequest()
	if method != http.MethodPost {
		t.Errorf("method = %s, want POST", method)
	}
	if rawURL != "/v1/prune" {
		t.Errorf("path = %s, want /v1/prune", rawURL)
	}
}

// The request carries no body, because there is nothing for the caller to
// say: the policy is the server's, and a client able to name its own
// could delete every finished job by asking for a retention of zero.
func TestPruneSendsNoPolicy(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"succeeded":1,"total":1,"retained":0}`)

	if _, err := c.Prune(context.Background()); err != nil {
		t.Fatalf("Prune = %v", err)
	}
	if _, _, body, _ := s.lastRequest(); body != "" {
		t.Errorf("request body = %q, want it empty", body)
	}
}

func TestPruneUnsupported(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusNotImplemented, `{"error":"this server has no retention policy configured"}`)

	_, err := c.Prune(context.Background())
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Prune on 501 = %v, want ErrUnsupported", err)
	}
	// The server's own message is what says which of the two 501s this
	// is, so it must survive into the error.
	if !strings.Contains(err.Error(), "no retention policy") {
		t.Errorf("error = %q, want the server's message preserved", err)
	}
}

func TestPruneServerError(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusInternalServerError, `{"error":"failed to prune finished jobs"}`)

	if _, err := c.Prune(context.Background()); !errors.Is(err, ErrServer) {
		t.Errorf("Prune on 500 = %v, want ErrServer", err)
	}
}

func TestPruneCancelledContext(t *testing.T) {
	s := newStub(t)
	c := s.client(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := c.Prune(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Prune with a cancelled context = %v, want context.Canceled", err)
	}
}

func TestPrunedAccessors(t *testing.T) {
	p := Pruned{Succeeded: 4, Dead: 1}

	if got := p.Get(StatusSucceeded); got != 4 {
		t.Errorf("Get(succeeded) = %d, want 4", got)
	}
	if got := p.Get(StatusPending); got != 0 {
		t.Errorf("Get(pending) = %d, want 0: a pending job was never a candidate", got)
	}
	if got := p.String(); !strings.Contains(got, "succeeded=4") || !strings.Contains(got, "dead=1") {
		t.Errorf("String() = %q, want both non-zero counts", got)
	}
	if strings.Contains(p.String(), "failed") {
		t.Errorf("String() = %q, want zero counts omitted", p.String())
	}
	if got := (Pruned{}).String(); got != "nothing pruned" {
		t.Errorf("empty String() = %q, want %q", got, "nothing pruned")
	}
	// Retained is not a deletion and must stay out of the total.
	if got := (Pruned{Retained: 9}).Total(); got != 0 {
		t.Errorf("Total() with only Retained set = %d, want 0", got)
	}
}
