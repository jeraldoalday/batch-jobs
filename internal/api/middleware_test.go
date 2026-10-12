package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthDisabledByDefault(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/stats", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (auth should be off with no AuthToken)", rec.Code, http.StatusOK)
	}
}

func TestAuthRejectsMissingKey(t *testing.T) {
	h := (&Server{Store: newFakeStore(), AuthToken: "secret"}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/stats", nil)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthRejectsWrongKey(t *testing.T) {
	h := (&Server{Store: newFakeStore(), AuthToken: "secret"}).Handler()

	r := httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	r.Header.Set("X-API-Key", "wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthAcceptsCorrectKey(t *testing.T) {
	h := (&Server{Store: newFakeStore(), AuthToken: "secret"}).Handler()

	r := httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	r.Header.Set("X-API-Key", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAuthExemptsHealthz(t *testing.T) {
	h := (&Server{Store: newFakeStore(), AuthToken: "secret"}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/healthz", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (/healthz should never require auth)", rec.Code, http.StatusOK)
	}
}

func TestRateLimitDisabledByDefault(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	for i := 0; i < 20; i++ {
		rec := doJSON(t, h, http.MethodGet, "/healthz", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want %d (no RateLimit configured)", i, rec.Code, http.StatusOK)
		}
	}
}

func TestRateLimitAllowsBurstThenBlocks(t *testing.T) {
	s := &Server{
		Store:     newFakeStore(),
		RateLimit: RateLimit{RPS: 1, Burst: 3},
	}
	h := s.Handler()

	for i := 0; i < 3; i++ {
		r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		r.RemoteAddr = "203.0.113.1:1234"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d within burst: status = %d, want %d", i, rec.Code, http.StatusOK)
		}
	}

	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.RemoteAddr = "203.0.113.1:1234"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("request beyond burst: status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestRateLimitTracksClientsSeparately(t *testing.T) {
	s := &Server{
		Store:     newFakeStore(),
		RateLimit: RateLimit{RPS: 1, Burst: 1},
	}
	h := s.Handler()

	for _, ip := range []string{"203.0.113.1:1", "203.0.113.2:1"} {
		r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		r.RemoteAddr = ip
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("client %s: status = %d, want %d", ip, rec.Code, http.StatusOK)
		}
	}
}

func TestValidToken(t *testing.T) {
	cases := []struct {
		name      string
		presented string
		want      string
		ok        bool
	}{
		{"matches", "secret", "secret", true},
		{"wrong value", "wrong", "secret", false},
		{"shorter than want", "sec", "secret", false},
		{"longer than want", "secretlonger", "secret", false},
		{"empty presented", "", "secret", false},
		{"both empty", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := validToken(c.presented, c.want); got != c.ok {
				t.Errorf("validToken(%q, %q) = %v, want %v", c.presented, c.want, got, c.ok)
			}
		})
	}
}

func TestClientIPFallsBackToRemoteAddr(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.RemoteAddr = "not-a-host-port"

	if got := clientIP(r); got != "not-a-host-port" {
		t.Errorf("clientIP = %q, want %q", got, "not-a-host-port")
	}
}
