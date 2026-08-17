package api

import (
	"net/http"
	"testing"
	"testing/fstest"
)

func TestHandlerServesAssetsAtRoot(t *testing.T) {
	assets := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<h1>dashboard</h1>")},
	}
	h := (&Server{Store: newFakeStore(), Assets: assets}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	if got := rec.Body.String(); got != "<h1>dashboard</h1>" {
		t.Errorf("body = %q, want %q", got, "<h1>dashboard</h1>")
	}
}

func TestHandlerWithoutAssetsReturnsNotFoundAtRoot(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/", nil)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandlerAssetsDoNotShadowAPIRoutes(t *testing.T) {
	assets := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<h1>dashboard</h1>")},
	}
	h := (&Server{Store: newFakeStore(), Assets: assets}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/healthz", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
}
