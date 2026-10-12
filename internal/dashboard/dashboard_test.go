package dashboard

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
)

// index.html is the file FS must be rooted so that http.FileServerFS (what
// api.Server mounts it with) serves it for "/" rather than 404ing on a
// path that still has "static" in front of it.
func TestFSIsRootedAtIndex(t *testing.T) {
	if _, err := fs.Stat(FS, "index.html"); err != nil {
		t.Fatalf("FS has no index.html at its root: %v", err)
	}
}

func TestFSContainsExpectedAssets(t *testing.T) {
	for _, name := range []string{"index.html", "app.js", "style.css"} {
		info, err := fs.Stat(FS, name)
		if err != nil {
			t.Errorf("FS is missing %q: %v", name, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("%q is empty", name)
		}
	}
}

// Exercises FS the way api.Server actually serves it, rather than just
// asserting on fs.Stat -- this is what would fail if init's fs.Sub ever
// rooted the tree one level off (e.g. at "." instead of "static").
func TestFSServesIndexAtRoot(t *testing.T) {
	srv := httptest.NewServer(http.FileServerFS(FS))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}
