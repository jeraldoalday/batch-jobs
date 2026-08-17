// Package dashboard embeds the static assets for batch-jobs' minimal web
// UI: stat tiles summarizing queue state and a filterable job table with
// cancel/retry actions. It has no dependency on internal/api -- it only
// exposes the asset tree, so any HTTP server can mount it.
package dashboard

import (
	"embed"
	"io/fs"
)

//go:embed static
var embedded embed.FS

// FS is the dashboard's asset tree, rooted at the directory containing
// index.html, ready to be served by http.FileServerFS or similar.
var FS fs.FS

func init() {
	sub, err := fs.Sub(embedded, "static")
	if err != nil {
		// static is embedded above; a failure here means the embed
		// directive itself is broken, which build would already have
		// caught.
		panic(err)
	}
	FS = sub
}
