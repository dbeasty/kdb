package control

import (
	_ "embed"
	"net/http"
	"strings"
)

// The control UI is one dependency-free HTML file compiled into the binary.
//
// The plan (§4.2) calls for a React + Vite SPA in packages/kdb-ui, and for the write, time-travel
// and revert milestones that is still the right answer - those screens have real client state. For
// the read-only viewer this milestone delivers, a framework would buy nothing and would cost the
// Go release pipeline an npm toolchain, a node_modules tree, and a build artifact to make
// reproducible (see docs/kdb-release-plan.md). One file, no build step, one binary.
//
// When the SPA arrives it replaces this handler and nothing else in the package changes: the API
// is the contract, and this page is only its first client.
//
//go:embed ui/index.html
var indexHTML []byte

func uiHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// One page, served at / and nowhere else: a stray path should 404 rather than silently
		// render the app, which makes a typo'd API URL look like a working page.
		if r.URL.Path != "/" && !strings.EqualFold(r.URL.Path, "/index.html") {
			writeError(w, http.StatusNotFound, "not_found", "no such path")
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The page carries no secrets and is versioned with the binary, but it reads live data, so
		// a stale cached shell against a newer API is a support call waiting to happen.
		w.Header().Set("Cache-Control", "no-cache")
		// The page loads nothing from anywhere: no CDN, no fonts, no analytics. Saying so in a
		// header means a browser enforces it even if a future edit forgets.
		w.Header().Set("Content-Security-Policy",
			"default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(indexHTML)
	})
}
