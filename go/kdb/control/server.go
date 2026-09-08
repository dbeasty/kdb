// Package control serves the KDB control plane: an authenticated HTTP/JSON API over a running
// server, plus the single-page control UI that consumes it.
//
// See docs/kdb-control-ui-plan.md. Two things about the shape of this package follow directly
// from that plan and should survive any refactor:
//
//   - It is HTTP, not new wire opcodes (§4.1). The wire protocol carries a Kotlin/Go parity
//     obligation and cross-language golden fixtures; history browsing would need a dozen new
//     message types, each doubling into Kotlin. The Go server is the production deployment target,
//     so a Go-only control surface is consistent, and the UI's needs - cursor pagination over a
//     long log, ETags on immutable commits, SSE, bearer auth - are HTTP-shaped anyway.
//
//   - It never bypasses the runtime. Every handler reads through *server.KdbServerRuntime or the
//     DAG behind it. There is no second path into storage here, and no business logic that a
//     later wire or gRPC exposure would have to reimplement.
//
// Reads - history, diffs, schema, settings, operational state, and reads at any past revision -
// need no write permission. Revert is the one mutating operation implemented, and it is gated on
// --control-write and split into plan and apply so that what is applied is what was previewed.
// Document CRUD and the SQL console are the next milestone; their routes are declared and answer
// 501 rather than 404.
package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/server"
)

// Options configures a control-plane server.
type Options struct {
	// Addr is the host:port to bind. Port 0 binds an ephemeral port, which Addr reports back.
	Addr string
	// Runtime is the server runtime this control plane inspects. Required.
	Runtime *server.KdbServerRuntime
	// Namespace is the namespace Runtime serves. One runtime is one namespace (see
	// KdbServerRuntime's own doc comment); the multi-namespace host is a later milestone, and
	// until it lands this is the only namespace the control plane can see.
	Namespace string
	// Settings is the resolved, provenance-annotated service configuration - see
	// config.Describe. Empty is allowed; the settings endpoint then reports nothing rather than
	// guessing.
	Settings []config.SettingDescriptor
	// AllowWrites opts into mutating endpoints. Off by default: turning the control plane on is
	// not, by itself, a decision to let a browser change the database.
	//
	// Revert is the operation this currently gates - and only applying it. Planning a revert is a
	// read, so it stays available either way and an operator can always see what one would do.
	// Endpoints that are specified but not built answer 501 rather than 403 whatever this says,
	// because "the server said no" and "the server cannot" are different operator problems.
	AllowWrites bool
	// Version is reported by /v1/health.
	Version string
	// ServeUI serves the embedded single-page UI at /. Off leaves the API alone on this
	// listener.
	ServeUI bool
	// Now is the clock, for tests. nil uses time.Now.
	Now func() time.Time
}

// Server is a running control plane.
type Server struct {
	opts    Options
	httpSrv *http.Server
	ln      net.Listener
	events  *eventHub
	started time.Time

	mu sync.RWMutex
	// settings is Options.Settings, held under mu so a later milestone that applies a live
	// setting change can update the reported set without racing readers.
	settings []config.SettingDescriptor
}

// New binds Addr and starts serving immediately.
func New(opts Options) (*Server, error) {
	if opts.Runtime == nil {
		return nil, errors.New("control: Runtime is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	ln, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("control listen %s: %w", opts.Addr, err)
	}
	s := &Server{
		opts:     opts,
		ln:       ln,
		events:   newEventHub(),
		started:  opts.Now(),
		settings: opts.Settings,
	}
	s.httpSrv = &http.Server{
		Handler: s.routes(),
		// The control plane is reachable by a browser, so a slow-loris on it must not tie up the
		// process the way it would on an unbounded handler.
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = s.httpSrv.Serve(ln) }()
	return s, nil
}

// Addr returns the bound address, which is how a caller that passed port 0 learns the port.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Close stops the listener and releases every SSE subscriber. Safe to call more than once.
func (s *Server) Close() error {
	s.events.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := s.httpSrv.Shutdown(ctx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// PublishCommit fans a committed write out to connected SSE subscribers. Wire it to
// KdbServerRuntime.CommitListener.
//
// It must stay fast and non-blocking: CommitListener is called synchronously from inside the
// transaction's success path, so anything slow here shows up as write latency. The hub's send is
// best-effort per subscriber for exactly that reason.
func (s *Server) PublishCommit(namespaceID string, commit document.Commit) {
	s.events.publish(commitEvent{
		Namespace: namespaceID,
		Hash:      commit.Hash.Hex(),
		ShortHash: shortHash(commit.Hash.Hex()),
		Message:   commit.Message,
		Timestamp: millisToRFC3339(commit.Timestamp.EpochMillis),
		OpCount:   len(commit.Operations),
	})
}

// SetSettings replaces the reported configuration. For the milestone that applies live setting
// changes; today only the service's startup wiring calls it.
func (s *Server) SetSettings(descriptors []config.SettingDescriptor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings = descriptors
}

func (s *Server) settingsSnapshot() []config.SettingDescriptor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]config.SettingDescriptor, len(s.settings))
	for i, d := range s.settings {
		out[i] = d.Redact()
	}
	return out
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// Namespace-scoped reads. Authorized as a read of that namespace, which is exactly what they
	// are - so an existing "read:orders/*" grant covers them with no new vocabulary.
	mux.Handle("GET /v1/namespaces", s.nsRead(s.handleNamespaces))
	mux.Handle("GET /v1/ns/{ns}/status", s.nsRead(s.handleStatus))
	mux.Handle("GET /v1/ns/{ns}/schema", s.nsRead(s.handleSchema))
	mux.Handle("GET /v1/ns/{ns}/log", s.nsRead(s.handleLog))
	mux.Handle("GET /v1/ns/{ns}/commits/{hash}", s.nsRead(s.handleCommit))
	mux.Handle("GET /v1/ns/{ns}/commits/{hash}/diff", s.nsRead(s.handleCommitDiff))
	mux.Handle("GET /v1/ns/{ns}/refs", s.nsRead(s.handleRefs))
	mux.Handle("GET /v1/ns/{ns}/docs/{id}", s.nsRead(s.handleDocument))
	mux.Handle("GET /v1/ns/{ns}/events", s.nsRead(s.handleEvents))

	// Process-scoped. Authorized as an admin action on the "control" scope, matching the
	// existing admin: grant vocabulary (auth.AdminAction -> kind "admin").
	mux.Handle("GET /v1/health", s.adminRead(s.handleHealth))
	mux.Handle("GET /v1/settings", s.adminRead(s.handleSettings))
	mux.Handle("GET /v1/ops/runtime", s.adminRead(s.handleOpsRuntime))

	// Revert. Planning is a read - it computes a diff and writes nothing - so it is available
	// whatever the write setting, and an operator can always see what a revert *would* do. Only
	// applying is gated.
	mux.Handle("POST /v1/ns/{ns}/revert/plan", s.nsRead(s.handleRevertPlan))
	mux.Handle("POST /v1/ns/{ns}/revert/apply", s.nsWrite(s.handleRevertApply))

	// Still specified but not built. Declared rather than omitted so the API's shape is honest
	// about what is coming and a client gets 501 rather than 404.
	for _, route := range []string{
		"PUT /v1/ns/{ns}/docs/{id}",
		"DELETE /v1/ns/{ns}/docs/{id}",
		"POST /v1/ns/{ns}/sql",
		"PATCH /v1/settings",
	} {
		mux.Handle(route, s.notImplemented())
	}

	if s.opts.ServeUI {
		mux.Handle("GET /", uiHandler())
	}
	return withRecovery(mux)
}

// withRecovery keeps a handler panic from taking the process down. The control plane runs inside
// the database server: a bug in a diff renderer must not kill the thing serving traffic.
func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeError(w, http.StatusInternalServerError, "internal",
					fmt.Sprintf("control plane panic: %v", rec))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) notImplemented() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.opts.AllowWrites {
			writeError(w, http.StatusForbidden, "read_only",
				"this control plane is read-only; start the service with --control-write to enable "+
					"mutating endpoints")
			return
		}
		writeError(w, http.StatusNotImplemented, "not_implemented",
			"this endpoint is specified in docs/kdb-control-ui-plan.md but not built yet")
	})
}
