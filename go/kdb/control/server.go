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

	"log/slog"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/server"
)

// serverRuntime is the per-namespace runtime the control plane reads through.
type serverRuntime = server.KdbServerRuntime

// Options configures a control-plane server.
type Options struct {
	// Addr is the host:port to bind. Port 0 binds an ephemeral port, which Addr reports back.
	Addr string
	// Runtime is a single runtime to serve, for a caller with exactly one. Either this or
	// Namespaces is required; Namespaces wins when both are set.
	Runtime *server.KdbServerRuntime
	// Namespace is the namespace Runtime serves, and the namespace a request that names none is
	// about.
	Namespace string
	// Namespaces is where this control plane finds runtimes when it serves more than one. A
	// service backed by embed.Host passes a source that can see every namespace the host has
	// opened; see the service's own wiring.
	Namespaces NamespaceSource
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
	// LogLevel is the process's live log level, when the caller holds one. Without it the log
	// level is reported but cannot be changed, and the refusal says so.
	LogLevel *slog.LevelVar
	// StagingDir is where staged restores are written. Empty disables them. Keep it off the data
	// volume: a restore is most needed exactly when the data volume is the problem.
	StagingDir string
	// BackupDir is where control-plane backups are written. Empty disables them, and the endpoints
	// say so rather than failing obscurely - a backup with nowhere to go is a configuration
	// question, not an error.
	BackupDir string
	// AllowSettingsPersist lets an applied setting also be written back to the config file. Off by
	// default: in a GitOps-managed deployment that file belongs to a deployment tool, and a server
	// rewriting it is a surprise rather than a feature. A change applied without it is still
	// reported as drift, so nothing is silently lost.
	AllowSettingsPersist bool
	// AllowPromotion opts into promoting a staged restore over the live namespace. Off by default,
	// and separately from AllowWrites, because it is the one control-plane operation that ends with
	// this process exiting: it stages the copy, records the intent, and asks to be restarted so the
	// next startup can install it. A deployment without a supervisor would simply stop.
	AllowPromotion bool
	// RequestRestart asks the process to shut down orderly and exit 75, the supervisor contract
	// --abort-after already uses. Without it promotion is refused, because staging a promotion this
	// process cannot then act on would leave an intent nobody asked for waiting on disk.
	//
	// It is a callback rather than something this package does itself: process lifecycle belongs to
	// the service that owns main, and a library that called os.Exit would be unusable from a test
	// or an embedded host.
	RequestRestart func(reason string) error
	// ConfigPath is the --config file this process resolved its settings from, and the only file a
	// persisted change is ever written to. Empty means there was none: a change can then still be
	// applied live, but there is nowhere to write it down, and asking to persist says so rather
	// than inventing a file that nothing would read on the next startup.
	ConfigPath string
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
	// settings is what this process is currently running on: Options.Settings at startup, updated
	// in place as changes are applied.
	settings []config.SettingDescriptor
	// startupSettings is the same set as it was when the process started, kept so drift can be
	// reported as "what a restart would undo". Comparing against a re-resolution of file and
	// environment would be wrong: flags set on the command line would be set again on a restart.
	startupSettings []config.SettingDescriptor
	// revision counts applied changes, for the compare-and-swap on a patch.
	revision int64

	// restore holds staging restore jobs and the read-only runtimes attached from them.
	restore *restoreState

	// recovery holds cached verification reports, so the UI can show the last result without
	// re-running a scan that walks the whole log.
	recovery *recoveryState

	// applyMu serializes whole patches. Settings that share one engine setter - the memory trio -
	// would otherwise let two concurrent patches install a combination neither asked for.
	applyMu sync.Mutex
}

// New binds Addr and starts serving immediately.
func New(opts Options) (*Server, error) {
	if opts.Runtime == nil && opts.Namespaces == nil {
		return nil, errors.New("control: one of Runtime or Namespaces is required")
	}
	if opts.Namespaces == nil && opts.Namespace == "" {
		return nil, errors.New("control: Namespace is required alongside Runtime")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	ln, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("control listen %s: %w", opts.Addr, err)
	}
	s := &Server{
		opts:            opts,
		ln:              ln,
		events:          newEventHub(),
		started:         opts.Now(),
		settings:        append([]config.SettingDescriptor(nil), opts.Settings...),
		startupSettings: append([]config.SettingDescriptor(nil), opts.Settings...),
		recovery:        newRecoveryState(),
		restore:         newRestoreState(),
		// Starts at 1, not 0: zero is what a caller sends to mean "do not check the revision", so
		// a real revision of zero would silently skip the compare-and-swap it asked for.
		revision: 1,
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
	// Release any staged copy before the process goes: each holds a shared lock on its own
	// directory, and leaving them open would outlive the thing that opened them.
	s.closeAttached()
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

// authEngine is the engine every request authenticates against. Namespaces may each carry their
// own; this is the one used before a namespace has been resolved, and as the fallback for a
// runtime that has none configured.
func (s *Server) authEngine() auth.Engine {
	if s.opts.Runtime != nil && s.opts.Runtime.AuthEngine != nil {
		return s.opts.Runtime.AuthEngine
	}
	for _, ns := range s.namespaces().Namespaces() {
		if rt, ok := s.namespaces().Runtime(ns); ok && rt.AuthEngine != nil {
			return rt.AuthEngine
		}
	}
	return nil
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
	mux.Handle("GET /v1/ns/{ns}/indexes", s.nsRead(s.handleIndexes))
	mux.Handle("GET /v1/ns/{ns}/log", s.nsRead(s.handleLog))
	mux.Handle("GET /v1/ns/{ns}/commits/{hash}", s.nsRead(s.handleCommit))
	mux.Handle("GET /v1/ns/{ns}/commits/{hash}/diff", s.nsRead(s.handleCommitDiff))
	mux.Handle("GET /v1/ns/{ns}/refs", s.nsRead(s.handleRefs))
	// Compare is a read: "what is different between these two points" is the one git-viewer
	// question the commit log cannot answer, and asking it changes nothing.
	mux.Handle("GET /v1/ns/{ns}/compare", s.nsRead(s.handleCompare))
	mux.Handle("GET /v1/ns/{ns}/docs", s.nsRead(s.handleDocuments))
	mux.Handle("GET /v1/ns/{ns}/docs/{id}", s.nsRead(s.handleDocument))
	mux.Handle("GET /v1/ns/{ns}/docs/{id}/history", s.nsRead(s.handleDocumentHistory))
	mux.Handle("POST /v1/ns/{ns}/sql", s.nsRead(s.handleSQL))
	mux.Handle("GET /v1/ns/{ns}/events", s.nsRead(s.handleEvents))

	// Process-scoped. Authorized as an admin action on the "control" scope, matching the
	// existing admin: grant vocabulary (auth.AdminAction -> kind "admin").
	mux.Handle("GET /v1/health", s.adminRead(s.handleHealth))
	mux.Handle("GET /v1/settings", s.adminRead(s.handleSettings))
	mux.Handle("GET /v1/settings/drift", s.adminRead(s.handleSettingsDrift))
	mux.Handle("GET /v1/settings/{key}", s.adminRead(s.handleSettingByKey))
	mux.Handle("PATCH /v1/settings", s.adminRead(s.handlePatchSettings))
	mux.Handle("GET /v1/ops/runtime", s.adminRead(s.handleOpsRuntime))
	mux.Handle("GET /v1/ops/locks", s.adminRead(s.handleOpsLocks))
	mux.Handle("GET /v1/ops/metrics", s.adminRead(s.handleOpsMetrics))
	// Draining is one-way and gated on --control-write plus a typed confirmation. It is a read
	// route only in the sense that every control route authorizes the same way; the handler
	// refuses without write permission.
	mux.Handle("POST /v1/ops/drain", s.adminRead(s.handleOpsDrain))

	// Revert. Planning is a read - it computes a diff and writes nothing - so it is available
	// whatever the write setting, and an operator can always see what a revert *would* do. Only
	// applying is gated.
	mux.Handle("PUT /v1/ns/{ns}/docs/{id}", s.nsWrite(s.handlePutDocument))
	mux.Handle("DELETE /v1/ns/{ns}/docs/{id}", s.nsWrite(s.handleDeleteDocument))
	mux.Handle("POST /v1/ns/{ns}/refs/branches", s.nsWrite(s.handleCreateBranch))
	mux.Handle("DELETE /v1/ns/{ns}/refs/branches/{name}", s.nsWrite(s.handleDeleteBranch))
	mux.Handle("POST /v1/ns/{ns}/refs/tags", s.nsWrite(s.handleCreateTag))
	mux.Handle("DELETE /v1/ns/{ns}/refs/tags/{name}", s.nsWrite(s.handleDeleteTag))

	// Recovery. Verifying and backing up are reads of the log and need no write permission: an
	// operator must always be able to find out whether their data is intact and take a copy of it.
	mux.Handle("GET /v1/ns/{ns}/integrity", s.nsRead(s.handleIntegrity))
	mux.Handle("POST /v1/ns/{ns}/integrity/verify", s.nsRead(s.handleVerify))
	mux.Handle("GET /v1/ns/{ns}/checkpoints", s.nsRead(s.handleCheckpoints))
	mux.Handle("GET /v1/ns/{ns}/maintenance/plan", s.nsRead(s.handleMaintenancePlan))
	mux.Handle("GET /v1/ns/{ns}/backups", s.nsRead(s.handleListBackups))
	mux.Handle("POST /v1/ns/{ns}/backups/{id}/verify", s.nsRead(s.handleVerifyBackup))
	// Creating one writes to the backup directory, so it is gated - not because it touches the
	// database, but because it consumes disk somewhere an operator did not ask for it to.
	mux.Handle("POST /v1/ns/{ns}/backups", s.nsWrite(s.handleCreateBackup))

	// Staged restore. Writing to a scratch directory rather than to the database, but it consumes
	// disk and opens a second view of the data, so it is gated the same way a backup is.
	mux.Handle("POST /v1/ns/{ns}/restore/staging", s.nsWrite(s.handleStartRestore))
	mux.Handle("GET /v1/restore/staging", s.adminRead(s.handleListRestoreJobs))
	mux.Handle("GET /v1/restore/staging/{jobId}", s.adminRead(s.handleRestoreJob))
	mux.Handle("POST /v1/restore/staging/{jobId}/attach", s.adminRead(s.handleAttachRestore))
	mux.Handle("POST /v1/restore/staging/{jobId}/detach", s.adminRead(s.handleDetachRestore))

	// Promotion. The plan is a read - an operator should always be able to see what promoting
	// would do, and what stands in the way, on a control plane that would refuse to do it.
	mux.Handle("GET /v1/restore/staging/{jobId}/promote/plan", s.adminRead(s.handlePromotionPlan))
	mux.Handle("POST /v1/restore/staging/{jobId}/promote", s.adminRead(s.handlePromote))
	mux.Handle("GET /v1/promotion", s.adminRead(s.handlePromotionStatus))
	mux.Handle("DELETE /v1/promotion", s.adminRead(s.handleAbandonPromotion))

	mux.Handle("POST /v1/ns/{ns}/revert/plan", s.nsRead(s.handleRevertPlan))
	mux.Handle("POST /v1/ns/{ns}/revert/apply", s.nsWrite(s.handleRevertApply))

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

// A declared-but-unbuilt endpoint used to answer 501 here, so that "the server said no" and "the
// server cannot" stayed distinguishable while the plan ran ahead of the code. Every endpoint this
// package declares is now built, so the helper is gone: a route that does not exist answers 404,
// which is the truth about it.
