package control

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/metrics"
)

// The operations dashboard beyond process state (§9 screen 9).
//
// §5 named four more endpoints here - sessions, leases, peers, metrics - and a drain. Two of them
// can be built from data that exists, one is a real operation, and two cannot be built at all
// today. Saying which is which is the point of this file: a dashboard that showed an empty
// "Sessions" table would be read as "nobody is connected", which is a different claim from "this
// server does not track that", and an operator would act on the wrong one.
//
//   - Locks and leases: the LockManager is runtime-global, so this is real. It answers the
//     question a count cannot - which document is stuck, and who is holding it.
//   - Metrics: metrics.Default has recorded the write-path stage latencies all along. They were
//     reachable only through the admin listener's Prometheus endpoint, which carries no auth and
//     is meant to be bound privately, so a control plane that already authenticates every request
//     is the better place to read them from.
//   - Drain: BeginDraining and WaitForWritesToDrain are exactly the first two steps of the
//     documented orderly shutdown. Exposed with a typed confirmation, because it is one-way.
//   - Sessions: not available. A SessionManager is created per connection (see
//     server/session_manager.go), so there is no runtime-global registry to enumerate. Building
//     one is a change to the server, not to this package.
//   - Peers: not available. There is no peer registry to read; peer sync is a listener, not a
//     tracked set of members.

// handleOpsLocks lists the held document locks and leases.
func (s *Server) handleOpsLocks(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	rt := s.opts.Runtime
	if rt == nil || rt.DocumentLocks == nil {
		writeError(w, http.StatusInternalServerError, "no_runtime", "no lock manager on this runtime")
		return
	}
	type held struct {
		Namespace string `json:"namespace"`
		DocID     string `json:"docId"`
		SessionID string `json:"sessionId"`
		Fence     uint64 `json:"fence"`
		// ExpiresAt is absent for a lock with no deadline, which is a different situation from one
		// about to expire: it is an implicit hold taken and dropped inside a single call, so seeing
		// one at all means a commit is in flight right now.
		ExpiresAt string `json:"expiresAt,omitempty"`
		ExpiresIn string `json:"expiresIn,omitempty"`
		Kind      string `json:"kind"`
	}
	now := s.opts.Now()
	locks := rt.DocumentLocks.Held()
	out := make([]held, 0, len(locks))
	for _, l := range locks {
		h := held{
			Namespace: l.NamespaceID, DocID: l.DocID.String(), SessionID: l.SessionID,
			Fence: l.Fence, Kind: "implicit",
		}
		if !l.ExpiresAt.IsZero() {
			h.Kind = "lease"
			h.ExpiresAt = l.ExpiresAt.UTC().Format(time.RFC3339)
			h.ExpiresIn = l.ExpiresAt.Sub(now).Truncate(time.Millisecond).String()
		}
		out = append(out, h)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"locks": out,
		"count": len(out),
		"note": "an \"implicit\" lock has no deadline: it is taken and dropped inside one commit, " +
			"so one appearing here means a write is in flight now. A \"lease\" is a client hold " +
			"that spans round trips and expires on its own - the fence is what makes an expired " +
			"holder's late commit refused rather than accepted.",
	})
}

// handleOpsMetrics reports the write-path stage latencies.
func (s *Server) handleOpsMetrics(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	type stage struct {
		Stage   string  `json:"stage"`
		Count   int64   `json:"count"`
		MeanMs  float64 `json:"meanMs"`
		P50Ms   float64 `json:"p50Ms"`
		P99Ms   float64 `json:"p99Ms"`
		MaxMs   float64 `json:"maxMs"`
		Meaning string  `json:"meaning,omitempty"`
	}
	ms := func(d time.Duration) float64 {
		return float64(d.Microseconds()) / 1000.0
	}
	snap := metrics.Default.Snapshot()
	out := make([]stage, 0, len(snap))
	for _, s := range snap {
		out = append(out, stage{
			Stage: s.Stage, Count: s.Count,
			MeanMs: ms(s.Mean), P50Ms: ms(s.P50), P99Ms: ms(s.P99), MaxMs: ms(s.Max),
			Meaning: stageMeaning[s.Stage],
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stages": out,
		"note": "cumulative since this process started, over a capped ring of recent samples - so " +
			"count is every sample taken and the percentiles describe the recent ones. A stage " +
			"with no samples is absent rather than zero: nothing has been through it.",
	})
}

// stageMeaning says what a stage being slow would mean, because a name and a number on their own
// do not tell an operator which knob is the relevant one.
var stageMeaning = map[string]string{
	metrics.StageLockWait: "time a commit spent waiting for the write gate and document locks. " +
		"High means write contention, not slow storage.",
	metrics.StageFsyncWait: "time spent waiting for the durability sync. High means the disk is " +
		"the limit; group commit amortizes this across concurrent writers, so it rises when " +
		"writes arrive alone.",
	metrics.StageTreeRebuild: "time spent rebuilding a document tree for a historical read. " +
		"High means history is being read on a namespace whose strategy is replay.",
}

type drainRequest struct {
	// Confirm must be the namespace, as on a promotion. A drain cannot be undone.
	Confirm string `json:"confirm"`
	// WaitSeconds bounds how long to wait for in-flight writes to finish. 0 uses a short default;
	// the wait is reported either way, so a caller learns whether the gate actually quiesced.
	WaitSeconds int `json:"waitSeconds"`
}

// handleOpsDrain stops this server admitting writes and waits for in-flight ones to finish.
//
// One-way on purpose, and the response says so. Draining is the first step of the documented
// orderly shutdown (kdb-spec-layer13 Component 50) and the runtime has no un-drain: the flag is
// what /readyz reports and what every write path checks. Adding a way back would mean a shutdown
// already under way could be resumed halfway by an operator, which is a worse hazard than having
// to restart a node that was drained by mistake.
func (s *Server) handleOpsDrain(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	if !s.opts.AllowWrites {
		writeError(w, http.StatusForbidden, "read_only",
			"this control plane is read-only; start the service with --control-write")
		return
	}
	rt := s.opts.Runtime
	if rt == nil {
		writeError(w, http.StatusInternalServerError, "no_runtime", "no runtime to drain")
		return
	}
	var req drainRequest
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req)
	}
	want := s.defaultNamespace()
	if req.Confirm != want {
		writeError(w, http.StatusBadRequest, "confirmation_required",
			"draining stops this server accepting writes and cannot be undone without a restart. "+
				"Send {\"confirm\":\""+want+"\"} to proceed.")
		return
	}

	already := rt.IsDraining()
	rt.BeginDraining()
	wait := time.Duration(req.WaitSeconds) * time.Second
	if wait <= 0 {
		wait = 10 * time.Second
	}
	if wait > 5*time.Minute {
		wait = 5 * time.Minute
	}
	quiesced := rt.WaitForWritesToDrain(wait)

	body := map[string]any{
		"draining":     true,
		"alreadyWas":   already,
		"writesQuiet":  quiesced,
		"waitedFor":    wait.String(),
		"readinessNow": "/readyz on the admin listener now answers 503 with reason \"draining\"",
		"note": "new writes are refused from now on; reads are unaffected. This cannot be undone " +
			"without restarting the process. Storage stays crash-consistent either way - the " +
			"replay path covers anything that did not get flushed.",
	}
	if !quiesced {
		body["warning"] = "the wait elapsed with writes still in flight. They are still running; " +
			"nothing was cancelled. Read this endpoint's writesQuiet again, or give it longer."
	}
	writeJSON(w, http.StatusOK, body)
}

// unavailableOps is what §5 asked for and this server cannot answer, with the reason.
//
// Reported rather than 404'd: "this endpoint does not exist" and "this server cannot see that" are
// different answers, and only the second tells an operator not to keep looking.
var unavailableOps = []map[string]string{
	{
		"what": "sessions",
		"why": "a SessionManager is created per connection, so there is no runtime-global registry " +
			"to enumerate. An empty list here would read as \"nobody is connected\", which is a " +
			"different claim. Building the registry is a change to the server, not to the control " +
			"plane.",
	},
	{
		"what": "peers",
		"why": "peer sync is a listener, not a tracked set of members: there is no peer registry to " +
			"read. What can be seen is whether the listener is bound, which /v1/settings reports " +
			"as listener.peerAddr.",
	},
}
