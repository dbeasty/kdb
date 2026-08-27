package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// ListenPeerSync starts a TCP peer-sync (Mode 3 full-peer) listener bound to addr, serving
// runtime's namespace to any connecting peer via CommitFetch/CommitPush/Handshake, with the same
// fast-forward/merge/conflict divergence handling as the Kotlin reference (component 39's
// ResolveDivergence, already implemented in go/kdb/peersync - this just gives it a real socket
// to listen on, which it previously had none of outside tests).
//
// Modeled directly on ListenSqlWire: same tcp.Transport, same accept-loop-per-connection shape,
// a distinct wire message set (peersync's Handshake/CommitFetch/CommitPush rather than SQL's).
//
// Every commit ingested from a peer (and any auto-merge commit ResolveDivergence produces) is
// materialized into runtime's storage immediately via embed.MaterializeCommit, so it is visible
// to SqlExec/Query/GetDocument on this same runtime right away - without this, a peer's writes
// would land in the DAG but never appear in a query, since dag.PutCommit alone only updates DAG
// bookkeeping, not document storage. It is also durably persisted via runtime's delta-log writer
// when the runtime is file-backed, matching ListenSqlWire's own durability contract
// (kdb-spec-layer13 §2.2).
//
// RBAC IS enforced on this listener (peersync.frameHandler.authorizePeerSync, gated on
// auth.PeerSyncAction), matching the Kotlin reference's PeerSyncFrameHandler. Component 39
// spec's own Non-Goals once deferred "RBAC interaction" (its test 8) as a possibly-separate fix
// - it wasn't: this front door had zero auth enforcement regardless of --rbac until this comment
// was updated (any TCP peer could CommitFetch/CommitPush the whole namespace), which the Kotlin
// side had already closed. See docs/kdb-finish-up-plan.md's 1-G9.
func ListenPeerSync(addr string, runtime *KdbServerRuntime, namespaceID string) (*Listener, error) {
	return ListenPeerSyncTLS(addr, runtime, namespaceID, nil)
}

// ListenPeerSyncTLS is ListenPeerSync with TLS settings for a tcps:// addr - see
// core.TransportTlsSettings. Pass nil for plaintext (equivalent to ListenPeerSync).
func ListenPeerSyncTLS(addr string, runtime *KdbServerRuntime, namespaceID string, tlsSettings *core.TransportTlsSettings) (*Listener, error) {
	if runtime.dag == nil {
		return nil, fmt.Errorf("kdb server: peer sync requires an InMemoryCommitDag (or a wrapper exposing one), got %T", runtime.Runtime.DAG)
	}
	opts := core.DefaultConnectOptions()
	opts.TLS = tlsSettings
	transport := tcp.NewTransport(opts)
	ln, err := transport.ListenBound(addr)
	if err != nil {
		return nil, err
	}
	codec := wire.NewCodec(wire.EncodingJSON)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l := &Listener{ln: ln, cancel: cancel, done: done}
	go func() {
		defer close(done)
		_ = transport.Serve(ctx, ln, func(conn stream.ConnectionHandle) {
			newPeerSyncConnHandler(codec, runtime, namespaceID).run(conn)
		})
	}()
	return l, nil
}

// peerSyncConnHandler dispatches peer-sync frames for one connection. Its ConnectionHost shares
// runtime's DAG/storage/AuthEngine with every other connection (peer sync's commits and heads
// are namespace-wide state, not per-connection, unlike the SQL listener's per-connection
// SessionManager).
type peerSyncConnHandler struct {
	host *peersync.ConnectionHost
}

func newPeerSyncConnHandler(codec wire.Codec, runtime *KdbServerRuntime, namespaceID string) *peerSyncConnHandler {
	cfg := peersync.HostConfig{
		NamespaceID: namespaceID,
		NodeID:      "kdb-service-go",
		MaterializeCommit: func(commit document.Commit) error {
			return embed.MaterializeCommit(runtime.Runtime.Storage, runtime.dag, namespaceID, commit)
		},
		Persist: func(commit document.Commit) error {
			if runtime.persister == nil {
				return nil
			}
			return runtime.persister.Persist(commit)
		},
	}
	host := peersync.NewConnectionHost(codec, runtime.dag, runtime.Runtime.Storage, cfg, runtime.AuthEngine, auth.EmptyContext)
	return &peerSyncConnHandler{host: host}
}

func (h *peerSyncConnHandler) run(conn stream.ConnectionHandle) {
	for frame := range conn.Incoming() {
		response, err := h.host.HandleFrame(frame)
		if err != nil {
			// A handler error used to be silently swallowed with no reply at all - the peer
			// blocked on its request timeout with zero diagnostics (kdb-finish-up-plan 4.H's
			// "clients hang" finding). The peer-sync wire has no generic error frame yet, so
			// the honest options are limited: log it and drop the connection, which the peer
			// sees immediately as a closed socket instead of a 30s silence.
			slog.Warn("peer-sync frame handler failed, dropping connection", "error", err)
			_ = conn.Close()
			return
		}
		if response == nil {
			continue
		}
		if err := conn.Send(response); err != nil {
			return
		}
	}
}
