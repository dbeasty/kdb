package peersync

import (
	"context"
	"encoding/json"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Host serves peer sync wire frames.
type Host interface {
	Start(config HostConfig) error
	Stop() error
	HandleFrame(frame []byte) ([]byte, error)
}

type defaultHost struct {
	wire    wire.Codec
	dag     *dag.InMemoryCommitDag
	storage storage.Adapter
	auth    auth.Engine
	connCtx auth.ConnectionContext
	handler *frameHandler
	config  *HostConfig
}

// NewHost creates an in-memory peer sync host. store is used for the document-level writes/
// deletes a non-conflicting auto-merge (see ResolveDivergence) stages when an incoming push
// diverges from local history - required, not optional, since a real push can hit that path.
func NewHost(w wire.Codec, dagInst *dag.InMemoryCommitDag, store storage.Adapter, engine auth.Engine, ctx auth.ConnectionContext) Host {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &defaultHost{wire: w, dag: dagInst, storage: store, auth: engine, connCtx: ctx}
}

func (h *defaultHost) Start(config HostConfig) error {
	h.config = &config
	h.handler = newFrameHandler(h.wire, h.dag, h.storage, config, h.auth, h.connCtx)
	hub := stream.HubFor(config.TransportHub)
	hub.ServerHandler = func(frame []byte) {
		if response, err := h.handler.handleFrame(frame); err == nil && response != nil {
			hub.ServerSend(response)
		}
	}
	return nil
}

func (h *defaultHost) Stop() error {
	if h.config == nil {
		return nil
	}
	stream.HubFor(h.config.TransportHub).ServerHandler = nil
	h.config = nil
	h.handler = nil
	return nil
}

func (h *defaultHost) HandleFrame(frame []byte) ([]byte, error) {
	if h.handler == nil {
		return nil, NewError("PeerSyncHost not started", nil)
	}
	return h.handler.handleFrame(frame)
}

// ConnectionHost wraps a per-connection frame handler.
type ConnectionHost struct {
	handler *frameHandler
}

// NewConnectionHost builds a host for one connection context.
func NewConnectionHost(w wire.Codec, dagInst *dag.InMemoryCommitDag, store storage.Adapter, config HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *ConnectionHost {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &ConnectionHost{handler: newFrameHandler(w, dagInst, store, config, engine, ctx)}
}

func (h *ConnectionHost) Start(HostConfig) error {
	return NewError("ConnectionHost does not support in-memory hub start", nil)
}

func (h *ConnectionHost) Stop() error { return nil }

func (h *ConnectionHost) HandleFrame(frame []byte) ([]byte, error) {
	return h.handler.handleFrame(frame)
}

type frameHandler struct {
	wire    wire.Codec
	dag     *dag.InMemoryCommitDag
	storage storage.Adapter
	cfg     HostConfig
	auth    auth.Engine
	ctx     auth.ConnectionContext

	// principal is the identity established by a successful Handshake (FULL_PEER mode,
	// authenticated, authorized for PeerSyncAction) - reused by every later CommitFetch/
	// CommitPush on this connection. Mirrors Kotlin's ConnectionAuthSupport.connectionPrincipal:
	// authenticated tracks whether it's actually been populated yet (Principal's zero value is
	// itself a valid-looking, if empty, value, so a bool is needed to distinguish "not yet
	// authenticated" from "authenticated as an anonymous/empty principal").
	principal     auth.Principal
	authenticated bool
}

func newFrameHandler(w wire.Codec, dagInst *dag.InMemoryCommitDag, store storage.Adapter, cfg HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *frameHandler {
	return &frameHandler{wire: w, dag: dagInst, storage: store, cfg: cfg, auth: engine, ctx: ctx}
}

// authorizePeerSync authorizes this connection's principal for PeerSyncAction on h.cfg.
// NamespaceID before honoring a CommitFetch/CommitPush - required on every such frame (not just
// cached from Handshake) so a grant revoked mid-connection takes effect immediately, matching
// Kotlin's PeerSyncFrameHandler.authorizePeerSync. If no Handshake has authenticated this
// connection yet (e.g. a peer sends CommitFetch/CommitPush first), authenticates now using h.ctx
// - the transport-provided connection context, empty for TCP the same way SqlWireHost's own
// Handshake-credentials-only model is (see wire_listen.go's principal field doc comment) - rather
// than treating an un-handshaken connection as implicitly trusted.
func (h *frameHandler) authorizePeerSync() error {
	if !h.authenticated {
		principal, err := h.auth.Authenticator().Authenticate(context.Background(), h.ctx.ToCredentials())
		if err != nil {
			return err
		}
		h.principal = principal
		h.authenticated = true
	}
	return h.auth.Authorizer().Authorize(context.Background(), h.principal, auth.PeerSyncAction{Namespace: h.cfg.NamespaceID})
}

func (h *frameHandler) handleFrame(frame []byte) ([]byte, error) {
	msg, err := h.wire.Decode(frame)
	if err != nil {
		return nil, err
	}
	switch m := msg.(type) {
	case wire.HandshakeMessage:
		if m.Request.ClientMode != wire.ClientFullPeer {
			reason := "FULL_PEER mode required"
			return h.wire.Encode(peerHandshakeAck(m, false, nil, &reason))
		}
		creds := auth.Credentials{User: m.Request.User, Password: m.Request.Password, Token: m.Request.Token}
		principal, err := h.auth.Authenticator().Authenticate(context.Background(), creds)
		if err != nil {
			reason := err.Error()
			return h.wire.Encode(peerHandshakeAck(m, false, nil, &reason))
		}
		if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.PeerSyncAction{Namespace: h.cfg.NamespaceID}); err != nil {
			reason := err.Error()
			return h.wire.Encode(peerHandshakeAck(m, false, nil, &reason))
		}
		h.principal = principal
		h.authenticated = true
		heads := map[string]string{h.cfg.NamespaceID: mustHeadHex(h.dag)}
		return h.wire.Encode(peerHandshakeAck(m, true, heads, nil))
	case wire.CommitFetchMessage:
		if err := h.authorizePeerSync(); err != nil {
			return nil, err
		}
		commits, err := h.fetchCommits(m.SinceHash, m.MaxCommits)
		if err != nil {
			return nil, err
		}
		push := wire.CommitPushMessage{
			H: wire.Header{
				MessageType:     wire.MsgCommitPush,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				CorrelationID:   m.H.CorrelationID,
			},
			Namespace: m.Namespace,
			Commits:   commits,
		}
		return h.wire.Encode(push)
	case wire.CommitPushMessage:
		if err := h.authorizePeerSync(); err != nil {
			return nil, err
		}
		// putCommit always stores, regardless of what happens to "main" below (component 39
		// spec §5: history must never be lost, only the branch-pointer decision is gated).
		applied := 0
		for _, commit := range m.Commits {
			if h.dag.HasCommit(commit.Hash) {
				continue
			}
			if err := h.dag.PutCommit(commit, true); err != nil {
				return nil, err
			}
			if h.cfg.MaterializeCommit != nil {
				_ = h.cfg.MaterializeCommit(commit)
			}
			// Fixes kdb-spec-layer13 §2.2: dag.PutCommit only mutates the in-memory DAG - without
			// this, a commit received from a peer lived only in memory and vanished on restart of
			// a file-backed node, even though the node re-fetching it from peers on next connect
			// would eventually paper over it cluster-wide (this is about *this* node's local
			// durability, not data loss overall).
			if h.cfg.Persist != nil {
				if err := h.cfg.Persist(commit); err != nil {
					return nil, err
				}
			}
			applied++
		}
		if len(m.Commits) > 0 {
			incomingHead := m.Commits[len(m.Commits)-1].Hash
			localHead, err := h.dag.Head()
			if err != nil {
				return nil, err
			}
			// Component 39-equivalent fix: an incoming push is not automatically "ahead" of
			// main just because it was pushed - this host's own history may have diverged
			// (e.g. local writes since the last sync). Same shared decision function as the
			// client's PullMissing, not two independently maintained copies - that's exactly
			// how the original blind dag.SetHead("main", ...) bug went unnoticed on one side
			// while looking "fine" on the other.
			outcome, err := ResolveDivergence(h.dag, h.storage, m.Namespace, localHead, incomingHead, ResolutionOptions{
				Policy:   h.cfg.ConflictPolicy,
				Resolver: h.cfg.ConflictResolver,
			})
			if err != nil {
				return nil, err
			}
			// The auto-merge case (OutcomeMerged) creates a brand new commit (via
			// AppendMergeCommit) that exists nowhere but this node - it needs the same
			// persistence as any other newly-created commit, not just the commits that arrived
			// over the wire above (kdb-spec-layer13 §2.2).
			if outcome.MergeCommit != nil && h.cfg.Persist != nil {
				if err := h.cfg.Persist(*outcome.MergeCommit); err != nil {
					return nil, err
				}
			}
			if outcome.Kind == OutcomeConflict {
				reportBytes, err := json.Marshal(outcome.Report)
				if err != nil {
					return nil, err
				}
				return h.wire.Encode(wire.ConflictReportMessage{
					H:           wire.Header{MessageType: wire.MsgConflictReport, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: m.H.CorrelationID},
					Namespace:   m.Namespace,
					ReportBytes: reportBytes,
				})
			}
			// NoOp/FastForwarded/Merged all succeed - fall through to the ack below.
		}
		// CommitPush is a request/response pair, not fire-and-forget (component 23 spec §5): the
		// client blocks on a correlated reply, so every non-conflicting outcome owes it one.
		// Returning nil here instead left a clean push with no reply at all and hung the caller
		// until its correlation wait expired.
		head, err := h.dag.Head()
		if err != nil {
			return nil, err
		}
		return h.wire.Encode(wire.CommitPushAckMessage{
			H:              wire.Header{MessageType: wire.MsgCommitPushAck, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: m.H.CorrelationID},
			Namespace:      m.Namespace,
			AppliedCommits: applied,
			HeadHex:        head.Hex(),
		})
	default:
		return nil, nil
	}
}

func (h *frameHandler) fetchCommits(sinceHash *codec.Hash, maxCommits int) ([]document.Commit, error) {
	head, err := h.dag.Head()
	if err != nil {
		return nil, err
	}
	if sinceHash != nil && *sinceHash == head {
		return nil, nil
	}
	walked := h.dag.Walk(head, sinceHash, maxCommits)
	out := make([]document.Commit, 0, len(walked))
	for i := len(walked) - 1; i >= 0; i-- {
		if full, ok := walked[i].(dag.FullEntry); ok {
			out = append(out, full.Commit)
		}
	}
	return out, nil
}

func mustHeadHex(d *dag.InMemoryCommitDag) string {
	head, err := d.Head()
	if err != nil {
		return ""
	}
	return head.Hex()
}

func peerHandshakeAck(msg wire.HandshakeMessage, accepted bool, remoteHeads map[string]string, rejectionReason *string) wire.HandshakeAckMessage {
	if remoteHeads == nil {
		remoteHeads = map[string]string{}
	}
	return wire.HandshakeAckMessage{
		H: wire.Header{
			MessageType:     wire.MsgHandshake,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   msg.H.CorrelationID,
		},
		Response: wire.HandshakeAckPayload{
			Accepted:           accepted,
			NegotiatedEncoding: wire.EncodingKdbBinary,
			ProtocolVersion:    wire.KdbWireProtocolVersion,
			RemoteHeads:        remoteHeads,
			RejectionReason:    rejectionReason,
		},
	}
}
