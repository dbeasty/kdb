package peersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
		// Nothing trustworthy to correlate a reply with; the caller drops the connection.
		return nil, err
	}
	reply, err := h.serve(msg)
	if err != nil {
		// Every request gets an answer. Before PeerErrorMessage existed an unknown frame got none
		// (the peer waited out its 20s correlation timeout) and any other failure dropped the
		// connection without saying why (D7).
		return h.wire.Encode(wire.PeerErrorMessage{
			H:         wire.Header{MessageType: wire.MsgPeerError, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.Header().CorrelationID},
			Namespace: h.cfg.NamespaceID,
			Code:      h.classify(err),
			Message:   err.Error(),
		})
	}
	if reply == nil {
		return nil, nil
	}
	return h.wire.Encode(reply)
}

// NamespaceMismatchError is a peer-sync frame naming a namespace this connection does not serve.
type NamespaceMismatchError struct {
	Served, Requested string
}

func (e *NamespaceMismatchError) Error() string {
	return fmt.Sprintf("peer sync: this listener serves namespace %q, not %q", e.Served, e.Requested)
}

// UnsupportedFrameError is a frame type the peer-sync host does not serve.
type UnsupportedFrameError struct {
	Type wire.MessageType
}

func (e *UnsupportedFrameError) Error() string {
	return "peer sync: unsupported message type " + e.Type.String()
}

func (h *frameHandler) classify(err error) wire.ErrorCode {
	if h.cfg.ClassifyError != nil {
		if code, ok := h.cfg.ClassifyError(err); ok {
			return code
		}
	}
	var ns *NamespaceMismatchError
	var unsupported *UnsupportedFrameError
	var mismatch *TreeMismatchError
	var authErr *auth.AuthorizationError
	switch {
	case errors.As(err, &ns):
		return wire.ErrorCodeNamespaceMismatch
	case errors.As(err, &unsupported):
		return wire.ErrorCodeUnsupported
	case errors.As(err, &mismatch):
		return wire.ErrorCodeIntegrity
	case errors.As(err, &authErr):
		return wire.ErrorCodeUnauthorized
	}
	return wire.ErrorCodeInternal
}

func (h *frameHandler) requireNamespace(ns string) error {
	if ns != h.cfg.NamespaceID {
		return &NamespaceMismatchError{Served: h.cfg.NamespaceID, Requested: ns}
	}
	return nil
}

func (h *frameHandler) serve(msg wire.Message) (wire.Message, error) {
	switch m := msg.(type) {
	case wire.HandshakeMessage:
		if m.Request.ClientMode != wire.ClientFullPeer {
			reason := "FULL_PEER mode required"
			return peerHandshakeAck(m, false, nil, &reason), nil
		}
		if h.cfg.NodeID != "" && m.Request.NodeID == h.cfg.NodeID {
			// Two nodes sharing an identity would record progress against each other as if they
			// were one - almost always a data root copied without deleting its NODE file.
			reason := "peer sync: the connecting node has this node's own identity " + h.cfg.NodeID +
				"; a copied data root must delete its NODE file to become a separate node"
			return peerHandshakeAck(m, false, nil, &reason), nil
		}
		if len(m.Request.Namespaces) > 0 && !containsString(m.Request.Namespaces, h.cfg.NamespaceID) {
			reason := (&NamespaceMismatchError{Served: h.cfg.NamespaceID, Requested: strings.Join(m.Request.Namespaces, ",")}).Error()
			return peerHandshakeAck(m, false, nil, &reason), nil
		}
		creds := auth.Credentials{User: m.Request.User, Password: m.Request.Password, Token: m.Request.Token}
		principal, err := h.auth.Authenticator().Authenticate(context.Background(), creds)
		if err != nil {
			reason := err.Error()
			return peerHandshakeAck(m, false, nil, &reason), nil
		}
		if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.PeerSyncAction{Namespace: h.cfg.NamespaceID}); err != nil {
			reason := err.Error()
			return peerHandshakeAck(m, false, nil, &reason), nil
		}
		h.principal = principal
		h.authenticated = true
		heads := map[string]string{h.cfg.NamespaceID: mustHeadHex(h.dag)}
		return peerHandshakeAck(m, true, heads, nil), nil
	case wire.CommitFetchMessage:
		if err := h.requireNamespace(m.Namespace); err != nil {
			return nil, err
		}
		if err := h.authorizePeerSync(); err != nil {
			return nil, err
		}
		commits, stubs, err := h.fetchCommits(m.SinceHash, m.Haves, m.MaxCommits)
		if err != nil {
			return nil, err
		}
		return wire.CommitPushMessage{
			H: wire.Header{
				MessageType:     wire.MsgCommitPush,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				CorrelationID:   m.H.CorrelationID,
			},
			Namespace: m.Namespace,
			Commits:   commits,
			Stubs:     stubs,
		}, nil
	case wire.CommitPushMessage:
		if err := h.requireNamespace(m.Namespace); err != nil {
			return nil, err
		}
		if err := h.authorizePeerSync(); err != nil {
			return nil, err
		}
		return h.commitPush(m)
	default:
		return nil, &UnsupportedFrameError{Type: msg.Header().MessageType}
	}
}

// commitPush stores a pushed page and, on the last page, decides the head - through Ingest, the
// same function the client's pull uses, so push and pull cannot drift - under this node's write
// serialization.
func (h *frameHandler) commitPush(m wire.CommitPushMessage) (wire.Message, error) {
	env := h.ingestEnv()
	stored, err := StoreCommits(env, m.Commits, m.Stubs)
	if err != nil {
		return nil, fmt.Errorf("%w (%d of %d commits in this page were stored before it)", err, stored, len(m.Commits))
	}
	ack := func() (wire.Message, error) {
		head, err := h.dag.Head()
		if err != nil {
			return nil, err
		}
		return wire.CommitPushAckMessage{
			H:              wire.Header{MessageType: wire.MsgCommitPushAck, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: m.H.CorrelationID},
			Namespace:      m.Namespace,
			AppliedCommits: stored,
			HeadHex:        head.Hex(),
		}, nil
	}
	if m.More || len(m.Commits) == 0 {
		return ack()
	}
	result, err := Adopt(env, m.Commits[len(m.Commits)-1].Hash)
	if err != nil {
		return nil, err
	}
	if result.Outcome.Kind == OutcomeConflict {
		reportBytes, err := json.Marshal(result.Outcome.Report)
		if err != nil {
			return nil, err
		}
		return wire.ConflictReportMessage{
			H:           wire.Header{MessageType: wire.MsgConflictReport, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: m.H.CorrelationID},
			Namespace:   m.Namespace,
			ReportBytes: reportBytes,
		}, nil
	}
	// CommitPush is a request/response pair, not fire-and-forget (component 23 spec §5): the
	// client blocks on a correlated reply, so every non-conflicting outcome owes it one.
	return ack()
}

func containsString(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

// ingestEnv describes this host's namespace to Ingest.
func (h *frameHandler) ingestEnv() IngestEnv {
	return IngestEnv{
		DAG:            h.dag,
		Storage:        h.storage,
		NamespaceID:    h.cfg.NamespaceID,
		Node:           h.cfg.Node,
		Persist:        h.cfg.Persist,
		PersistAsync:   h.cfg.PersistAsync,
		ApplyToStorage: h.cfg.MaterializeCommit != nil || h.cfg.ApplyToStorage,
		Resolution:     ResolutionOptions{Policy: h.cfg.ConflictPolicy, Resolver: h.cfg.ConflictResolver},
	}
}

// fetchCommits pages what the fetcher lacks: commits reachable from this host's head and not
// from sinceHash or any of haves, parents first (D1). The old walk went newest-first from the
// head and cut at the cap, so a fetcher more than one page behind received the newest page -
// whose oldest commit's parent it did not have - and could never catch up.
func (h *frameHandler) fetchCommits(sinceHash *codec.Hash, haves []codec.Hash, maxCommits int) ([]document.Commit, []document.CommitStub, error) {
	head, err := h.dag.Head()
	if err != nil {
		return nil, nil, err
	}
	if maxCommits <= 0 {
		maxCommits = DefaultPageCommits
	}
	known := append([]codec.Hash(nil), haves...)
	if sinceHash != nil {
		known = append(known, *sinceHash)
	}
	return MissingCommits(h.dag, head, known, maxCommits)
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
