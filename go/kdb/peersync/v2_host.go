package peersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// NamespaceProvider is what a v2 host or client needs from the node it runs on: which namespaces
// exist, and how to feed each one.
type NamespaceProvider interface {
	// List returns every namespace this node holds, for matching against a peer's patterns.
	List() []string
	// Env returns the ingest environment for ns. create permits opening a namespace this node
	// does not hold yet; without it an unknown namespace is an error.
	Env(ns string, create bool) (IngestEnv, error)
}

// V2HostConfig configures a v2 host.
type V2HostConfig struct {
	// NodeID is this node's identity; a peer presenting the same one is refused.
	NodeID string
	// Namespaces supplies the namespaces this host serves.
	Namespaces NamespaceProvider
	// ClassifyError maps a failure to the code sent back in PEER_ERROR - see HostConfig.
	ClassifyError func(error) (wire.ErrorCode, bool)
	// CreateOnPush lets a peer push into a namespace this node does not hold yet, creating it.
	// Off by default: whether a peer may create namespaces here is an operator's decision.
	CreateOnPush bool
}

// V2Host serves one v2 peer-sync connection.
type V2Host struct {
	wire   wire.Codec
	cfg    V2HostConfig
	auth   auth.Engine
	connCx auth.ConnectionContext

	mu        sync.Mutex
	principal auth.Principal
	helloDone bool
	granted   map[string]bool
}

// NewV2Host returns a host for one connection.
func NewV2Host(w wire.Codec, cfg V2HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *V2Host {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &V2Host{wire: w, cfg: cfg, auth: engine, connCx: ctx, granted: map[string]bool{}}
}

// HostCapabilities are what a v2 host of this build can do.
var HostCapabilities = []string{wire.SyncCapBranches, wire.SyncCapTags, wire.SyncCapStubs}

// HandleFrame serves one frame, returning the reply. Every request gets one; failures are
// PEER_ERROR. Only a frame that cannot be decoded at all returns an error, and the caller drops
// the connection.
func (h *V2Host) HandleFrame(frame []byte) ([]byte, error) {
	msg, err := h.wire.Decode(frame)
	if err != nil {
		return nil, err
	}
	reply, err := h.serve(msg)
	if err != nil {
		return h.wire.Encode(wire.PeerErrorMessage{
			H:       wire.Header{MessageType: wire.MsgPeerError, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.Header().CorrelationID},
			Code:    h.classify(err),
			Message: err.Error(),
		})
	}
	return h.wire.Encode(reply)
}

func (h *V2Host) classify(err error) wire.ErrorCode {
	if h.cfg.ClassifyError != nil {
		if code, ok := h.cfg.ClassifyError(err); ok {
			return code
		}
	}
	return (&frameHandler{}).classify(err)
}

func header(t wire.MessageType, correlation int) wire.Header {
	return wire.Header{MessageType: t, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: correlation}
}

// NotGrantedError is a frame naming a namespace this session was not granted at hello.
type NotGrantedError struct{ Namespace string }

func (e *NotGrantedError) Error() string {
	return fmt.Sprintf("peer sync: namespace %q was not granted to this session", e.Namespace)
}

func (h *V2Host) serve(msg wire.Message) (wire.Message, error) {
	if hello, ok := msg.(wire.SyncHelloMessage); ok {
		return h.hello(hello)
	}
	h.mu.Lock()
	ready := h.helloDone
	h.mu.Unlock()
	if !ready {
		return nil, errors.New("peer sync: SYNC_HELLO must come first")
	}
	switch m := msg.(type) {
	case wire.RefsRequestMessage:
		refs, err := h.refs(m.Namespaces)
		if err != nil {
			return nil, err
		}
		return wire.RefsResultMessage{H: header(wire.MsgRefsResult, m.H.CorrelationID), Refs: refs}, nil
	case wire.FetchRequestMessage:
		env, err := h.env(m.Namespace, false)
		if err != nil {
			return nil, err
		}
		commits, stubs, done, err := MissingCommitsFrom(env.DAG, m.Wants, m.Haves, m.MaxBytes)
		if err != nil {
			return nil, err
		}
		return wire.PackPageMessage{
			H: header(wire.MsgPackPage, m.H.CorrelationID), Namespace: m.Namespace,
			Commits: commits, Stubs: stubs, Done: done,
		}, nil
	case wire.RefUpdateMessage:
		return h.refUpdate(m)
	default:
		return nil, &UnsupportedFrameError{Type: msg.Header().MessageType}
	}
}

func (h *V2Host) hello(m wire.SyncHelloMessage) (wire.Message, error) {
	reject := func(reason string) (wire.Message, error) {
		return wire.SyncHelloAckMessage{
			H: header(wire.MsgSyncHelloAck, m.H.CorrelationID), Accepted: false,
			NodeID: h.cfg.NodeID, Protocol: wire.SyncProtocolVersion, Reason: reason,
		}, nil
	}
	if h.cfg.NodeID != "" && m.NodeID == h.cfg.NodeID {
		return reject("peer sync: the connecting node has this node's own identity " + h.cfg.NodeID +
			"; a copied data root must delete its NODE file to become a separate node")
	}
	principal, err := h.auth.Authenticator().Authenticate(context.Background(),
		auth.Credentials{User: m.User, Password: m.Password, Token: m.Token})
	if err != nil {
		return reject(err.Error())
	}
	// Each matching namespace is granted individually: a pattern is a request, and a principal
	// allowed to sync some of what it names gets exactly those, not a refusal of the whole.
	matched := SelectNamespaces(m.Namespaces, h.cfg.Namespaces.List())
	granted := map[string]bool{}
	var names []string
	for _, ns := range matched {
		if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.PeerSyncAction{Namespace: ns}); err == nil {
			granted[ns] = true
			names = append(names, ns)
		}
	}
	// A literal (wildcard-free) name the node does not hold yet can still be granted for a push
	// that creates it, when the operator allows that.
	if h.cfg.CreateOnPush {
		for _, p := range m.Namespaces {
			if isLiteralNamespace(p) && !granted[p] {
				if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.PeerSyncAction{Namespace: p}); err == nil {
					granted[p] = true
				}
			}
		}
	}
	h.mu.Lock()
	h.principal, h.helloDone, h.granted = principal, true, granted
	h.mu.Unlock()
	refs, err := h.refs(names)
	if err != nil {
		return nil, err
	}
	return wire.SyncHelloAckMessage{
		H: header(wire.MsgSyncHelloAck, m.H.CorrelationID), Accepted: true,
		NodeID: h.cfg.NodeID, Protocol: wire.SyncProtocolVersion,
		Capabilities: intersect(HostCapabilities, m.Capabilities), Refs: refs,
	}, nil
}

func isLiteralNamespace(p string) bool {
	for _, r := range p {
		if r == '*' || r == '!' {
			return false
		}
	}
	return p != ""
}

func intersect(a, b []string) []string {
	var out []string
	for _, x := range a {
		if containsString(b, x) {
			out = append(out, x)
		}
	}
	return out
}

func (h *V2Host) refs(namespaces []string) ([]wire.NamespaceRefs, error) {
	out := make([]wire.NamespaceRefs, 0, len(namespaces))
	for _, ns := range namespaces {
		env, err := h.env(ns, false)
		if err != nil {
			return nil, err
		}
		out = append(out, RefsOf(ns, env.DAG))
	}
	return out, nil
}

// env returns ns's ingest environment, re-authorizing on every use so a grant revoked
// mid-session takes effect on the next frame.
func (h *V2Host) env(ns string, create bool) (IngestEnv, error) {
	h.mu.Lock()
	ok, principal := h.granted[ns], h.principal
	h.mu.Unlock()
	if !ok {
		return IngestEnv{}, &NotGrantedError{Namespace: ns}
	}
	if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.PeerSyncAction{Namespace: ns}); err != nil {
		return IngestEnv{}, err
	}
	return h.cfg.Namespaces.Env(ns, create)
}

func (h *V2Host) refUpdate(m wire.RefUpdateMessage) (wire.Message, error) {
	env, err := h.env(m.Namespace, h.cfg.CreateOnPush)
	if err != nil {
		return nil, err
	}
	stored, err := StoreCommits(env, m.Commits, m.Stubs)
	if err != nil {
		return nil, fmt.Errorf("%w (%d of %d commits in this page were stored before it)", err, stored, len(m.Commits))
	}
	ack := wire.RefUpdateAckMessage{
		H: header(wire.MsgRefUpdateAck, m.H.CorrelationID), Namespace: m.Namespace,
		Kind: m.Kind, Ref: m.Ref, Stored: stored, Outcome: wire.RefStored,
	}
	if m.More {
		return ack, nil
	}
	proposed, err := codec.HashFromHex(m.NewHex)
	if err != nil {
		return nil, err
	}
	res, err := ApplyRef(env, m.Kind, m.Ref, proposed)
	if err != nil {
		return nil, err
	}
	ack.Outcome, ack.HeadHex = res.Outcome, res.Head.Hex()
	if res.Conflict != nil {
		if ack.ConflictBytes, err = json.Marshal(res.Conflict); err != nil {
			return nil, err
		}
	}
	return ack, nil
}
