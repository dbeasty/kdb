package peersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/sql"
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
	// OnCaughtUp, when set, is told each time a peer finishes fetching a namespace - the last
	// page of a fetch, after which the peer holds everything this node's refs named. It is how a
	// node learns what inbound peers have, for peer-aware retention.
	OnCaughtUp func(ns, peer string, since time.Time)
	// WriteBack, when set, applies a filtered projection's local write to its source namespace
	// as principal, and says what became of it. An error means the source could not decide now
	// (unavailable, not the home): it goes back as PEER_ERROR and the replica tries again later.
	// Unset, the host does not offer write-back.
	WriteBack func(principal auth.Principal, m wire.ProjectWriteMessage) (wire.ProjectWriteResultMessage, error)
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
	// helloAt is when the session began: the peer holds everything committed before it once it
	// catches up, not everything committed before the last page.
	helloAt time.Time
	granted map[string]bool
	peer    string
}

// NewV2Host returns a host for one connection.
func NewV2Host(w wire.Codec, cfg V2HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *V2Host {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &V2Host{wire: w, cfg: cfg, auth: engine, connCx: ctx, granted: map[string]bool{}}
}

// HostCapabilities are what a v2 host of this build can do.
var HostCapabilities = []string{wire.SyncCapBranches, wire.SyncCapTags, wire.SyncCapStubs, wire.SyncCapSnapshot, wire.SyncCapFilter}

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
		if done && h.cfg.OnCaughtUp != nil {
			h.mu.Lock()
			peer, since := h.peer, h.helloAt
			h.mu.Unlock()
			h.cfg.OnCaughtUp(m.Namespace, peer, since)
		}
		return wire.PackPageMessage{
			H: header(wire.MsgPackPage, m.H.CorrelationID), Namespace: m.Namespace,
			Commits: commits, Stubs: stubs, Done: done,
		}, nil
	case wire.RefUpdateMessage:
		return h.refUpdate(m)
	case wire.ProjectFetchMessage:
		return h.projectFetch(m)
	case wire.ProjectWriteMessage:
		return h.projectWrite(m)
	case wire.SnapshotFetchMessage:
		env, err := h.env(m.Namespace, false)
		if err != nil {
			return nil, err
		}
		at, err := env.DAG.Head()
		if err != nil {
			return nil, err
		}
		if m.AtHex != "" {
			if at, err = codec.HashFromHex(m.AtHex); err != nil {
				return nil, err
			}
		}
		page, err := snapshotPage(env, at, m.After, m.MaxBytes)
		if err != nil {
			return nil, err
		}
		page.H = header(wire.MsgSnapshotPage, m.H.CorrelationID)
		return page, nil
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
	h.principal, h.helloDone, h.granted, h.peer, h.helloAt = principal, true, granted, m.NodeID, time.Now()
	h.mu.Unlock()
	refs, err := h.refs(names)
	if err != nil {
		return nil, err
	}
	return wire.SyncHelloAckMessage{
		H: header(wire.MsgSyncHelloAck, m.H.CorrelationID), Accepted: true,
		NodeID: h.cfg.NodeID, Protocol: wire.SyncProtocolVersion,
		Capabilities: intersect(h.capabilities(), m.Capabilities), Refs: refs,
	}, nil
}

func (h *V2Host) capabilities() []string {
	if h.cfg.WriteBack == nil {
		return HostCapabilities
	}
	return append(slices.Clone(HostCapabilities), wire.SyncCapWriteBack)
}

// projectWrite hands a projection's write to the source. The namespace must be one the session
// may read as a projection; what the write itself may touch is the source's to authorize.
func (h *V2Host) projectWrite(m wire.ProjectWriteMessage) (wire.Message, error) {
	if h.cfg.WriteBack == nil {
		return nil, &UnsupportedFrameError{Type: m.H.MessageType}
	}
	h.mu.Lock()
	principal := h.principal
	h.mu.Unlock()
	if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.StreamSubscribeAction{Namespace: m.Namespace}); err != nil {
		return nil, err
	}
	res, err := h.cfg.WriteBack(principal, m)
	if err != nil {
		return nil, err
	}
	res.H, res.Namespace = header(wire.MsgProjectWriteResult, m.H.CorrelationID), m.Namespace
	return res, nil
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
	ok, principal, peer := h.granted[ns], h.principal, h.peer
	h.mu.Unlock()
	if !ok {
		return IngestEnv{}, &NotGrantedError{Namespace: ns}
	}
	if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.PeerSyncAction{Namespace: ns}); err != nil {
		return IngestEnv{}, err
	}
	env, err := h.cfg.Namespaces.Env(ns, create)
	env.Peer = peer
	return env, err
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

// projectFetch serves one page of a filtered projection. It needs only read access: a
// projection is a filtered read of the namespace, not a peer of it, so it is authorized like a
// stream subscription (read on the namespace) and then document by document, rather than
// needing sync rights. A document the principal may not read is sent as a delete.
func (h *V2Host) projectFetch(m wire.ProjectFetchMessage) (wire.Message, error) {
	h.mu.Lock()
	principal := h.principal
	h.mu.Unlock()
	if err := h.auth.Authorizer().Authorize(context.Background(), principal, auth.StreamSubscribeAction{Namespace: m.Namespace}); err != nil {
		return nil, err
	}
	filter, err := sql.ParseFilter(m.Filter)
	if err != nil {
		return nil, fmt.Errorf("peer sync: projection filter: %w", err)
	}
	env, err := h.cfg.Namespaces.Env(m.Namespace, false)
	if err != nil {
		return nil, err
	}
	canRead := func(id codec.UUID) bool {
		return h.auth.Authorizer().Authorize(context.Background(), principal,
			auth.DocumentReadAction{Namespace: m.Namespace, DocID: id.String()}) == nil
	}
	page, err := projectPage(env, filter, canRead, m.FromHex, m.AtHex, m.After, m.MaxBytes)
	if err != nil {
		return nil, err
	}
	page.H = header(wire.MsgProjectPage, m.H.CorrelationID)
	return page, nil
}
