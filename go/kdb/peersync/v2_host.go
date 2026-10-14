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
	"github.com/limidus/kdb/go/kdb/document"
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
	// AuthorizeDocuments asks the auth engine about every document a pushed commit writes or
	// deletes (DocumentWriteAction / DocumentDeleteAction), on top of the namespace's push grant.
	// For engines whose rules go below the namespace. Merge commits carry every document their
	// sides disagreed on, so a peer needs write rights on those too.
	AuthorizeDocuments bool
	// MetaView answers META_VIEW: the definitions a session may see, given which namespaces it
	// was granted. nil answers with none.
	MetaView func(canSee func(ns string) bool) ([]wire.MetaDefinition, error)
	// HomeRequest decides a HOME_REQUEST from peer (its node id at hello), authenticated as
	// principal and granted push on the namespace. nil refuses every request.
	HomeRequest func(principal auth.Principal, peer string, m wire.HomeRequestMessage) (wire.HomeRequestResultMessage, error)
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
	// MaxGraftPushBytes bounds the document bytes one session may push for a graft (GRAFT_PUSH).
	// The pages are staged on disk as they arrive, but verifying and storing the graft then holds
	// its whole state in memory (storage.ForeignTreeStore takes every body at once), so this is
	// what bounds that. 0 means DefaultMaxGraftPushBytes; negative means unlimited.
	MaxGraftPushBytes int64
	// WriteBack, when set, applies a filtered projection's local write to its source namespace
	// as principal, and says what became of it. An error means the source could not decide now
	// (unavailable, not the home): it goes back as PEER_ERROR and the replica tries again later.
	// Unset, the host does not offer write-back.
	WriteBack func(principal auth.Principal, m wire.ProjectWriteMessage) (wire.ProjectWriteResultMessage, error)
	// AdoptEveryBytes is V2ClientConfig.AdoptEveryBytes for pushes into this node: once that much
	// (estimated commit bytes) of a push of main has been stored and not adopted, a page that
	// fast-forwards main is adopted before the push's last page arrives. 0 means
	// DefaultAdoptEveryBytes; negative adopts only on the last page.
	AdoptEveryBytes int
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
	// zstdPack: the session negotiated SyncCapZstdPack, so pages go to the peer compressed.
	zstdPack bool
	granted  map[string]bool
	// access is each granted namespace's direction limit: "" both, wire.AccessPull or AccessPush.
	access map[string]string
	peer   string
	// grafts stages GRAFT_PUSH pages per namespace, on disk, until the last one arrives.
	grafts map[string]*graftStage
	// boundaryGaps are, per namespace, the parents behind this node's snapshot root that a push
	// on this session was refused for (parent -> the pushed commit naming it). A GRAFT_PUSH of
	// one of them is a boundary extension, not a graft (boundary.go).
	boundaryGaps map[string]map[codec.Hash]codec.Hash

	// fetchMu guards fetch, the session's FETCH transfer in progress: the namespace and the
	// cursor serving its pages (see missingCursor), kept so each next page costs the page, not
	// another walk of the namespace's history.
	fetchMu sync.Mutex
	fetch   *hostFetch

	// pushWaiting is, per namespace, the estimated bytes of a push of main this session has
	// stored and not yet adopted (see refUpdate). Pages of one session arrive in order, served
	// serially; pushMu guards it anyway.
	pushMu      sync.Mutex
	pushWaiting map[string]int
}

type hostFetch struct {
	namespace string
	cursor    *missingCursor
}

// fetchPage serves a FETCH_REQUEST: the next page of the session's transfer when the request
// continues it - same namespace and wants, and the peer names the last page's tips as held -
// and otherwise a transfer worked out afresh, which is what every page used to be.
func (h *V2Host) fetchPage(env IngestEnv, m wire.FetchRequestMessage) ([]document.Commit, [][]byte, []document.CommitStub, bool, error) {
	h.fetchMu.Lock()
	defer h.fetchMu.Unlock()
	var cur *missingCursor
	if f := h.fetch; f != nil && f.namespace == m.Namespace && f.cursor.continues(m.Wants, m.Haves) {
		cur = f.cursor
		cur.advance()
	}
	for attempt := 0; ; attempt++ {
		if cur == nil {
			cur = newMissingCursor(env.DAG, m.Wants, m.Haves)
		}
		commits, payloads, stubs, done, err := cur.pagePayloads(env.DAG, m.MaxBytes)
		if errors.Is(err, errCursorStale) && attempt == 0 {
			cur = nil
			continue
		}
		if err != nil || done {
			h.fetch = nil
			return commits, payloads, stubs, done, err
		}
		h.fetch = &hostFetch{namespace: m.Namespace, cursor: cur}
		return commits, payloads, stubs, done, nil
	}
}

// boundaryGapsMax bounds what one session remembers of refused parents; a page that misses more
// than this is refused again when it is sent again, naming the rest.
const boundaryGapsMax = 1024

func (h *V2Host) noteBoundaryGaps(ns string, gaps []boundaryGap) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.boundaryGaps == nil {
		h.boundaryGaps = map[string]map[codec.Hash]codec.Hash{}
	}
	m := h.boundaryGaps[ns]
	if m == nil {
		m = map[codec.Hash]codec.Hash{}
		h.boundaryGaps[ns] = m
	}
	for _, g := range gaps {
		if len(m) >= boundaryGapsMax {
			return
		}
		m[g.parent] = g.child
	}
}

// takeBoundaryGap reports whether parent is one this session refused a push for, forgetting it.
func (h *V2Host) takeBoundaryGap(ns string, parent codec.Hash) (boundaryGap, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	child, ok := h.boundaryGaps[ns][parent]
	if ok {
		delete(h.boundaryGaps[ns], parent)
	}
	return boundaryGap{child: child, parent: parent}, ok
}

// NewV2Host returns a host for one connection.
func NewV2Host(w wire.Codec, cfg V2HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *V2Host {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &V2Host{wire: w, cfg: cfg, auth: engine, connCx: ctx, granted: map[string]bool{}}
}

// HostCapabilities are what a v2 host of this build can do.
var HostCapabilities = []string{wire.SyncCapBranches, wire.SyncCapTags, wire.SyncCapStubs, wire.SyncCapSnapshot, wire.SyncCapFilter, wire.SyncCapRepair, wire.SyncCapDocFetch, wire.SyncCapGraft, wire.SyncCapMetaView, wire.SyncCapHome, wire.SyncCapZstdPack, wire.SyncCapSquash}

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
		env, err := h.env(m.Namespace, false, dirPull)
		if err != nil {
			return nil, err
		}
		commits, payloads, stubs, done, err := h.fetchPage(env, m)
		if err != nil {
			return nil, err
		}
		h.mu.Lock()
		peer, since, zstd := h.peer, h.helloAt, h.zstdPack
		h.mu.Unlock()
		if done && h.cfg.OnCaughtUp != nil {
			h.cfg.OnCaughtUp(m.Namespace, peer, since)
		}
		return wire.PackPageMessage{
			H: header(wire.MsgPackPage, m.H.CorrelationID), Namespace: m.Namespace,
			Commits: commits, Payloads: payloads, Stubs: stubs, Done: done, Zstd: zstd,
		}, nil
	case wire.RefUpdateMessage:
		return h.refUpdate(m)
	case wire.ProjectFetchMessage:
		return h.projectFetch(m)
	case wire.ProjectWriteMessage:
		return h.projectWrite(m)
	case wire.SnapshotFetchMessage:
		env, err := h.env(m.Namespace, false, dirPull)
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
	case wire.DocFetchMessage:
		return h.docFetch(m)
	case wire.GraftPushMessage:
		return h.graftPush(m)
	case wire.HomeRequestMessage:
		reply := wire.HomeRequestResultMessage{H: header(wire.MsgHomeRequestResult, m.H.CorrelationID), Namespace: m.Namespace}
		// Only a node that may write the namespace may ask to become its home, and only for itself.
		if _, err := h.env(m.Namespace, false, dirPush); err != nil {
			return nil, err
		}
		h.mu.Lock()
		principal, peer := h.principal, h.peer
		h.mu.Unlock()
		if m.Node != peer {
			reply.Reason = "a node may ask to become a namespace's home only for itself (asked for " + m.Node + ", connected as " + peer + ")"
			return reply, nil
		}
		if h.cfg.HomeRequest == nil {
			reply.Reason = "this node does not hand namespaces over on request"
			return reply, nil
		}
		res, err := h.cfg.HomeRequest(principal, peer, m)
		if err != nil {
			return nil, err
		}
		res.H = reply.H
		res.Namespace = m.Namespace
		return res, nil
	case wire.MetaViewMessage:
		reply := wire.MetaViewResultMessage{H: header(wire.MsgMetaViewResult, m.H.CorrelationID)}
		if h.cfg.MetaView == nil {
			return reply, nil
		}
		h.mu.Lock()
		granted := make(map[string]bool, len(h.granted))
		for ns, ok := range h.granted {
			granted[ns] = ok
		}
		h.mu.Unlock()
		defs, err := h.cfg.MetaView(func(ns string) bool { return granted[ns] })
		if err != nil {
			return nil, err
		}
		reply.Definitions = defs
		return reply, nil
	case wire.TreeNodesMessage:
		env, err := h.env(m.Namespace, false, dirPull)
		if err != nil {
			return nil, err
		}
		res, err := treeNodesPage(env, m)
		if err != nil {
			return nil, err
		}
		res.H = header(wire.MsgTreeNodesResult, m.H.CorrelationID)
		return res, nil
	case wire.SquashHeadMessage:
		env, err := h.env(m.Namespace, false, dirPull)
		if err != nil {
			return nil, err
		}
		res, err := squashHeadPage(env, m)
		if err != nil {
			return nil, err
		}
		res.H = header(wire.MsgSquashHeadResult, m.H.CorrelationID)
		return res, nil
	case wire.ObjectFetchMessage:
		env, err := h.env(m.Namespace, false, dirPull)
		if err != nil {
			return nil, err
		}
		res, err := objectFetchPage(env, m)
		if err != nil {
			return nil, err
		}
		res.H = header(wire.MsgObjectFetchResult, m.H.CorrelationID)
		return res, nil
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
	creds := auth.Credentials{User: m.User, Password: m.Password, Token: m.Token}
	if creds.User == nil && creds.Password == nil && creds.Token == nil {
		// None in the hello: the transport's (an HTTP upgrade's Authorization header).
		creds = h.connCx.ToCredentials()
	}
	principal, err := h.auth.Authenticator().Authenticate(context.Background(), creds)
	if err != nil {
		return reject(err.Error())
	}
	// Each matching namespace is granted individually: a pattern is a request, and a principal
	// allowed to sync some of what it names gets exactly those, not a refusal of the whole.
	matched := SelectNamespaces(m.Namespaces, h.cfg.Namespaces.List())
	granted := map[string]bool{}
	access := map[string]string{}
	grant := func(ns string) bool {
		pull, push := auth.PeerDirections(context.Background(), h.auth.Authorizer(), principal, ns)
		switch {
		case pull && push:
			access[ns] = ""
		case pull:
			access[ns] = wire.AccessPull
		case push:
			access[ns] = wire.AccessPush
		default:
			return false
		}
		granted[ns] = true
		return true
	}
	var names []string
	for _, ns := range matched {
		if grant(ns) {
			names = append(names, ns)
		}
	}
	// A literal (wildcard-free) name the node does not hold yet can still be granted for a push
	// that creates it, when the operator allows that. It has no refs of its own to look up - see
	// the "creating" namespaces below.
	var creating []string
	if h.cfg.CreateOnPush {
		for _, p := range m.Namespaces {
			if isLiteralNamespace(p) && !granted[p] {
				if !grant(p) {
					continue
				}
				if access[p] == wire.AccessPull {
					delete(granted, p) // nothing to pull from a namespace this node does not hold
					continue
				}
				creating = append(creating, p)
			}
		}
	}
	h.mu.Lock()
	h.principal, h.helloDone, h.granted, h.access, h.peer, h.helloAt = principal, true, granted, access, m.NodeID, time.Now()
	h.mu.Unlock()
	refs, err := h.refs(names)
	if err != nil {
		return nil, err
	}
	// A "creating" namespace doesn't exist here yet, so it has no DAG to read refs from - advertise
	// it as holding nothing. That makes every one of the client's refs look new, so it pushes them
	// all, and the REF_UPDATE handler's own h.env(ns, h.cfg.CreateOnPush, dirPush) creates the
	// namespace when the push actually arrives (not here, so a hello that never pushes anything
	// leaves nothing behind).
	for _, ns := range creating {
		refs = append(refs, wire.NamespaceRefs{
			Namespace: ns, Branches: map[string]string{}, Tags: map[string]string{}, Access: access[ns],
		})
	}
	caps := intersect(h.capabilities(), m.Capabilities)
	h.mu.Lock()
	h.zstdPack = slices.Contains(caps, wire.SyncCapZstdPack)
	h.mu.Unlock()
	return wire.SyncHelloAckMessage{
		H: header(wire.MsgSyncHelloAck, m.H.CorrelationID), Accepted: true,
		NodeID: h.cfg.NodeID, Protocol: wire.SyncProtocolVersion,
		Capabilities: caps, Refs: refs,
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
		env, err := h.env(ns, false, dirEither)
		if err != nil {
			return nil, err
		}
		refs := RefsOf(ns, env.DAG)
		refs.ResolutionHash = env.Resolution.AdvertisedHash()
		h.mu.Lock()
		refs.Access = h.access[ns]
		h.mu.Unlock()
		out = append(out, refs)
	}
	return out, nil
}

// direction is what a frame does with a namespace, for authorization.
type direction int

const (
	dirPull   direction = iota // reads history: fetch, snapshot, tree nodes, bodies
	dirPush                    // writes history: ref updates, pushed grafts
	dirEither                  // reads only refs, which a peer syncing either way needs
)

// env returns ns's ingest environment, re-authorizing the frame's direction on every use so a
// grant revoked mid-session takes effect on the next frame.
func (h *V2Host) env(ns string, create bool, dir direction) (IngestEnv, error) {
	h.mu.Lock()
	ok, principal, peer := h.granted[ns], h.principal, h.peer
	h.mu.Unlock()
	if !ok {
		return IngestEnv{}, &NotGrantedError{Namespace: ns}
	}
	var err error
	switch dir {
	case dirPull:
		err = auth.AuthorizePeer(context.Background(), h.auth.Authorizer(), principal, ns, false)
	case dirPush:
		err = auth.AuthorizePeer(context.Background(), h.auth.Authorizer(), principal, ns, true)
	default:
		if pull, push := auth.PeerDirections(context.Background(), h.auth.Authorizer(), principal, ns); !pull && !push {
			err = auth.AuthorizePeer(context.Background(), h.auth.Authorizer(), principal, ns, false)
		}
	}
	if err != nil {
		return IngestEnv{}, err
	}
	env, err := h.cfg.Namespaces.Env(ns, create)
	env.Peer = peer
	return env, err
}

// authorizePushedDocuments asks the engine about every document the pushed commits write or
// delete (V2HostConfig.AuthorizeDocuments), refusing the page at the first denial.
func (h *V2Host) authorizePushedDocuments(ns string, commits []document.Commit) error {
	h.mu.Lock()
	principal := h.principal
	h.mu.Unlock()
	authz := h.auth.Authorizer()
	for _, c := range commits {
		for _, op := range c.Operations {
			var action auth.Action
			switch o := op.(type) {
			case document.WriteOp:
				action = auth.DocumentWriteAction{Namespace: ns, DocID: o.DocID.String()}
			case document.DeleteOp:
				action = auth.DocumentDeleteAction{Namespace: ns, DocID: o.DocID.String()}
			default:
				continue
			}
			if err := authz.Authorize(context.Background(), principal, action); err != nil {
				return fmt.Errorf("peer sync: commit %s writes a document this peer may not: %w", c.Hash.Hex(), err)
			}
		}
	}
	return nil
}

// DefaultMaxGraftPushBytes is V2HostConfig.MaxGraftPushBytes when unset. It was a fixed 1 GiB of
// pages held in memory per session; the pages are now staged on disk, and the cap bounds what the
// final graft holds while it verifies and stores the state.
const DefaultMaxGraftPushBytes = 256 << 20

func (h *V2Host) maxGraftPushBytes() int64 {
	switch m := h.cfg.MaxGraftPushBytes; {
	case m < 0:
		return 0
	case m == 0:
		return DefaultMaxGraftPushBytes
	default:
		return m
	}
}

// graftStage is one namespace's pushed graft in flight: its pages, staged on disk.
type graftStage struct {
	spool  *pageSpool
	commit document.Commit
	served int // documents handed back to the graft so far
}

// graftPush stages one GRAFT_PUSH page and, on the last, grafts the pushed root (see Graft).
func (h *V2Host) graftPush(m wire.GraftPushMessage) (wire.Message, error) {
	ns := m.Page.Namespace
	env, err := h.env(ns, false, dirPush)
	if err != nil {
		return nil, err
	}
	reply := wire.GraftPushResultMessage{H: header(wire.MsgGraftPushResult, m.H.CorrelationID), Namespace: ns}
	stage, err := h.stageGraftPage(env, m.Page)
	if err != nil || stage == nil {
		return reply, err
	}
	defer stage.spool.close()
	next, err := stage.spool.reader(DefaultPageBytes, -1)
	if err != nil {
		return nil, err
	}
	// The staged pages are served back in batches of DefaultPageBytes rather than as they were
	// pushed; a batch is what a page is to Graft (docs in id order, the same commit on each), and
	// the last one says Done.
	fetch := func(string) (wire.SnapshotPageMessage, error) {
		docs, err := next()
		if err != nil {
			return wire.SnapshotPageMessage{}, err
		}
		if len(docs) == 0 && stage.served > 0 {
			return wire.SnapshotPageMessage{}, errors.New("peer sync: pushed graft ended early")
		}
		stage.served += len(docs)
		page := wire.SnapshotPageMessage{Namespace: ns, Commit: stage.commit, Docs: docs, Total: stage.spool.docs,
			Done: stage.served >= stage.spool.docs}
		if !page.Done {
			page.Next = docs[len(docs)-1].DocID
		}
		return page, nil
	}
	// A parent this session refused a push for, behind this node's snapshot root: history this
	// node's root descends from, not an unrelated one, so no allowUnrelated (boundary.go).
	if gap, ok := h.takeBoundaryGap(ns, stage.commit.Hash); ok {
		if err := extendBoundary(env, gap, fetch); err != nil {
			return nil, err
		}
		reply.Grafted = true
		return reply, nil
	}
	if _, err := Graft(env, stage.commit.Hash, fetch); err != nil {
		return nil, err
	}
	reply.Grafted = true
	return reply, nil
}

// stageGraftPage appends one pushed page to its namespace's stage, creating the stage for the
// first. Returns the stage, taken out of the session, once the last page is in; nil before. A
// push over the session's cap, or whose pages disagree about their commit, is dropped and refused.
func (h *V2Host) stageGraftPage(env IngestEnv, page wire.SnapshotPageMessage) (*graftStage, error) {
	ns := page.Namespace
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.grafts == nil {
		h.grafts = map[string]*graftStage{}
	}
	stage := h.grafts[ns]
	if stage == nil {
		spool, err := newPageSpool(env.StagingDir)
		if err != nil {
			return nil, err
		}
		stage = &graftStage{spool: spool, commit: page.Commit}
		h.grafts[ns] = stage
	}
	fail := func(err error) (*graftStage, error) {
		delete(h.grafts, ns)
		stage.spool.close()
		return nil, err
	}
	if page.Commit.Hash != stage.commit.Hash {
		return fail(fmt.Errorf("peer sync: pushed graft pages disagree about their commit (%s, then %s)", stage.commit.Hash.Hex(), page.Commit.Hash.Hex()))
	}
	for _, d := range page.Docs {
		if err := stage.spool.add(d.DocID, d.Body); err != nil {
			return fail(err)
		}
	}
	if max := h.maxGraftPushBytes(); max > 0 && stage.spool.bodyLen > max {
		return fail(fmt.Errorf("peer sync: a pushed graft of %s exceeds %d bytes", ns, max))
	}
	if !page.Done {
		return nil, nil
	}
	delete(h.grafts, ns)
	return stage, nil
}

// Close releases what the session staged and never finished: pushed grafts whose last page did
// not arrive. The connection's owner calls it when the connection ends.
func (h *V2Host) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ns, stage := range h.grafts {
		stage.spool.close()
		delete(h.grafts, ns)
	}
}

func (h *V2Host) refUpdate(m wire.RefUpdateMessage) (wire.Message, error) {
	env, err := h.env(m.Namespace, h.cfg.CreateOnPush, dirPush)
	if err != nil {
		return nil, err
	}
	if h.cfg.AuthorizeDocuments {
		if err := h.authorizePushedDocuments(m.Namespace, m.Commits); err != nil {
			return nil, err
		}
	}
	stored, err := StoreCommits(env, m.Commits, m.Stubs)
	if err != nil {
		err = fmt.Errorf("%w (%d of %d commits in this page were stored before it)", err, stored, len(m.Commits))
		// A parent behind this node's snapshot root, which it cannot fetch from the node that
		// dialled it: say so, and remember which, so the pusher can send their state (boundary.go).
		if blocked, gaps := pushBlockedByBoundary(env, m.Commits, err); blocked != nil {
			h.noteBoundaryGaps(m.Namespace, gaps)
			return nil, blocked
		}
		return nil, err
	}
	ack := wire.RefUpdateAckMessage{
		H: header(wire.MsgRefUpdateAck, m.H.CorrelationID), Namespace: m.Namespace,
		Kind: m.Kind, Ref: m.Ref, Stored: stored, Outcome: wire.RefStored,
	}
	if m.More {
		if err := h.adoptPushEarly(env, m); err != nil {
			return nil, err
		}
		return ack, nil
	}
	h.pushMu.Lock()
	delete(h.pushWaiting, m.Namespace)
	h.pushMu.Unlock()
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

// adoptPushEarly is the push side of a pull's early adoption (fetchRef): a node receiving a long
// push of main stored every page and adopted only on the last, so the whole range - every
// received commit's operations, pinned until logged - stayed in memory for the length of the
// transfer. Once AdoptEveryBytes is waiting, a page that ends in one tip main fast-forwards to is
// adopted now (AdoptFastForward: never a merge, a no-op if main has diverged), and the decision
// on the last page proceeds from there exactly as it would have from the start.
func (h *V2Host) adoptPushEarly(env IngestEnv, m wire.RefUpdateMessage) error {
	if (m.Kind != wire.RefBranch && m.Kind != "") || m.Ref != mainBranch {
		return nil
	}
	every := h.cfg.AdoptEveryBytes
	if every == 0 {
		every = DefaultAdoptEveryBytes
	}
	if every < 0 {
		return nil
	}
	h.pushMu.Lock()
	if h.pushWaiting == nil {
		h.pushWaiting = map[string]int{}
	}
	for _, c := range m.Commits {
		h.pushWaiting[m.Namespace] += commitSize(c)
	}
	waiting := h.pushWaiting[m.Namespace]
	h.pushMu.Unlock()
	if waiting < every {
		return nil
	}
	adopted, err := adoptEarly(env, pageTips(m.Commits))
	if err != nil {
		return err
	}
	if adopted {
		h.pushMu.Lock()
		h.pushWaiting[m.Namespace] = 0
		h.pushMu.Unlock()
	}
	return nil
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
