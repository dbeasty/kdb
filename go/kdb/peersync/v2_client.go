package peersync

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/ws"
	"github.com/limidus/kdb/go/kdb/wire"
)

// SyncMode is which directions a v2 sync runs.
type SyncMode int

const (
	SyncPull SyncMode = 1 << iota
	SyncPush
	SyncBoth = SyncPull | SyncPush
)

// V2ClientConfig configures one v2 sync against a peer.
type V2ClientConfig struct {
	NodeID            string
	PeerURI           string
	ConnectionContext auth.ConnectionContext
	TLS               *core.TransportTlsSettings
	// Namespaces are the namespaces or patterns to sync (see MatchNamespace; "!" excludes).
	Namespaces []string
	Mode       SyncMode
	// PageBytes caps each page in both directions; 0 means DefaultPageBytes.
	PageBytes int
	// Local supplies this node's namespaces.
	Local NamespaceProvider
	// CreateLocal lets a pull create a namespace the peer has and this node does not.
	CreateLocal bool
	// Timeout bounds each request's wait for its reply; 0 means 30s.
	Timeout time.Duration
	// ExtraHaves adds, per namespace, commits this node already stored that its refs do not
	// reach - the ReceivedTips of an interrupted earlier sync. Naming them resumes that sync
	// instead of fetching its pages again.
	ExtraHaves map[string][]codec.Hash
}

// NamespaceSyncResult is what a v2 sync did to one namespace.
type NamespaceSyncResult struct {
	Namespace string
	// Pulled / Pushed count commits new to the receiving side.
	Pulled, Pushed int
	// Local / Remote record each ref's outcome on this node and on the peer, keyed
	// "branch:<name>" or "tag:<name>".
	Local, Remote map[string]wire.RefUpdateOutcome
	// Conflicts are every ref, on either side, whose update was refused as a conflict.
	Conflicts []RefConflict
	// LocalMain / RemoteMain are both sides' main heads when this sync finished, as far as it
	// knows: the peer's is its advertised head, or what it reported after the last push to it.
	LocalMain, RemoteMain string
	// ReceivedTips are the tips of every page this sync stored, so a caller whose sync is
	// interrupted can pass them back as ExtraHaves and resume.
	ReceivedTips []codec.Hash
	// Err is set when this namespace failed; others may still have succeeded.
	Err error
}

// RefConflict is one refused ref update.
type RefConflict struct {
	// Side is "local" or "remote": which node refused the update.
	Side   string
	Ref    string
	Report *kdberr.ConflictReport
}

// V2Result is what a v2 sync did.
type V2Result struct {
	RemoteNodeID string
	// Protocol is 2, or 1 when the peer only speaks v1 and the sync fell back to it.
	Protocol   int
	Namespaces []NamespaceSyncResult
}

// ClientCapabilities are what a v2 client of this build can do.
var ClientCapabilities = HostCapabilities

// SyncV2 runs one v2 sync against cfg.PeerURI: hello, then for every granted namespace a pull
// (fetch what is missing, then decide each ref locally) and a push (send what the peer lacks,
// proposing each ref). Pull runs first so that a push after a merge is a fast-forward on the peer.
//
// A peer that answers SYNC_HELLO with PEER_ERROR UNSUPPORTED only speaks v1; each literal
// namespace in cfg.Namespaces is then synced over v1 (main only) instead.
func SyncV2(w wire.Codec, transport stream.Transport, cfg V2ClientConfig) (V2Result, error) {
	if cfg.Mode == 0 {
		cfg.Mode = SyncBoth
	}
	conn, err := dial(transport, cfg.PeerURI, cfg.TLS, cfg.ConnectionContext)
	if err != nil {
		return V2Result{}, err
	}
	c := &v2Conn{wire: w, conn: conn, correlation: 10000, timeout: cfg.Timeout}
	defer conn.Close()

	reply, err := c.request(wire.SyncHelloMessage{
		H: header(wire.MsgSyncHello, c.next()), NodeID: cfg.NodeID, Protocol: wire.SyncProtocolVersion,
		Capabilities: ClientCapabilities, Namespaces: cfg.Namespaces,
		User: cfg.ConnectionContext.User, Password: cfg.ConnectionContext.Password, Token: cfg.ConnectionContext.Token,
	})
	var remote *RemoteError
	if errors.As(err, &remote) && remote.Code == wire.ErrorCodeUnsupported {
		conn.Close()
		return syncV1Fallback(w, transport, cfg)
	}
	if err != nil {
		return V2Result{}, err
	}
	ack, ok := reply.(wire.SyncHelloAckMessage)
	if !ok {
		return V2Result{}, NewError(fmt.Sprintf("expected SYNC_HELLO_ACK, got %T", reply), nil)
	}
	if !ack.Accepted {
		return V2Result{}, NewError("peer refused sync: "+ack.Reason, nil)
	}
	out := V2Result{RemoteNodeID: ack.NodeID, Protocol: wire.SyncProtocolVersion}
	c.remoteNode = ack.NodeID
	for _, refs := range ack.Refs {
		out.Namespaces = append(out.Namespaces, c.syncNamespace(cfg, refs))
	}
	return out, nil
}

func (c *v2Conn) syncNamespace(cfg V2ClientConfig, remote wire.NamespaceRefs) NamespaceSyncResult {
	res := NamespaceSyncResult{Namespace: remote.Namespace, Local: map[string]wire.RefUpdateOutcome{}, Remote: map[string]wire.RefUpdateOutcome{}}
	env, err := cfg.Local.Env(remote.Namespace, cfg.CreateLocal && cfg.Mode&SyncPull != 0)
	if err != nil {
		res.Err = err
		return res
	}
	env.Peer = c.remoteNode
	if cfg.Mode&SyncPull != 0 {
		if err := c.pull(cfg, env, remote, &res); err != nil {
			res.Err = err
			return res
		}
	}
	res.RemoteMain = remote.Branches[mainBranch]
	if cfg.Mode&SyncPush != 0 {
		if err := c.push(cfg, env, remote, &res); err != nil {
			res.Err = err
		}
	}
	if h, err := env.DAG.Head(); err == nil {
		res.LocalMain = h.Hex()
	}
	return res
}

func (c *v2Conn) pull(cfg V2ClientConfig, env IngestEnv, remote wire.NamespaceRefs, res *NamespaceSyncResult) error {
	targets, err := refTargets(remote)
	if err != nil {
		return err
	}
	var wants []codec.Hash
	for _, h := range targets {
		if !env.DAG.HasCommit(h) {
			wants = append(wants, h)
		}
	}
	if len(wants) > 0 {
		localHead, err := env.DAG.Head()
		if err != nil {
			return err
		}
		haves := append(spreadAncestors(env.DAG, localHead), localRefHeads(env.DAG)...)
		haves = append(haves, cfg.ExtraHaves[remote.Namespace]...)
		for {
			reply, err := c.request(wire.FetchRequestMessage{
				H: header(wire.MsgFetchRequest, c.next()), Namespace: remote.Namespace,
				Wants: wants, Haves: haves, MaxBytes: cfg.PageBytes,
			})
			if err != nil {
				return err
			}
			page, ok := reply.(wire.PackPageMessage)
			if !ok {
				return NewError(fmt.Sprintf("expected PACK_PAGE, got %T", reply), nil)
			}
			n, err := StoreCommits(env, page.Commits, page.Stubs)
			res.Pulled += n
			if err != nil {
				return err
			}
			tips := pageTips(page.Commits)
			res.ReceivedTips = append(res.ReceivedTips, tips...)
			if page.Done || len(page.Commits) == 0 {
				break
			}
			haves = append(haves, tips...)
		}
	}
	// main first: it is the ref with live documents behind it, and the one a push depends on.
	for _, ref := range orderedRefs(remote) {
		h, err := codec.HashFromHex(ref.hex)
		if err != nil {
			return err
		}
		if !env.DAG.HasCommit(h) {
			// Behind a stub or a floor this node cannot cross yet (Phase 4); not fatal to the rest.
			continue
		}
		r, err := ApplyRef(env, ref.kind, ref.name, h)
		if err != nil {
			return err
		}
		res.Local[ref.key()] = r.Outcome
		if r.Outcome == wire.RefConflict {
			res.Conflicts = append(res.Conflicts, RefConflict{Side: "local", Ref: ref.key(), Report: r.Conflict})
		}
	}
	return nil
}

func (c *v2Conn) push(cfg V2ClientConfig, env IngestEnv, remote wire.NamespaceRefs, res *NamespaceSyncResult) error {
	remoteTargets, err := refTargets(remote)
	if err != nil {
		return err
	}
	// Everything the peer's refs name that this node also holds is known to both; after a pull
	// that is all of it.
	var known []codec.Hash
	for _, h := range remoteTargets {
		if env.DAG.HasCommit(h) {
			known = append(known, h)
		}
	}
	for _, ref := range orderedRefs(RefsOf(remote.Namespace, env.DAG)) {
		remoteHex := remote.Branches[ref.name]
		if ref.kind == wire.RefTag {
			remoteHex = remote.Tags[ref.name]
		}
		if remoteHex == ref.hex {
			continue
		}
		local, err := codec.HashFromHex(ref.hex)
		if err != nil {
			return err
		}
		haves := append([]codec.Hash(nil), known...)
		for {
			commits, stubs, done, err := MissingCommitsFrom(env.DAG, []codec.Hash{local}, haves, cfg.PageBytes)
			if err != nil {
				return err
			}
			reply, err := c.request(wire.RefUpdateMessage{
				H: header(wire.MsgRefUpdate, c.next()), Namespace: remote.Namespace,
				Kind: ref.kind, Ref: ref.name, OldHex: remoteHex, NewHex: ref.hex,
				Commits: commits, Stubs: stubs, More: !done,
			})
			if err != nil {
				return err
			}
			ack, ok := reply.(wire.RefUpdateAckMessage)
			if !ok {
				return NewError(fmt.Sprintf("expected REF_UPDATE_ACK, got %T", reply), nil)
			}
			res.Pushed += ack.Stored
			if !done {
				haves = append(haves, pageTips(commits)...)
				continue
			}
			res.Remote[ref.key()] = ack.Outcome
			if ref.kind == wire.RefBranch && ref.name == mainBranch && ack.HeadHex != "" {
				res.RemoteMain = ack.HeadHex
			}
			if ack.Outcome == wire.RefConflict {
				var report kdberr.ConflictReport
				if len(ack.ConflictBytes) > 0 {
					_ = json.Unmarshal(ack.ConflictBytes, &report)
				}
				res.Conflicts = append(res.Conflicts, RefConflict{Side: "remote", Ref: ref.key(), Report: &report})
			}
			known = append(known, local)
			break
		}
	}
	return nil
}

type namedRef struct {
	kind wire.RefKind
	name string
	hex  string
}

func (r namedRef) key() string { return string(r.kind) + ":" + r.name }

// orderedRefs lists a namespace's refs: main, then the other branches, then tags, each sorted.
func orderedRefs(refs wire.NamespaceRefs) []namedRef {
	var out []namedRef
	if hex, ok := refs.Branches[mainBranch]; ok {
		out = append(out, namedRef{wire.RefBranch, mainBranch, hex})
	}
	var branches, tags []string
	for name := range refs.Branches {
		if name != mainBranch {
			branches = append(branches, name)
		}
	}
	for name := range refs.Tags {
		tags = append(tags, name)
	}
	sort.Strings(branches)
	sort.Strings(tags)
	for _, name := range branches {
		out = append(out, namedRef{wire.RefBranch, name, refs.Branches[name]})
	}
	for _, name := range tags {
		out = append(out, namedRef{wire.RefTag, name, refs.Tags[name]})
	}
	return out
}

// syncV1Fallback syncs each literal namespace over protocol v1: main only, one connection each.
func syncV1Fallback(w wire.Codec, transport stream.Transport, cfg V2ClientConfig) (V2Result, error) {
	out := V2Result{Protocol: 1}
	for _, ns := range cfg.Namespaces {
		if !isLiteralNamespace(ns) {
			continue
		}
		res := NamespaceSyncResult{Namespace: ns, Local: map[string]wire.RefUpdateOutcome{}, Remote: map[string]wire.RefUpdateOutcome{}}
		env, err := cfg.Local.Env(ns, false)
		if err != nil {
			res.Err = err
			out.Namespaces = append(out.Namespaces, res)
			continue
		}
		client := NewClient(w, transport, env.DAG, env.Storage)
		session, err := client.Connect(ClientConfig{
			NamespaceID: ns, NodeID: cfg.NodeID, PeerURI: cfg.PeerURI, ConnectionContext: cfg.ConnectionContext,
			TLS: cfg.TLS, Node: env.Node, ApplyToStorage: env.ApplyToStorage, PersistAsync: env.PersistAsync,
			Persist: env.Persist, ConflictPolicy: env.Resolution.Policy, ConflictResolver: env.Resolution.Resolver,
		})
		if err != nil {
			res.Err = err
			out.Namespaces = append(out.Namespaces, res)
			continue
		}
		var r Result
		if cfg.Mode&SyncPush != 0 {
			r, err = session.SyncBidirectional()
		} else {
			r, err = session.PullMissing()
		}
		client.Disconnect()
		res.Pulled, res.Pushed, res.Err = r.AppliedCommits, r.PushedCommits, err
		if r.Conflict != nil {
			res.Conflicts = append(res.Conflicts, RefConflict{Side: "local", Ref: "branch:main", Report: r.Conflict})
		}
		out.Namespaces = append(out.Namespaces, res)
	}
	return out, nil
}

// v2Conn is one v2 connection's request/response plumbing.
type v2Conn struct {
	remoteNode  string
	wire        wire.Codec
	conn        stream.ConnectionHandle
	mu          sync.Mutex
	correlation int
	timeout     time.Duration
}

func (c *v2Conn) next() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.correlation++
	return c.correlation
}

func (c *v2Conn) request(m wire.Message) (wire.Message, error) {
	frame, err := c.wire.Encode(m)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(frame); err != nil {
		return nil, err
	}
	timeout := c.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	cid := m.Header().CorrelationID
	for {
		select {
		case frame, ok := <-c.conn.Incoming():
			if !ok {
				return nil, NewError("peer closed the connection", nil)
			}
			decoded, err := c.wire.Decode(frame)
			if err != nil {
				return nil, err
			}
			if decoded.Header().CorrelationID != cid {
				continue
			}
			if pe, ok := decoded.(wire.PeerErrorMessage); ok {
				return nil, &RemoteError{Code: pe.Code, Message: pe.Message}
			}
			return decoded, nil
		case <-timer.C:
			return nil, NewError(fmt.Sprintf("no reply to %s within %s", m.Header().MessageType, timeout), nil)
		}
	}
}

func dial(transport stream.Transport, uri string, tls *core.TransportTlsSettings, cc auth.ConnectionContext) (stream.ConnectionHandle, error) {
	if wsTransport, ok := transport.(ws.Transport); ok {
		opts := core.DefaultConnectOptions()
		opts.TLS = tls
		if cc.Headers != nil {
			opts.ConnectHeaders = cc.Headers
		}
		return wsTransport.ConnectWithOptions(uri, opts)
	}
	return transport.Connect(uri)
}
