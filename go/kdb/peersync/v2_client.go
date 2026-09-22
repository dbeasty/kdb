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
	"github.com/limidus/kdb/go/kdb/transaction"
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
	// MetaView, when set, makes this a scoped session for definitions: right after hello - before
	// any namespace syncs, so resolution chains are in place for its merges - the client asks
	// the host for the definitions it may see (META_VIEW) and hands them here. The metadata
	// namespace itself should then not be among Namespaces.
	MetaView          func([]wire.MetaDefinition) error
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
	// PreferSnapshot bootstraps an empty namespace from a snapshot of the peer's main even when
	// the peer could send its whole history - faster for a large history, at the cost of not
	// holding it. An empty namespace whose peer cannot send its whole history (it holds a
	// shallow root or a retention floor) is bootstrapped from a snapshot regardless.
	PreferSnapshot bool
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
	// Received counts every commit the peer sent in a pull, including ones this node already
	// held - Received minus Pulled is what negotiation over-sent (overshoot).
	Received int
	// Local / Remote record each ref's outcome on this node and on the peer, keyed
	// "branch:<name>" or "tag:<name>".
	Local, Remote map[string]wire.RefUpdateOutcome
	// Conflicts are every ref, on either side, whose update was refused as a conflict.
	Conflicts []RefConflict
	// Snapshot is the commit this sync bootstrapped the namespace from, when it did.
	Snapshot string
	// Grafted lists the peer's shallow roots this sync grafted in (see Graft).
	Grafted []string
	// Access is the peer's grant for this namespace: "" both directions, wire.AccessPull or
	// wire.AccessPush. The direction not granted was skipped.
	Access string
	// LocalMain / RemoteMain are both sides' main heads when this sync finished, as far as it
	// knows: the peer's is its advertised head, or what it reported after the last push to it.
	LocalMain, RemoteMain string
	// RefErrors records refs other than main this sync could not take, by ref key; the rest of
	// the namespace synced regardless.
	RefErrors map[string]string
	// ReceivedTips are the tips of every page this sync stored, so a caller whose sync is
	// interrupted can pass them back as ExtraHaves and resume.
	ReceivedTips []codec.Hash
	// ResolutionMismatch is set when the two nodes' resolution chains for this namespace differ.
	// Neither then merges: a divergence pulled here is queued as a conflict, and main is pushed
	// only where it fast-forwards the peer. It clears once the chain definition has replicated.
	ResolutionMismatch bool
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
	// MetaDefinitions counts the definitions a scoped session received (V2ClientConfig.MetaView).
	MetaDefinitions int
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
	c.caps = ack.Capabilities
	if cfg.MetaView != nil && containsString(c.caps, wire.SyncCapMetaView) {
		reply, err := c.request(wire.MetaViewMessage{H: header(wire.MsgMetaView, c.next())})
		if err != nil {
			return out, err
		}
		view, ok := reply.(wire.MetaViewResultMessage)
		if !ok {
			return out, NewError(fmt.Sprintf("expected META_VIEW_RESULT, got %T", reply), nil)
		}
		if err := cfg.MetaView(view.Definitions); err != nil {
			return out, fmt.Errorf("peer sync: adopting the peer's definitions: %w", err)
		}
		out.MetaDefinitions = len(view.Definitions)
	}
	for _, refs := range ack.Refs {
		out.Namespaces = append(out.Namespaces, c.syncNamespace(cfg, refs))
	}
	return out, nil
}

func (c *v2Conn) syncNamespace(cfg V2ClientConfig, remote wire.NamespaceRefs) NamespaceSyncResult {
	res := NamespaceSyncResult{Namespace: remote.Namespace, Local: map[string]wire.RefUpdateOutcome{}, Remote: map[string]wire.RefUpdateOutcome{}, RefErrors: map[string]string{}}
	env, err := cfg.Local.Env(remote.Namespace, cfg.CreateLocal && cfg.Mode&SyncPull != 0)
	if err != nil {
		res.Err = err
		return res
	}
	env.Peer = c.remoteNode
	if env.Resolution.AdvertisedHash() != remote.ResolutionHash {
		// The two nodes would settle a conflict differently and so build different merges. Hold
		// off merging on either side until they agree - report instead of resolving here, and
		// do not propose a head the peer would have to merge (see push).
		env.Resolution = ResolutionOptions{Policy: transaction.ConflictPolicyStrict}
		res.ResolutionMismatch = true
	}
	res.Access = remote.Access
	// A direction the peer does not grant is skipped, not attempted and refused.
	if cfg.Mode&SyncPull != 0 && remote.Access != wire.AccessPush {
		if err := c.pull(cfg, env, remote, &res); err != nil {
			res.Err = err
			return res
		}
	}
	res.RemoteMain = remote.Branches[mainBranch]
	if cfg.Mode&SyncPush != 0 && remote.Access != wire.AccessPull {
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
	if needsSnapshot(env, remote, cfg.PreferSnapshot) {
		at := remote.Branches[mainBranch]
		installed, err := InstallSnapshot(env, c.snapshotFetcher(cfg, remote.Namespace, at))
		if err != nil {
			return err
		}
		res.Snapshot = installed.Hash.Hex()
	}
	localHead, err := env.DAG.Head()
	if err != nil {
		return err
	}
	haves := append(spreadAncestors(env.DAG, localHead), localRefHeads(env.DAG)...)
	// Only commits this node holds: a have tells the peer it need not send anything below it. The
	// peer's main as of the last sync (the replicator's extra have) is not held when that sync
	// ended with the peer merging this node's push - offering it would make the peer send
	// nothing, and the ref could not be adopted.
	for _, h := range cfg.ExtraHaves[remote.Namespace] {
		if env.DAG.HasCommit(h) {
			haves = append(haves, h)
		}
	}
	// One ref at a time, main first. A ref this node cannot take - a side branch forked below
	// the snapshot this node was bootstrapped from, whose parents no peer can send it - is
	// recorded and skipped; it must not stop main, or every later sync of the namespace fails
	// the same way.
	for _, ref := range orderedRefs(remote) {
		h, err := codec.HashFromHex(ref.hex)
		if err != nil {
			return err
		}
		if !env.DAG.HasCommit(h) {
			if haves, err = c.fetchRef(cfg, env, remote.Namespace, remote.Shallow, h, haves, res); err != nil {
				if ref.kind == wire.RefBranch && ref.name == mainBranch {
					return err
				}
				res.RefErrors[ref.key()] = err.Error()
				continue
			}
		}
		r, err := ApplyRef(env, ref.kind, ref.name, h)
		if err != nil {
			if ref.kind == wire.RefBranch && ref.name == mainBranch {
				return err
			}
			res.RefErrors[ref.key()] = err.Error()
			continue
		}
		res.Local[ref.key()] = r.Outcome
		if r.Outcome == wire.RefConflict {
			res.Conflicts = append(res.Conflicts, RefConflict{Side: "local", Ref: ref.key(), Report: r.Conflict})
		}
	}
	return nil
}

// graftRootsFor makes sure the peer can store what a push of local would send. That history may
// reach down to one of this node's shallow roots; the peer can store the root only if it holds
// the root already or its parents. When it holds neither, and both nodes merge unrelated
// histories, the root's state goes first by GRAFT_PUSH. Otherwise the ref is not pushed, and the
// returned reason says why: the peer takes it when it pulls, if it ever can.
func (c *v2Conn) graftRootsFor(cfg V2ClientConfig, env IngestEnv, remote wire.NamespaceRefs, local codec.Hash, known []codec.Hash) (string, error) {
	roots := env.DAG.ShallowRoots()
	if len(roots) == 0 {
		return "", nil
	}
	have := env.DAG.AncestorSetOf(known)
	for _, root := range roots {
		if _, ok := have[root]; ok {
			continue
		}
		if root != local && !env.DAG.IsAncestor(root, local) {
			continue
		}
		rc, err := env.DAG.GetCommitOrThrow(root)
		if err != nil {
			return "", err
		}
		// Does the peer hold the root, or what is below it? Any commit back means yes.
		reply, err := c.request(wire.FetchRequestMessage{
			H: header(wire.MsgFetchRequest, c.next()), Namespace: remote.Namespace,
			Wants: append([]codec.Hash{root}, rc.ParentHashes...), MaxBytes: 1,
		})
		if err != nil {
			return "", err
		}
		if page, ok := reply.(wire.PackPageMessage); ok && len(page.Commits) > 0 {
			continue
		}
		if !env.Resolution.Chain.AllowsUnrelated() || !containsString(c.caps, wire.SyncCapGraft) {
			return fmt.Sprintf("not pushed: the peer holds none of the history below this node's root %s, and cannot graft it", root.Hex()), nil
		}
		for after := ""; ; {
			page, err := snapshotPage(env, root, after, cfg.PageBytes)
			if err != nil {
				return "", err
			}
			reply, err := c.request(wire.GraftPushMessage{H: header(wire.MsgGraftPush, c.next()), Page: page})
			if err != nil {
				return "", err
			}
			if _, ok := reply.(wire.GraftPushResultMessage); !ok {
				return "", NewError(fmt.Sprintf("expected GRAFT_PUSH_RESULT, got %T", reply), nil)
			}
			if page.Done {
				break
			}
			after = page.Next
		}
	}
	return "", nil
}

// snapshotFetcher pages the peer's state at the commit at, for InstallSnapshot and Graft.
func (c *v2Conn) snapshotFetcher(cfg V2ClientConfig, ns, at string) func(after string) (wire.SnapshotPageMessage, error) {
	return func(after string) (wire.SnapshotPageMessage, error) {
		reply, err := c.request(wire.SnapshotFetchMessage{
			H: header(wire.MsgSnapshotFetch, c.next()), Namespace: ns,
			AtHex: at, After: after, MaxBytes: cfg.PageBytes,
		})
		if err != nil {
			return wire.SnapshotPageMessage{}, err
		}
		page, ok := reply.(wire.SnapshotPageMessage)
		if !ok {
			return wire.SnapshotPageMessage{}, NewError(fmt.Sprintf("expected SNAPSHOT_PAGE, got %T", reply), nil)
		}
		return page, nil
	}
}

// fetchRef pages in everything want needs that this node lacks, returning the haves grown by
// what arrived.
func (c *v2Conn) fetchRef(cfg V2ClientConfig, env IngestEnv, ns string, shallow []string, want codec.Hash, haves []codec.Hash, res *NamespaceSyncResult) ([]codec.Hash, error) {
	for {
		reply, err := c.request(wire.FetchRequestMessage{
			H: header(wire.MsgFetchRequest, c.next()), Namespace: ns,
			Wants: []codec.Hash{want}, Haves: haves, MaxBytes: cfg.PageBytes,
		})
		if err != nil {
			return haves, err
		}
		page, ok := reply.(wire.PackPageMessage)
		if !ok {
			return haves, NewError(fmt.Sprintf("expected PACK_PAGE, got %T", reply), nil)
		}
		n, err := StoreCommits(env, page.Commits, page.Stubs)
		res.Pulled += n
		res.Received += len(page.Commits)
		// A page can reach down to more than one of the peer's roots; each graft lets the retry
		// store further, so there are at most as many rounds as commits.
		for round := 0; err != nil && round < len(page.Commits); round++ {
			root, ok := unsharedRoot(env, page.Commits, shallow)
			if !ok {
				return haves, err
			}
			if !env.Resolution.Chain.AllowsUnrelated() {
				return haves, env.noteUnrelated(root, err)
			}
			// The peer's history is rooted where this node's is not, and the namespace merges
			// unrelated histories: graft the root, then the page stores.
			if _, gerr := Graft(env, root, c.snapshotFetcher(cfg, ns, root.Hex())); gerr != nil {
				return haves, env.noteUnrelated(root, fmt.Errorf("%w; grafting it failed: %v", err, gerr))
			}
			res.Grafted = append(res.Grafted, root.Hex())
			n, err = StoreCommits(env, page.Commits, page.Stubs)
			res.Pulled += n
		}
		if err != nil {
			return haves, err
		}
		tips := pageTips(page.Commits)
		res.ReceivedTips = append(res.ReceivedTips, tips...)
		haves = append(haves, tips...)
		if page.Done || len(page.Commits) == 0 {
			return haves, nil
		}
	}
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
		if res.ResolutionMismatch && ref.kind == wire.RefBranch && ref.name == mainBranch && !fastForwards(env, remoteHex, local) {
			res.RefErrors[ref.key()] = "not pushed: the peer's conflict resolution chain differs from this node's, so it must not merge"
			continue
		}
		if skip, err := c.graftRootsFor(cfg, env, remote, local, known); err != nil {
			return err
		} else if skip != "" {
			res.RefErrors[ref.key()] = skip
			continue
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
		// v1 carries no chain hash: with a chain, merge nothing over it - queue what diverges
		// and push only fast-forwards (see ClientConfig.FastForwardPushOnly).
		policy, resolver, ffOnly := env.Resolution.Policy, env.Resolution.Resolver, false
		if env.Resolution.Chain != nil {
			policy, resolver, ffOnly = transaction.ConflictPolicyStrict, nil, true
		}
		client := NewClient(w, transport, env.DAG, env.Storage)
		session, err := client.Connect(ClientConfig{
			NamespaceID: ns, NodeID: cfg.NodeID, PeerURI: cfg.PeerURI, ConnectionContext: cfg.ConnectionContext,
			TLS: cfg.TLS, Node: env.Node, ApplyToStorage: env.ApplyToStorage, PersistAsync: env.PersistAsync,
			Persist: env.Persist, ConflictPolicy: policy, ConflictResolver: resolver, FastForwardPushOnly: ffOnly,
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
	// caps are the peer's capabilities, from its SYNC_HELLO_ACK.
	caps []string
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

// fastForwards reports whether moving a ref from remoteHex to local is a fast-forward: remoteHex
// is empty or an ancestor of local that this node holds.
func fastForwards(env IngestEnv, remoteHex string, local codec.Hash) bool {
	if remoteHex == "" {
		return true
	}
	r, err := codec.HashFromHex(remoteHex)
	if err != nil || !env.DAG.HasCommit(r) {
		return false
	}
	return r == local || env.DAG.IsAncestor(r, local)
}
