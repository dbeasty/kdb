// Package syncnode makes a process a KDB sync node: the replicator and its peers, the peer-sync
// listener, the replicated definitions namespace (_kdb/meta), conflict delivery to a resolver
// authority, the retention floor peers impose, and background scrub. kdb-service is one caller;
// an application that embeds KDB (through embed.Host and its own server.NamespaceSet) is another,
// and gets exactly the service's behaviour without copying its wiring.
//
// Lifecycle:
//
//	node, err := syncnode.Open(host, set, primary, cfg) // definitions, conflict delivery
//	set.SetOpener(node.Opener(nil))                      // or call node.Prepare from your own
//	... open the namespaces the process serves ...
//	node.Start()                                         // definitions applied, peers syncing
//	defer node.Close()
//
// host may be nil: namespaces are then in-memory runtimes, which is what tests use.
package syncnode

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Config configures a Node. The zero value is a node with no peers that still serves peers who
// dial it, keeps definitions and delivers nothing to a resolver authority.
type Config struct {
	// Peers are the nodes this one dials. The metadata namespace is added to every unfiltered
	// peer's patterns unless they exclude it by name: definitions travel with the data.
	Peers []replication.PeerConfig
	// DataDir, when set, keeps replication progress under it (replication.StateDir) and is
	// where the default opener looks for existing namespaces. Empty keeps progress in memory.
	DataDir string
	// TLS is used to dial tcps:// and wss:// peers, and by Listen.
	TLS *core.TransportTlsSettings
	// Transport, when set, chooses the transport per peer (replication.Config.Transport).
	Transport func(replication.PeerConfig) stream.Transport
	// PeerRetentionGrace is how long past an active peer's known position history is kept.
	PeerRetentionGrace time.Duration
	// AuthorityExpiryInterval is how often held conflicts past their authority timeout are
	// settled; 0 means 30s.
	AuthorityExpiryInterval time.Duration
	// ConflictWebhook, when set, delivers conflicts to a resolver authority.
	ConflictWebhook *server.ConflictWebhook
	// ScrubInterval, when positive, re-verifies every open namespace at this interval and
	// repairs damage from peers.
	ScrubInterval time.Duration
	// AuthorizePushedDocuments also asks the auth engine about every document a peer's push
	// writes or deletes (DocumentWriteAction / DocumentDeleteAction), for engines whose rules go
	// below the namespace. Namespace-level push rights are always checked.
	AuthorizePushedDocuments bool
	// HandoverPolicy decides peers' requests to become a namespace's home (Node.RequestHome on
	// their side); nil refuses them all. See server.HandoverRequest.
	HandoverPolicy server.HandoverPolicy
	// Idle, when set, closes namespaces nobody has used for a while (and the least recently used
	// beyond a cap), reopening each on its next use - for a process with a namespace per user or
	// per match. Needs a host: an in-memory namespace closed is gone. The primary is never closed.
	Idle *IdleConfig
	// Debounce, MaxBackoff and Timeout tune the replicator (see replication.Config).
	Debounce, MaxBackoff, Timeout time.Duration
}

// IdleConfig is Config.Idle; see server.IdlePolicy.
type IdleConfig struct {
	MaxOpen   int
	IdleAfter time.Duration
	MinIdle   time.Duration
	Interval  time.Duration
	// OnClose, when set, runs after the node has closed a namespace's storage - for whatever the
	// application opened alongside it.
	OnClose func(rt *server.KdbServerRuntime)
}

// Node is one sync node. Its methods are safe for concurrent use.
type Node struct {
	host    *embed.Host
	set     *server.NamespaceSet
	primary *server.KdbServerRuntime
	cfg     Config

	meta      *server.KdbServerRuntime
	metaRT    *embed.EmbeddedKdbRuntime // closed with the node when this node opened it in memory
	metaStore *server.MetaStore

	replicator atomic.Pointer[replication.Replicator]
	// projectionPeers maps each filtered peer's local projection namespace to its config.
	projectionPeers sync.Map
	peers           []replication.PeerConfig

	mu         sync.Mutex
	started    bool
	closed     bool
	listeners  []*server.Listener
	conns      map[stream.ConnectionHandle]struct{}
	stopExpiry chan struct{}
	hook       *server.ConflictWebhook
	stopScrub  chan struct{}
	scrubDone  chan struct{}
	stopIdle   func()
}

// Open prepares primary - a runtime already in set - to sync, opens the metadata namespace and
// starts conflict delivery. Peers do not sync until Start.
func Open(host *embed.Host, set *server.NamespaceSet, primary *server.KdbServerRuntime, cfg Config) (*Node, error) {
	if set == nil || primary == nil {
		return nil, errors.New("syncnode: a namespace set and its primary runtime are required")
	}
	if cfg.AuthorityExpiryInterval <= 0 {
		cfg.AuthorityExpiryInterval = 30 * time.Second
	}
	if host != nil && primary.NodeID == server.ProcessNodeID() {
		// The data root's identity (its NODE file), as kdb-service uses: two hosts in one process -
		// or a device and the cloud in one test - are two nodes, and a restart is the same node.
		id, err := embed.LoadOrCreateNodeID(host.DataRoot())
		if err != nil {
			return nil, fmt.Errorf("syncnode: node identity: %w", err)
		}
		primary.NodeID = id
	}
	n := &Node{host: host, set: set, primary: primary, cfg: cfg, stopExpiry: make(chan struct{})}
	primary.Namespaces = set
	primary.PeerAuthorizeDocuments = cfg.AuthorizePushedDocuments
	primary.HandoverPolicy = cfg.HandoverPolicy
	primary.Runtime.SetPeerRetentionFloor(n.peerFloorFor(primary))

	// Definitions (schema, index DDL, resolution chains, homes) live as documents in a reserved
	// namespace so they are durable and replicate with the data - see server.MetaStore.
	var err error
	if host != nil {
		n.metaRT, err = host.Namespace(embed.CatalogFromNamespace(server.MetaNamespace), server.MetaNamespace, schema.None())
	} else {
		n.metaRT, err = embed.OpenMemoryRuntime(embed.CatalogFromNamespace(server.MetaNamespace), server.MetaNamespace, schema.None())
	}
	if err != nil {
		return nil, fmt.Errorf("syncnode: opening %s: %w", server.MetaNamespace, err)
	}
	n.meta = server.NewKdbServerRuntime(n.metaRT)
	n.meta.NodeID = primary.NodeID
	n.meta.AuthEngine = primary.AuthEngine
	n.meta.CommitListener = func(ns string, _ document.Commit) { n.notify(ns) }
	set.AddSystem(n.meta)
	n.metaStore = server.NewMetaStore(n.meta, set)

	// A resolver authority learns of the conflicts it owns by webhook, or by polling the
	// control plane's conflict list and acknowledging each.
	server.StartAuthorityExpiry(set, cfg.AuthorityExpiryInterval, n.stopExpiry)
	if cfg.ConflictWebhook != nil {
		n.hook = server.StartConflictWebhook(set, primary.NodeID.String(), cfg.ConflictWebhook)
	}

	if cfg.Idle != nil && host == nil {
		return nil, errors.New("syncnode: idle close needs a host - an in-memory namespace closed is gone")
	}
	if host != nil {
		// Every namespace on disk is served, open or not: a peer's sync opens the one it reaches.
		if ids, err := embed.ListNamespaces(host.DataRoot()); err == nil {
			var known []string
			for _, id := range ids {
				if !strings.HasPrefix(id, "_") {
					known = append(known, id)
				}
			}
			set.AddKnown(known...)
		}
	}

	n.peers = append([]replication.PeerConfig(nil), cfg.Peers...)
	for i := range n.peers {
		p := &n.peers[i]
		if p.Filter != "" {
			// Known before any namespace opens, so a projection opened offline is already
			// configured (read-only, or writable with write-back).
			n.projectionPeers.Store(peersync.ProjectionNamespace(p.Namespaces[0], p.Filter), *p)
			continue // a projection carries documents only; definitions stay with the source
		}
		if p.ScopedMeta {
			continue // definitions come as a view (META_VIEW); the replicator excludes the namespace
		}
		if len(peersync.SelectNamespaces(p.Namespaces, []string{server.MetaNamespace})) == 0 &&
			!excludes(p.Namespaces, server.MetaNamespace) {
			p.Namespaces = append(p.Namespaces, server.MetaNamespace)
		}
	}
	return n, nil
}

// excludes reports whether an exclusion among patterns ("!pattern") matches ns.
func excludes(patterns []string, ns string) bool {
	for _, p := range patterns {
		if strings.HasPrefix(p, "!") && peersync.MatchNamespace(p[1:], ns) {
			return true
		}
	}
	return false
}

// notify tells the replicator ns changed, once it runs.
func (n *Node) notify(ns string) {
	if r := n.replicator.Load(); r != nil {
		r.OnLocalCommit(ns)
	}
}

// peerFloorFor is the retention floor replication imposes on one namespace: the oldest point an
// active peer - pushed to by the replicator, or fetching from this node - is known to have
// reached.
func (n *Node) peerFloorFor(rt *server.KdbServerRuntime) func() (time.Time, bool) {
	return func() (time.Time, bool) {
		now := time.Now()
		floor, ok := rt.InboundPeerFloor(n.cfg.PeerRetentionGrace, now)
		if r := n.replicator.Load(); r != nil {
			if f, has := r.PeerFloor(rt.Runtime.DefaultNamespace, n.cfg.PeerRetentionGrace, now); has && (!ok || f.Before(floor)) {
				floor, ok = f, true
			}
		}
		return floor, ok
	}
}

// Prepare wires a runtime the application opened (through set's opener) for sync: its commits
// reach the replicator, peers hold its history back, a projection is configured from its peer,
// and the stored definitions apply to it. Call it for every runtime but the primary, before the
// runtime is used; an opener built with Opener does.
func (n *Node) Prepare(rt *server.KdbServerRuntime) {
	id := rt.Runtime.DefaultNamespace
	if source, isProjection := peersync.ProjectionSource(id); isProjection {
		rt.ProjectionOf = source // read-only from the moment it opens, not from the first sync
		if p, ok := n.projectionPeers.Load(id); ok {
			cfg := p.(replication.PeerConfig)
			rt.ProjectionFilter, rt.ProjectionWriteBack = cfg.Filter, cfg.WriteBack
			if cfg.ReadThrough {
				projectionNS := id
				rt.ReadThrough = &server.ReadThrough{Open: func() (*peersync.RepairSession, string, error) {
					r := n.replicator.Load()
					if r == nil {
						return nil, "", fmt.Errorf("read-through: replication has not started")
					}
					return r.OpenDocSession(projectionNS)
				}}
			}
		}
	}
	if rt.NodeID != n.primary.NodeID {
		rt.NodeID = n.primary.NodeID
	}
	rt.Namespaces = n.set
	rt.PeerSyncConflictPolicy = n.primary.PeerSyncConflictPolicy
	rt.PeerCreateOnPush = n.primary.PeerCreateOnPush
	previous := rt.CommitListener
	rt.CommitListener = func(ns string, c document.Commit) {
		if previous != nil {
			previous(ns, c)
		}
		n.notify(ns)
	}
	rt.Runtime.SetPeerRetentionFloor(n.peerFloorFor(rt))
	n.metaStore.ApplyTo(rt)
}

// Opener returns an opener for set.SetOpener that opens namespaces on the node's host (in
// memory when it has none), prepares each for sync, then hands it to configure - where an
// application sets its own auth engine, indexes or limits. A read never creates a namespace;
// only a write (create=true), which is authorized against that namespace, may.
func (n *Node) Opener(configure func(*server.KdbServerRuntime) error) func(id string, create bool) (*server.KdbServerRuntime, error) {
	var mu sync.Mutex
	memory := map[string]bool{}
	return func(id string, create bool) (*server.KdbServerRuntime, error) {
		// An id becomes a directory under the data root: validate it before anything touches the
		// filesystem.
		if err := embed.ValidateNamespaceID(id); err != nil {
			return nil, err
		}
		var rt *embed.EmbeddedKdbRuntime
		var err error
		if n.host != nil {
			if !create && !embed.NamespaceExists(n.host.DataRoot(), id) {
				return nil, fmt.Errorf("%w: %s", server.ErrUnknownNamespace, id)
			}
			rt, err = n.host.Namespace(embed.CatalogFromNamespace(id), id, schema.None())
		} else {
			mu.Lock()
			known := memory[id]
			memory[id] = memory[id] || create
			mu.Unlock()
			if !create && !known {
				return nil, fmt.Errorf("%w: %s", server.ErrUnknownNamespace, id)
			}
			rt, err = embed.OpenMemoryRuntime(embed.CatalogFromNamespace(id), id, schema.None())
		}
		if err != nil {
			return nil, err
		}
		sec := server.NewKdbServerRuntime(rt)
		sec.AuthEngine = n.primary.AuthEngine
		sec.WriteTimeout = n.primary.WriteTimeout
		n.Prepare(sec)
		if configure != nil {
			if err := configure(sec); err != nil {
				return nil, err
			}
		}
		return sec, nil
	}
}

// Start applies every stored definition to the namespaces open now, then starts syncing with the
// configured peers and, when configured, scrubbing. Call it once the namespaces the process
// serves are open. The primary's commit listener is chained here rather than at Open, so
// listeners the application installed in between (a stream hub, a control plane) keep working.
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.closed {
		return errors.New("syncnode: already started or closed")
	}
	n.started = true
	// Its own recorded schema and indexes, and whatever arrived from peers while it was down.
	if err := n.metaStore.ReconcileAll(); err != nil {
		slog.Warn("could not apply stored definitions", "error", err)
	}
	previous := n.primary.CommitListener
	n.primary.CommitListener = func(ns string, c document.Commit) {
		if previous != nil {
			previous(ns, c)
		}
		n.notify(ns)
	}
	if len(n.peers) > 0 {
		stateDir := ""
		if n.cfg.DataDir != "" {
			stateDir = replication.StateDir(n.cfg.DataDir)
		}
		state, err := replication.NewStateStore(stateDir)
		if err != nil {
			return fmt.Errorf("syncnode: replication state: %w", err)
		}
		r, err := replication.New(replication.Config{
			NodeID: n.primary.NodeID.String(), Local: n.primary.PeerNamespaces(), Peers: n.peers, State: state,
			TLS: n.cfg.TLS, Transport: n.cfg.Transport,
			Debounce: n.cfg.Debounce, MaxBackoff: n.cfg.MaxBackoff, Timeout: n.cfg.Timeout,
			MetaNamespace: server.MetaNamespace,
			MetaView: func(_ string, defs []wire.MetaDefinition) error {
				_, err := n.metaStore.AdoptView(defs)
				return err
			},
			Projections: func(source, filter string, writeBack bool) (peersync.ProjectionTarget, error) {
				rt, err := n.set.Resolve(peersync.ProjectionNamespace(source, filter), true)
				if err != nil {
					return nil, err
				}
				rt.ProjectionOf, rt.ProjectionFilter, rt.ProjectionWriteBack = source, filter, writeBack
				return rt.ProjectionTarget(), nil
			},
		})
		if err != nil {
			return err
		}
		n.replicator.Store(r)
		r.Start()
	}
	if n.cfg.Idle != nil {
		n.stopIdle = n.set.StartIdleClose(n.idlePolicy())
	}
	n.stopScrub, n.scrubDone = make(chan struct{}), make(chan struct{})
	if n.cfg.ScrubInterval <= 0 {
		close(n.scrubDone)
	} else {
		go n.scrubLoop()
	}
	return nil
}

// idlePolicy is Config.Idle as the set applies it.
func (n *Node) idlePolicy() server.IdlePolicy {
	ic := n.cfg.Idle
	primaryNS := n.primary.Runtime.DefaultNamespace
	return server.IdlePolicy{
		MaxOpen: ic.MaxOpen, IdleAfter: ic.IdleAfter, MinIdle: ic.MinIdle, Interval: ic.Interval,
		Pinned: func(ns string) bool { return ns == primaryNS },
		Close: func(rt *server.KdbServerRuntime) error {
			err := n.host.CloseNamespace(rt.Runtime.DefaultNamespace)
			if ic.OnClose != nil {
				ic.OnClose(rt)
			}
			return err
		},
	}
}

// CloseIdle runs one idle-close sweep now, as if at now, returning what it closed - for an
// application that wants to shed namespaces at a moment of its choosing (backgrounded on a phone,
// say), and for tests. Nothing without Config.Idle.
func (n *Node) CloseIdle(now time.Time) []string {
	if n.cfg.Idle == nil {
		return nil
	}
	return n.set.CloseIdle(n.idlePolicy(), now)
}

// scrubLoop re-verifies every open namespace each interval, repairing damage from peers. It is a
// writer when it repairs, so Close waits for a pass in flight.
func (n *Node) scrubLoop() {
	defer close(n.scrubDone)
	t := time.NewTicker(n.cfg.ScrubInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopScrub:
			return
		case <-t.C:
		}
		var fetch server.BodyFetcher
		if r := n.replicator.Load(); r != nil {
			fetch = r.FetchBodies
		}
		for ns, rt := range n.set.Runtimes() {
			rep, err := rt.Scrub(fetch)
			switch {
			case err != nil:
				slog.Warn("scrub found damage it could not repair", "namespace", ns, "damaged", len(rep.Damaged), "repaired", len(rep.Repaired), "error", err)
			case len(rep.Repaired) > 0:
				slog.Info("scrub repaired damaged documents from peers", "namespace", ns, "repaired", len(rep.Repaired), "commit", rep.RepairCommit)
			}
		}
	}
}

// Listen serves peers that dial this node over TCP (tcp://, or tcps:// with the node's TLS).
// Every namespace in the set is served, subject to the auth engine; the listener closes with
// the node.
func (n *Node) Listen(addr string) (*server.Listener, error) {
	ln, err := server.ListenPeerSyncTLS(addr, n.primary, n.primary.Runtime.DefaultNamespace, n.cfg.TLS)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	n.listeners = append(n.listeners, ln)
	n.mu.Unlock()
	return ln, nil
}

// Replicator is the running replicator, or nil before Start or when no peers are configured.
func (n *Node) Replicator() *replication.Replicator { return n.replicator.Load() }

// Meta is the node's definitions store.
func (n *Node) Meta() *server.MetaStore { return n.metaStore }

// MetaRuntime is the metadata namespace's runtime.
func (n *Node) MetaRuntime() *server.KdbServerRuntime { return n.meta }

// Set is the node's namespace set.
func (n *Node) Set() *server.NamespaceSet { return n.set }

// Conflicts is ns's conflict queue, when ns is open.
func (n *Node) Conflicts(ns string) (*peersync.ConflictQueue, bool) {
	rt, ok := n.set.Get(ns)
	if !ok || rt.Conflicts == nil {
		return nil, false
	}
	return rt.Conflicts, true
}

// SyncNow syncs with the named peer now and waits for it.
func (n *Node) SyncNow(peer string) (peersync.V2Result, error) {
	r := n.replicator.Load()
	if r == nil {
		return peersync.V2Result{}, errors.New("syncnode: no replication peers are running")
	}
	return r.SyncNow(peer)
}

// StopSync stops the replicator and scrub, waiting for a sync or a scrub pass in flight - each is
// a writer - and leaves listeners and conflict delivery running. The first step of an orderly
// shutdown, before writes drain; Close does the rest.
func (n *Node) StopSync() {
	n.mu.Lock()
	started, stop := n.started, n.stopScrub
	n.stopScrub = nil
	n.mu.Unlock()
	if started && stop != nil {
		close(stop)
		<-n.scrubDone
	}
	if r := n.replicator.Load(); r != nil {
		r.Stop()
	}
}

// Close stops syncing, scrubbing, conflict delivery and every listener the node opened. The
// namespaces themselves belong to the application (or its host) and stay open, except the
// metadata namespace when the node opened it in memory.
func (n *Node) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	listeners := n.listeners
	stopIdle := n.stopIdle
	var conns []stream.ConnectionHandle
	for c := range n.conns {
		conns = append(conns, c)
	}
	n.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	n.StopSync()
	if stopIdle != nil {
		stopIdle()
	}
	for _, ln := range listeners {
		_ = ln.Close()
	}
	if n.hook != nil {
		n.hook.Close()
	}
	close(n.stopExpiry)
	n.metaStore.Close()
	if n.host == nil && n.metaRT != nil {
		n.metaRT.Close()
	}
	return nil
}
