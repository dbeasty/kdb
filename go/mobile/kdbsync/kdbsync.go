// Package kdbsync is KDB's sync node for mobile apps, shaped for gomobile bind: every exported
// method takes and returns strings, bools, ints and errors only. It opens a file host under the
// app's data directory, serves the namespaces the app writes, and syncs them with peers - a cloud
// behind an HTTPS load balancer over wss://, typically - with a bearer token the app refreshes.
//
//	node, _ := kdbsync.Open(dir, "app/device")
//	node.AddPeer("cloud", "wss://api.example.com/kdb/sync", "app/u/42", true)
//	node.SetToken("cloud", token)
//	node.Start()
//	node.Put("app/u/42", id, `{"theme":"dark"}`)
//
// gomobile: `gomobile bind -target=android ./mobile/kdbsync` (or -target=ios).
package kdbsync

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/syncnode"
)

// Node is one device's sync node.
type Node struct {
	dir     string
	host    *embed.Host
	set     *server.NamespaceSet
	primary *server.KdbServerRuntime
	node    *syncnode.Node
	peers   []replication.PeerConfig

	mu      sync.Mutex
	tokens  map[string]string
	started bool
}

// Open opens (creating) the data directory and the device's primary namespace.
func Open(dataDir, primaryNamespace string) (*Node, error) {
	host, err := embed.OpenFileHost(dataDir, embed.FileRuntimeOptions{})
	if err != nil {
		return nil, err
	}
	rt, err := host.Namespace(embed.CatalogFromNamespace(primaryNamespace), primaryNamespace, schema.None())
	if err != nil {
		host.Close()
		return nil, err
	}
	primary := server.NewKdbServerRuntime(rt)
	set := server.NewNamespaceSet(host.Transactions())
	if err := set.Add(primary); err != nil {
		host.Close()
		return nil, err
	}
	return &Node{dir: dataDir, host: host, set: set, primary: primary, tokens: map[string]string{}}, nil
}

// AddPeer configures a peer before Start: its address (tcp://, tcps://, ws:// or wss://), the
// namespaces to sync as a comma-separated list of names or patterns, and whether definitions come
// from it as a view (scopedMeta - what a phone that must not learn other users' namespace names
// wants).
func (n *Node) AddPeer(name, addr, namespacesCSV string, scopedMeta bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return errors.New("kdbsync: add peers before Start")
	}
	var patterns []string
	for _, p := range strings.Split(namespacesCSV, ",") {
		if p = strings.TrimSpace(p); p != "" {
			patterns = append(patterns, p)
		}
	}
	peerName := name
	n.peers = append(n.peers, replication.PeerConfig{
		Name: name, Addr: addr, Namespaces: patterns, Mode: peersync.SyncBoth,
		CreateLocal: true, ScopedMeta: scopedMeta, Interval: replication.DefaultInterval,
		Credentials: func() (auth.ConnectionContext, error) {
			n.mu.Lock()
			tok, ok := n.tokens[peerName]
			n.mu.Unlock()
			if !ok || tok == "" {
				return auth.ConnectionContext{}, nil
			}
			return auth.ConnectionContext{Token: &tok}, nil
		},
	})
	return nil
}

// SetToken sets (or refreshes) the bearer token sent to a peer; the next connection uses it.
func (n *Node) SetToken(peer, token string) {
	n.mu.Lock()
	n.tokens[peer] = token
	n.mu.Unlock()
}

// Start begins syncing with the configured peers.
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return nil
	}
	node, err := syncnode.Open(n.host, n.set, n.primary, syncnode.Config{Peers: n.peers, DataDir: n.dir})
	if err != nil {
		return err
	}
	n.set.SetOpener(node.Opener(nil))
	if err := node.Start(); err != nil {
		node.Close()
		return err
	}
	n.node, n.started = node, true
	return nil
}

// AddNamespaces adds comma-separated namespaces to those synced with peer - joining a match, say.
func (n *Node) AddNamespaces(peer, namespacesCSV string) error {
	r, err := n.replicator()
	if err != nil {
		return err
	}
	return r.AddNamespaces(peer, strings.Split(namespacesCSV, ",")...)
}

// RemoveNamespaces stops syncing comma-separated namespaces with peer.
func (n *Node) RemoveNamespaces(peer, namespacesCSV string) error {
	r, err := n.replicator()
	if err != nil {
		return err
	}
	return r.RemoveNamespaces(peer, strings.Split(namespacesCSV, ",")...)
}

// SyncNow syncs with peer now and waits for it.
func (n *Node) SyncNow(peer string) error {
	if n.node == nil {
		return errors.New("kdbsync: not started")
	}
	_, err := n.node.SyncNow(peer)
	return err
}

// Put replaces document id of ns with body, through the write gate.
func (n *Node) Put(ns, id, body string) error {
	rt, doc, err := n.target(ns, id, true)
	if err != nil {
		return err
	}
	_, err = rt.PutJSON(ns, doc, body, nil, noPrincipal)
	return err
}

// Get returns document id of ns, or "" when it does not exist.
func (n *Node) Get(ns, id string) (string, error) {
	rt, doc, err := n.target(ns, id, false)
	if errors.Is(err, server.ErrUnknownNamespace) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	body, _, found, err := rt.GetDocument(ns, doc)
	if err != nil || !found {
		return "", err
	}
	return body, nil
}

func (n *Node) target(ns, id string, create bool) (*server.KdbServerRuntime, codec.UUID, error) {
	rt, err := n.set.Resolve(ns, create)
	return rt, docID(id), err
}

// RequestHome asks peer to make this device the home of ns (resuming a match here); reason is
// passed to the peer's handover policy.
func (n *Node) RequestHome(peer, ns, addr, reason string) error {
	if n.node == nil {
		return errors.New("kdbsync: not started")
	}
	_, err := n.node.RequestHome(peer, ns, addr, reason, false)
	return err
}

// Close stops syncing and closes every namespace.
func (n *Node) Close() error {
	if n.node != nil {
		n.node.Close()
	}
	return n.host.Close()
}

func (n *Node) replicator() (*replication.Replicator, error) {
	if n.node == nil || n.node.Replicator() == nil {
		return nil, fmt.Errorf("kdbsync: not started, or no peers")
	}
	return n.node.Replicator(), nil
}

// docID maps an id to a document id: a UUID as itself, anything else to a stable derived one.
func docID(id string) codec.UUID {
	if u, err := codec.ParseUUID(id); err == nil {
		return u
	}
	return codec.DerivedUUID("kdbsync:" + id)
}

var noPrincipal = auth.Principal{}
