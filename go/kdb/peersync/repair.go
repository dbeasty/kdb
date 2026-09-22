package peersync

import (
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Anti-entropy and repair (docs/kdb-distributed-self-healing-research.md, Phases 12-13).
//
// TREE_NODES lets two nodes compare document trees by subtree hash and find the documents they
// hold differently in a round trip per trie level (document.DiffTrees). OBJECT_FETCH hands over
// document bodies by content hash; the receiver verifies every one against the hash it asked for,
// so a body fetched from any peer - trusted or not - is either exactly right or refused.

const (
	// MaxTreeNodePrefixes bounds one TREE_NODES request.
	MaxTreeNodePrefixes = 4096
	// MaxObjectFetchItems bounds one OBJECT_FETCH request.
	MaxObjectFetchItems = 1024
	// DefaultTreeEntryLimit is the subtree size below which TREE_NODES lists entries outright.
	DefaultTreeEntryLimit = 32
)

// treeAt resolves ns's tree by hash from store: directly when the store can, else by walking it.
func treeAt(store storage.Adapter, ns string, h codec.Hash) (document.DocumentTree, error) {
	if r, ok := store.(storage.TreeResolver); ok {
		t, found, err := r.TreeAt(h)
		if err != nil {
			return document.DocumentTree{}, err
		}
		if found {
			return t, nil
		}
	}
	w, ok := store.(storage.TreeWalker)
	if !ok {
		return document.DocumentTree{}, fmt.Errorf("peer sync: storage for %s cannot resolve trees", ns)
	}
	entries := map[codec.UUID]codec.Hash{}
	if err := w.WalkTree(ns, h, func(id codec.UUID, ch codec.Hash) bool {
		entries[id] = ch
		return true
	}); err != nil {
		return document.DocumentTree{}, err
	}
	t, err := document.BuildDocumentTree(entries)
	if err != nil {
		return document.DocumentTree{}, err
	}
	if t.TreeHash != h {
		return document.DocumentTree{}, fmt.Errorf("peer sync: tree %s of %s walks to %s", h.Hex(), ns, t.TreeHash.Hex())
	}
	return t, nil
}

// headTree is the tree at env's main head.
func headTree(env IngestEnv) (codec.Hash, error) {
	_, head, ok, err := env.DAG.HeadCommit()
	if err != nil {
		return codec.Hash{}, err
	}
	if !ok {
		return codec.Hash{}, errors.New("peer sync: namespace has no head")
	}
	return head.DocumentTreeHash, nil
}

// treeNodesPage answers TREE_NODES.
func treeNodesPage(env IngestEnv, m wire.TreeNodesMessage) (wire.TreeNodesResultMessage, error) {
	if len(m.Prefixes) > MaxTreeNodePrefixes {
		return wire.TreeNodesResultMessage{}, fmt.Errorf("peer sync: TREE_NODES asks for %d prefixes, at most %d", len(m.Prefixes), MaxTreeNodePrefixes)
	}
	var th codec.Hash
	var err error
	if m.TreeHex == "" {
		th, err = headTree(env)
	} else {
		th, err = codec.HashFromHex(m.TreeHex)
	}
	if err != nil {
		return wire.TreeNodesResultMessage{}, err
	}
	tree, err := treeAt(env.Storage, env.NamespaceID, th)
	if err != nil {
		return wire.TreeNodesResultMessage{}, err
	}
	limit := m.EntryLimit
	if limit <= 0 {
		limit = DefaultTreeEntryLimit
	}
	out := wire.TreeNodesResultMessage{Namespace: env.NamespaceID, TreeHex: th.Hex(), Nodes: make([]wire.TreeNodeInfo, len(m.Prefixes))}
	for i, p := range m.Prefixes {
		n, err := tree.Node(p, limit)
		if err != nil {
			return wire.TreeNodesResultMessage{}, err
		}
		out.Nodes[i] = nodeToWire(n)
	}
	return out, nil
}

func hexOrEmpty(h codec.Hash) string {
	if h == (codec.Hash{}) {
		return ""
	}
	return h.Hex()
}

func nodeToWire(n document.TreeNode) wire.TreeNodeInfo {
	info := wire.TreeNodeInfo{Prefix: n.Prefix, Hash: hexOrEmpty(n.Hash)}
	if n.Hash != (codec.Hash{}) {
		info.Children = make([]string, 16)
		for i, c := range n.Children {
			info.Children[i] = hexOrEmpty(c)
		}
	}
	if n.Entries != nil {
		info.HasEntries = true
		info.Entries = make(map[string]string, len(n.Entries))
		for id, h := range n.Entries {
			info.Entries[id.String()] = h.Hex()
		}
	}
	return info
}

func nodeFromWire(info wire.TreeNodeInfo) (document.TreeNode, error) {
	n := document.TreeNode{Prefix: info.Prefix}
	parse := func(s string) (codec.Hash, error) {
		if s == "" {
			return codec.Hash{}, nil
		}
		return codec.HashFromHex(s)
	}
	var err error
	if n.Hash, err = parse(info.Hash); err != nil {
		return n, err
	}
	for i, c := range info.Children {
		if i >= 16 {
			break
		}
		if n.Children[i], err = parse(c); err != nil {
			return n, err
		}
	}
	if info.HasEntries {
		n.Entries = make(map[codec.UUID]codec.Hash, len(info.Entries))
		for id, h := range info.Entries {
			uid, err := codec.ParseUUID(id)
			if err != nil {
				return n, err
			}
			ch, err := codec.HashFromHex(h)
			if err != nil {
				return n, err
			}
			n.Entries[uid] = ch
		}
	}
	return n, nil
}

// objectFetchPage answers OBJECT_FETCH: each body the host can find whose content hash is the one
// asked for - in the named tree when it has it, else in its main head's tree.
func objectFetchPage(env IngestEnv, m wire.ObjectFetchMessage) (wire.ObjectFetchResultMessage, error) {
	if len(m.Items) > MaxObjectFetchItems {
		return wire.ObjectFetchResultMessage{}, fmt.Errorf("peer sync: OBJECT_FETCH asks for %d objects, at most %d", len(m.Items), MaxObjectFetchItems)
	}
	head, err := headTree(env)
	if err != nil {
		return wire.ObjectFetchResultMessage{}, err
	}
	out := wire.ObjectFetchResultMessage{Namespace: env.NamespaceID}
	for _, it := range m.Items {
		body, ok := findBody(env, it, head)
		if ok {
			out.Docs = append(out.Docs, wire.SnapshotDoc{DocID: it.DocID, Body: body})
		} else {
			out.Missing = append(out.Missing, it.DocID)
		}
	}
	return out, nil
}

func findBody(env IngestEnv, it wire.ObjectRef, head codec.Hash) (string, bool) {
	id, err := codec.ParseUUID(it.DocID)
	if err != nil {
		return "", false
	}
	want, err := codec.HashFromHex(it.ContentHex)
	if err != nil {
		return "", false
	}
	trees := []codec.Hash{head}
	if it.TreeHex != "" {
		if th, err := codec.HashFromHex(it.TreeHex); err == nil && th != head {
			trees = append([]codec.Hash{th}, trees...)
		}
	}
	for _, th := range trees {
		doc, err := env.Storage.GetDocument(env.NamespaceID, id, th)
		if err != nil || doc == nil {
			continue
		}
		if h, err := doc.ContentHash(); err == nil && h == want {
			return doc.JSON, true
		}
	}
	return "", false
}

// RepairSession is a v2 connection to one peer for anti-entropy: comparing trees and fetching
// bodies. Open it with OpenRepairSession; it is not safe for concurrent use.
type RepairSession struct {
	conn *v2Conn
	// Peer is the peer's node id, from its hello.
	Peer string
	caps []string
}

// OpenRepairSession connects to cfg.PeerURI and says hello for cfg.Namespaces. The peer must
// grant each namespace it will be asked about, and speak the repair capability.
func OpenRepairSession(w wire.Codec, transport stream.Transport, cfg V2ClientConfig) (*RepairSession, error) {
	return openSession(w, transport, cfg, wire.SyncCapRepair)
}

func openSession(w wire.Codec, transport stream.Transport, cfg V2ClientConfig, needCap string) (*RepairSession, error) {
	conn, err := dial(transport, cfg.PeerURI, cfg.TLS, cfg.ConnectionContext)
	if err != nil {
		return nil, err
	}
	c := &v2Conn{wire: w, conn: conn, correlation: 20000, timeout: cfg.Timeout}
	reply, err := c.request(wire.SyncHelloMessage{
		H: header(wire.MsgSyncHello, c.next()), NodeID: cfg.NodeID, Protocol: wire.SyncProtocolVersion,
		Capabilities: ClientCapabilities, Namespaces: cfg.Namespaces,
		User: cfg.ConnectionContext.User, Password: cfg.ConnectionContext.Password, Token: cfg.ConnectionContext.Token,
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	ack, ok := reply.(wire.SyncHelloAckMessage)
	if !ok || !ack.Accepted {
		conn.Close()
		if ok {
			return nil, NewError("peer refused sync: "+ack.Reason, nil)
		}
		return nil, NewError(fmt.Sprintf("expected SYNC_HELLO_ACK, got %T", reply), nil)
	}
	if !containsString(ack.Capabilities, needCap) {
		conn.Close()
		return nil, NewError("peer does not support "+needCap, nil)
	}
	return &RepairSession{conn: c, Peer: ack.NodeID, caps: ack.Capabilities}, nil
}

// Close ends the session.
func (s *RepairSession) Close() { s.conn.conn.Close() }

// Nodes asks for the peer's tree nodes under prefixes, in the tree treeHex (empty: its head's).
// It returns the tree the peer answered for.
func (s *RepairSession) Nodes(ns, treeHex string, prefixes []string, entryLimit int) (string, []document.TreeNode, error) {
	reply, err := s.conn.request(wire.TreeNodesMessage{
		H: header(wire.MsgTreeNodes, s.conn.next()), Namespace: ns, TreeHex: treeHex, Prefixes: prefixes, EntryLimit: entryLimit,
	})
	if err != nil {
		return "", nil, err
	}
	res, ok := reply.(wire.TreeNodesResultMessage)
	if !ok {
		return "", nil, NewError(fmt.Sprintf("expected TREE_NODES_RESULT, got %T", reply), nil)
	}
	if len(res.Nodes) != len(prefixes) {
		return "", nil, NewError(fmt.Sprintf("asked for %d tree nodes, got %d", len(prefixes), len(res.Nodes)), nil)
	}
	out := make([]document.TreeNode, len(res.Nodes))
	for i, info := range res.Nodes {
		if info.Prefix != prefixes[i] {
			return "", nil, NewError(fmt.Sprintf("tree node %d is for %q, asked for %q", i, info.Prefix, prefixes[i]), nil)
		}
		if out[i], err = nodeFromWire(info); err != nil {
			return "", nil, err
		}
	}
	return res.TreeHex, out, nil
}

// Diff finds the documents local holds differently from the peer's tree treeHex (empty: the
// peer's head), and returns the tree compared against.
func (s *RepairSession) Diff(ns string, local document.DocumentTree, treeHex string) (string, []document.TreeDifference, error) {
	// Pin the peer's tree: the first request may name its head, which can move between levels.
	th, root, err := s.Nodes(ns, treeHex, []string{""}, DefaultTreeEntryLimit)
	if err != nil {
		return "", nil, err
	}
	first := true
	remote := func(prefixes []string) ([]document.TreeNode, error) {
		if first && len(prefixes) == 1 && prefixes[0] == "" {
			first = false
			return root, nil
		}
		_, nodes, err := s.Nodes(ns, th, prefixes, DefaultTreeEntryLimit)
		return nodes, err
	}
	diff, err := document.DiffTrees(document.LocalNodes(local, DefaultTreeEntryLimit), remote)
	return th, diff, err
}

// Fetch asks the peer for the bodies of wanted (document id to the content hash it must have),
// looking first in tree treeHex when set. Every body returned has been verified to hash to what
// was asked for; a body that does not is dropped, as if the peer did not have it.
func (s *RepairSession) Fetch(ns string, wanted map[codec.UUID]codec.Hash, treeHex string) (map[codec.UUID]string, error) {
	out := map[codec.UUID]string{}
	ids := make([]codec.UUID, 0, len(wanted))
	for id := range wanted {
		ids = append(ids, id)
	}
	for start := 0; start < len(ids); start += MaxObjectFetchItems {
		end := min(start+MaxObjectFetchItems, len(ids))
		items := make([]wire.ObjectRef, 0, end-start)
		for _, id := range ids[start:end] {
			items = append(items, wire.ObjectRef{DocID: id.String(), ContentHex: wanted[id].Hex(), TreeHex: treeHex})
		}
		reply, err := s.conn.request(wire.ObjectFetchMessage{H: header(wire.MsgObjectFetch, s.conn.next()), Namespace: ns, Items: items})
		if err != nil {
			return out, err
		}
		res, ok := reply.(wire.ObjectFetchResultMessage)
		if !ok {
			return out, NewError(fmt.Sprintf("expected OBJECT_FETCH_RESULT, got %T", reply), nil)
		}
		for _, d := range res.Docs {
			id, err := codec.ParseUUID(d.DocID)
			if err != nil {
				continue
			}
			want, asked := wanted[id]
			if !asked {
				continue
			}
			if h, err := (document.Document{ID: id, JSON: d.Body}).ContentHash(); err != nil || h != want {
				continue // not what was asked for: never accept an unverified body
			}
			out[id] = d.Body
		}
	}
	return out, nil
}

// RequestHome asks the peer to make this node - reachable for clients at addr - the home of ns
// (HOME_REQUEST). force asks a node the namespace's chain names as its authority to do it without
// the current home. The answer says whether it was granted and, if not, why and who the home is.
func (s *RepairSession) RequestHome(ns, node, addr, reason string, force bool) (wire.HomeRequestResultMessage, error) {
	if !containsString(s.caps, wire.SyncCapHome) {
		return wire.HomeRequestResultMessage{}, NewError("peer sync: the peer does not take home requests", nil)
	}
	reply, err := s.conn.request(wire.HomeRequestMessage{
		H: header(wire.MsgHomeRequest, s.conn.next()), Namespace: ns, Node: node, Addr: addr, Force: force, Reason: reason,
	})
	if err != nil {
		return wire.HomeRequestResultMessage{}, err
	}
	res, ok := reply.(wire.HomeRequestResultMessage)
	if !ok {
		return wire.HomeRequestResultMessage{}, NewError(fmt.Sprintf("expected HOME_REQUEST_RESULT, got %T", reply), nil)
	}
	return res, nil
}
