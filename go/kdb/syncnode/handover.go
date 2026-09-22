package syncnode

import (
	"errors"

	"github.com/limidus/kdb/go/kdb/server"
)

// Handover moves ns's home from this node - its current home, or any node while ns is
// multi-leader - to node, reachable for clients at addr. The assignment replicates like any
// definition; node takes writes once it holds the handover point.
func (n *Node) Handover(ns, node, addr string) (server.Home, error) {
	return n.primary.Handover(ns, node, addr)
}

// ForceHandover reassigns ns's home without its current home, which is unreachable. Only a node
// the namespace's resolution chain names as its authority may.
func (n *Node) ForceHandover(ns, node, addr string) (server.Home, error) {
	return n.primary.ForceHandover(ns, node, addr)
}

// RequestHome asks peer - ns's current home, or with force the namespace's authority - to make
// this node ns's home, clients reaching it at addr. On a grant it syncs with peer at once, so this
// node holds the new assignment and the previous home's last writes (the handover point) before
// it returns; writes are accepted from then on. A refusal is *server.HandoverRefusedError, naming
// the current home when the peer is not it.
func (n *Node) RequestHome(peer, ns, addr, reason string, force bool) (server.Home, error) {
	r := n.replicator.Load()
	if r == nil {
		return server.Home{}, errors.New("syncnode: no replication peers are running")
	}
	s, err := r.OpenRepairSessionTo(peer, ns)
	if err != nil {
		return server.Home{}, err
	}
	res, err := s.RequestHome(ns, n.primary.NodeID.String(), addr, reason, force)
	s.Close()
	if err != nil {
		return server.Home{}, err
	}
	h := server.Home{Node: res.HomeNode, Addr: res.HomeAddr, Fence: res.Fence, Since: res.Since}
	if !res.Granted {
		return server.Home{}, &server.HandoverRefusedError{Namespace: ns, Reason: res.Reason, Home: h}
	}
	if _, err := r.SyncNow(peer); err != nil {
		return h, err
	}
	// Definitions that arrive by sync are applied in the background; the caller is about to
	// write, so apply the new assignment now.
	if err := n.metaStore.ReconcileAll(); err != nil {
		return h, err
	}
	return h, nil
}
