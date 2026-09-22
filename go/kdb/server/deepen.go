package server

import (
	"github.com/limidus/kdb/go/kdb/peersync"
)

// Deepen fetches the history below this namespace's shallow roots from the peer open connects to,
// until none is left or the peer has no more to give (peersync.Deepen). A namespace bootstrapped
// from a snapshot becomes one with history again: readable below its root, and able to sync with
// nodes that have history of their own.
func (s *KdbServerRuntime) Deepen(open func() (*peersync.RepairSession, error)) ([]peersync.DeepenResult, error) {
	if len(s.dag.ShallowRoots()) == 0 {
		return nil, nil
	}
	session, err := open()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	env := s.PeerIngestEnv()
	env.Peer = session.Peer
	return peersync.DeepenAll(env, session)
}

// ShallowRoots lists the namespace's shallow roots: commits held without their history.
func (s *KdbServerRuntime) ShallowRoots() []string {
	var out []string
	for _, h := range s.dag.ShallowRoots() {
		out = append(out, h.Hex())
	}
	return out
}
