package server

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// inboundPeers records, per peer, when it last finished fetching this namespace - what this node
// knows about peers that pull from it, as opposed to the ones it pushes to (those are the
// replicator's). Persisted beside the replicator's state for a file-backed namespace, so a
// restart does not forget a peer that is merely offline.
type inboundPeers struct {
	mu   sync.Mutex
	path string
	seen map[string]time.Time
}

func (s *KdbServerRuntime) inbound() *inboundPeers {
	s.inboundOnce.Do(func() {
		p := &inboundPeers{seen: map[string]time.Time{}}
		if root := s.Runtime.DataRoot; root != "" && !s.Runtime.ReadOnly {
			p.path = filepath.Join(root, "replication", "inbound", url.PathEscape(s.Runtime.DefaultNamespace)+".json")
			if raw, err := os.ReadFile(p.path); err == nil {
				_ = json.Unmarshal(raw, &p.seen)
			}
		}
		s.inboundState = p
	})
	return s.inboundState
}

// NoteInboundPeer records that peer finished fetching this namespace at t.
func (s *KdbServerRuntime) NoteInboundPeer(peer string, t time.Time) {
	if peer == "" {
		return
	}
	p := s.inbound()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen[peer] = t.UTC()
	if p.path == "" {
		return
	}
	raw, err := json.Marshal(p.seen)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return
	}
	tmp := p.path + ".tmp"
	if os.WriteFile(tmp, raw, 0o644) == nil {
		_ = os.Rename(tmp, p.path)
	}
}

// InboundPeerFloor is the oldest time an inbound peer active within grace last caught up.
func (s *KdbServerRuntime) InboundPeerFloor(grace time.Duration, now time.Time) (time.Time, bool) {
	p := s.inbound()
	p.mu.Lock()
	defer p.mu.Unlock()
	var floor time.Time
	found := false
	for _, t := range p.seen {
		if now.Sub(t) > grace {
			continue
		}
		if !found || t.Before(floor) {
			floor, found = t, true
		}
	}
	return floor, found
}
