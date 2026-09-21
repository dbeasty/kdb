package replication

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PeerState is what the replicator remembers about one peer.
type PeerState struct {
	Name string `json:"name"`
	// PeerNodeID is the node id the peer last introduced itself with.
	PeerNodeID  string    `json:"peerNodeId,omitempty"`
	LastAttempt time.Time `json:"lastAttempt,omitempty"`
	LastSuccess time.Time `json:"lastSuccess,omitempty"`
	LastError   string    `json:"lastError,omitempty"`
	// ConsecutiveFailures drives the backoff and resets on success.
	ConsecutiveFailures int `json:"consecutiveFailures"`
	// Namespaces holds per-namespace progress, by namespace.
	Namespaces map[string]NamespaceState `json:"namespaces,omitempty"`
}

// NamespaceState is one namespace's progress with one peer.
type NamespaceState struct {
	// LocalMain / RemoteMain are both sides' main heads when this namespace last synced without
	// error. RemoteMain is the peer's acknowledged position - what Phase 4's peer-aware retention
	// floor reads.
	LocalMain  string    `json:"localMain,omitempty"`
	RemoteMain string    `json:"remoteMain,omitempty"`
	LastSync   time.Time `json:"lastSync,omitempty"`
	// PendingTips are the tips of pages an interrupted pull stored; the next sync names them so
	// it resumes instead of refetching.
	PendingTips []string `json:"pendingTips,omitempty"`
	Pulled      int64    `json:"pulled"`
	Pushed      int64    `json:"pushed"`
	// OpenConflicts counts refused ref updates in the last sync.
	OpenConflicts int    `json:"openConflicts"`
	LastError     string `json:"lastError,omitempty"`
}

// StateStore keeps PeerState, one JSON file per peer under a directory - or in memory, for a node
// with no data root.
type StateStore struct {
	dir string
	mu  sync.Mutex
	mem map[string]PeerState
}

// NewStateStore returns a store under dir; empty keeps state in memory.
func NewStateStore(dir string) (*StateStore, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &StateStore{dir: dir, mem: map[string]PeerState{}}, nil
}

// StateDir is where a data root keeps replication peer state.
func StateDir(dataRoot string) string { return filepath.Join(dataRoot, "replication", "peers") }

// Load returns the state for peer name, or a fresh one.
func (s *StateStore) Load(name string) (PeerState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.mem[name]; ok {
		return st.clone(), nil
	}
	st := PeerState{Name: name, Namespaces: map[string]NamespaceState{}}
	if s.dir == "" {
		return st, nil
	}
	raw, err := os.ReadFile(filepath.Join(s.dir, name+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	// A state file that does not parse costs one full negotiation, not the peer: start fresh.
	if err := json.Unmarshal(raw, &st); err != nil {
		return PeerState{Name: name, Namespaces: map[string]NamespaceState{}}, nil
	}
	if st.Namespaces == nil {
		st.Namespaces = map[string]NamespaceState{}
	}
	s.mem[name] = st
	return st.clone(), nil
}

// clone copies st deeply enough that the caller can change it without touching the stored one.
func (st PeerState) clone() PeerState {
	out := st
	out.Namespaces = make(map[string]NamespaceState, len(st.Namespaces))
	for k, v := range st.Namespaces {
		v.PendingTips = append([]string(nil), v.PendingTips...)
		out.Namespaces[k] = v
	}
	return out
}

// Save records st.
func (s *StateStore) Save(st PeerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st = st.clone()
	s.mem[st.Name] = st
	if s.dir == "" {
		return nil
	}
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, st.Name+".json.tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, st.Name+".json"))
}

// All returns every peer's state held in memory.
func (s *StateStore) All() []PeerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PeerState, 0, len(s.mem))
	for _, st := range s.mem {
		out = append(out, st.clone())
	}
	return out
}
