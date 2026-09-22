package client

import (
	"context"
	"sync"
)

// Session gives a client Bayou's session guarantees across replicas: read-your-writes and
// monotonic reads. It remembers, per namespace, the newest commit it has written or read, and
// sends it with every read; a replica serves the read only once its main contains that commit,
// waiting briefly for replication if it must, and otherwise answers BUSY with a retry-after
// (BusyError) rather than an older value.
//
// The token moves with the user: Token exports it and Resume imports it into a session on another
// Client - connected to another replica - so the guarantees hold across the switch.
//
// Only a Go kdb-service honours the token; other servers ignore it and give no guarantee.
type Session struct {
	c    *Client
	mu   sync.Mutex
	seen map[string]string
}

// NewSession starts a session on c with no history.
func (c *Client) NewSession() *Session { return &Session{c: c, seen: map[string]string{}} }

// Token returns the session's newest commit per namespace.
func (s *Session) Token() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.seen))
	for ns, h := range s.seen {
		out[ns] = h
	}
	return out
}

// Resume adopts a token exported from another session - typically one on a client connected to a
// different replica.
func (s *Session) Resume(token map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ns, h := range token {
		if h != "" {
			s.seen[ns] = h
		}
	}
}

func (s *Session) observe(ns, commit string) {
	if commit == "" {
		return
	}
	s.mu.Lock()
	s.seen[ns] = commit
	s.mu.Unlock()
}

func (s *Session) token(ns string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[ns]
}

// GetJSON is Client.GetJSON with the session's guarantees.
func (s *Session) GetJSON(ctx context.Context, ns, docID string) ([]byte, string, error) {
	body, commit, err := s.c.getJSON(ctx, ns, docID, s.token(ns))
	if err == nil || err == ErrNotFound {
		s.observe(ns, commit)
	}
	return body, commit, err
}

// PutJSON is Client.PutJSON, recording the write in the session.
func (s *Session) PutJSON(ctx context.Context, ns, docID string, jsonBody []byte) (string, error) {
	commit, err := s.c.PutJSON(ctx, ns, docID, jsonBody)
	if err == nil {
		s.observe(ns, commit)
	}
	return commit, err
}

// Upsert is Client.Upsert, recording the write in the session.
func (s *Session) Upsert(ctx context.Context, ns, docID string, jsonBody []byte) (string, error) {
	commit, err := s.c.Upsert(ctx, ns, docID, jsonBody)
	if err == nil {
		s.observe(ns, commit)
	}
	return commit, err
}

// Commit is Client.Commit, recording the write in the session.
func (s *Session) Commit(ctx context.Context, tx Transaction) (string, error) {
	commit, err := s.c.Commit(ctx, tx)
	if err == nil {
		s.observe(tx.Namespace, commit)
	}
	return commit, err
}
