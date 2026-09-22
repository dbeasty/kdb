package server

import (
	"fmt"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Session guarantees across replicas (Bayou; docs/kdb-distributed-self-healing-research.md,
// Phase 15). A client that wrote through one replica and reads through another - or reads from
// one, then another - carries the newest commit it has seen as a token. A replica serves the read
// only once its main contains that commit, so the client never reads older than its own writes
// (read-your-writes) or than what it has already read (monotonic reads). A replica that has not
// caught up within a short wait answers BUSY with a retry-after rather than an older answer.

// DefaultSessionWait bounds how long a read waits for a replica to catch up to its token.
const DefaultSessionWait = 2 * time.Second

// ReplicaBehindError is a read whose session token names a commit this replica's main does not
// yet contain.
type ReplicaBehindError struct {
	Namespace string
	Commit    string
	Waited    time.Duration
}

func (e *ReplicaBehindError) Error() string {
	return fmt.Sprintf("kdb server: namespace %s has not yet received commit %s (waited %s); retry, or read from a replica that has it",
		e.Namespace, e.Commit, e.Waited)
}

// RetryAfter is how long a client should wait before reading here again.
func (e *ReplicaBehindError) RetryAfter() time.Duration { return 250 * time.Millisecond }

// AwaitCommit waits up to wait for this namespace's main to contain commitHex. It returns at once
// when commitHex is empty or already contained.
func (s *KdbServerRuntime) AwaitCommit(commitHex string, wait time.Duration) error {
	if commitHex == "" {
		return nil
	}
	want, err := codec.HashFromHex(commitHex)
	if err != nil {
		return fmt.Errorf("kdb server: session token %q is not a commit hash: %w", commitHex, err)
	}
	contains := func() bool {
		head, err := s.dag.Head()
		return err == nil && (head == want || (s.dag.HasCommit(want) && s.dag.IsAncestor(want, head)))
	}
	if contains() {
		return nil
	}
	start := time.Now()
	deadline := start.Add(wait)
	for delay := time.Millisecond; time.Now().Before(deadline); delay = min(delay*2, 50*time.Millisecond) {
		time.Sleep(min(delay, time.Until(deadline)))
		if contains() {
			return nil
		}
	}
	return &ReplicaBehindError{Namespace: s.Runtime.DefaultNamespace, Commit: commitHex, Waited: time.Since(start).Round(time.Millisecond)}
}

func (s *KdbServerRuntime) sessionWait() time.Duration {
	if s.SessionWait > 0 {
		return s.SessionWait
	}
	return DefaultSessionWait
}
