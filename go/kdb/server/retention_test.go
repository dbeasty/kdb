package server

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// advance commits one write and returns the new head, so a test can push a previously-pinned
// commit into the interior of the history where compaction is allowed to reclaim it.
func advance(t *testing.T, srv *KdbServerRuntime, ns string) codec.Hash {
	t.Helper()
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert(ns, docID, `{"v":1}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return head
}

// A SNAPSHOT session reads at a fixed commit for the length of its transaction, and nothing used
// to stop that commit being squashed away underneath it: a read pin is not a branch head, which
// was the only retention root Squash consulted. This is that hole, closed.
func TestSnapshotSessionPinBlocksSquash(t *testing.T) {
	srv := newTestRuntime(t)
	ns := "app/data"
	sessions := NewSessionManager(srv)

	pinnedHead := advance(t, srv, ns)
	sess, err := sessions.Begin(ns, Snapshot, "", "", auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.dag.IsPinned(pinnedHead) {
		t.Fatal("a SNAPSHOT session did not pin the commit it reads at")
	}

	// Two more commits, so the pinned one is interior history - exactly what compaction targets.
	advance(t, srv, ns)
	advance(t, srv, ns)

	_, err = srv.dag.Squash([]codec.Hash{pinnedHead}, pinnedHead, document.EmptyDocumentTree(), nil, "squash")
	var safety *dag.CompactionSafetyError
	if !errors.As(err, &safety) {
		t.Fatalf("expected squash to refuse a commit an open SNAPSHOT session reads at, got %v", err)
	}

	// Ending the session releases it - a pin is held for the length of a transaction, not forever.
	sessions.End(sess.ID.Value)
	if srv.dag.IsPinned(pinnedHead) {
		t.Fatal("session end did not release the read pin")
	}
	if _, err := srv.dag.Squash([]codec.Hash{pinnedHead}, pinnedHead, document.EmptyDocumentTree(), nil, "squash"); err != nil {
		t.Fatalf("squash after the session ended: %v", err)
	}
}

// The pin follows the transaction, not the session: committing starts a new transaction at a new
// commit, and the version the previous one was reading at must stop being held. Otherwise a
// long-lived session accumulates a pin per transaction and compaction never reclaims anything.
func TestSnapshotPinMovesAtTransactionBoundary(t *testing.T) {
	srv := newTestRuntime(t)
	ns := "app/data"
	sessions := NewSessionManager(srv)

	first := advance(t, srv, ns)
	sess, err := sessions.Begin(ns, Snapshot, "", "", auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.dag.IsPinned(first) {
		t.Fatalf("expected the opening head to be pinned")
	}

	second := advance(t, srv, ns)
	sess.startTransactionAt(second)
	if srv.dag.IsPinned(first) {
		t.Fatal("the previous transaction's pin outlived its transaction")
	}
	if !srv.dag.IsPinned(second) {
		t.Fatal("the new transaction did not pin its own read version")
	}
	if got := srv.dag.PinnedCount(); got != 1 {
		t.Fatalf("expected exactly one live pin, got %d", got)
	}

	sessions.End(sess.ID.Value)
	if got := srv.dag.PinnedCount(); got != 0 {
		t.Fatalf("expected pins to drain to 0 after session end, got %d", got)
	}
}

// READ_COMMITTED and READ_YOUR_WRITES follow the live head, which is a branch head and therefore
// already a retention root. Pinning it too would be a second, redundant hold that compaction
// would then have to wait on.
func TestNonSnapshotSessionsDoNotPin(t *testing.T) {
	srv := newTestRuntime(t)
	sessions := NewSessionManager(srv)
	for _, consistency := range []ReadConsistency{ReadCommitted, ReadYourWrites} {
		if _, err := sessions.Begin("app/data", consistency, "", "", auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := srv.dag.PinnedCount(); got != 0 {
		t.Fatalf("%s/%s should not pin, got %d live pins", ReadCommitted, ReadYourWrites, got)
	}
}

// An in-flight commit's base version is the other thing nothing rooted: it is resolved before the
// writer queues at the write gate and not consulted until it reaches the front, and Commit hard
// -fails if it was reclaimed in between. Checked by observing the pin from inside the commit.
func TestInFlightCommitPinsItsBaseVersion(t *testing.T) {
	srv := newTestRuntime(t)
	ns := "app/data"
	base := advance(t, srv, ns)

	pinnedDuringCommit := false
	srv.CommitListener = func(string, document.Commit) {
		pinnedDuringCommit = srv.dag.IsPinned(base)
	}
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Commit(ns, document.Transaction{
		ID:          txID,
		BaseVersion: base,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":1}`}},
		Timestamp:   codec.TimestampNow(),
	}, "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if !pinnedDuringCommit {
		t.Fatal("a commit in flight did not pin the base version it depends on")
	}
	if srv.dag.IsPinned(base) {
		t.Fatal("the base-version pin outlived the commit")
	}
}
