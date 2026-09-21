package peersync

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// TestIngestTreeMismatchLeavesHeadAndStorageUntouched is D6: a peer whose commits do not build
// the tree they declare must not move this node's head, and must not leave storage holding a
// tree no commit names. The host used to swallow MaterializeCommit's error and move the head
// anyway.
func TestIngestTreeMismatchLeavesHeadAndStorageUntouched(t *testing.T) {
	ns := "app/ingest-mismatch"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	genesis, _ := d.Head()
	genesisCommit, _ := d.GetCommitOrThrow(genesis)

	docID := newUUID(t)
	// A commit that writes a document but claims its parent's (empty) tree - internally
	// consistent as a commit (its hash covers what it says), wrong about its own state.
	liar, err := document.BuildCommit(
		[]codec.Hash{genesis}, ns, newUUID(t), codec.TimestampNow(), newUUID(t),
		[]document.Op{document.WriteOp{DocID: docID, Patch: `{"v":1}`}},
		genesisCommit.DocumentTreeHash, nil, "lying about its tree",
	)
	if err != nil {
		t.Fatal(err)
	}
	env := IngestEnv{DAG: d, Storage: store, NamespaceID: ns, ApplyToStorage: true}
	_, err = Ingest(env, []document.Commit{liar}, liar.Hash)
	var mismatch *TreeMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected TreeMismatchError, got %v", err)
	}
	if head, _ := d.Head(); head != genesis {
		t.Fatalf("head moved to %s despite the mismatch", head.Hex())
	}
	if !d.HasCommit(liar.Hash) {
		t.Fatal("the commit itself must still be stored - history is never refused, only adopted")
	}

	// Storage is back on the genesis tree: an honest commit on genesis now applies cleanly.
	other, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	remote := side{dag: other, storage: mem.NewInMemoryStorageAdapter()}
	honest := writeDoc(t, remote, ns, genesis, newUUID(t), `{"v":"honest"}`)
	if _, err := Ingest(env, []document.Commit{honest}, honest.Hash); err != nil {
		t.Fatalf("honest ingest after a refused one: %v", err)
	}
	if head, _ := d.Head(); head != honest.Hash {
		t.Fatalf("expected fast-forward to the honest commit, head is %s", head.Hex())
	}
	if doc, _ := store.GetDocument(ns, docID, honest.DocumentTreeHash); doc != nil {
		t.Fatal("the refused commit's document leaked into storage")
	}
}

// TestIngestFastForwardAcrossMergeWithForeignFirstParent pins why fast-forward applies a net
// effect instead of replaying commit by commit: a merge whose first parent is the *other* side's
// branch. Replay-by-commit checks the other side's commits against a tree that already contains
// this side's writes and refuses a valid history.
func TestIngestFastForwardAcrossMergeWithForeignFirstParent(t *testing.T) {
	ns := "app/ingest-ff-merge"
	// Merge parents are ordered by hash, so write until the remote's commit sorts first: that is
	// the case where the merge's first parent is the side the local node does not have.
	var local, remote side
	var l1, r1 document.Commit
	for {
		local, remote = forkTwoSides(t, ns)
		genesis, _ := local.dag.Head()
		l1 = writeDoc(t, local, ns, genesis, newUUID(t), `{"side":"local"}`)
		r1 = writeDoc(t, remote, ns, genesis, newUUID(t), `{"side":"remote"}`)
		if p0, _ := mergeCommitParents(l1.Hash, r1.Hash); p0 == r1.Hash {
			break
		}
	}
	setHead(t, remote, r1.Hash)
	setHead(t, local, l1.Hash)

	// The remote merges local's commit in with its own branch as first parent, then writes on.
	if err := remote.dag.PutCommit(l1, true); err != nil {
		t.Fatal(err)
	}
	remoteEnv := IngestEnv{DAG: remote.dag, Storage: remote.storage, NamespaceID: ns, ApplyToStorage: true}
	res, err := Ingest(remoteEnv, nil, l1.Hash)
	if err != nil || res.Outcome.Kind != OutcomeMerged {
		t.Fatalf("remote merge: outcome=%v err=%v", res.Outcome.Kind, err)
	}
	merge := *res.Outcome.MergeCommit
	if merge.ParentHashes[0] != r1.Hash {
		t.Fatalf("fixture wants the remote branch as first parent, got %v", merge.ParentHashes)
	}
	r2 := writeDoc(t, remote, ns, merge.Hash, newUUID(t), `{"after":"merge"}`)

	walked, err := commitsBetween(remote.dag, r2.Hash, l1.Hash)
	if err != nil {
		t.Fatal(err)
	}
	localEnv := IngestEnv{DAG: local.dag, Storage: local.storage, NamespaceID: ns, ApplyToStorage: true}
	res, err = Ingest(localEnv, walked, r2.Hash)
	if err != nil {
		t.Fatalf("fast-forward across the merge: %v", err)
	}
	if res.Outcome.Kind != OutcomeFastForwarded {
		t.Fatalf("expected fast-forward, got %v", res.Outcome.Kind)
	}
	if head, _ := local.dag.Head(); head != r2.Hash {
		t.Fatalf("expected head %s, got %s", r2.Hash.Hex(), head.Hex())
	}
}
