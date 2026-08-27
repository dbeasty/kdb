package embed

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// TestMaterializeCommitRejectsTreeHashMismatch is the regression test for the finding recorded
// in docs/kdb-finish-up-plan.md as 1-G2: MaterializeCommit built the tree from a commit's
// operations and discarded the result without ever comparing it against the commit's own
// declared DocumentTreeHash - a peer-ingested commit whose declared tree didn't match what its
// operations actually produce (corruption or tampering in transit) was silently accepted, and
// its documents became unreadable through the ordinary GetDocument path (which reads by the
// commit's declared tree hash), not obviously broken at the point where it actually went wrong.
func TestMaterializeCommitRejectsTreeHashMismatch(t *testing.T) {
	const ns = "app/data"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("NewInMemoryCommitDag: %v", err)
	}
	genesis, err := d.Head()
	if err != nil {
		t.Fatalf("genesis head: %v", err)
	}

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	authorID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}

	// A tampered/corrupt commit: declares a DocumentTreeHash that has nothing to do with the
	// operation it actually carries.
	bogusTree, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	bogusTreeHash, err := codec.HashFromBytes(append(bogusTree.Bytes(), bogusTree.Bytes()...))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := document.BuildCommit(
		[]codec.Hash{genesis}, ns, txID, codec.TimestampNow(), authorID,
		[]document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"tampered"}`}},
		bogusTreeHash, nil, "tampered",
	)
	if err != nil {
		t.Fatalf("BuildCommit: %v", err)
	}
	if err := d.PutCommit(commit, true); err != nil {
		t.Fatalf("PutCommit: %v", err)
	}

	store := mem.NewInMemoryStorageAdapter()
	err = MaterializeCommit(store, d, ns, commit)
	if err == nil {
		t.Fatal("expected MaterializeCommit to reject a commit whose declared tree hash doesn't match its operations")
	}
	var mismatch *TreeHashMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected *TreeHashMismatchError, got %T: %v", err, err)
	}
	if mismatch.CommitHash != commit.Hash {
		t.Fatalf("expected CommitHash %s, got %s", commit.Hash.Hex(), mismatch.CommitHash.Hex())
	}
	if mismatch.Declared != bogusTreeHash {
		t.Fatalf("expected Declared %s, got %s", bogusTreeHash.Hex(), mismatch.Declared.Hex())
	}
	if mismatch.Built == bogusTreeHash {
		t.Fatal("expected Built to be the tree actually produced by the operations, not the bogus declared one")
	}
}

// TestMaterializeCommitAcceptsCorrectTreeHash is the positive counterpart: a commit whose
// declared DocumentTreeHash genuinely matches what its operations produce still materializes
// cleanly (the fix in 1-G2 must not reject legitimate commits).
func TestMaterializeCommitAcceptsCorrectTreeHash(t *testing.T) {
	const ns = "app/data"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("NewInMemoryCommitDag: %v", err)
	}
	genesis, err := d.Head()
	if err != nil {
		t.Fatalf("genesis head: %v", err)
	}
	genesisCommit, err := d.GetCommitOrThrow(genesis)
	if err != nil {
		t.Fatal(err)
	}

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}

	// Precompute the real resulting tree the same way the real write path does (finalize
	// Transaction's CommitTree call), on a throwaway store, so the test commit's declared
	// DocumentTreeHash is genuinely correct.
	scratch := mem.NewInMemoryStorageAdapter()
	if err := scratch.PutDocument(ns, document.Document{ID: docID, JSON: `{"v":"real"}`}); err != nil {
		t.Fatal(err)
	}
	tree, err := scratch.CommitTree(ns, genesisCommit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}

	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	authorID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	commit, err := document.BuildCommit(
		[]codec.Hash{genesis}, ns, txID, codec.TimestampNow(), authorID,
		[]document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"real"}`}},
		tree.TreeHash, nil, "legit",
	)
	if err != nil {
		t.Fatalf("BuildCommit: %v", err)
	}
	if err := d.PutCommit(commit, true); err != nil {
		t.Fatalf("PutCommit: %v", err)
	}

	store := mem.NewInMemoryStorageAdapter()
	if err := MaterializeCommit(store, d, ns, commit); err != nil {
		t.Fatalf("expected a correctly-declared commit to materialize cleanly, got: %v", err)
	}
	got, err := store.GetDocument(ns, docID, tree.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JSON != `{"v":"real"}` {
		t.Fatalf("expected materialized document, got %+v", got)
	}
}
