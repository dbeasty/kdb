package dag

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// shallowRoot builds a commit whose parent is not, and never will be, resident here - the shape
// of a snapshot's root: admitted without its history, hashing to itself.
func shallowRoot(t *testing.T, ns string, ops []document.Op) document.Commit {
	t.Helper()
	absentParent := codec.Hash{Bytes: [32]byte{0x5a}}
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	c, err := document.BuildCommit(
		[]codec.Hash{absentParent}, ns, txID, codec.TimestampNow(), author,
		ops, document.EmptyDocumentTree().TreeHash, nil, "snapshot root",
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestDropShallowCommitLeavesTheDagAsItWas: the inverse of PutShallowCommit undoes every index
// that call touched, so a namespace whose bootstrap failed looks untouched - which is what lets
// it be bootstrapped again (embed.CanInstallSnapshot counts commits).
func TestDropShallowCommitLeavesTheDagAsItWas(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := d.Head()
	// Ask for the tree index before the commit arrives, so it is being maintained rather than
	// built lazily afterwards - the state in which a stale entry would survive.
	d.CommitForTree(document.EmptyDocumentTree().TreeHash)
	before := d.AncestryVersion()

	root := shallowRoot(t, "ns", nil)
	if err := d.PutShallowCommit(root); err != nil {
		t.Fatal(err)
	}
	if d.CommitCount() != 2 || !d.IsShallow(root.Hash) {
		t.Fatalf("the root was not admitted: count=%d shallow=%v", d.CommitCount(), d.IsShallow(root.Hash))
	}

	if err := d.DropShallowCommit(root.Hash); err != nil {
		t.Fatalf("dropping the root: %v", err)
	}
	if n := d.CommitCount(); n != 1 {
		t.Fatalf("CommitCount is %d after the root was taken back, want 1 (genesis) - a bootstrap could never be retried", n)
	}
	if d.HasCommit(root.Hash) || d.IsShallow(root.Hash) || len(d.ShallowRoots()) != 0 {
		t.Fatal("the root is still resident or still listed as shallow")
	}
	if _, ok := d.GetCommitByTransactionID(root.TransactionID); ok {
		t.Fatal("the transaction index still resolves the dropped root")
	}
	if h, ok := d.CommitForTree(root.DocumentTreeHash); ok && h == root.Hash {
		t.Fatal("the tree index still names the dropped root")
	}
	if d.AncestryVersion() == before {
		t.Fatal("ancestry version did not change across an admission and its undo")
	}
	if h, err := d.Head(); err != nil || h != genesis {
		t.Fatalf("head is %v (err %v), want genesis", h, err)
	}
	if len(d.Horizon()) != 0 {
		t.Fatalf("the dropped root is still on the horizon: %v", d.Horizon())
	}
	// And the namespace really is bootstrappable again: the same root goes back in.
	if err := d.PutShallowCommit(root); err != nil {
		t.Fatalf("re-admitting the root after it was taken back: %v", err)
	}
	if !d.IsShallow(root.Hash) {
		t.Fatal("the re-admitted root is not shallow")
	}
}

// TestDropShallowCommitIsNotAGeneralDelete: an undo path may call it without knowing how far the
// admission got, but it will not take away a commit PutShallowCommit did not put there.
func TestDropShallowCommitIsNotAGeneralDelete(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.DropShallowCommit(codec.Hash{Bytes: [32]byte{0x11}}); err != nil {
		t.Fatalf("dropping a hash that is not here should be a no-op, got %v", err)
	}
	genesis, _ := d.Head()
	err = d.DropShallowCommit(genesis)
	var cons *ConsistencyError
	if !errors.As(err, &cons) {
		t.Fatalf("dropping an ordinary commit: want ConsistencyError, got %v", err)
	}
	if !d.HasCommit(genesis) {
		t.Fatal("a refused drop removed the commit anyway")
	}
}

// TestDropShallowCommitRefusesWhatSomethingStillNeeds: the same three retention roots Squash and
// StubCommit consult, plus a resident child - taking the root away would truncate its history
// rather than undo it.
func TestDropShallowCommitRefusesWhatSomethingStillNeeds(t *testing.T) {
	newDag := func(t *testing.T) (*InMemoryCommitDag, document.Commit) {
		t.Helper()
		d, err := NewInMemoryCommitDag("ns")
		if err != nil {
			t.Fatal(err)
		}
		root := shallowRoot(t, "ns", nil)
		if err := d.PutShallowCommit(root); err != nil {
			t.Fatal(err)
		}
		return d, root
	}
	wantRefused := func(t *testing.T, d *InMemoryCommitDag, root document.Commit, err error, detail string) {
		t.Helper()
		var safety *CompactionSafetyError
		if !errors.As(err, &safety) {
			t.Fatalf("want CompactionSafetyError, got %v", err)
		}
		// The blocker and the reason are what a caller reports; the message alone does not say
		// which branch, tag or child stood in the way.
		if safety.Blocker != root.Hash {
			t.Fatalf("the refusal blames %s, not the root %s", safety.Blocker.Hex(), root.Hash.Hex())
		}
		if !strings.Contains(err.Error()+" "+safety.Reason, detail) {
			t.Fatalf("the refusal does not say why (%s): %v reason=%q", detail, err, safety.Reason)
		}
		if !d.HasCommit(root.Hash) || !d.IsShallow(root.Hash) {
			t.Fatal("a refused drop took the root away anyway")
		}
	}

	t.Run("pinned by a reader", func(t *testing.T) {
		d, root := newDag(t)
		release := d.Pin(root.Hash)
		defer release()
		wantRefused(t, d, root, d.DropShallowCommit(root.Hash), "pinned")
	})

	t.Run("named by a branch head", func(t *testing.T) {
		d, root := newDag(t)
		if err := d.SetHead(mainBranch, root.Hash); err != nil {
			t.Fatal(err)
		}
		wantRefused(t, d, root, d.DropShallowCommit(root.Hash), "branch=main")
	})

	t.Run("tagged", func(t *testing.T) {
		d, root := newDag(t)
		if _, err := d.CreateTag("v1", root.Hash, ""); err != nil {
			t.Fatal(err)
		}
		wantRefused(t, d, root, d.DropShallowCommit(root.Hash), "tag=v1")
	})

	t.Run("has a resident child", func(t *testing.T) {
		d, root := newDag(t)
		if err := d.SetHead(mainBranch, root.Hash); err != nil {
			t.Fatal(err)
		}
		child, err := d.AppendCommit(newTx(root.Hash), root.Hash, document.EmptyDocumentTree(), nil, "")
		if err != nil {
			t.Fatal(err)
		}
		// Move main off the root so the branch-head check is not the one that refuses.
		if err := d.SetHead(mainBranch, child.Hash); err != nil {
			t.Fatal(err)
		}
		wantRefused(t, d, root, d.DropShallowCommit(root.Hash), "child="+child.Hash.Hex())
	})
}

// TestDropShallowCommitReturnsItsOperationsToTheBudget: the retention accounting is given back
// what the admission charged it. Left counted, the budget would evict operations from commits
// that did not need to go, for a commit that no longer exists.
func TestDropShallowCommitReturnsItsOperationsToTheBudget(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	loader := CommitOperationsLoader(func(codec.Hash) ([]document.Op, error) { return nil, nil })
	d.SetOperationsLoader(loader, 1<<20)
	before := d.OperationsResidentBytes()

	docID, _ := codec.RandomUUID()
	root := shallowRoot(t, "ns", []document.Op{document.WriteOp{DocID: docID, Patch: `{"pad":"` + strings.Repeat("z", 4096) + `"}`}})
	if err := d.PutShallowCommit(root); err != nil {
		t.Fatal(err)
	}
	if d.OperationsResidentBytes() <= before {
		t.Fatal("admitting a commit with operations did not charge the budget")
	}
	if err := d.DropShallowCommit(root.Hash); err != nil {
		t.Fatal(err)
	}
	if got := d.OperationsResidentBytes(); got != before {
		t.Fatalf("resident operation bytes are %d after the undo, want %d", got, before)
	}
	// Re-admitting charges it exactly once more: the LRU entry went with the commit, so the
	// "already tracked" short-circuit in trackOpsLocked cannot silently skip it.
	if err := d.PutShallowCommit(root); err != nil {
		t.Fatal(err)
	}
	charged := d.OperationsResidentBytes() - before
	if err := d.DropShallowCommit(root.Hash); err != nil {
		t.Fatal(err)
	}
	if got := d.OperationsResidentBytes(); got != before {
		t.Fatalf("after a second admission of %d bytes and its undo, resident is %d, want %d", charged, got, before)
	}
}
