package dag

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// peerCommit builds a commit on parent carrying a padded write, as a peer would send it.
func peerCommit(t *testing.T, parent codec.Hash, pad int) document.Commit {
	t.Helper()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	docID, _ := codec.RandomUUID()
	ops := []document.Op{document.WriteOp{DocID: docID, Patch: `{"pad":"` + strings.Repeat("p", pad) + `"}`}}
	c, err := document.BuildCommit(
		[]codec.Hash{parent}, "ns", txID, codec.TimestampNow(), author,
		ops, document.EmptyDocumentTree().TreeHash, nil, "from a peer",
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func checkpointHolds(state CheckpointState, h codec.Hash) bool {
	for _, cc := range state.Commits {
		if cc.Commit.Hash == h {
			return true
		}
	}
	return false
}

// TestCheckpointLeavesOutCommitsNeverLogged: a commit received from a peer and not adopted is
// not in the delta log, so a checkpoint - which carries no operations and relies on the log for
// them - must not describe it, nor keep a ref (a conflict's tracking branch, a tag) naming it.
// Once logged it is described like any other commit.
func TestCheckpointLeavesOutCommitsNeverLogged(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := d.Head()
	c := peerCommit(t, genesis, 64)
	child := peerCommit(t, c.Hash, 64)
	for _, x := range []document.Commit{c, child} {
		if err := d.PutUnloggedCommit(x); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.CreateBranch("peer-sync/tracking/n2/main", child.Hash); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CreateTag("seen", child.Hash, ""); err != nil {
		t.Fatal(err)
	}

	state := d.CheckpointSnapshot()
	if checkpointHolds(state, c.Hash) || checkpointHolds(state, child.Hash) {
		t.Fatal("the checkpoint describes commits that were never logged")
	}
	for _, b := range state.Branches {
		if b.HeadHash == child.Hash {
			t.Fatalf("the checkpoint keeps branch %s, which names an unlogged commit", b.Name)
		}
	}
	if len(state.Tags) != 0 {
		t.Fatal("the checkpoint keeps a tag naming an unlogged commit")
	}
	if !checkpointHolds(state, genesis) || len(state.Branches) != 1 {
		t.Fatalf("the checkpoint lost logged state: %d branches", len(state.Branches))
	}

	// Restoring it gives a DAG without them, as a crash would have.
	restored, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreCheckpoint(state); err != nil {
		t.Fatal(err)
	}
	if restored.HasCommit(c.Hash) || restored.HasCommit(child.Hash) {
		t.Fatal("the restored DAG holds the unlogged commits")
	}

	d.MarkLogged(c.Hash)
	if d.IsUnlogged(c.Hash) || !d.IsUnlogged(child.Hash) {
		t.Fatal("MarkLogged cleared the wrong mark")
	}
	if !checkpointHolds(d.CheckpointSnapshot(), c.Hash) {
		t.Fatal("a logged commit is missing from the checkpoint")
	}
}

// TestUnloggedOperationsAreNotEvicted: the operations budget evicts only what the log can load
// back. An unlogged commit's operations exist only in memory, so they stay until it is logged.
func TestUnloggedOperationsAreNotEvicted(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	loader := CommitOperationsLoader(func(h codec.Hash) ([]document.Op, error) {
		return nil, fmt.Errorf("no frame for %s: %w", h.Hex(), ErrOperationsNotLogged)
	})
	d.SetOperationsLoader(loader, 1024)
	genesis, _ := d.Head()
	c := peerCommit(t, genesis, 4096)
	if err := d.PutUnloggedCommit(c); err != nil {
		t.Fatal(err)
	}
	next := peerCommit(t, c.Hash, 4096)
	if err := d.PutUnloggedCommit(next); err != nil {
		t.Fatal(err)
	}
	if d.OperationsEvicted(c.Hash) {
		t.Fatal("an unlogged commit's operations were evicted")
	}
	if _, err := d.CommitOperations(c.Hash); err != nil {
		t.Fatalf("reading an unlogged commit's operations: %v", err)
	}
}

// TestRefillUnloggedCommit: a commit restored without operations the log can supply - what an
// earlier version's checkpoint left behind - takes them back from a peer's copy that reproduces
// its hash, and only from one that does.
func TestRefillUnloggedCommit(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	loader := CommitOperationsLoader(func(h codec.Hash) ([]document.Op, error) {
		return nil, fmt.Errorf("no frame for %s: %w", h.Hex(), ErrOperationsNotLogged)
	})
	d.SetOperationsLoader(loader, 1<<20)
	genesis, _ := d.Head()
	c := peerCommit(t, genesis, 64)
	stripped := c
	stripped.Operations = nil
	if err := d.RestoreCheckpoint(CheckpointState{Commits: []CheckpointCommit{{Commit: stripped, OperationCount: 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CommitOperations(c.Hash); !errors.Is(err, ErrOperationsNotLogged) {
		t.Fatalf("precondition: want the loader's no-frame error, got %v", err)
	}

	forged := c
	forged.Operations = []document.Op{document.WriteOp{DocID: c.Operations[0].(document.WriteOp).DocID, Patch: `{"forged":true}`}}
	if err := d.RefillUnloggedCommit(forged); err == nil {
		t.Fatal("a copy that does not reproduce the hash was accepted")
	}
	if err := d.RefillUnloggedCommit(c); err != nil {
		t.Fatal(err)
	}
	ops, err := d.CommitOperations(c.Hash)
	if err != nil || len(ops) != 1 {
		t.Fatalf("after the refill: %d ops, err %v", len(ops), err)
	}
	if !d.IsUnlogged(c.Hash) {
		t.Fatal("a refilled commit is not marked unlogged, so the next checkpoint would describe it again")
	}
}
