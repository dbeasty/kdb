package embed

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

func newID(t *testing.T) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestGroupMarkerRoundTrips(t *testing.T) {
	m := GroupMarker{Epoch: 42, Group: newID(t), Parts: []string{"bank/accounts", "bank/ledger"}}
	got, ok := ParseGroupMarker(m.Message())
	if !ok || got.Epoch != 42 || got.Group != m.Group || len(got.Parts) != 2 || got.Parts[1] != "bank/ledger" {
		t.Fatalf("round trip: %+v %v", got, ok)
	}
	for _, msg := range []string{"", "revert to abc", "kdb:xns/1 epoch=x group=y", "kdb:xns/1 parts=a"} {
		if _, ok := ParseGroupMarker(msg); ok {
			t.Errorf("%q parsed as a group marker", msg)
		}
	}
}

// A torn record at the tail is an unacknowledged write, and reading stops there without an error.
func TestDecisionLogReadsBackAndToleratesATornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "decisions-0000000000000001.log")
	l, err := createDecisionLog(path, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	ids := []codec.UUID{newID(t), newID(t), newID(t)}
	for _, id := range ids {
		wait, err := l.enqueue(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := wait(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.enqueue(newID(t)); !errors.Is(err, ErrDecisionLogClosed) {
		t.Errorf("enqueue after close: %v", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5, 6, 7}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	got, err := readDecisions(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) {
		t.Fatalf("read %d decisions, want %d", len(got), len(ids))
	}
	for _, id := range ids {
		if _, ok := got[id]; !ok {
			t.Errorf("decision %s missing", id)
		}
	}
}

func partCommit(epoch uint64, group codec.UUID) document.Commit {
	return hostPart("", epoch, group)
}

func hostPart(host string, epoch uint64, group codec.UUID) document.Commit {
	return document.Commit{Message: GroupMarker{Host: host, Epoch: epoch, Group: group, Parts: []string{"a", "b"}}.Message()}
}

func writeDecisionFile(t *testing.T, root string, epoch uint64, suffix string, committed ...codec.UUID) {
	t.Helper()
	if err := os.MkdirAll(txnDir(root), 0o755); err != nil {
		t.Fatal(err)
	}
	l, err := createDecisionLog(decisionPath(root, epoch, decisionLiveSuffix), epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range committed {
		wait, err := l.enqueue(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := wait(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.close(); err != nil {
		t.Fatal(err)
	}
	if suffix != decisionLiveSuffix {
		if err := os.Rename(decisionPath(root, epoch, decisionLiveSuffix), decisionPath(root, epoch, suffix)); err != nil {
			t.Fatal(err)
		}
	}
}

// The resolution table in docs/kdb-cross-namespace-transactions-plan.md §4.2, row by row.
func TestPartResolution(t *testing.T) {
	root := t.TempDir()
	committed, cutOff := newID(t), newID(t)
	writeDecisionFile(t, root, 5, decisionLiveSuffix, committed)

	// A reader attached while epoch 5's writer may still be alive: undecided means held.
	reader, err := openTxnCoordinator(root, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	if d, _ := reader.resolvePart(partCommit(5, committed)); d != partCommitted {
		t.Errorf("reader, decided group: %v", d)
	}
	if d, _ := reader.resolvePart(partCommit(5, cutOff)); d != partHeld {
		t.Errorf("reader, undecided group of a live epoch: want held, got %v", d)
	}

	// A writer opening now holds the lock, so epoch 5's writer is gone: its file becomes dead.
	writer, err := openTxnCoordinator(root, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(decisionPath(root, 5, decisionDeadSuffix)); err != nil {
		t.Fatalf("the leftover epoch was not marked dead: %v", err)
	}
	if d, _ := writer.resolvePart(partCommit(5, committed)); d != partCommitted {
		t.Errorf("writer, decided group of a dead epoch: %v", d)
	}
	if d, _ := writer.resolvePart(partCommit(5, cutOff)); d != partAborted {
		t.Errorf("writer, undecided group of a dead epoch: want aborted, got %v", d)
	}
	if d, _ := reader.resolvePart(partCommit(5, cutOff)); d != partAborted {
		t.Errorf("reader, once the epoch is dead: want aborted, got %v", d)
	}
	// No file at all: the epoch was sealed, which means everything in it committed.
	if d, _ := writer.resolvePart(partCommit(3, cutOff)); d != partCommitted {
		t.Errorf("sealed epoch: want committed, got %v", d)
	}
	if _, isPart := writer.resolvePart(document.Commit{Message: "ordinary"}); isPart {
		t.Error("an ordinary commit resolved as a part")
	}

	// The writer's own epoch: in flight is held, finished-without-decision is aborted.
	g, err := writer.Begin([]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if g.Epoch != 6 {
		t.Errorf("new epoch %d, want 6 (one past the dead one)", g.Epoch)
	}
	if writer.hostID == "" {
		t.Fatal("starting an epoch did not assign the data root a host id")
	}
	own := func() document.Commit { return hostPart(writer.hostID, g.Epoch, g.ID) }
	if d, _ := writer.resolvePart(own()); d != partHeld {
		t.Errorf("own epoch, in flight: want held, got %v", d)
	}
	g.Abandon()
	if d, _ := writer.resolvePart(own()); d != partAborted {
		t.Errorf("own epoch, never decided: want aborted, got %v", d)
	}
	// The same epoch and group from another data root - a part that arrived by peer sync - is
	// not this log's to judge.
	if d, isPart := writer.resolvePart(hostPart("some-other-host", g.Epoch, g.ID)); d != partCommitted || !isPart {
		t.Errorf("foreign part: want committed, got %v", d)
	}
	if err := writer.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(decisionPath(root, 6, decisionLiveSuffix)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a clean close did not seal the epoch: %v", err)
	}
	if _, err := os.Stat(decisionPath(root, 5, decisionDeadSuffix)); err != nil {
		t.Errorf("sealing the new epoch removed the dead one's record: %v", err)
	}
}

// A checkpoint must never capture a part whose group is undecided.
func TestCheckpointIsDeferredWhileAPartIsProvisional(t *testing.T) {
	root := t.TempDir()
	rt, err := OpenFileRuntime(root, "demo", "demo/a", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	d := rt.DAG.(*PersistingCommitDAG).Delegate()
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	headCommit, _ := d.GetCommit(head)
	doc, err := document.FromJSONWithID(newID(t), `{"x":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Storage.PutDocument("demo/a", doc); err != nil {
		t.Fatal(err)
	}
	tree, err := rt.Storage.CommitTree("demo/a", headCommit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	c, err := d.AppendCommitWith(document.Transaction{ID: newID(t), BaseVersion: head, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.WriteOp{DocID: doc.ID, Patch: doc.JSON}}},
		head, tree, nil, "part", dag.AppendOptions{Provisional: true})
	if err != nil {
		t.Fatal(err)
	}
	err = saveCheckpoint(d, rt.Storage, rt.deltaReader, rt.shim, "demo/a", 0, 0, false)
	if !errors.Is(err, ErrCheckpointDeferred) {
		t.Fatalf("checkpoint with a provisional commit: want ErrCheckpointDeferred, got %v", err)
	}
	d.SettleProvisional(c.Hash)
	if err := saveCheckpoint(d, rt.Storage, rt.deltaReader, rt.shim, "demo/a", 0, 0, false); errors.Is(err, ErrCheckpointDeferred) {
		t.Fatal("checkpoint still deferred after the part settled")
	}
}

func TestValidateNamespaceID(t *testing.T) {
	for _, ok := range []string{"bank/accounts", "zolik", "a.b/c-d_e", "A1/B2/C3"} {
		if err := ValidateNamespaceID(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "../etc", "a/../b", "a//b", "/abs", "a/", "_system/users", "a b", "a\\b", "ns/.", string(make([]byte, 201))} {
		if err := ValidateNamespaceID(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
