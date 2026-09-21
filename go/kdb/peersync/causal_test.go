package peersync

import (
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// TestLastWriteRespectsCausalityUnderClockSkew: node A's clock runs ten minutes slow. It has
// seen a commit made at T0+60s, then overwrites document X - a write that happened, in real
// time, after B's write of X at T0. Stamped by A's clock alone it would read as T0-9min and lose
// last-write-wins to B's older write. Commits are stamped after their parents, so it reads as
// T0+60s+1µs and wins, on every node.
func TestLastWriteRespectsCausalityUnderClockSkew(t *testing.T) {
	ns := "app/causal-lww"
	a, b := forkTwoSides(t, ns)
	c := newSide(t, ns)
	genesis, _ := a.dag.Head()
	t0 := time.Now().Add(-time.Hour).UnixMicro()
	x, other := newUUID(t), newUUID(t)

	bWrite := writeDocAt(t, b, ns, genesis, x, `{"v":"b"}`, codec.TimestampFromEpochMicros(t0))
	setHead(t, b, bWrite.Hash)
	seen := writeDocAt(t, c, ns, genesis, other, `{"v":"c"}`, codec.TimestampFromEpochMicros(t0+60_000_000))

	envA := IngestEnv{DAG: a.dag, Storage: a.storage, NamespaceID: ns, ApplyToStorage: true}
	if _, err := Ingest(envA, []document.Commit{seen}, seen.Hash); err != nil {
		t.Fatal(err)
	}
	aWrite := writeDocAt(t, a, ns, seen.Hash, x, `{"v":"a"}`, codec.TimestampFromEpochMicros(t0-9*60_000_000))
	if aWrite.Timestamp.EpochMicros() != seen.Timestamp.EpochMicros()+1 {
		t.Fatalf("A's write should be stamped just after what it had seen, got %d", aWrite.Timestamp.EpochMicros())
	}

	envB := IngestEnv{DAG: b.dag, Storage: b.storage, NamespaceID: ns, ApplyToStorage: true,
		Resolution: ResolutionOptions{Policy: transaction.ConflictPolicyLastWrite}}
	res, err := Ingest(envB, []document.Commit{seen, aWrite}, aWrite.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome.Kind != OutcomeMerged {
		t.Fatalf("expected a merge, got %v", res.Outcome.Kind)
	}
	head, _ := b.dag.GetCommitOrThrow(res.Head)
	doc, err := b.storage.GetDocument(ns, x, head.DocumentTreeHash)
	if err != nil || doc == nil {
		t.Fatalf("read X: %v", err)
	}
	if doc.JSON != `{"v":"a"}` {
		t.Fatalf("the causally later write lost: X is %s", doc.JSON)
	}
}

// TestIngestRefusesFutureCommit: a commit from a node whose clock runs far ahead would drag every
// descendant into that future, so it is refused - and not stored.
func TestIngestRefusesFutureCommit(t *testing.T) {
	ns := "app/future"
	a, b := forkTwoSides(t, ns)
	genesis, _ := b.dag.Head()
	future := writeDocAt(t, b, ns, genesis, newUUID(t), `{"v":1}`, codec.TimestampFromEpochMicros(time.Now().Add(time.Hour).UnixMicro()))
	env := IngestEnv{DAG: a.dag, Storage: a.storage, NamespaceID: ns}
	_, err := Ingest(env, []document.Commit{future}, future.Hash)
	var skew *ClockSkewError
	if !errors.As(err, &skew) {
		t.Fatalf("expected ClockSkewError, got %v", err)
	}
	if a.dag.HasCommit(future.Hash) {
		t.Fatal("a refused commit was stored")
	}
}

func newSide(t *testing.T, ns string) side {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	return side{dag: d, storage: mem.NewInMemoryStorageAdapter()}
}

func setHead(t *testing.T, s side, h codec.Hash) {
	t.Helper()
	if err := s.dag.SetHead(mainBranch, h); err != nil {
		t.Fatal(err)
	}
}
