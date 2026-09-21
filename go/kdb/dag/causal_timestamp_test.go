package dag

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TestCommitTimestampExceedsParents: a transaction stamped earlier than its parent - a writer
// whose clock runs behind the node that made the parent - still produces a commit that is later
// than the parent, by exactly one microsecond.
func TestCommitTimestampExceedsParents(t *testing.T) {
	d, err := NewInMemoryCommitDag("app/causal")
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := d.Head()
	gc, _ := d.GetCommitOrThrow(genesis)
	id, _ := codec.RandomUUID()
	future := codec.TimestampFromEpochMicros(gc.Timestamp.EpochMicros() + 10_000_000)
	first, err := d.AppendCommit(document.Transaction{ID: id, BaseVersion: genesis, Timestamp: future}, genesis, document.EmptyDocumentTree(), nil, "first")
	if err != nil {
		t.Fatal(err)
	}
	if first.Timestamp != future {
		t.Fatalf("a timestamp already past the parent must be kept as given, got %d want %d", first.Timestamp.EpochMicros(), future.EpochMicros())
	}
	id2, _ := codec.RandomUUID()
	past := codec.TimestampFromEpochMicros(future.EpochMicros() - 5_000_000)
	second, err := d.AppendCommit(document.Transaction{ID: id2, BaseVersion: first.Hash, Timestamp: past}, first.Hash, document.EmptyDocumentTree(), nil, "second")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := second.Timestamp.EpochMicros(), future.EpochMicros()+1; got != want {
		t.Fatalf("commit timestamp %d, want parent+1µs = %d", got, want)
	}
}
