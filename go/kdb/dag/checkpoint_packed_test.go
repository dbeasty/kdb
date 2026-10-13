package dag

import (
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func TestPackedCheckpointCommitsRoundTrip(t *testing.T) {
	schema := h(5)
	in := []CheckpointCommit{
		{Commit: document.Commit{Hash: h(1), NamespaceID: "ns"}},
		{Commit: document.Commit{Hash: h(2), NamespaceID: "ns", ParentHashes: []codec.Hash{h(1)}, Message: "hello",
			Timestamp: codec.Timestamp{EpochMillis: 123456789, MicroRemainder: 999}, SchemaHash: &schema,
			TransactionID: codec.UUID{MSB: -1, LSB: 7}, AuthorNodeID: codec.UUID{MSB: 3, LSB: -9}, DocumentTreeHash: h(8)},
			OperationCount: 4},
		{Commit: document.Commit{Hash: h(3), NamespaceID: "ns", ParentHashes: []codec.Hash{h(1), h(2), h(9), h(10)}}, OperationCount: 1},
	}
	packed, err := PackCheckpointCommits(in)
	if err != nil {
		t.Fatal(err)
	}
	var out []CheckpointCommit
	if err := ForEachPackedCommit(packed, "ns", func(cc CheckpointCommit) error {
		out = append(out, cc)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("packed commits did not round-trip:\n in  %+v\n out %+v", in, out)
	}
	if err := ForEachPackedCommit(packed[:len(packed)-3], "ns", func(CheckpointCommit) error { return nil }); err == nil {
		t.Fatal("truncated packed commits accepted")
	}
}

// A checkpoint snapshot packs its commits and RestoreCheckpoint restores them to the same graph.
func TestCheckpointSnapshotPackedRestores(t *testing.T) {
	b := newDagBuilder(t)
	richGraph(b)
	b.diamond()
	state, _, _, err := b.dag.CheckpointSnapshotWithGraph(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Commits) != 0 || len(state.PackedCommits) == 0 {
		t.Fatalf("snapshot listed %d commits and packed %d bytes; want everything packed", len(state.Commits), len(state.PackedCommits))
	}
	fresh, _ := NewInMemoryCommitDag("ns")
	if err := fresh.RestoreCheckpoint(state); err != nil {
		t.Fatal(err)
	}
	if fresh.CommitCount() != b.dag.CommitCount() {
		t.Fatalf("restored %d commits, want %d", fresh.CommitCount(), b.dag.CommitCount())
	}
	for _, h := range allHashes(b) {
		want, _ := b.dag.GetCommit(h)
		got, ok := fresh.GetCommit(h)
		want.Operations, got.Operations = nil, nil
		if !ok || !reflect.DeepEqual(want, got) {
			t.Fatalf("commit %s changed through a packed checkpoint", b.name(h))
		}
	}
}
