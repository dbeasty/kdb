package dag

import (
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func h(b byte) codec.Hash { var x codec.Hash; x.Bytes[0] = b; return x }

// The reason this type exists. The resident tier must hold commits in memory the collector does
// not scan: if a chunk ever became something other than raw bytes, every commit would be traced
// again and nothing else would notice.
func TestResidentChunksArePointerFree(t *testing.T) {
	elem := reflect.TypeOf(nodeStore{}.chunks).Elem().Elem() // *[N]byte -> [N]byte
	if elem.Kind() != reflect.Array || elem.Elem().Kind() != reflect.Uint8 {
		t.Fatalf("resident chunks are %s, want an array of bytes", elem)
	}
	if elem.Len() != slotsPerChunk*graphRecordSize {
		t.Fatalf("chunk holds %d bytes, want %d records of %d", elem.Len(), slotsPerChunk, graphRecordSize)
	}
}

// Memory and disk share the record layout, including its spare bytes.
func TestRecordLayoutHasSpareBytes(t *testing.T) {
	if graphRecordSize-grReserved < 8 {
		t.Fatalf("only %d spare bytes in a record", graphRecordSize-grReserved)
	}
	var r [graphRecordSize]byte
	encodeRecord(r[:], document.Commit{Hash: h(1), Message: "m"}, 3, 2)
	for i := grReserved; i < graphRecordSize; i++ {
		if r[i] != 0 {
			t.Fatalf("encodeRecord wrote into the reserved byte %d", i)
		}
	}
}

func TestNodeStoreRoundTrip(t *testing.T) {
	schema := h(9)
	cases := []document.Commit{
		{Hash: h(1), NamespaceID: "ns", Message: "root"},
		{Hash: h(2), ParentHashes: []codec.Hash{h(1)}, NamespaceID: "ns", SchemaHash: &schema,
			Timestamp: codec.Timestamp{EpochMillis: 1234, MicroRemainder: 56}},
		{Hash: h(3), ParentHashes: []codec.Hash{h(1), h(2)}, NamespaceID: "other"},
		{Hash: h(4), ParentHashes: []codec.Hash{h(1), h(2), h(3), h(5)}, NamespaceID: "ns", Message: "octopus",
			DocumentTreeHash: h(7)},
	}
	s := newNodeStore()
	for i, c := range cases {
		s.put(c, uint32(i+1))
	}
	for i, want := range cases {
		got, ok := s.commit(want.Hash)
		if !ok {
			t.Fatalf("commit %d missing", i)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("commit %d round trip:\n got %+v\nwant %+v", i, got, want)
		}
		if g := s.generation(want.Hash); g != uint32(i+1) {
			t.Fatalf("generation %d = %d", i, g)
		}
	}
	var ps []codec.Hash
	s.forEachParent(h(4), func(p codec.Hash) bool { ps = append(ps, p); return true })
	if !reflect.DeepEqual(ps, cases[3].ParentHashes) {
		t.Fatalf("forEachParent = %v", ps)
	}
}

func TestNodeStoreDeleteReusesSlotAndClearsSideMaps(t *testing.T) {
	s := newNodeStore()
	s.put(document.Commit{Hash: h(1), Message: "m", Operations: []document.Op{document.DeleteOp{}}}, 1)
	s.delete(h(1))
	if s.has(h(1)) || len(s.messages) != 0 || len(s.ops) != 0 {
		t.Fatal("delete left state behind")
	}
	s.put(document.Commit{Hash: h(2)}, 1)
	if s.used != 1 {
		t.Fatalf("freed slot not reused: %d slots used", s.used)
	}
	if c, _ := s.commit(h(2)); c.Message != "" || len(c.Operations) != 0 {
		t.Fatalf("reused slot leaked old state: %+v", c)
	}
}

func TestNodeStoreReplaceKeepsGeneration(t *testing.T) {
	s := newNodeStore()
	s.put(document.Commit{Hash: h(1)}, 7)
	if !s.replace(document.Commit{Hash: h(1), Message: "x"}) {
		t.Fatal("replace of resident commit failed")
	}
	if s.generation(h(1)) != 7 {
		t.Fatal("replace lost the derived generation")
	}
	if s.replace(document.Commit{Hash: h(2)}) {
		t.Fatal("replace of absent commit must report false")
	}
}

// GraphMemoryGo must behave exactly like the record store: same answers for the whole rich graph,
// and the genesis commit carried across the switch.
func TestGoMemoryModeMatchesRecordMode(t *testing.T) {
	views := map[GraphMemory]graphView{}
	for _, mode := range []GraphMemory{GraphMemoryKDB, GraphMemoryGo} {
		b := newDagBuilder(t)
		b.dag.SetGraphSettings(GraphSettings{AncestryPruning: true, Memory: mode})
		if got := b.dag.nodes.goMode; got != (mode == GraphMemoryGo) {
			t.Fatalf("%s: goMode = %v", mode, got)
		}
		if !b.dag.HasCommit(b.root) {
			t.Fatalf("%s: genesis lost switching memory mode", mode)
		}
		richGraph(b)
		b.diamond()
		views[mode] = viewOfByName(t, b)
	}
	if !reflect.DeepEqual(views[GraphMemoryKDB].walk, views[GraphMemoryGo].walk) {
		t.Fatalf("walks differ:\n kdb %v\n go  %v", views[GraphMemoryKDB].walk, views[GraphMemoryGo].walk)
	}
	if views[GraphMemoryKDB].count != views[GraphMemoryGo].count {
		t.Fatal("commit counts differ between memory modes")
	}
}

// viewOfByName is viewOf keyed for comparing two DAGs whose hashes differ (each has its own
// genesis and random transaction ids): walk order by name, and the count.
func viewOfByName(t *testing.T, b *dagBuilder) graphView {
	t.Helper()
	v := graphView{count: b.dag.CommitCount()}
	head, _ := b.dag.Head()
	for _, e := range b.dag.Walk(head, nil, 1<<30) {
		if f, ok := e.(FullEntry); ok {
			v.walk = append(v.walk, b.name(f.Commit.Hash))
		}
	}
	return v
}
