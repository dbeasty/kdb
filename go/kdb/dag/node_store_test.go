package dag

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func h(b byte) codec.Hash { var x codec.Hash; x.Bytes[0] = b; return x }

// The reason this type exists. The resident tier must hold commits in memory the collector does
// not scan: if a chunk ever became something other than raw bytes, every commit would be traced
// again and nothing else would notice.
func TestResidentChunksArePointerFree(t *testing.T) {
	elem := reflect.TypeOf(nodeStore{}.chunks).Elem() // []byte
	if elem.Kind() != reflect.Slice || elem.Elem().Kind() != reflect.Uint8 {
		t.Fatalf("resident chunks are %s, want byte slices", elem)
	}
}

// A namespace of a few commits must not pay for a full chunk, and growth must keep every record.
func TestFirstChunkStartsSmallAndGrows(t *testing.T) {
	s := newNodeStore()
	s.setChunkSlots(64)
	s.put(document.Commit{Hash: h(1), Message: "m"}, 1)
	if got := cap(s.chunks[0]); got != minChunkSlots*graphRecordSize {
		t.Fatalf("first chunk %d bytes, want %d", got, minChunkSlots*graphRecordSize)
	}
	for i := 2; i <= 200; i++ {
		var x codec.Hash
		x.Bytes[0], x.Bytes[1] = byte(i), byte(i>>8)
		s.put(document.Commit{Hash: x, Message: "m"}, uint32(i))
	}
	if got := len(s.chunks[0]); got != 64*graphRecordSize {
		t.Fatalf("first chunk %d bytes, want it grown to %d", got, 64*graphRecordSize)
	}
	if len(s.chunks) != 4 {
		t.Fatalf("%d chunks for 200 commits at 64 a chunk, want 4", len(s.chunks))
	}
	for i := 2; i <= 200; i++ {
		var x codec.Hash
		x.Bytes[0], x.Bytes[1] = byte(i), byte(i>>8)
		if c, ok := s.commit(x); !ok || c.Message != "m" {
			t.Fatalf("commit %d lost across growth", i)
		}
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

// The regression this guards: every namespace's first commit allocated a full 1 MiB chunk, so a
// server holding thousands of small namespaces (one per match, say) held gigabytes of mostly
// empty graph. A small namespace must cost kilobytes, by the accounting and by the real heap.
func TestThousandsOfSmallNamespacesStaySmall(t *testing.T) {
	const n = 2000
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	dags := make([]*InMemoryCommitDag, n)
	for i := range dags {
		d, err := NewInMemoryCommitDag(fmt.Sprintf("match/%d", i))
		if err != nil {
			t.Fatal(err)
		}
		dags[i] = d
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	if per := dags[0].GraphResidentBytes(); per > 16<<10 {
		t.Fatalf("a fresh namespace accounts %d bytes of graph, want a few KiB", per)
	}
	if per := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / n; per > 64<<10 {
		t.Fatalf("a fresh namespace holds %d heap bytes, want well under the old 1 MiB chunk", per)
	}
	runtime.KeepAlive(dags)
}

func TestChunkBytesSettingBoundsTheChunk(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	s := d.GraphSettings()
	s.ChunkBytes = 100 // below one growth step: clamped, not honoured literally
	if got := s.normalized().ChunkBytes; got != minChunkSlots*graphRecordSize {
		t.Fatalf("ChunkBytes clamped to %d, want %d", got, minChunkSlots*graphRecordSize)
	}
	s.ChunkBytes = 64 * graphRecordSize
	d2, _ := NewInMemoryCommitDag("ns2")
	d2.SetGraphSettings(s)
	for i := 0; i < 200; i++ {
		var x codec.Hash
		x.Bytes[0], x.Bytes[1] = byte(i+1), byte((i+1)>>8)
		d2.nodes.put(document.Commit{Hash: x}, 1)
	}
	for i, c := range d2.nodes.chunks {
		if len(c) > 64*graphRecordSize {
			t.Fatalf("chunk %d is %d bytes, over the configured %d", i, len(c), 64*graphRecordSize)
		}
	}
}
