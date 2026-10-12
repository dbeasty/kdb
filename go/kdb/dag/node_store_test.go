package dag

import (
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func h(b byte) codec.Hash { var x codec.Hash; x.Bytes[0] = b; return x }

// The reason this type exists. If a field with a pointer is ever added to slot, the garbage
// collector goes back to scanning every commit and nothing else would notice.
func TestSlotHasNoPointers(t *testing.T) {
	var check func(rt reflect.Type, path string)
	check = func(rt reflect.Type, path string) {
		switch rt.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.String, reflect.Map, reflect.Interface, reflect.Chan, reflect.Func, reflect.UnsafePointer:
			t.Fatalf("%s is a %s: slot must be pointer-free", path, rt.Kind())
		case reflect.Struct:
			for i := 0; i < rt.NumField(); i++ {
				check(rt.Field(i).Type, path+"."+rt.Field(i).Name)
			}
		case reflect.Array:
			check(rt.Elem(), path+"[]")
		}
	}
	check(reflect.TypeOf(slot{}), "slot")
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
