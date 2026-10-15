package index

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// KeyGroups pages one key type in key order, whole keys at a time, and never runs into the next
// type; Unfiled names the documents filed under no key; KeysOnlyOfTypes sees a key of another
// type; a read that needs the undo window is refused rather than answered from the head.
func TestKeyGroupsPageOneTypeInOrder(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("app/kg")
	if err != nil {
		t.Fatal(err)
	}
	n := int64(0)
	commit := func() codec.Hash {
		head, _ := d.Head()
		n++
		c, err := d.AppendCommit(document.Transaction{ID: codec.UUID{MSB: n, LSB: 3}, BaseVersion: head}, head, document.EmptyDocumentTree(), nil, "")
		if err != nil {
			t.Fatal(err)
		}
		return c.Hash
	}
	s := NewMemoryStore(Descriptor{FieldName: "a", Fields: []string{"a"}, Type: IndexTypeBTree}, d)
	c1 := commit()
	docs := []string{`{"a":3}`, `{"a":1}`, `{"a":"x"}`, `{"a":[2,"y",true]}`, `{"b":1}`, `{"a":{"o":1}}`, `{"a":null}`, `{"a":1.5}`, `{"a":1}`}
	ids := make([]codec.UUID, len(docs))
	for i, body := range docs {
		ids[i], _ = codec.RandomUUID()
		if err := s.PutDocument(ids[i], c1, body); err != nil {
			t.Fatal(err)
		}
	}
	keysOf := func(kind, after Key, max int) string {
		t.Helper()
		groups, err := s.KeyGroups(kind, after, &c1, max)
		if err != nil {
			t.Fatal(err)
		}
		out := ""
		for _, g := range groups {
			out += fmt.Sprintf("%s:%d ", KeyString(g.Key), len(g.DocIDs))
		}
		return out
	}
	all := keysOf(Int64Key{}, nil, 100)
	if all != fmt.Sprintf("%s:2 %s:1 %s:1 ", KeyString(Int64Key{Value: 1}), KeyString(Int64Key{Value: 2}), KeyString(Int64Key{Value: 3})) {
		t.Fatalf("ints: %s", all)
	}
	if got := keysOf(Int64Key{}, nil, 2); got != fmt.Sprintf("%s:2 ", KeyString(Int64Key{Value: 1})) {
		t.Fatalf("first page: %s", got)
	}
	if got := keysOf(Int64Key{}, Int64Key{Value: 1}, 1); got != fmt.Sprintf("%s:1 ", KeyString(Int64Key{Value: 2})) {
		t.Fatalf("after 1: %s", got)
	}
	if got := keysOf(Int64Key{}, Int64Key{Value: 3}, 10); got != "" {
		t.Fatalf("after the last int: %s", got)
	}
	if got := keysOf(StringKey{}, nil, 10); got != fmt.Sprintf("%s:1 %s:1 ", KeyString(StringKey{Value: "x"}), KeyString(StringKey{Value: "y"})) {
		t.Fatalf("strings: %s", got)
	}
	if got := keysOf(NullKey{}, nil, 10); got != KeyString(NullKey{})+":1 " {
		t.Fatalf("null: %s", got)
	}
	unfiled, err := s.Unfiled(ids, &c1)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(unfiled) != fmt.Sprint([]codec.UUID{ids[4], ids[5]}) {
		t.Fatalf("unfiled: %v, want the absent field and the object", unfiled)
	}
	natural := []Key{NullKey{}, BoolKey{}, Int64Key{}, Float64Key{}, StringKey{}}
	if ok, err := s.KeysOnlyOfTypes(natural, &c1); err != nil || !ok {
		t.Fatalf("natural keys: %v %v", ok, err)
	}
	if ok, _ := s.KeysOnlyOfTypes([]Key{Int64Key{}, StringKey{}}, &c1); ok {
		t.Fatal("bool, float and null keys went unseen")
	}

	// A later commit moves a document; a read at c1 now needs the undo window.
	c2 := commit()
	if err := s.PutDocument(ids[0], c2, `{"a":7}`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.KeyGroups(Int64Key{}, nil, &c2, 10); err != nil {
		t.Fatalf("at head: %v", err)
	}
	if _, err := s.KeyGroups(Int64Key{}, nil, &c1, 10); !errors.Is(err, ErrKeyGroupsUnsupported) {
		t.Fatalf("behind the head: %v, want ErrKeyGroupsUnsupported", err)
	}
	if _, err := s.Unfiled(ids, &c1); !errors.Is(err, ErrKeyGroupsUnsupported) {
		t.Fatalf("unfiled behind the head: %v", err)
	}
}
