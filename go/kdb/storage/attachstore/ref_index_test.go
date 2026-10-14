package attachstore

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func refHash(i int) codec.Hash {
	var h codec.Hash
	h.Bytes[0], h.Bytes[1], h.Bytes[2], h.Bytes[31] = byte(i>>16), byte(i>>8), byte(i), 0xaa
	return h
}

// The reference index round-trips (across its write buffer's size too), survives a reopen and
// Collect, reads a torn newer generation as the older one, and keeps one segment once rewritten.
func TestRefIndexSurvivesReopenCollectAndTears(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	if _, ok, err := s.RefIndex(); ok || err != nil {
		t.Fatalf("fresh store has an index: %v %v", ok, err)
	}
	var refs []codec.Hash
	for i := 0; i < 40_000; i++ { // 1.28 MB: more than one write
		refs = append(refs, refHash(i*7%40_000))
	}
	refs = SortHashes(refs)
	ix := RefIndex{Heads: []codec.Hash{refHash(1), refHash(2)}, Floor: refHash(3), Refs: refs}
	if err := s.SetRefIndex(ix); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(func(codec.Hash) bool { return false }); err != nil {
		t.Fatal(err)
	}
	torn := s.refIndexName(5)
	if _, err := shim.AppendToSegment(torn, []byte(refIndexMagic+"\x00\x00")); err != nil {
		t.Fatal(err)
	}
	again := New(shim, "app/data", small, Config{})
	got, ok, err := again.RefIndex()
	if err != nil || !ok {
		t.Fatalf("reopened index: %v %v", ok, err)
	}
	if len(got.Refs) != len(refs) || got.Floor != ix.Floor || len(got.Heads) != 2 || got.Heads[1] != ix.Heads[1] {
		t.Fatalf("reopened index differs: %d refs, floor %v", len(got.Refs), got.Floor)
	}
	for _, i := range []int{0, 1, 39_999, 12_345} {
		if !got.Has(refHash(i)) {
			t.Fatalf("index lost ref %d", i)
		}
	}
	if got.Has(refHash(40_000)) || got.Has(codec.Hash{}) {
		t.Fatal("index has a ref it was never given")
	}
	// Rewritten: the next generation past the torn one, and the only index segment left.
	if err := again.SetRefIndex(RefIndex{Heads: ix.Heads[:1], Refs: refs[:3]}); err != nil {
		t.Fatal(err)
	}
	third := New(shim, "app/data", small, Config{})
	if got, ok, _ := third.RefIndex(); !ok || len(got.Refs) != 3 || len(got.Heads) != 1 {
		t.Fatalf("rewritten index: %+v %v", got, ok)
	}
	names, _ := shim.ListSegments("app/data")
	count := 0
	for _, n := range names {
		if strings.HasPrefix(n, s.prefix+"r-") {
			count++
			if n != s.refIndexName(6) {
				t.Fatalf("index segment %s, want generation 6", n)
			}
		}
	}
	if count != 1 {
		t.Fatalf("%d index segments, want 1", count)
	}
}

// A damaged byte anywhere makes the index unreadable - read as none, a rebuild - never as other
// references.
func TestRefIndexDamageReadsAsNone(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	if err := s.SetRefIndex(RefIndex{Heads: []codec.Hash{refHash(1)}, Refs: []codec.Hash{refHash(2), refHash(3)}}); err != nil {
		t.Fatal(err)
	}
	name := s.refIndexName(1)
	raw, err := shim.ReadFromSegment(name, 0, len(refIndexMagic)+4+32+32+8+64+4)
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{len(raw) - 1, len(raw) - 40, len(refIndexMagic) + 10} {
		bad := append([]byte(nil), raw...)
		bad[at] ^= 0x40
		_ = shim.DeleteSegment(name)
		if _, err := shim.AppendToSegment(name, bad); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := New(shim, "app/data", small, Config{}).RefIndex(); ok || err != nil {
			t.Fatalf("byte %d flipped: index still read (%v)", at, err)
		}
	}
}
