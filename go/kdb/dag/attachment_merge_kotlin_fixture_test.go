package dag

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Fixtures for Kotlin parity of merged attachment tables (kdb-dag AttachmentMergeTest): merges in
// both parent orders on a fresh "app/t" DAG. Kotlin builds the same graph and must reach the same
// merge commit hashes - which pins the merge ops appendCommitLocked adds, their order, and the
// conflict rules. Run with -v to print the values after an intentional change.
const (
	kotlinMergeFixtureM1     = "f858638eca2a0b24de69b72439980592d035d9402f5aecc8536bf79bf2b08612" // conflicts, ours first
	kotlinMergeFixtureM2     = "25881f5ae35922cfda5575f3835654a9fed0109d8a70e586acd2872ea886fbd9" // conflicts, theirs first
	kotlinMergeFixtureRoot   = "4e6079dc728984919d9abf6e721e0933b43e3f7c8323b64e614c3f249c467b4c" // the merged table, the same in both orders
	kotlinMergeFixtureM3     = "e80ca239c4bc149b229fef1a38441dc7550e71260b4441d2b66f5fde2d609939" // the document deleted by the merge
	kotlinMergeFixtureRepro1 = "61729ba938295a1d5f6e8117db72aec8829fdbc437009a51c0376cb9ba809233" // JSON side first
	kotlinMergeFixtureRepro2 = "08e87ef107a6edd513cc0b6e69577c5a8bd515d8bd907db741f6718937631341" // attaching side first
)

func TestAttachmentMergeKotlinFixtures(t *testing.T) {
	uuid := func(s string) codec.UUID {
		u, err := codec.UUIDFromString(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	fill := func(b byte) codec.Hash {
		var h codec.Hash
		for i := range h.Bytes {
			h.Bytes[i] = b
		}
		return h
	}
	x := uuid("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	y := uuid("11111111-2222-4333-8444-555555555555")
	k0 := uuid("00000000-0000-4000-8000-000000000009")
	k1 := uuid("00000000-0000-4000-8000-00000000000a")
	k2 := uuid("00000000-0000-4000-8000-00000000000b")
	k3 := uuid("00000000-0000-4000-8000-00000000000c")
	k4 := uuid("00000000-0000-4000-8000-00000000000d")
	author := uuid("00000000-0000-4000-8000-0000000000bb")
	put := func(doc, id codec.UUID, name string, content byte) document.Op {
		return document.AttachmentPutOp{DocID: doc, AttachmentID: id, Name: name, Scheme: 1,
			ContentHash: fill(content), Size: int64(content), Mime: "x/y"}
	}
	tree := func(ids ...codec.UUID) document.DocumentTree {
		tr := document.EmptyDocumentTree()
		for _, id := range ids {
			var err error
			if tr, err = tr.With(id, fill(99)); err != nil {
				t.Fatal(err)
			}
		}
		return tr
	}

	d, err := NewInMemoryCommitDag("app/t")
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := d.Head()
	seq := 0
	commit := func(parents []codec.Hash, tr document.DocumentTree, ops ...document.Op) document.Commit {
		seq++
		tx := document.Transaction{ID: codec.UUID{MSB: 0x77, LSB: int64(seq)}, BaseVersion: parents[0], Operations: ops,
			Timestamp: codec.Timestamp{EpochMillis: 1750000000000 + int64(seq)}, AuthorNodeID: author}
		c, err := d.AppendCommitOnto(nil, tx, parents, tr, nil, "m")
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	base := commit([]codec.Hash{genesis}, tree(x), document.WriteOp{DocID: x, Patch: "{}"},
		put(x, k1, "keep", 1), put(x, k2, "both", 2), put(x, k3, "gone", 3))
	ours := commit([]codec.Hash{base.Hash}, tree(x), put(x, k2, "both", 20),
		document.AttachmentDeleteOp{DocID: x, AttachmentID: k3}, put(x, k4, "new", 40))
	theirs := commit([]codec.Hash{base.Hash}, tree(x), put(x, k2, "both", 21), put(x, k3, "gone", 30), put(x, k0, "new", 50))
	m1 := commit([]codec.Hash{ours.Hash, theirs.Hash}, tree(x))
	m2 := commit([]codec.Hash{theirs.Hash, ours.Hash}, tree(x))
	if *m1.AttachmentTreeHash != *m2.AttachmentTreeHash {
		t.Fatal("the merged table depends on the parent order")
	}
	m3 := commit([]codec.Hash{ours.Hash, theirs.Hash}, tree(), document.DeleteOp{DocID: x})

	// The replica repro: one side writes JSON, the other attaches; a second document y is attached
	// on both sides under different IDs and one name.
	rb := commit([]codec.Hash{genesis}, tree(x, y), document.WriteOp{DocID: x, Patch: "{}"}, document.WriteOp{DocID: y, Patch: "{}"})
	l := commit([]codec.Hash{rb.Hash}, tree(x, y), document.WriteOp{DocID: x, Patch: `{"a":1}`}, put(y, k2, "same", 7))
	hc := commit([]codec.Hash{rb.Hash}, tree(x, y), put(x, k1, "d.txt", 1), put(y, k1, "same", 8))
	r1 := commit([]codec.Hash{l.Hash, hc.Hash}, tree(x, y), document.WriteOp{DocID: x, Patch: `{"a":1}`})
	r2 := commit([]codec.Hash{hc.Hash, l.Hash}, tree(x, y), document.WriteOp{DocID: x, Patch: `{"a":1}`})

	got := map[string]string{
		"m1": m1.Hash.Hex(), "m2": m2.Hash.Hex(), "root": m1.AttachmentTreeHash.Hex(), "m3": m3.Hash.Hex(),
		"repro1": r1.Hash.Hex(), "repro2": r2.Hash.Hex(),
	}
	want := map[string]string{
		"m1": kotlinMergeFixtureM1, "m2": kotlinMergeFixtureM2, "root": kotlinMergeFixtureRoot, "m3": kotlinMergeFixtureM3,
		"repro1": kotlinMergeFixtureRepro1, "repro2": kotlinMergeFixtureRepro2,
	}
	for k, v := range got {
		t.Logf("%s = %s", k, v)
		if want[k] != v {
			t.Errorf("%s: got %s, pinned %s - Kotlin's AttachmentMergeTest pins the same value", k, v, want[k])
		}
	}
}
