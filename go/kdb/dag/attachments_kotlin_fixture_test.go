package dag

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Fixtures for Kotlin parity (kdb-dag AttachmentDagTest): commits appended to a fresh "app/t" DAG,
// whose attachment roots the DAG derives. Kotlin appends the same transactions to its own DAG and
// must reach the same commit hashes. Run with -v to print the values after an intentional change.
const (
	kotlinDagFixtureRoot1   = "de6abf4c2f833d64af1691558fbf5ffe4720659252f8c644aaf1f56bc62f4238"
	kotlinDagFixtureCommit1 = "268c9447c13c34dda56062ff8da6486bade8b62ff738549bcbd2a01e19197053"
	kotlinDagFixtureRoot2   = "ba364b68a1ef152eff592288b3a7ae38619ce67b154f62f306159b6789e6b9e8"
	kotlinDagFixtureCommit2 = "0d3070272cd0d28ccf4cc4ff480eafd374a7c35fa15e4c32f78e36b98b6cf2c8"
	kotlinDagFixtureCommit3 = "e29ec7e15fd88bb85aaba751481a91472147e52455e6fb03aa1c43fe033b097e"
)

func kotlinDagFixtureHash(b byte) codec.Hash {
	var h codec.Hash
	for i := range h.Bytes {
		h.Bytes[i] = b
	}
	return h
}

func TestAttachmentKotlinDagFixtures(t *testing.T) {
	uuid := func(s string) codec.UUID {
		u, err := codec.UUIDFromString(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	docA := uuid("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	docB := uuid("11111111-2222-4333-8444-555555555555")
	att1 := uuid("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
	att2 := uuid("ffffffff-0000-4000-8000-000000000001")
	author := uuid("00000000-0000-4000-8000-0000000000bb")
	hello := document.AttachmentContentHash([]byte("hello"))

	d, err := NewInMemoryCommitDag("app/t")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	tree := document.EmptyDocumentTree()
	appendTx := func(id string, millis int64, ops []document.Op) document.Commit {
		tx := document.Transaction{ID: uuid(id), BaseVersion: head, Operations: ops,
			Timestamp: codec.Timestamp{EpochMillis: millis}, AuthorNodeID: author}
		c, err := d.AppendCommit(tx, head, tree, nil, "m")
		if err != nil {
			t.Fatal(err)
		}
		head = c.Hash
		return c
	}
	c1 := appendTx("00000000-0000-4000-8000-000000000101", 1750000000000, []document.Op{
		document.WriteOp{DocID: docA, Patch: `{"a":1}`},
		document.WriteOp{DocID: docB, Patch: `{"b":2}`},
		document.AttachmentPutOp{DocID: docA, AttachmentID: att1, Name: "photo.png", Scheme: 1, ContentHash: hello, Size: 5, Mime: "image/png"},
		document.AttachmentPutOp{DocID: docB, AttachmentID: att2, Name: "b.bin", Scheme: 1, ContentHash: kotlinDagFixtureHash(0xab), Size: 1234567, Mime: ""},
	})
	// A rename of att1 and a delete of docB (which drops its attachment).
	c2 := appendTx("00000000-0000-4000-8000-000000000102", 1750000000001, []document.Op{
		document.AttachmentPutOp{DocID: docA, AttachmentID: att1, Name: "renamed.png", Scheme: 1, ContentHash: hello, Size: 5, Mime: "image/png"},
		document.DeleteOp{DocID: docB},
	})
	// A JSON-only write: the root is inherited.
	c3 := appendTx("00000000-0000-4000-8000-000000000103", 1750000000002, []document.Op{
		document.WriteOp{DocID: docA, Patch: `{"a":2}`},
	})
	if c3.AttachmentTreeHash == nil || *c3.AttachmentTreeHash != *c2.AttachmentTreeHash {
		t.Fatal("a JSON-only commit did not inherit its parent's attachment root")
	}
	got := map[string]string{
		"root1": c1.AttachmentTreeHash.Hex(), "commit1": c1.Hash.Hex(),
		"root2": c2.AttachmentTreeHash.Hex(), "commit2": c2.Hash.Hex(),
		"commit3": c3.Hash.Hex(),
	}
	want := map[string]string{
		"root1": kotlinDagFixtureRoot1, "commit1": kotlinDagFixtureCommit1,
		"root2": kotlinDagFixtureRoot2, "commit2": kotlinDagFixtureCommit2,
		"commit3": kotlinDagFixtureCommit3,
	}
	for k, v := range got {
		t.Logf("%s = %s", k, v)
		if want[k] != v {
			t.Errorf("%s: got %s, pinned %s - Kotlin's AttachmentDagTest pins the same value", k, v, want[k])
		}
	}
}
