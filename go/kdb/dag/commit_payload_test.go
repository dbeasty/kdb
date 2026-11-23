package dag

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// The payload an append keeps for the log is exactly the commit's canonical encoding - the bytes
// its hash covers - and is handed over once, to the caller naming that commit.
func TestTakeCommitPayloadIsTheCommitsEncoding(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	docID, _ := codec.RandomUUID()
	tx := newTx(head)
	tx.Operations = []document.Op{document.WriteOp{DocID: docID, Patch: `{"a":"` + strings.Repeat("x", 900) + `"}`}}
	schema := codec.Hash{Bytes: [32]byte{9}}
	c, err := d.AppendCommit(tx, head, document.EmptyDocumentTree(), &schema, "message")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.TakeCommitPayload(codec.Hash{Bytes: [32]byte{1}}); ok {
		t.Fatal("a payload handed over for a commit that was not appended last")
	}
	got, ok := d.TakeCommitPayload(c.Hash)
	if !ok {
		t.Fatal("no payload kept for the commit just appended")
	}
	want, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("kept payload differs from the commit's encoding")
	}
	if sha256.Sum256(got) != c.Hash.Bytes {
		t.Fatal("kept payload does not hash to the commit hash")
	}
	if _, ok := d.TakeCommitPayload(c.Hash); ok {
		t.Fatal("the payload was handed over twice")
	}

	// A second append replaces it; the first commit's is gone.
	c2, err := d.AppendCommit(newTx(c.Hash), c.Hash, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.TakeCommitPayload(c.Hash); ok {
		t.Fatal("an older commit's payload was handed over")
	}
	if p, ok := d.TakeCommitPayload(c2.Hash); !ok || sha256.Sum256(p) != c2.Hash.Bytes {
		t.Fatal("second commit's payload missing or wrong")
	}
}
