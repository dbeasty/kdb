package peersync

import (
	"bytes"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/wire"
)

// partFor builds what a host sends for blob data from offset: its chunks with their proofs.
func partFor(data []byte, offset int64) wire.BlobFetchPart {
	const chunk = document.AttachmentChunkBytes
	var leaves []codec.Hash
	for off := 0; off < len(data) || off == 0; off += chunk {
		leaves = append(leaves, document.ChunkLeafHash(data[off:min(off+chunk, len(data))]))
		if len(data) == 0 {
			break
		}
	}
	tree := document.MerkleTreeHash(leaves)
	p := wire.BlobFetchPart{Hash: document.AttachmentIdentity(tree, int64(len(data))), Size: int64(len(data)), Tree: tree,
		Offset: offset, Data: append([]byte(nil), data[offset:]...), EOF: true}
	for i := int(offset / chunk); i < len(leaves); i++ {
		p.Chunks = append(p.Chunks, wire.BlobChunkProof{Leaf: int64(i), Path: document.MerkleInclusionProof(leaves, i)})
	}
	return p
}

func TestFetchedChunksAreCheckedAgainstTheirProofs(t *testing.T) {
	store := attachstore.New(storio.NewInMemoryPlatformIO(), "ns", nil, attachstore.Config{})
	data := bytes.Repeat([]byte("0123456789"), 300_000) // 3 MB, three chunks
	good := partFor(data, 0)

	n := &blobNeed{entry: wire.BlobFetchEntry{Hash: good.Hash}}
	if err := n.take(store, good); err != nil || !n.done {
		t.Fatalf("a good part was refused: %v", err)
	}
	if has, _ := store.Has(good.Hash); !has {
		t.Fatal("the fetched blob is not stored")
	}

	bad := partFor(data, 0)
	bad.Data[document.AttachmentChunkBytes+5] ^= 1 // corrupt the second chunk
	n = &blobNeed{entry: wire.BlobFetchEntry{Hash: bad.Hash}}
	err := n.take(store, bad)
	if err == nil || !strings.Contains(err.Error(), "chunk 1") {
		t.Fatalf("a corrupted chunk was accepted: %v", err)
	}
	n.up.Abort()

	// A tree that does not hash to the blob asked for is refused before any byte is looked at.
	other := partFor([]byte("something else"), 0)
	n = &blobNeed{entry: wire.BlobFetchEntry{Hash: good.Hash}}
	if err := n.take(store, other); err == nil {
		t.Fatal("a part for another blob was accepted")
	}
}

func TestTreeNodesOfTheAttachmentTable(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	doc, att := codec.UUID{MSB: 5}, codec.UUID{MSB: 6}
	put := document.AttachmentPutOp{DocID: doc, AttachmentID: att, Name: "a.txt", Scheme: document.AttachmentHashSchemeChunked1,
		ContentHash: document.AttachmentContentHash([]byte("a")), Size: 1, Mime: "text/plain"}
	c, err := d.AppendCommit(document.Transaction{Operations: []document.Op{document.WriteOp{DocID: doc, Patch: "{}"}, put}}, head, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	res, err := treeNodesPage(IngestEnv{DAG: d, NamespaceID: "ns"}, wire.TreeNodesMessage{Tree: wire.TreeAttachments, Prefixes: []string{""}})
	if err != nil {
		t.Fatal(err)
	}
	if res.TreeHex != c.AttachmentTreeHash.Hex() || len(res.Nodes) != 1 || !res.Nodes[0].HasEntries {
		t.Fatalf("%+v", res)
	}
	items := res.Nodes[0].Attachments[doc.String()]
	if len(items) != 1 || items[0].Name != "a.txt" || items[0].ID != att.String() || items[0].Size != 1 {
		t.Fatalf("attachments listed: %+v", items)
	}
	if _, err := treeNodesPage(IngestEnv{DAG: d}, wire.TreeNodesMessage{Tree: "nope", Prefixes: []string{""}}); err == nil {
		t.Fatal("an unknown tree was served")
	}
}
