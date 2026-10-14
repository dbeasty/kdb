package document

import (
	"encoding/hex"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Fixtures for Kotlin parity (kdb-document AttachmentParityTest, kdb-dag AttachmentDagTest). Go is
// the reference: each value here is pinned, and Kotlin asserts the same literals. Run with -v to
// print them after an intentional change, then update both sides.
const (
	// AttachmentContentHash of 3 MiB + 5 bytes, byte i = i*7: four leaves, split 2+2.
	kotlinFixtureMultiChunk = "c9b63a6ef1b604ed023c66d0ea3f9935caf4713028a5a0b06255522879842cf7"
	// The same data's first 3 MiB: three leaves, split 2+1.
	kotlinFixtureThreeChunks = "356146ad0b53c660daa6370d94ece3770d1add8fbbe7e4197408c9a6718e8e0f"
	// A zero-byte attachment.
	kotlinFixtureEmpty = "c0e50f0d90d6e2fea1e6f53d4f758ed5fc9035a399a8eb215c29c6ff3ea05425"
	// The audit path of leaf 2 of the four-chunk data, sibling hashes concatenated, bottom up.
	kotlinFixtureProof2 = "52de9c7c5d4c7e62f1d725dcb1d56782cbe3e5826028d45ed1a081fc00e14eba08ca36f4851a8761dcec63ebaac67067e1749b7b3c3622e69332965d805d5bf1"
	// The root of kotlinFixtureTable().
	kotlinFixtureTableRoot = "f5d1f686bf4e4507ef7881b95b6d250b9312adfcfe6bbf4bdfcbd7afaaf35712"
	// kotlinFixtureTable() packed (one table).
	kotlinFixturePacked = "010311111111222243338444555555555555000000000000400080000000000000aa07646f632e70646601c0e50f0d90d6e2fea1e6f53d4f758ed5fc9035a399a8eb215c29c6ff3ea0542500000000000000000f6170706c69636174696f6e2f7064666f9619ff8b86d011b42d00cf4fc964ff0b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b0970686f746f2e706e6701cf1b6c96f0fcdb59fffdc5a353309421c73530cf0639f595d3a42546131c168b000000000000000509696d6167652f706e676f9619ff8b86d011b42d00cf4fc964ffffffffff0000400080000000000000010ac3a920e298832e74787401abababababababababababababababababababababababababababababababab000000000012d68700"
	// The hash of kotlinFixtureCommit(): its attachment root derived from empty + its ops.
	kotlinFixtureCommitHash = "6078b2d1eb03b628bb2d9c304f38295adff19901bf3d1c8b8021f1a0975695a4"
)

func kotlinFixtureData() []byte {
	data := make([]byte, 3*AttachmentChunkBytes+5)
	for i := range data {
		data[i] = byte(i * 7)
	}
	return data
}

func kotlinFixtureDocs() (codec.UUID, codec.UUID) {
	a, _ := codec.UUIDFromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	b, _ := codec.UUIDFromString("11111111-2222-4333-8444-555555555555")
	return a, b
}

func kotlinFixtureOps() []Op {
	docA, docB := kotlinFixtureDocs()
	att1, _ := codec.UUIDFromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
	att2, _ := codec.UUIDFromString("ffffffff-0000-4000-8000-000000000001")
	att3, _ := codec.UUIDFromString("00000000-0000-4000-8000-0000000000aa")
	return []Op{
		WriteOp{DocID: docA, Patch: `{"a":1}`},
		WriteOp{DocID: docB, Patch: `{"b":2}`},
		AttachmentPutOp{DocID: docA, AttachmentID: att1, Name: "photo.png", Scheme: 1, ContentHash: AttachmentContentHash([]byte("hello")), Size: 5, Mime: "image/png"},
		AttachmentPutOp{DocID: docA, AttachmentID: att2, Name: "é ☃.txt", Scheme: 1, ContentHash: attachHash(0xab), Size: 1234567, Mime: ""},
		AttachmentPutOp{DocID: docB, AttachmentID: att3, Name: "doc.pdf", Scheme: 1, ContentHash: AttachmentContentHash(nil), Size: 0, Mime: "application/pdf"},
		AttachmentPutOp{DocID: docB, AttachmentID: att1, Name: "tmp", Scheme: 1, ContentHash: attachHash(1), Size: 1, Mime: "x/y"},
		AttachmentDeleteOp{DocID: docB, AttachmentID: att1},
	}
}

func kotlinFixtureTable(t *testing.T) AttachmentTable {
	tab, err := EmptyAttachmentTable().Apply(kotlinFixtureOps())
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

func kotlinFixtureCommit(t *testing.T) Commit {
	docA, _ := kotlinFixtureDocs()
	root := kotlinFixtureTable(t).Root()
	c, err := BuildCommitWithAttachments([]codec.Hash{attachHash(1)}, "app/data", docA,
		codec.Timestamp{EpochMillis: 1750000000000, MicroRemainder: 5}, docA, kotlinFixtureOps(), attachHash(2), nil, "m", root)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAttachmentKotlinFixtures(t *testing.T) {
	data := kotlinFixtureData()
	leaves := make([]codec.Hash, 0, 4)
	for off := 0; off < len(data); off += AttachmentChunkBytes {
		leaves = append(leaves, ChunkLeafHash(data[off:min(off+AttachmentChunkBytes, len(data))]))
	}
	var proof []byte
	for _, p := range MerkleInclusionProof(leaves, 2) {
		proof = append(proof, p.Bytes[:]...)
	}
	tab := kotlinFixtureTable(t)
	got := map[string]string{
		"multiChunk":  AttachmentContentHash(data).Hex(),
		"threeChunks": AttachmentContentHash(data[:3*AttachmentChunkBytes]).Hex(),
		"empty":       AttachmentContentHash(nil).Hex(),
		"proof2":      hex.EncodeToString(proof),
		"tableRoot":   tab.Root().Hex(),
		"packed":      hex.EncodeToString(AppendPackedAttachmentTables(nil, []AttachmentTable{tab})),
		"commitHash":  kotlinFixtureCommit(t).Hash.Hex(),
	}
	want := map[string]string{
		"multiChunk":  kotlinFixtureMultiChunk,
		"threeChunks": kotlinFixtureThreeChunks,
		"empty":       kotlinFixtureEmpty,
		"proof2":      kotlinFixtureProof2,
		"tableRoot":   kotlinFixtureTableRoot,
		"packed":      kotlinFixturePacked,
		"commitHash":  kotlinFixtureCommitHash,
	}
	for k, v := range got {
		t.Logf("%s = %s", k, v)
		if want[k] != v {
			t.Errorf("%s: got %s, pinned %s - Kotlin's AttachmentParityTest pins the same value", k, v, want[k])
		}
	}
	if tab.Len() != 3 || tab.Documents() != 2 {
		t.Fatalf("fixture table: len %d docs %d", tab.Len(), tab.Documents())
	}
}
