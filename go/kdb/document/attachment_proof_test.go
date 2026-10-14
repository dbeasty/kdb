package document

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// MerkleTree must agree with the recursive RFC 6962 definitions: the same root, the same audit
// paths, each verifying.
func TestMerkleTreeMatchesRFC6962(t *testing.T) {
	for n := 1; n <= 70; n++ {
		leaves := make([]codec.Hash, n)
		for i := range leaves {
			leaves[i] = ChunkLeafHash([]byte{byte(i), byte(n)})
		}
		tree := NewMerkleTree(leaves)
		if tree.Root() != MerkleTreeHash(leaves) || tree.Leaves() != n {
			t.Fatalf("n=%d: root differs", n)
		}
		for i := range leaves {
			p := tree.Proof(i)
			if want := MerkleInclusionProof(leaves, i); !reflect.DeepEqual(p, want) && !(len(p) == 0 && len(want) == 0) {
				t.Fatalf("n=%d leaf %d: path differs", n, i)
			}
			if !VerifyMerkleInclusion(leaves[i], i, n, p, tree.Root()) {
				t.Fatalf("n=%d leaf %d: path does not verify", n, i)
			}
		}
	}
	if NewMerkleTree(nil).Root() != ChunkLeafHash(nil) {
		t.Fatal("the empty tree is not the tree of one empty chunk")
	}
}

// RangeProofGoldenBytes is the attachment the proof goldens describe: 4 MiB + 5 bytes, byte i =
// i mod 251, so five chunks, the last 5 bytes long.
func rangeProofGoldenBytes() []byte {
	b := make([]byte, 4*AttachmentChunkBytes+5)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// Golden values for verified ranged reads, which the TS and Kotlin clients reproduce: the content
// hash, the tree hash, and each chunk's leaf hash and audit path (hex, bottom up).
var (
	rangeProofGoldenHash = "1a57b9052a9bae5f2bbaa720791a30f99ea84caecaa0898551c84d1746d3ae27"
	rangeProofGoldenTree = "b6b9d1a58b2d9dc4d118b3347037b67ed5ba981c51684aad96da01b56091d758"
	rangeProofGoldenLeaf = []string{
		"f4e53704c07aef05b5b12a89d6c1e54292fa7d9d8c3ee431c40b6f6ef6d19280",
		"23124c5e2cfe3dc827a1b85edc1a2da44b2797cfaccc2120830b0663a818c044",
		"eff5a4ce2d94f327c9dc3df962c7a293b9dd39731f7ad77cf9c000ae46893633",
		"4e4be56a1af3ce8199934fa45f00954b81a1789f1bdd37fd0c9a176bb84e9c61",
		"80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3",
	}
	rangeProofGoldenPath = [][]string{
		{"23124c5e2cfe3dc827a1b85edc1a2da44b2797cfaccc2120830b0663a818c044", "43d36e796ccb572050e71018e752c7b68495fdc5b025bf606d7d249205fc80fb", "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3"},
		{"f4e53704c07aef05b5b12a89d6c1e54292fa7d9d8c3ee431c40b6f6ef6d19280", "43d36e796ccb572050e71018e752c7b68495fdc5b025bf606d7d249205fc80fb", "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3"},
		{"4e4be56a1af3ce8199934fa45f00954b81a1789f1bdd37fd0c9a176bb84e9c61", "1661cb740559458c2a1dce898f8f510ea07d28501f983c733aec692565131eb7", "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3"},
		{"eff5a4ce2d94f327c9dc3df962c7a293b9dd39731f7ad77cf9c000ae46893633", "1661cb740559458c2a1dce898f8f510ea07d28501f983c733aec692565131eb7", "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3"},
		{"b70f08b29c8bc80f288233ea601ba8396567682f18ca787267d5c695ec190f78"},
	}
)

func TestRangeProofGoldens(t *testing.T) {
	data := rangeProofGoldenBytes()
	var leaves []codec.Hash
	for off := 0; off < len(data); off += AttachmentChunkBytes {
		leaves = append(leaves, ChunkLeafHash(data[off:min(off+AttachmentChunkBytes, len(data))]))
	}
	tree := NewMerkleTree(leaves)
	if got := AttachmentContentHash(data).Hex(); got != rangeProofGoldenHash {
		t.Errorf("hash %s", got)
	}
	if got := tree.Root().Hex(); got != rangeProofGoldenTree {
		t.Errorf("tree %s", got)
	}
	if AttachmentIdentity(tree.Root(), int64(len(data))) != AttachmentContentHash(data) {
		t.Error("identity of tree and size is not the content hash")
	}
	for i, l := range leaves {
		if i >= len(rangeProofGoldenLeaf) || l.Hex() != rangeProofGoldenLeaf[i] {
			t.Errorf("leaf %d %s", i, l.Hex())
			continue
		}
		var path []string
		for _, h := range tree.Proof(i) {
			path = append(path, hex.EncodeToString(h.Bytes[:]))
		}
		if !reflect.DeepEqual(path, rangeProofGoldenPath[i]) {
			t.Errorf("leaf %d path %q", i, path)
		}
	}
	if AttachmentChunkCount(0) != 1 || AttachmentChunkCount(1) != 1 || AttachmentChunkCount(AttachmentChunkBytes) != 1 || AttachmentChunkCount(AttachmentChunkBytes+1) != 2 {
		t.Error("AttachmentChunkCount")
	}
}
