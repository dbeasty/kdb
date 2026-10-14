package document

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Golden values other implementations must reproduce. The ref, set and "hello" hashes were checked
// against an independent computation of their definitions; the commit is Go's encoding, which
// kdb-document's AttachmentCommitGoldenTest decodes in Kotlin.
const (
	attachmentRefGolden        = "365f1bd0c0b790d6812664a83229589447f5a9a3156bfa137b65320621ff0a68"
	attachmentSetGolden        = "dd3ca4b42f1408538c2e5a1c84c888b6670fb9da99b2029a80eb9c8d411630d4"
	attachmentHelloGolden      = "cf1b6c96f0fcdb59fffdc5a353309421c73530cf0639f595d3a42546131c168b"
	attachmentCommitGolden     = "dc02010a0101010101010101010101010101010101010101010101010101010101010101010209086170702f64617461030f6f9619ff8b86d011b42d00cf4fc964ff040505605ba09d370600050f6f9619ff8b86d011b42d00cf4fc964ff060a03001c010f6f9619ff8b86d011b42d00cf4fc964ff0209077b2261223a317d046e010f6f9619ff8b86d011b42d00cf4fc964ff020f0b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b03090970686f746f2e706e67040401000000050fabababababababababababababababababababababababababababababababab060587d6120000000000070909696d6167652f706e670524010f6f9619ff8b86d011b42d00cf4fc964ff020f0b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b070f02020202020202020202020202020202020202020202020202020202020202020909016d0a0e01cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
	attachmentCommitGoldenHash = "ca9ab1ab70b327ae4cbc9092c4a87a55e10a1d47c6fcac17b72e0ded7beee9c3"
)

// A commit can carry an attachment root with only JSON ops in it (a document write after an
// attachment exists). The direct codec does not write field 10, so it must hand such a commit to the
// generic codec rather than hash or encode it without the root.
func TestAttachmentRootCommitBypassesDirectCodec(t *testing.T) {
	root := codec.Hash{}
	root.Bytes[0] = 7
	doc := codec.UUID{MSB: 1, LSB: 2}
	ops := []Op{WriteOp{DocID: doc, Patch: `{"a":1}`}}
	with, err := BuildCommitWithAttachments(nil, "ns", codec.UUID{MSB: 3}, codec.TimestampFromEpochMicros(5),
		codec.UUID{MSB: 4}, ops, codec.Hash{}, nil, "m", &root)
	if err != nil {
		t.Fatal(err)
	}
	without, err := BuildCommit(nil, "ns", codec.UUID{MSB: 3}, codec.TimestampFromEpochMicros(5),
		codec.UUID{MSB: 4}, ops, codec.Hash{}, nil, "m")
	if err != nil {
		t.Fatal(err)
	}
	if with.Hash == without.Hash {
		t.Fatal("the attachment root does not change the commit hash")
	}
	generic, err := codec.EncodeBytes(with.ToCommitPayloadValue(), CommitPayloadType, WireRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if got := (codec.Hash{Bytes: sha256.Sum256(generic)}); got != with.Hash {
		t.Fatalf("hash %x, generic encoding hashes to %x", with.Hash.Bytes, got.Bytes)
	}
	b, err := with.AppendPayloadBytes(nil)
	if err != nil || !bytes.Equal(b, generic) {
		t.Fatalf("AppendPayloadBytes differs from the generic encoding (err %v)", err)
	}
	back, err := FromPayloadBytes(generic)
	if err != nil || back.AttachmentTreeHash == nil || *back.AttachmentTreeHash != root || back.Hash != with.Hash {
		t.Fatalf("decode lost the root: %+v %v", back.AttachmentTreeHash, err)
	}
}
