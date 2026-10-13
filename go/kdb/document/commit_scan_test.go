package document

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// randomScanCommit builds a commit mixing every operation kind: several writes of the same
// document, deletes, file writes, schema migrations, empty and multi-byte patches, and optional
// fields set and unset.
func randomScanCommit(rng *rand.Rand, ids []codec.UUID) Commit {
	var ops []Op
	for j, n := 0, rng.Intn(40); j < n; j++ {
		id := ids[rng.Intn(len(ids))]
		switch r := rng.Intn(10); {
		case r < 6:
			var patch string
			switch rng.Intn(4) {
			case 0:
				patch = ""
			case 1:
				patch = fmt.Sprintf("not json %d ü€", rng.Intn(100))
			default:
				patch = fmt.Sprintf(`{"j":%d,"pad":%q}`, j, strings.Repeat("x", rng.Intn(300)))
			}
			ops = append(ops, WriteOp{DocID: id, Patch: patch})
		case r < 8:
			ops = append(ops, DeleteOp{DocID: id})
		case r < 9:
			var h codec.Hash
			rng.Read(h.Bytes[:])
			ops = append(ops, FileWriteOp{Path: fmt.Sprintf("f/%d", j), BlobHash: h})
		default:
			ops = append(ops, SchemaMigrationOp{MigrationID: id, MigrationPayload: "{}"})
		}
	}
	parents := make([]codec.Hash, rng.Intn(4))
	for i := range parents {
		rng.Read(parents[i].Bytes[:])
	}
	c := Commit{
		ParentHashes: parents, NamespaceID: "ns/scan", Timestamp: codec.Timestamp{EpochMillis: rng.Int63n(1 << 40)},
		TransactionID: codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}, AuthorNodeID: codec.UUID{MSB: rng.Int63()},
		Operations: ops, DocumentTreeHash: EmptyDocumentTree().TreeHash, Message: strings.Repeat("m", rng.Intn(20)),
	}
	if rng.Intn(2) == 0 {
		var h codec.Hash
		rng.Read(h.Bytes[:])
		c.SchemaHash = &h
	}
	return c
}

// TestScanCommitWritesMatchesFullDecode checks, for every document of many random commits (and one
// that is in none of them), that the single-document scan yields exactly the patches a full decode
// does, in the same order.
func TestScanCommitWritesMatchesFullDecode(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	ids := make([]codec.UUID, 12)
	for i := range ids {
		ids[i] = codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}
	}
	absent := codec.UUID{MSB: -1, LSB: -1}
	for n := 0; n < 500; n++ {
		c := randomScanCommit(rng, ids)
		payload, err := c.ToPayloadBytes()
		if err != nil {
			t.Fatal(err)
		}
		full, err := FromPayloadBytes(payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range append(append([]codec.UUID(nil), ids...), absent) {
			var want []string
			for _, op := range full.Operations {
				if w, ok := op.(WriteOp); ok && w.DocID == id {
					want = append(want, w.Patch)
				}
			}
			var got []string
			if err := ScanCommitWrites(payload, id, func(p []byte) bool {
				got = append(got, string(p))
				return true
			}); err != nil {
				t.Fatalf("commit %d: %v", n, err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) || len(got) != len(want) {
				t.Fatalf("commit %d doc %v: scan %q, full decode %q", n, id, got, want)
			}
			// Stopping after the first candidate sees exactly the first.
			calls := 0
			if err := ScanCommitWrites(payload, id, func(p []byte) bool {
				calls++
				if string(p) != want[0] {
					t.Fatalf("first candidate %q, want %q", p, want[0])
				}
				return false
			}); err != nil {
				t.Fatal(err)
			}
			if (len(want) > 0) != (calls == 1) || calls > 1 {
				t.Fatalf("stop: %d calls for %d candidates", calls, len(want))
			}
		}
	}
}

// TestScanCommitWritesRejectsTruncation: every proper prefix of a payload is refused, as
// FromPayloadBytes refuses it, and so is a payload with bytes after it.
func TestScanCommitWritesRejectsTruncation(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	ids := []codec.UUID{{MSB: 1}, {MSB: 2}}
	for n := 0; n < 20; n++ {
		c := randomScanCommit(rng, ids)
		payload, err := c.ToPayloadBytes()
		if err != nil {
			t.Fatal(err)
		}
		for cut := 0; cut < len(payload); cut++ {
			if err := ScanCommitWrites(payload[:cut], ids[0], func([]byte) bool { return true }); err == nil {
				t.Fatalf("commit %d: prefix of %d/%d bytes accepted", n, cut, len(payload))
			}
		}
		if err := ScanCommitWrites(append(payload[:len(payload):len(payload)], 0), ids[0], func([]byte) bool { return true }); err == nil {
			t.Fatal("trailing byte accepted")
		}
	}
}

// TestScanCommitWritesPatchAliasesPayload documents the borrow: the patch handed to fn is the
// payload's own bytes, so the scan allocates nothing.
func TestScanCommitWritesAllocatesNothing(t *testing.T) {
	id := codec.UUID{MSB: 7, LSB: 9}
	ops := make([]Op, 100)
	for i := range ops {
		ops[i] = WriteOp{DocID: codec.UUID{MSB: int64(i)}, Patch: `{"a":1}`}
	}
	ops[57] = WriteOp{DocID: id, Patch: `{"wanted":true}`}
	c := Commit{NamespaceID: "n", Operations: ops, DocumentTreeHash: EmptyDocumentTree().TreeHash}
	payload, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	fn := func(p []byte) bool { found = string(p) == `{"wanted":true}`; return false }
	allocs := testing.AllocsPerRun(100, func() {
		if err := ScanCommitWrites(payload, id, fn); err != nil {
			t.Fatal(err)
		}
	})
	if !found || allocs != 0 {
		t.Fatalf("found=%v allocs=%v", found, allocs)
	}
}

func TestContentHasherHashBytesMatchesHash(t *testing.T) {
	var h ContentHasher
	id := codec.UUID{MSB: 3, LSB: -4}
	for _, s := range []string{"", "{}", `{"x":"` + strings.Repeat("y", 300) + `"}`} {
		if h.HashBytes(id, []byte(s)) != h.Hash(id, s) {
			t.Fatalf("HashBytes differs from Hash for %q", s)
		}
	}
}
