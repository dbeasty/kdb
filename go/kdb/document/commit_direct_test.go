package document

import (
	"bytes"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func genericEncodeCommit(t *testing.T, c Commit) []byte {
	t.Helper()
	b, err := codec.EncodeBytes(c.ToCommitPayloadValue(), CommitPayloadType, WireRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func genericDecodeCommit(b []byte) (Commit, error) {
	v, err := codec.DecodeBytes(b, CommitPayloadType, WireRegistry())
	if err != nil {
		return Commit{}, err
	}
	rec, ok := v.(codec.RecordValue)
	if !ok {
		return Commit{}, NewCommitDecodeError("commit payload: expected record", nil)
	}
	return parseCommitFromRecord(rec, codec.Hash{Bytes: sha256Sum(b)})
}

func randomCommit(r *rand.Rand) Commit {
	str := func() string {
		switch r.Intn(4) {
		case 0:
			return ""
		case 1:
			return "ü-" + strings.Repeat("x", r.Intn(20))
		case 2:
			return strings.Repeat("y", 120+r.Intn(20)) // straddles the one-byte LEB128 length
		default:
			return strings.Repeat("z", r.Intn(20000))
		}
	}
	hash := func() (h codec.Hash) {
		r.Read(h.Bytes[:])
		return h
	}
	uuid := func() codec.UUID { return codec.UUID{MSB: r.Int63() - r.Int63(), LSB: r.Int63() - r.Int63()} }
	c := Commit{
		ParentHashes:     make([]codec.Hash, r.Intn(3)),
		NamespaceID:      str(),
		TransactionID:    uuid(),
		Timestamp:        codec.TimestampFromEpochMicros(r.Int63() - r.Int63()),
		AuthorNodeID:     uuid(),
		Operations:       make([]Op, r.Intn(140)),
		DocumentTreeHash: hash(),
		Message:          str(),
	}
	for i := range c.ParentHashes {
		c.ParentHashes[i] = hash()
	}
	for i := range c.Operations {
		switch r.Intn(4) {
		case 0:
			c.Operations[i] = WriteOp{DocID: uuid(), Patch: str()}
		case 1:
			c.Operations[i] = DeleteOp{DocID: uuid()}
		case 2:
			c.Operations[i] = FileWriteOp{Path: str(), BlobHash: hash()}
		default:
			c.Operations[i] = SchemaMigrationOp{MigrationID: uuid(), MigrationPayload: str()}
		}
	}
	if r.Intn(2) == 0 {
		h := hash()
		c.SchemaHash = &h
	}
	return c
}

// TestDirectCommitCodecMatchesGeneric holds the direct CommitPayload codec to the generic one:
// the same bytes from an encode, the same hash, and the same commit (or failure) from a decode -
// for canonical payloads, for non-canonical ones the direct decoder hands back to the generic
// path, and for corrupted ones.
func TestDirectCommitCodecMatchesGeneric(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 400; i++ {
		c := randomCommit(r)
		want := genericEncodeCommit(t, c)
		got, err := c.ToPayloadBytes()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("commit %d: direct encode differs from the generic codec (err %v)", i, err)
		}
		if len(got) != cap(got) {
			t.Fatalf("commit %d: encode sized its buffer %d for %d bytes", i, cap(got), len(got))
		}
		h, err := ComputeCommitHash(c)
		if err != nil || h.Bytes != sha256Sum(want) {
			t.Fatalf("commit %d: hash differs", i)
		}
		built, err := BuildCommit(c.ParentHashes, c.NamespaceID, c.TransactionID, c.Timestamp, c.AuthorNodeID,
			c.Operations, c.DocumentTreeHash, c.SchemaHash, c.Message)
		if err != nil || built.Hash != h {
			t.Fatalf("commit %d: BuildCommit hash differs", i)
		}
		if _, ok := decodeCommitDirect(want); !ok {
			t.Fatalf("commit %d: direct decoder refused a canonical payload", i)
		}
		assertDecodeMatches(t, want)
		// Corrupt it: every outcome must be the generic decoder's.
		for j := 0; j < 20; j++ {
			bad := append([]byte(nil), want...)
			switch r.Intn(3) {
			case 0:
				bad[r.Intn(len(bad))] ^= byte(1 + r.Intn(255))
			case 1:
				bad = bad[:r.Intn(len(bad))]
			default:
				bad = append(bad, byte(r.Intn(256)))
			}
			assertDecodeMatches(t, bad)
		}
	}
}

// TestDirectCommitDecodeFallsBack covers layouts the generic codec accepts and the direct decoder
// does not write: a null schemaHash written out, an unknown field, and fields out of id order.
func TestDirectCommitDecodeFallsBack(t *testing.T) {
	c := randomCommit(rand.New(rand.NewSource(2)))
	c.SchemaHash = nil
	canonical := genericEncodeCommit(t, c)
	// The record body: the payload without its leading LEB128 length.
	n, k := uvarintAt(canonical)
	body := canonical[k:]
	if int(n) != len(body) {
		t.Fatal("bad canonical layout")
	}
	// Split the body at field 7 / field 9 boundary: field 9 is the trailing message.
	msgField := append([]byte{9, tagString}, appendStr(nil, c.Message)...)
	if !bytes.HasSuffix(body, msgField) {
		t.Fatal("message field is not last")
	}
	head := body[:len(body)-len(msgField)]
	wrap := func(b []byte) []byte { return append(appendLeb(nil, len(b)), b...) }

	nullSchema := wrap(concat(head, []byte{8, tagUnion, 0}, msgField))
	unknownField := wrap(concat(head, []byte{12, byte(4), 1, 2, 3, 4}, msgField)) // int32 field 12
	reordered := wrap(concat(msgField, head))
	for name, b := range map[string][]byte{"null schemaHash": nullSchema, "unknown field": unknownField, "reordered": reordered} {
		if _, ok := decodeCommitDirect(b); ok {
			t.Fatalf("%s: direct decoder accepted a non-canonical payload", name)
		}
		got, err := FromPayloadBytes(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want := c
		want.Hash = codec.Hash{Bytes: sha256Sum(b)}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: fallback decoded a different commit", name)
		}
	}
}

func assertDecodeMatches(t *testing.T, b []byte) {
	t.Helper()
	want, wantErr := genericDecodeCommit(b)
	got, gotErr := FromPayloadBytes(b)
	if (wantErr == nil) != (gotErr == nil) {
		t.Fatalf("decode: generic err %v, direct err %v", wantErr, gotErr)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decode: direct and generic commits differ")
	}
}

func uvarintAt(b []byte) (uint64, int) {
	s := commitScanner{raw: b}
	v, _ := s.uvarint("")
	return v, s.pos
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func BenchmarkCommitPayload(b *testing.B) {
	c := randomCommit(rand.New(rand.NewSource(3)))
	c.Operations = []Op{WriteOp{DocID: codec.UUID{MSB: 1}, Patch: `{"i":1,"pad":"` + strings.Repeat("x", 200) + `"}`}}
	payload, _ := c.ToPayloadBytes()
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = c.ToPayloadBytes()
		}
	})
	b.Run("hash", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = ComputeCommitHash(c)
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = FromPayloadBytes(payload)
		}
	})
	b.Run("decode-generic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = genericDecodeCommit(payload)
		}
	})
}
