package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// legacyEncode is the encode path Encode replaced: json.Marshal, a tagged payload copy, then
// EncodeFrameOnly's frame copy. Kept here as the byte-for-byte oracle.
func legacyEncode(enc PayloadEncoding, message Message) ([]byte, error) {
	env, err := messageToEnvelope(message)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 1+len(body))
	payload[0] = encodingTag(enc)
	copy(payload[1:], body)
	h := message.Header()
	h.PayloadLength = len(payload)
	return EncodeFrameOnly(h, payload)
}

func sampleMessages() []Message {
	errMsg := "boom <&> \"quoted\""
	code := ErrorCodeResourceExhausted
	id, _ := codec.UUIDFromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
	doc := `{"name":"x<y>","n":1,"nested":{"a":[1,2,3]},"s":"line\nbreak"}`
	rows := make([][]string, 200)
	for i := range rows {
		rows[i] = []string{fmt.Sprintf("id-%d", i), doc}
	}
	return []Message{
		SqlResultMessage{H: Header{MessageType: MsgSqlResult, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: 7},
			Namespace: "ns", SessionID: "s", Columns: []string{"kdb_id", "doc"}, Rows: rows, ResolvedCommitHex: "ab", ReadOnly: true},
		SqlResultMessage{H: Header{MessageType: MsgSqlResult, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: -3},
			Error: &errMsg, ErrorCode: &code},
		DocumentGetResultMessage{H: Header{MessageType: MsgDocumentGetResult, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: 1 << 30},
			Namespace: "ns", DocID: id.String(), JSON: &doc, CommitHex: "cd"},
		DeltaCommitMessage{H: Header{MessageType: MsgDeltaCommit, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: 9},
			Payload: DeltaCommitPayload{Namespace: "ns", Operations: []document.Op{
				document.WriteOp{DocID: id, Patch: doc}, document.DeleteOp{DocID: id}}}},
	}
}

func TestEncodeIsByteIdenticalToTheLegacyPath(t *testing.T) {
	for _, enc := range []PayloadEncoding{EncodingKdbBinary, EncodingJSON} {
		c := NewCodec(enc)
		for i, msg := range sampleMessages() {
			want, err := legacyEncode(enc, msg)
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.Encode(msg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("message %d (%T, %v): Encode differs from the legacy encoding", i, msg, enc)
			}
			pooled, release, err := c.EncodePooled(msg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(pooled, want) {
				t.Fatalf("message %d (%T, %v): EncodePooled differs from the legacy encoding", i, msg, enc)
			}
			release()
		}
	}
}

// A pooled buffer reused for a smaller frame must not leak the previous frame's tail: the frame
// is exactly its declared length.
func TestEncodePooledReuseDoesNotLeakThePreviousFrame(t *testing.T) {
	c := NewCodec(EncodingJSON)
	msgs := sampleMessages()
	for round := 0; round < 50; round++ {
		for i := len(msgs) - 1; i >= 0; i-- {
			want, _ := c.Encode(msgs[i])
			got, release, err := c.EncodePooled(msgs[i])
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("round %d message %d: pooled frame differs", round, i)
			}
			decoded, err := c.Decode(got)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Header().CorrelationID != msgs[i].Header().CorrelationID {
				t.Fatal("correlation id lost")
			}
			release()
		}
	}
}

func TestEncodeRejectsAFrameOverTheMaximum(t *testing.T) {
	big := strings.Repeat("x", DefaultMaxFrameBytes)
	msg := DocumentGetResultMessage{H: Header{MessageType: MsgDocumentGetResult, ProtocolVersion: KdbWireProtocolVersion}, JSON: &big}
	c := NewCodec(EncodingJSON)
	_, err := c.Encode(msg)
	var tooLarge *FrameTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("want *FrameTooLargeError, got %v", err)
	}
	if _, _, err := c.EncodePooled(msg); !errors.As(err, &tooLarge) {
		t.Fatalf("pooled: want *FrameTooLargeError, got %v", err)
	}
}

func BenchmarkCodecEncodeSqlResult200(b *testing.B) {
	benchEncode(b, sampleMessages()[0], false)
}

func BenchmarkCodecEncodePooledSqlResult200(b *testing.B) {
	benchEncode(b, sampleMessages()[0], true)
}

func BenchmarkCodecEncodeDocumentGetResult(b *testing.B) {
	benchEncode(b, sampleMessages()[2], false)
}

func BenchmarkCodecEncodePooledDocumentGetResult(b *testing.B) {
	benchEncode(b, sampleMessages()[2], true)
}

// benchCodecs are the encodings the codec benchmarks compare: the JSON envelope, binary frames
// (B4), and binary frames with zstd above the threshold.
var benchCodecs = []struct {
	name  string
	codec *DefaultCodec
}{
	{"json", NewCodec(EncodingJSON)},
	{"binary", NewCodec(EncodingJSON).WithWireFeatures([]string{WireFeatureBinary, WireFeatureBinary2})},
	{"binary+zstd", NewCodec(EncodingJSON).WithWireFeatures(SupportedWireFeatures)},
}

func benchEncode(b *testing.B, msg Message, pooled bool) {
	for _, bc := range benchCodecs {
		b.Run(bc.name, func(b *testing.B) { benchEncodeWith(b, bc.codec, msg, pooled) })
	}
}

func benchEncodeWith(b *testing.B, c *DefaultCodec, msg Message, pooled bool) {
	frame, err := c.Encode(msg)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(frame)), "wire-B/op")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if pooled {
			_, release, err := c.EncodePooled(msg)
			if err != nil {
				b.Fatal(err)
			}
			release()
			continue
		}
		if _, err := c.Encode(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodecDecodeSqlResult200(b *testing.B) {
	benchDecode(b, sampleMessages()[0])
}

func BenchmarkCodecDecodeDocumentGetResult(b *testing.B) {
	benchDecode(b, sampleMessages()[2])
}

func benchDecode(b *testing.B, msg Message) {
	for _, bc := range benchCodecs {
		b.Run(bc.name, func(b *testing.B) { benchDecodeWith(b, bc.codec, msg) })
	}
}

func benchDecodeWith(b *testing.B, c *DefaultCodec, msg Message) {
	frame, err := c.Encode(msg)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		if _, err := c.Decode(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// TestSetCorrelationIDMatchesAFreshEncode: a frame encoded under one correlation id and patched to
// another is byte-identical to the same message encoded under the second - the id is in the header
// only, which is what lets the stream hub encode a delta once and send it to every subscriber.
func TestSetCorrelationIDMatchesAFreshEncode(t *testing.T) {
	c := NewCodec(EncodingJSON)
	msg := func(cid int) Message {
		return DeltaCommitMessage{
			H: Header{MessageType: MsgDeltaCommit, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: cid},
			Payload: DeltaCommitPayload{
				Namespace: "app/data", TimestampMicros: 42,
				Operations: []document.Op{document.WriteOp{DocID: codec.UUID{MSB: 1, LSB: 2}, Patch: `{"a":1}`}},
			},
		}
	}
	patched, err := c.Encode(msg(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, cid := range []int{1, 5000, 1 << 30, -7} {
		SetCorrelationID(patched, cid)
		want, err := c.Encode(msg(cid))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(patched, want) {
			t.Fatalf("cid %d: patched frame differs from a fresh encode", cid)
		}
		h, err := DecodeHeader(patched)
		if err != nil || h.CorrelationID != cid {
			t.Fatalf("cid %d: decoded %d (%v)", cid, h.CorrelationID, err)
		}
	}
}

// moreSamples are messages binary-frames-2 covers: a 100-operation stream delta and a 20-hit
// search result with bodies.
func moreSamples() (DeltaCommitMessage, SearchResultMessage) {
	doc := `{"name":"x<y>","n":1,"nested":{"a":[1,2,3]},"s":"line\nbreak"}`
	ops := make([]document.Op, 100)
	for i := range ops {
		ops[i] = document.WriteOp{DocID: codec.UUID{MSB: int64(i), LSB: 7}, Patch: doc}
	}
	delta := DeltaCommitMessage{H: Header{MessageType: MsgDeltaCommit, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: 3},
		Payload: DeltaCommitPayload{Namespace: "ns", TimestampMicros: 1, Operations: ops}}
	hits := make([]SearchHit, 20)
	for i := range hits {
		hits[i] = SearchHit{DocID: fmt.Sprintf("0b6f3c1e-2a4d-4e8f-9a1b-%012d", i), Score: 1 / float64(i+1), JSON: &doc}
	}
	search := SearchResultMessage{H: Header{MessageType: MsgSearchResult, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: 4},
		Namespace: "ns", Hits: hits, ResolvedCommitHex: "ab"}
	return delta, search
}

func BenchmarkCodecEncodePooledDeltaCommit100(b *testing.B) {
	d, _ := moreSamples()
	benchEncode(b, d, true)
}

func BenchmarkCodecDecodeDeltaCommit100(b *testing.B) {
	d, _ := moreSamples()
	benchDecode(b, d)
}

func BenchmarkCodecEncodePooledSearchResult20(b *testing.B) {
	_, s := moreSamples()
	benchEncode(b, s, true)
}

func BenchmarkCodecDecodeSearchResult20(b *testing.B) {
	_, s := moreSamples()
	benchDecode(b, s)
}
