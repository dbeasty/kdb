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

func benchEncode(b *testing.B, msg Message, pooled bool) {
	c := NewCodec(EncodingJSON)
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
	c := NewCodec(EncodingJSON)
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
