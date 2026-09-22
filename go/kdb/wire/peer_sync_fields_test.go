package wire_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

func testHash(b byte) codec.Hash {
	var h codec.Hash
	for i := range h.Bytes {
		h.Bytes[i] = b
	}
	return h
}

func TestCommitFetchHavesRoundTrip(t *testing.T) {
	since := testHash(1)
	in := wire.CommitFetchMessage{
		H:         wire.Header{MessageType: wire.MsgCommitFetch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 7},
		Namespace: "a/b", SinceHash: &since, MaxCommits: 50,
		Haves: []codec.Hash{testHash(2), testHash(3)},
	}
	out := roundTrip(t, in).(wire.CommitFetchMessage)
	if !reflect.DeepEqual(out.Haves, in.Haves) || *out.SinceHash != since || out.MaxCommits != 50 {
		t.Fatalf("round trip lost fields: %+v", out)
	}
}

// A fetch without haves must encode exactly as it did before the field existed, so an older
// peer sees nothing new.
func TestCommitFetchWithoutHavesOmitsField(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	frame, err := c.Encode(wire.CommitFetchMessage{H: wire.Header{MessageType: wire.MsgCommitFetch, ProtocolVersion: 1}, Namespace: "a/b"})
	if err != nil {
		t.Fatal(err)
	}
	if contains(frame, "haveHexes") {
		t.Fatal("empty Haves must be omitted from the payload")
	}
}

func TestCommitPushStubsRoundTrip(t *testing.T) {
	in := wire.CommitPushMessage{
		H:         wire.Header{MessageType: wire.MsgCommitPush, ProtocolVersion: wire.KdbWireProtocolVersion},
		Namespace: "a/b",
		Stubs: []document.CommitStub{{
			OriginalHash: testHash(9), ArchiveLocation: "s3://bucket/x", StubbedAt: codec.TimestampFromEpochMicros(1234567),
		}},
	}
	out := roundTrip(t, in).(wire.CommitPushMessage)
	if !reflect.DeepEqual(out.Stubs, in.Stubs) {
		t.Fatalf("stubs lost: %+v", out.Stubs)
	}
}

func TestPeerErrorRoundTrip(t *testing.T) {
	in := wire.PeerErrorMessage{
		H:         wire.Header{MessageType: wire.MsgPeerError, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 3},
		Namespace: "a/b", Code: wire.ErrorCodeUnsupported, Message: "no",
	}
	out := roundTrip(t, in).(wire.PeerErrorMessage)
	out.H.PayloadLength = 0
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func contains(b []byte, s string) bool { return strings.Contains(string(b), s) }
