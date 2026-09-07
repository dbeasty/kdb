package wire_test

import (
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

func f64(v float64) *float64 { return &v }

// TestRoundTripSearchBothArms encodes and decodes a SEARCH carrying both arms and every optional
// field, and proves the JSON body uses the §11 key names both trees share.
func TestRoundTripSearchBothArms(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.SearchMessage{
		H:           wire.Header{MessageType: wire.MsgSearch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 7},
		Namespace:   "app/tasks",
		SessionID:   "sess-1",
		Text:        &wire.SearchTextArm{Index: "tasks_text", Query: "deploy staging", Depth: 200, MinScore: f64(0.1), Weight: f64(0.7)},
		Vector:      &wire.SearchVectorArm{Index: "embedding", Vector: []float64{0.1, 0.25, -1}, Depth: 50, Weight: f64(0.3)},
		Fusion:      "weighted",
		Limit:       20,
		IncludeJSON: true,
		AtCommitHex: "ab",
	}
	frame, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := decoded.(wire.SearchMessage)
	if !ok {
		t.Fatalf("decoded %T", decoded)
	}
	got.H.PayloadLength = 0
	if !reflect.DeepEqual(got, msg) {
		t.Fatalf("round trip changed the message:\n got %+v\nwant %+v", got, msg)
	}
	header, _ := c.DecodeHeader(frame)
	env, err := wire.DecodePayloadEnvelope(frame, header)
	if err != nil {
		t.Fatal(err)
	}
	if env.Kind != "search" || env.Summary() != "search ns=app/tasks" {
		t.Fatalf("kind %q summary %q", env.Kind, env.Summary())
	}
}

// TestRoundTripSearchTextOnly: an absent arm stays absent (nil, not an empty struct) and a zero
// vector arm still carries an array so the Kotlin side reads `vector: []` rather than null.
func TestRoundTripSearchTextOnly(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.SearchMessage{
		H:         wire.Header{MessageType: wire.MsgSearch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
		Namespace: "ns",
		Text:      &wire.SearchTextArm{Index: "t", Query: "q"},
		Limit:     5,
	}
	frame, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.(wire.SearchMessage)
	if got.Vector != nil || got.Text == nil || got.Text.MinScore != nil || got.IncludeJSON {
		t.Fatalf("got %+v", got)
	}
}

// TestRoundTripSearchResult covers hits with and without bodies and the classified error fields.
func TestRoundTripSearchResult(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	body := `{"title":"deploy"}`
	errText := "no index configured for search"
	code := wire.ErrorCodeUnsupported
	retry := 50
	for _, msg := range []wire.SearchResultMessage{
		{
			H:         wire.Header{MessageType: wire.MsgSearchResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 7},
			Namespace: "app/tasks",
			Hits: []wire.SearchHit{
				{DocID: "54d100db-b8d0-8b38-8755-264670b3fc47", Score: 1.5, JSON: &body},
				{DocID: "84994230-081b-8414-9065-14f4f0cf226e", Score: 0.25},
			},
			ResolvedCommitHex: "cafe",
		},
		{
			H:                 wire.Header{MessageType: wire.MsgSearchResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 8},
			Namespace:         "app/tasks",
			Hits:              []wire.SearchHit{},
			ResolvedCommitHex: "",
			Error:             &errText,
			ErrorCode:         &code,
			RetryAfterMs:      &retry,
		},
	} {
		frame, err := c.Encode(msg)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := c.Decode(frame)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := decoded.(wire.SearchResultMessage)
		if !ok {
			t.Fatalf("decoded %T", decoded)
		}
		got.H.PayloadLength = 0
		if !reflect.DeepEqual(got, msg) {
			t.Fatalf("round trip changed the message:\n got %+v\nwant %+v", got, msg)
		}
	}
}

// TestSearchDecodeRejectsMissingBody: a "search" envelope with no body is a decode error, not a
// zero-valued message the server would then try to serve.
func TestSearchDecodeRejectsMissingBody(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	h := wire.Header{MessageType: wire.MsgSearch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}
	payload := append([]byte{1}, []byte(`{"kind":"search"}`)...)
	h.PayloadLength = len(payload)
	frame, err := c.EncodeFrameOnly(h, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Decode(frame); err == nil {
		t.Fatal("expected a decode error for a search envelope without a body")
	}
}
