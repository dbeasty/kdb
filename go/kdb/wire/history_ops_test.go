package wire_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

func TestRoundTripHistoryList(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.HistoryListMessage{
		H:         wire.Header{MessageType: wire.MsgHistoryList, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 11},
		Namespace: "app/tasks",
		From:      "head~10",
		Skip:      20,
		Limit:     25,
	}
	frame, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := decoded.(wire.HistoryListMessage)
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
	if env.Kind != "historyList" || env.Summary() != "historyList ns=app/tasks" {
		t.Fatalf("kind %q summary %q", env.Kind, env.Summary())
	}
}

// An omitted From and Limit stay omitted on the wire, so the server's
// defaults apply rather than the client accidentally asking for zero
// commits starting at nothing.
func TestHistoryListOmitsWhatWasNotAsked(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.HistoryListMessage{
		H:         wire.Header{MessageType: wire.MsgHistoryList, ProtocolVersion: wire.KdbWireProtocolVersion},
		Namespace: "app/tasks",
	}
	frame, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	body := string(frame)
	if strings.Contains(body, `"from"`) || strings.Contains(body, `"limit"`) {
		t.Fatalf("absent fields were serialized: %s", body)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.(wire.HistoryListMessage)
	if got.From != "" || got.Limit != 0 {
		t.Fatalf("absent fields came back set: %+v", got)
	}
}

func TestRoundTripHistoryResult(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.HistoryResultMessage{
		H:         wire.Header{MessageType: wire.MsgHistoryResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 11},
		Namespace: "app/tasks",
		Commits: []wire.HistoryCommit{
			{
				CommitHex: "aa", ParentHexes: []string{"bb", "cc"},
				TransactionID: "8b6f0d5c-0000-4000-8000-000000000001",
				// Microsecond resolution has to survive: rounding to
				// milliseconds here would make two commits in the same
				// millisecond indistinguishable in a log.
				TimestampMicros: 1757000000123456,
				AuthorNodeID:    "8b6f0d5c-0000-4000-8000-000000000002",
				TreeHex:         "dd", Message: "revert to bb", OperationCount: 3,
			},
			{CommitHex: "ee", TimestampMicros: 1, OperationCount: -1, Stubbed: true},
		},
		ResolvedHex: "aa",
	}
	frame, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.(wire.HistoryResultMessage)
	got.H.PayloadLength = 0
	if !reflect.DeepEqual(got, msg) {
		t.Fatalf("round trip changed the message:\n got %+v\nwant %+v", got, msg)
	}
	if got.Commits[0].TimestampMicros != 1757000000123456 {
		t.Fatal("microsecond precision was lost on the wire")
	}
}

func TestRoundTripRevertAndResult(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	req := wire.RevertMessage{
		H:         wire.Header{MessageType: wire.MsgRevert, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 3},
		Namespace: "app/tasks",
		To:        "head~1",
	}
	frame, err := c.Encode(req)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	gotReq := decoded.(wire.RevertMessage)
	gotReq.H.PayloadLength = 0
	if !reflect.DeepEqual(gotReq, req) {
		t.Fatalf("round trip changed the request:\n got %+v\nwant %+v", gotReq, req)
	}

	res := wire.RevertResultMessage{
		H:         wire.Header{MessageType: wire.MsgRevertResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 3},
		Namespace: "app/tasks",
		CommitHex: "ff", TargetHex: "aa", Restored: 2, Removed: 1,
	}
	frame, err = c.Encode(res)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	gotRes := decoded.(wire.RevertResultMessage)
	gotRes.H.PayloadLength = 0
	if !reflect.DeepEqual(gotRes, res) {
		t.Fatalf("round trip changed the result:\n got %+v\nwant %+v", gotRes, res)
	}
}

// Error frames follow the same additive convention as every other result
// message: the fields are absent unless something went wrong.
func TestHistoryErrorFieldsAreAdditive(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	frame, err := c.Encode(wire.HistoryResultMessage{
		H:         wire.Header{MessageType: wire.MsgHistoryResult, ProtocolVersion: wire.KdbWireProtocolVersion},
		Namespace: "app/tasks",
		Commits:   []wire.HistoryCommit{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(frame), `"error"`) {
		t.Fatalf("a successful result carried an error field: %s", frame)
	}

	msg := "no such revision"
	code := wire.ErrorCodeUnsupported
	frame, err = c.Encode(wire.HistoryResultMessage{
		H:         wire.Header{MessageType: wire.MsgHistoryResult, ProtocolVersion: wire.KdbWireProtocolVersion},
		Namespace: "app/tasks",
		Commits:   []wire.HistoryCommit{},
		Error:     &msg, ErrorCode: &code,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.(wire.HistoryResultMessage)
	if got.Error == nil || *got.Error != msg || got.ErrorCode == nil || *got.ErrorCode != code {
		t.Fatalf("the error did not survive: %+v", got)
	}
}

// The message codes have to stay where they are: a client built against
// 0x1F must keep speaking to a server that reads 0x1F.
func TestHistoryMessageCodesAreStable(t *testing.T) {
	for code, want := range map[uint16]wire.MessageType{
		0x1F: wire.MsgHistoryList,
		0x20: wire.MsgHistoryResult,
		0x21: wire.MsgRevert,
		0x22: wire.MsgRevertResult,
	} {
		got, ok := wire.MessageTypeFromCode(code)
		if !ok || got != want {
			t.Fatalf("code 0x%02X mapped to %v (ok=%v), want %v", code, got, ok, want)
		}
	}
	for _, tc := range []struct {
		t    wire.MessageType
		name string
	}{
		{wire.MsgHistoryList, "HISTORY_LIST"},
		{wire.MsgHistoryResult, "HISTORY_RESULT"},
		{wire.MsgRevert, "REVERT"},
		{wire.MsgRevertResult, "REVERT_RESULT"},
	} {
		if tc.t.String() != tc.name {
			t.Fatalf("%v renders as %q, want %q", tc.t, tc.t.String(), tc.name)
		}
	}
}

// The JSON keys are the contract a second implementation would be written
// against, so they are asserted literally rather than by round trip.
func TestHistoryJSONKeys(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	frame, err := c.Encode(wire.HistoryListMessage{
		H:         wire.Header{MessageType: wire.MsgHistoryList, ProtocolVersion: wire.KdbWireProtocolVersion},
		Namespace: "app/tasks", From: "head", Skip: 1, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	header, _ := c.DecodeHeader(frame)
	env, err := wire.DecodePayloadEnvelope(frame, header)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env.HistoryList)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"namespace":"app/tasks","from":"head","skip":1,"limit":2}`
	if string(raw) != want {
		t.Fatalf("historyList body is %s, want %s", raw, want)
	}
}
