package wire_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

func TestSquashHeadRoundTrip(t *testing.T) {
	req := roundTrip(t, wire.SquashHeadMessage{H: wire.Header{MessageType: wire.MsgSquashHead, CorrelationID: 4},
		Namespace: "app/data", HaveHex: "ab"}).(wire.SquashHeadMessage)
	if req.Namespace != "app/data" || req.HaveHex != "ab" {
		t.Fatalf("request: %+v", req)
	}
	c := buildTestCommit(t, "head")
	res := roundTrip(t, wire.SquashHeadResultMessage{H: wire.Header{MessageType: wire.MsgSquashHeadResult, CorrelationID: 4},
		Namespace: "app/data", Commit: c, Descends: true, Backlog: 12345}).(wire.SquashHeadResultMessage)
	if res.Commit.Hash != c.Hash || !res.Descends || res.Backlog != 12345 {
		t.Fatalf("result: %+v", res)
	}
	refused := roundTrip(t, wire.SquashHeadResultMessage{H: wire.Header{MessageType: wire.MsgSquashHeadResult, CorrelationID: 5},
		Namespace: "app/data", Refused: "full history required"}).(wire.SquashHeadResultMessage)
	if refused.Refused == "" || refused.Descends {
		t.Fatalf("refused: %+v", refused)
	}
	if wire.MsgSquashHead.String() != "SQUASH_HEAD" || wire.MsgSquashHeadResult.String() != "SQUASH_HEAD_RESULT" {
		t.Fatal("names")
	}
}
