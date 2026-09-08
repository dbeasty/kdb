package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/wire"
)

// historyHandler builds a handler on a file-backed runtime with an
// authenticated principal, which is what both history frames require.
//
// File-backed rather than memory: a revert has to resolve historical
// trees, and that is the path a real deployment takes.
func historyHandler(t *testing.T) *sqlWireConnHandler {
	t.Helper()
	rt, err := embed.OpenFileRuntime(t.TempDir(), "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Close() })
	srv := NewKdbServerRuntime(rt)
	h := newSqlWireConnHandler(wire.NewCodec(wire.EncodingJSON), srv)
	h.principal = auth.Principal{}
	h.authenticated = true
	return h
}

func seedCommits(t *testing.T, h *sqlWireConnHandler, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := embed.PutJSONDocument(h.runtime.Runtime, "app/data",
			fmt.Sprintf(`{"id":"a","n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
}

func listHistory(t *testing.T, h *sqlWireConnHandler, msg wire.HistoryListMessage) wire.HistoryResultMessage {
	t.Helper()
	msg.H = wire.Header{MessageType: wire.MsgHistoryList, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 5}
	reply, ok := h.handleHistoryList(msg).(wire.HistoryResultMessage)
	if !ok {
		t.Fatal("handleHistoryList did not reply with a HISTORY_RESULT")
	}
	return reply
}

func TestHistoryListOverTheWire(t *testing.T) {
	h := historyHandler(t)
	seedCommits(t, h, 5)

	res := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data"})
	if res.Error != nil {
		t.Fatalf("history list failed: %s", *res.Error)
	}
	// Five puts plus genesis.
	if len(res.Commits) != 6 {
		t.Fatalf("want 6 commits, got %d", len(res.Commits))
	}
	if res.Commits[0].CommitHex != res.ResolvedHex {
		t.Fatal("the listing should start at the revision it resolved")
	}
	if res.Commits[0].TimestampMicros == 0 {
		t.Fatal("commits should carry their timestamp")
	}
	if len(res.Commits[0].ParentHexes) == 0 {
		t.Fatal("commits should carry their parents")
	}
	// The correlation id has to come back, or a client cannot match the
	// reply to its request.
	if res.H.CorrelationID != 5 {
		t.Fatalf("correlation id came back as %d", res.H.CorrelationID)
	}
}

func TestHistoryListPagesAndDefaults(t *testing.T) {
	h := historyHandler(t)
	seedCommits(t, h, 8)

	page1 := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", Limit: 3})
	if len(page1.Commits) != 3 {
		t.Fatalf("limit 3 returned %d", len(page1.Commits))
	}
	page2 := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", Skip: 3, Limit: 3})
	if len(page2.Commits) != 3 {
		t.Fatalf("second page returned %d", len(page2.Commits))
	}
	if page1.Commits[0].CommitHex == page2.Commits[0].CommitHex {
		t.Fatal("skip did not advance the page")
	}

	// A zero limit takes the server default rather than returning nothing,
	// which is what a client that omits it means.
	none := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data"})
	if len(none.Commits) == 0 {
		t.Fatal("an omitted limit returned an empty page")
	}
	// And an absurd one is capped rather than honoured.
	huge := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", Limit: 1 << 20})
	if len(huge.Commits) > maxHistoryLimit {
		t.Fatalf("an unbounded limit was honoured: %d commits", len(huge.Commits))
	}
}

func TestHistoryListAcceptsRelativeRevisions(t *testing.T) {
	h := historyHandler(t)
	seedCommits(t, h, 6)

	full := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", Limit: 10})
	back := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", From: "head~2", Limit: 10})
	if back.Error != nil {
		t.Fatalf("head~2 failed: %s", *back.Error)
	}
	if back.Commits[0].CommitHex != full.Commits[2].CommitHex {
		t.Fatal("from=head~2 did not start two commits back")
	}
}

func TestHistoryListRefusesAnUnknownRevision(t *testing.T) {
	h := historyHandler(t)
	seedCommits(t, h, 2)

	res := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", From: "head~99"})
	if res.Error == nil {
		t.Fatal("an unresolvable revision should be an error, not an empty page")
	}
	if len(res.Commits) != 0 {
		t.Fatal("an error frame should carry no commits")
	}
}

func TestHistoryFramesRequireAuthentication(t *testing.T) {
	h := historyHandler(t)
	h.authenticated = false

	res := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data"})
	if res.Error == nil || !strings.Contains(*res.Error, "not authenticated") {
		t.Fatalf("unauthenticated history list returned %+v", res)
	}
	rev, ok := h.handleRevert(wire.RevertMessage{
		H:         wire.Header{MessageType: wire.MsgRevert, ProtocolVersion: wire.KdbWireProtocolVersion},
		Namespace: "app/data", To: "head~1",
	}).(wire.RevertResultMessage)
	if !ok || rev.Error == nil || !strings.Contains(*rev.Error, "not authenticated") {
		t.Fatalf("unauthenticated revert returned %+v", rev)
	}
}

func TestRevertOverTheWire(t *testing.T) {
	h := historyHandler(t)
	seedCommits(t, h, 4)

	before := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", Limit: 10})
	target := before.Commits[1].CommitHex

	reply, ok := h.handleRevert(wire.RevertMessage{
		H:         wire.Header{MessageType: wire.MsgRevert, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 9},
		Namespace: "app/data", To: "head~1",
	}).(wire.RevertResultMessage)
	if !ok {
		t.Fatal("handleRevert did not reply with a REVERT_RESULT")
	}
	if reply.Error != nil {
		t.Fatalf("revert failed: %s", *reply.Error)
	}
	if reply.TargetHex != target {
		t.Fatalf("revert targeted %s, want %s", reply.TargetHex, target)
	}
	if reply.CommitHex == reply.TargetHex {
		t.Fatal("a revert must write a new commit, not move to the old one")
	}
	if reply.Restored == 0 {
		t.Fatal("the revert restored nothing")
	}
	if reply.H.CorrelationID != 9 {
		t.Fatalf("correlation id came back as %d", reply.H.CorrelationID)
	}

	// History grew: the revert is itself a commit, and the state it
	// reverted away from is still listed.
	after := listHistory(t, h, wire.HistoryListMessage{Namespace: "app/data", Limit: 10})
	if len(after.Commits) != len(before.Commits)+1 {
		t.Fatalf("history went from %d to %d commits", len(before.Commits), len(after.Commits))
	}
	if !strings.HasPrefix(after.Commits[0].Message, "revert to ") {
		t.Fatalf("the new commit's message is %q", after.Commits[0].Message)
	}
}

func TestRevertRefusesAnEmptyOrUnknownRevision(t *testing.T) {
	h := historyHandler(t)
	seedCommits(t, h, 2)

	for _, to := range []string{"", "head~99", "not-a-revision"} {
		reply, ok := h.handleRevert(wire.RevertMessage{
			H:         wire.Header{MessageType: wire.MsgRevert, ProtocolVersion: wire.KdbWireProtocolVersion},
			Namespace: "app/data", To: to,
		}).(wire.RevertResultMessage)
		if !ok || reply.Error == nil {
			t.Fatalf("revert to %q should have failed, got %+v", to, reply)
		}
		if reply.CommitHex != "" {
			t.Fatalf("a failed revert reported commit %s", reply.CommitHex)
		}
	}
}
