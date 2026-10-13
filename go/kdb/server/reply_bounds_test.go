package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

func listenForTest(t *testing.T, rt *KdbServerRuntime) *rawWireClient {
	t.Helper()
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	c := dialRawWireClient(t, "tcp://"+ln.Addr().String())
	t.Cleanup(func() { _ = c.conn.Close() })
	c.handshake(t, wire.ClientSQL, "app/data")
	return c
}

func seedPaddedDocs(t *testing.T, rt *KdbServerRuntime, n, bodyBytes int) []codec.UUID {
	t.Helper()
	pad := strings.Repeat("p", bodyBytes)
	ids := make([]codec.UUID, n)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
		if _, err := rt.Upsert("app/data", ids[i], fmt.Sprintf(`{"n":%d,"pad":%q}`, i, pad), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}

func requireResourceExhausted(t *testing.T, errMsg *string, code *wire.ErrorCode) {
	t.Helper()
	if errMsg == nil || code == nil || *code != wire.ErrorCodeResourceExhausted {
		t.Fatalf("want a RESOURCE_EXHAUSTED error, got error=%v code=%v", deref(errMsg), code)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

// Bug 1: a SELECT whose rows exceed the frame limit used to be encoded, refused by the frame
// check, and dropped - the client heard nothing until its own timeout. It now gets a typed
// RESOURCE_EXHAUSTED reply, well inside the raw client's 2s wait.
func TestOversizedSelectIsAnswered(t *testing.T) {
	rt := newTestRuntime(t)
	seedPaddedDocs(t, rt, 18, 1<<20) // ~18 MiB of rows: over the 16 MiB frame
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	start := time.Now()
	res := c.sqlExec(t, "app/data", ack.SessionID, "SELECT * FROM t")
	requireResourceExhausted(t, res.Error, res.ErrorCode)
	if !strings.Contains(*res.Error, "reply limit") || len(res.Rows) != 0 {
		t.Fatalf("unexpected reply: %q, %d rows", *res.Error, len(res.Rows))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("answered after %v", elapsed)
	}
	// The connection is still usable.
	if res := c.sqlExec(t, "app/data", ack.SessionID, "SELECT COUNT(*) FROM t"); res.Error != nil {
		t.Fatalf("follow-up query failed: %s", *res.Error)
	}
}

// The byte cap pages instead of failing when the request asked for pages: each page ends early,
// and walking the continuations returns every row exactly once.
func TestByteCapEndsPagesEarlyAndContinues(t *testing.T) {
	rt := newTestRuntime(t)
	ids := seedPaddedDocs(t, rt, 40, 4096)
	rt.SetMaxResultBytes(20_000) // ~4 rows per reply
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	unpaged := c.sqlExec(t, "app/data", ack.SessionID, "SELECT * FROM t")
	requireResourceExhausted(t, unpaged.Error, unpaged.ErrorCode)

	seen := map[string]int{}
	token, pages := "", 0
	for {
		reply := c.request(t, wire.SqlExecMessage{
			H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
			Namespace: "app/data", SessionID: ack.SessionID, SQL: "SELECT * FROM t", PageSize: 100, Continuation: token,
		}).(wire.SqlResultMessage)
		if reply.Error != nil {
			t.Fatalf("page %d: %s", pages, *reply.Error)
		}
		pages++
		size := 0
		for _, row := range reply.Rows {
			seen[row[0]]++
			size += rowEncodedBytes(row)
		}
		if size > 20_000 {
			t.Fatalf("page %d carries %d row bytes, over the 20000 cap", pages, size)
		}
		if reply.ContinuationToken == "" {
			break
		}
		token = reply.ContinuationToken
		if pages > 100 {
			t.Fatal("paging does not terminate")
		}
	}
	if pages < 5 {
		t.Fatalf("expected the byte cap to split the result into several pages, got %d", pages)
	}
	if len(seen) != len(ids) {
		t.Fatalf("paged walk returned %d distinct rows, want %d", len(seen), len(ids))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("row %s returned %d times", id, n)
		}
	}
}

// Bug 1, generally: any reply too large to encode is answered with the error form of its own
// reply type. A document over the frame limit is the reachable case for a point read.
func TestOversizedDocumentGetIsAnswered(t *testing.T) {
	rt := newTestRuntime(t)
	id := mustRandomUUID(t)
	if _, err := rt.Upsert("app/data", id, fmt.Sprintf(`{"pad":%q}`, strings.Repeat("x", wire.DefaultMaxFrameBytes)), auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	c := listenForTest(t, rt)
	reply := c.request(t, wire.DocumentGetMessage{
		H:         wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: "app/data", DocID: id.String(),
	})
	res, ok := reply.(wire.DocumentGetResultMessage)
	if !ok {
		t.Fatalf("want DocumentGetResultMessage, got %T", reply)
	}
	requireResourceExhausted(t, res.Error, res.ErrorCode)
	if res.JSON != nil {
		t.Fatal("an error reply must not carry the document")
	}
}

func TestOversizedReplyKeepsTypeAndCorrelation(t *testing.T) {
	tooLarge := func() error {
		_, err := wire.NewCodec(wire.EncodingJSON).Encode(wire.DocumentGetResultMessage{
			H: header(1, wire.MsgDocumentGetResult), JSON: ptr(strings.Repeat("x", wire.DefaultMaxFrameBytes)),
		})
		return err
	}()
	if tooLarge == nil {
		t.Fatal("expected an encode failure")
	}
	replies := []wire.Message{
		wire.SqlResultMessage{H: header(11, wire.MsgSqlResult), Namespace: "ns", SessionID: "s"},
		wire.DocumentGetResultMessage{H: header(12, wire.MsgDocumentGetResult), Namespace: "ns", DocID: "d"},
		wire.UpsertResultMessage{H: header(13, wire.MsgUpsertResult)},
		wire.SearchResultMessage{H: header(14, wire.MsgSearchResult)},
		wire.HistoryResultMessage{H: header(15, wire.MsgHistoryResult)},
		wire.RevertResultMessage{H: header(16, wire.MsgRevertResult)},
		wire.TxCommitMultiResultMessage{H: header(17, wire.MsgTxCommitMultiResult)},
		wire.LockResultMessage{H: header(18, wire.MsgLockResult)},
	}
	codec := wire.NewCodec(wire.EncodingJSON)
	for _, r := range replies {
		got := oversizedReply(r, tooLarge)
		if got.Header().MessageType != r.Header().MessageType || got.Header().CorrelationID != r.Header().CorrelationID {
			t.Fatalf("%T: header changed to %+v", r, got.Header())
		}
		frame, err := codec.Encode(got)
		if err != nil {
			t.Fatalf("%T: error reply does not encode: %v", r, err)
		}
		decoded, err := codec.Decode(frame)
		if err != nil {
			t.Fatalf("%T: %v", r, err)
		}
		if code := errorCodeOf(decoded); code == nil || *code != wire.ErrorCodeResourceExhausted {
			t.Fatalf("%T: decoded reply carries no RESOURCE_EXHAUSTED: %+v", r, decoded)
		}
	}
}

// A handler panic on the listener path is recovered and answered: dispatchAndSend used to call
// dispatch without the backstop, so a panic took the whole process down.
func TestListenerPathRecoversHandlerPanic(t *testing.T) {
	rt := newTestRuntime(t)
	var once sync.Once
	rt.SetBeforeSqlExecHookForTest(func(wire.SqlExecMessage) {
		once.Do(func() { panic("boom") })
	})
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	res := c.sqlExec(t, "app/data", ack.SessionID, "SELECT COUNT(*) FROM t")
	if res.ErrorCode == nil || *res.ErrorCode != wire.ErrorCodeInternal {
		t.Fatalf("want INTERNAL for the panicking request, got %v", deref(res.Error))
	}
	if res := c.sqlExec(t, "app/data", ack.SessionID, "SELECT COUNT(*) FROM t"); res.Error != nil {
		t.Fatalf("server did not survive the panic: %s", *res.Error)
	}
}

func TestBoundResultRows(t *testing.T) {
	rows := [][]string{{"a", "bb"}, {"ccc"}, {strings.Repeat("d", 100)}}
	sizes := []int{rowEncodedBytes(rows[0]), rowEncodedBytes(rows[1]), rowEncodedBytes(rows[2])}
	if fit, _ := boundResultRows(rows, 1<<20); fit != 3 {
		t.Fatalf("everything fits, got %d", fit)
	}
	if fit, _ := boundResultRows(rows, sizes[0]+sizes[1]); fit != 2 {
		t.Fatalf("want 2, got %d", fit)
	}
	if fit, _ := boundResultRows(rows, sizes[0]-1); fit != 0 {
		t.Fatalf("want 0, got %d", fit)
	}
	// The estimate is an upper bound on what the rows add to a real encoded reply.
	codec := wire.NewCodec(wire.EncodingJSON)
	empty, _ := codec.Encode(wire.SqlResultMessage{H: header(1, wire.MsgSqlResult), Rows: [][]string{}})
	full, _ := codec.Encode(wire.SqlResultMessage{H: header(1, wire.MsgSqlResult), Rows: rows})
	if got, est := len(full)-len(empty), sizes[0]+sizes[1]+sizes[2]; got > est {
		t.Fatalf("rows added %d bytes, estimate was %d", got, est)
	}
}

func errorCodeOf(m wire.Message) *wire.ErrorCode {
	switch r := m.(type) {
	case wire.SqlResultMessage:
		return r.ErrorCode
	case wire.DocumentGetResultMessage:
		return r.ErrorCode
	case wire.UpsertResultMessage:
		return r.ErrorCode
	case wire.SearchResultMessage:
		return r.ErrorCode
	case wire.HistoryResultMessage:
		return r.ErrorCode
	case wire.RevertResultMessage:
		return r.ErrorCode
	case wire.TxCommitMultiResultMessage:
		return r.ErrorCode
	case wire.LockResultMessage:
		return r.ErrorCode
	}
	return nil
}
