package server

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// pageAll runs sqlText page by page and returns every row's first cell (kdb_id for SELECT *), how
// many pages it took, and how many of the continuation tokens carried a key.
func pageAll(t *testing.T, c *rawWireClient, session, sqlText string, pageSize int) (ids []string, pages, keyed int) {
	t.Helper()
	token := ""
	for {
		reply := c.request(t, wire.SqlExecMessage{
			H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
			Namespace: "app/data", SessionID: session, SQL: sqlText, PageSize: pageSize, Continuation: token,
		}).(wire.SqlResultMessage)
		if reply.Error != nil {
			t.Fatalf("%s, page %d: %s", sqlText, pages, *reply.Error)
		}
		pages++
		for _, row := range reply.Rows {
			ids = append(ids, row[0])
		}
		if reply.ContinuationToken == "" {
			return ids, pages, keyed
		}
		if _, _, after, err := decodePageToken(reply.ContinuationToken, sqlText, nil); err == nil && after != nil {
			keyed++
		}
		token = reply.ContinuationToken
		if pages > 10_000 {
			t.Fatal("paging does not terminate")
		}
	}
}

func seedNumbered(t *testing.T, rt *KdbServerRuntime, n int) []codec.UUID {
	t.Helper()
	ids := make([]codec.UUID, n)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
		g := []string{"a", "b", "c"}[i%3]
		if _, err := rt.Upsert("app/data", ids[i], fmt.Sprintf(`{"n":%d,"g":%q}`, i, g), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}

func requireOncePerID(t *testing.T, what string, got []string, want int) {
	t.Helper()
	seen := map[string]int{}
	for _, id := range got {
		seen[id]++
	}
	if len(got) != want || len(seen) != want {
		t.Fatalf("%s: %d rows, %d distinct, want %d", what, len(got), len(seen), want)
	}
}

func requireIDOrder(t *testing.T, what string, got []string) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		a, errA := codec.ParseUUID(got[i-1])
		b, errB := codec.ParseUUID(got[i])
		if errA != nil || errB != nil {
			t.Fatalf("%s: rows are not document ids: %q %q", what, got[i-1], got[i])
		}
		if bytes.Compare(a.Bytes(), b.Bytes()) >= 0 {
			t.Fatalf("%s: row %d is not after row %d in id order", what, i, i-1)
		}
	}
}

// A paged SELECT with no ORDER BY pages by key: every row exactly once, in document id order, with
// every continuation carrying the key it resumes after - with and without a WHERE the scan
// filters, and through a LIMIT that ends mid-page.
func TestKeysetPagingReturnsEveryRowOnceInIDOrder(t *testing.T) {
	rt := newTestRuntime(t)
	seedNumbered(t, rt, 500)
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	ids, pages, keyed := pageAll(t, c, ack.SessionID, "SELECT * FROM t", 37)
	requireOncePerID(t, "SELECT *", ids, 500)
	requireIDOrder(t, "SELECT *", ids)
	if keyed != pages-1 {
		t.Fatalf("SELECT *: %d of %d continuations carried a key", keyed, pages-1)
	}

	ids, _, keyed = pageAll(t, c, ack.SessionID, "SELECT * FROM t WHERE n >= 100", 37)
	requireOncePerID(t, "WHERE n >= 100", ids, 400)
	requireIDOrder(t, "WHERE n >= 100", ids)
	if keyed == 0 {
		t.Fatal("WHERE n >= 100: no continuation carried a key")
	}

	ids, pages, _ = pageAll(t, c, ack.SessionID, "SELECT * FROM t LIMIT 90", 37)
	requireOncePerID(t, "LIMIT 90", ids, 90)
	if pages != 3 {
		t.Fatalf("LIMIT 90 in pages of 37: %d pages, want 3", pages)
	}
}

// An index access path pages by key too: its ids are read in document id order, after the key.
func TestKeysetPagingThroughAnIndex(t *testing.T) {
	rt := indexedRuntime(t)
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := c.sqlExec(t, "app/data", ack.SessionID, "CREATE INDEX by_g ON docs (g) USING HASH"); r.Error != nil {
		t.Fatal(*r.Error)
	}
	seedNumbered(t, rt, 300)
	if r := c.sqlExec(t, "app/data", ack.SessionID, "EXPLAIN SELECT * FROM docs WHERE g = 'b'"); r.Error == nil && len(r.Rows) > 0 {
		t.Logf("plan: %v", r.Rows)
	}
	ids, _, keyed := pageAll(t, c, ack.SessionID, "SELECT * FROM docs WHERE g = 'b'", 17)
	requireOncePerID(t, "WHERE g = 'b'", ids, 100)
	requireIDOrder(t, "WHERE g = 'b'", ids)
	if keyed == 0 {
		t.Fatal("WHERE g = 'b': no continuation carried a key")
	}
}

// Queries keyset paging does not serve - ordered, distinct, grouped, with an OFFSET - page by
// offset as before, and still return every row once.
func TestQueriesKeysetDoesNotServePageByOffset(t *testing.T) {
	rt := newTestRuntime(t)
	seedNumbered(t, rt, 200)
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	for _, q := range []struct {
		sql  string
		want int
	}{
		{"SELECT * FROM t ORDER BY n DESC", 200},
		{"SELECT DISTINCT g FROM t", 3},
		{"SELECT g, COUNT(*) FROM t GROUP BY g", 3},
		{"SELECT * FROM t LIMIT 150 OFFSET 20", 150},
	} {
		ids, _, keyed := pageAll(t, c, ack.SessionID, q.sql, 7)
		if len(ids) != q.want {
			t.Fatalf("%s: %d rows, want %d", q.sql, len(ids), q.want)
		}
		if keyed != 0 {
			t.Fatalf("%s: %d continuations carried a key; this query pages by offset", q.sql, keyed)
		}
	}
}

// A continuation minted before keyset paging - commit and offset, no key - still continues, by
// offset, and the query's rows come back once each.
func TestOffsetContinuationStillHonoured(t *testing.T) {
	rt := newTestRuntime(t)
	seedNumbered(t, rt, 120)
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	sqlText := "SELECT * FROM t ORDER BY n"
	first := c.request(t, wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: "app/data", SessionID: ack.SessionID, SQL: sqlText, PageSize: 50,
	}).(wire.SqlResultMessage)
	commit, offset, after, err := decodePageToken(first.ContinuationToken, sqlText, nil)
	if err != nil || after != nil || offset != 50 {
		t.Fatalf("an ordered query's token: offset=%d after=%v err=%v", offset, after, err)
	}
	legacy := encodePageToken(commit, offset, nil, sqlText, nil)
	rest := c.request(t, wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: "app/data", SessionID: ack.SessionID, SQL: sqlText, PageSize: 100, Continuation: legacy,
	}).(wire.SqlResultMessage)
	if rest.Error != nil || len(rest.Rows) != 70 {
		t.Fatalf("the rest after a key-less token: %d rows, err %v", len(rest.Rows), rest.Error)
	}
}
