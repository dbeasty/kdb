package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"

	"github.com/limidus/kdb/go/kdb/wire"
)

// BenchmarkPageThroughScan reads a whole namespace through a paged SELECT, the way a client
// exports or syncs a table: one op is every page, start to end. Paging by offset re-reads every row
// before each page, so the op grows with the square of the namespace over the page size; paging by
// key reads each row once.
func BenchmarkPageThroughScan(b *testing.B) {
	for _, tc := range []struct {
		docs, page int
		sql        string
	}{
		{5_000, 500, "SELECT * FROM t"},
		{20_000, 500, "SELECT * FROM t"},
		{20_000, 500, "SELECT * FROM t WHERE n >= 0"},
	} {
		b.Run(fmt.Sprintf("%s/docs=%d/page=%d", map[bool]string{true: "star", false: "where"}[tc.sql == "SELECT * FROM t"], tc.docs, tc.page), func(b *testing.B) {
			client, session := pagingBenchServer(b, tc.docs, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				token, rows := "", 0
				for {
					reply := client.requestB(b, wire.SqlExecMessage{
						H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: client.nextCorrelation()},
						Namespace: "app/data", SessionID: session, SQL: tc.sql, PageSize: tc.page, Continuation: token,
					}).(wire.SqlResultMessage)
					if reply.Error != nil {
						b.Fatal(*reply.Error)
					}
					rows += len(reply.Rows)
					if reply.ContinuationToken == "" {
						break
					}
					token = reply.ContinuationToken
				}
				if rows != tc.docs {
					b.Fatalf("paged %d rows, want %d", rows, tc.docs)
				}
			}
		})
	}
}

// pagingBenchServer is benchGovernedWireServer without a memory limit: the guard would sample this
// whole benchmark process - every earlier case's namespace included - and refuse writes and scans
// on a zone that is not what this measures.
func pagingBenchServer(b *testing.B, docCount, docBytes int) (*rawWireClient, string) {
	b.Helper()
	rt, err := embed.OpenMemoryRuntime("bench", "app/data", schema.None())
	if err != nil {
		b.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	filler := strings.Repeat("x", docBytes)
	for i := 0; i < docCount; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := srv.Upsert("app/data", id, fmt.Sprintf(`{"n":%d,"v":%q}`, i, filler), auth.Principal{}); err != nil {
			b.Fatal(err)
		}
	}
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = ln.Close() })
	client := dialRawWireClientB(b, "tcp://"+ln.Addr().String())
	client.handshakeB(b, wire.ClientSQL, "app/data")
	ack := client.sessionBeginB(b, "app/data", "READ_COMMITTED")
	return client, ack.SessionID
}
