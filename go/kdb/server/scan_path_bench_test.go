package server

// Benchmarks for the read-path admission cost added by the adaptive estimator, and an
// A/B-portable wire-level read/scan benchmark. The wire benchmark deliberately uses only API
// that exists both before and after the estimator change, so the identical file can be dropped
// into a checkout of either build for an interleaved comparison (the methodology
// docs/benchmarks/lightsail-sim/README.md prescribes).

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/wire"
)

// benchGovernedWireServer stands up a real wire listener over an in-memory runtime with
// admission enabled and docCount pre-seeded documents, mirroring the e2e test harness.
func benchGovernedWireServer(b *testing.B, docCount, docBytes int) (*KdbServerRuntime, *rawWireClient, string) {
	b.Helper()
	rt, err := embed.OpenMemoryRuntime("bench", "app/data", schema.None())
	if err != nil {
		b.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	srv.SetMemoryLimit(1<<30, 0.85)
	b.Cleanup(func() { srv.memGuard.Stop() })

	filler := make([]byte, docBytes)
	for i := range filler {
		filler[i] = 'a' + byte(i%26)
	}
	for i := 0; i < docCount; i++ {
		docID, err := codec.RandomUUID()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := srv.Upsert("app/data", docID, fmt.Sprintf(`{"n":%d,"v":%q}`, i, string(filler)), auth.Principal{}); err != nil {
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
	return srv, client, ack.SessionID
}

// BenchmarkWireSelect measures the full wire round trip of a SELECT - TCP loopback, JSON codec,
// parse, (on governed builds) admission, execution, response. Run identically against the
// pre-estimator build for the end-to-end read-path A/B.
func BenchmarkWireSelect(b *testing.B) {
	for _, tc := range []struct {
		name string
		sql  string
		docs int
	}{
		{"limit5-of-1000", "SELECT kdb_id FROM t LIMIT 5", 1000},
		{"star-200", "SELECT * FROM t", 200},
		{"count-1000", "SELECT COUNT(*) FROM t", 1000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			_, client, session := benchGovernedWireServer(b, tc.docs, 512)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res := client.sqlExecB(b, "app/data", session, tc.sql)
				if res.Error != nil {
					b.Fatalf("%s: %s", tc.sql, *res.Error)
				}
			}
		})
	}
}

// BenchmarkWireDocumentGet is the point-read equivalent of BenchmarkWireSelect.
func BenchmarkWireDocumentGet(b *testing.B) {
	srv, client, _ := benchGovernedWireServer(b, 100, 2048)
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		b.Fatal(err)
	}
	commit, ok := srv.Runtime.DAG.GetCommit(head)
	if !ok {
		b.Fatal("head commit missing")
	}
	tree, ok := srv.Runtime.DAG.GetDocumentTree(commit.DocumentTreeHash)
	if !ok {
		b.Fatal("head tree missing")
	}
	var docID codec.UUID
	for id := range tree.MaterializedEntries() {
		docID = id
		break
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reply := client.requestB(b, wire.DocumentGetMessage{
			H:         wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: client.nextCorrelation()},
			Namespace: "app/data",
			DocID:     docID.String(),
		})
		res, ok := reply.(wire.DocumentGetResultMessage)
		if !ok {
			b.Fatalf("expected DocumentGetResultMessage, got %T", reply)
		}
		if res.Error != nil {
			b.Fatal(*res.Error)
		}
	}
}

// Benchmark-friendly variants of the test-helper methods (which take *testing.T).

func dialRawWireClientB(b *testing.B, addr string) *rawWireClient {
	b.Helper()
	var t testing.T
	c := dialRawWireClient(&t, addr)
	if t.Failed() {
		b.Fatal("dial failed")
	}
	return c
}

// requestB is the benchmark request path: same wire round trip as request, but with a tight
// poll instead of its 5ms sleep, which would otherwise put a floor under every measurement.
func (c *rawWireClient) requestB(b *testing.B, msg wire.Message) wire.Message {
	b.Helper()
	frame, err := c.codec.Encode(msg)
	if err != nil {
		b.Fatal(err)
	}
	if err := c.conn.Send(frame); err != nil {
		b.Fatal(err)
	}
	cid := msg.Header().CorrelationID
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reply := c.conn.TryPoll()
		if reply == nil {
			runtime.Gosched()
			continue
		}
		decoded, err := c.codec.Decode(reply)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.Header().CorrelationID == cid {
			return decoded
		}
	}
	b.Fatalf("no response for correlation %d", cid)
	return nil
}

func (c *rawWireClient) handshakeB(b *testing.B, mode wire.ClientMode, namespace string) {
	b.Helper()
	var t testing.T
	c.handshake(&t, mode, namespace)
	if t.Failed() {
		b.Fatal("handshake failed")
	}
}

func (c *rawWireClient) sessionBeginB(b *testing.B, namespace, consistency string) wire.SessionBeginAckMessage {
	b.Helper()
	var t testing.T
	ack := c.sessionBegin(&t, namespace, consistency)
	if t.Failed() {
		b.Fatal("session begin failed")
	}
	return ack
}

func (c *rawWireClient) sqlExecB(b *testing.B, namespace, session, sqlText string) wire.SqlResultMessage {
	b.Helper()
	reply := c.requestB(b, wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		SessionID: session,
		SQL:       sqlText,
	})
	res, ok := reply.(wire.SqlResultMessage)
	if !ok {
		b.Fatalf("expected SqlResultMessage, got %T", reply)
	}
	return res
}
