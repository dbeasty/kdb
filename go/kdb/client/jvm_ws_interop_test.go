//go:build interop

// jvm_ws_interop_test.go proves Connect's new ws:// dial path (dialTransport) for real, against
// a real JVM kdb-server - the counterpart to go/kdb/interop's jvm_server_interop_test.go, which
// exercises the same server by hand-rolling the wire protocol rather than going through this
// package's public API. Gated behind the "interop" build tag and a running server the same way:
// go test ./... (no tags) skips this file entirely.
//
// To run: start a JVM server with SQL wire over WebSocket, e.g.
//
//	./gradlew :kdb-service:runService --args="--listen-sql-ws kdb-ws://127.0.0.1:17444/kdb?bind=true --namespace demo/interop"
//
// then:
//
//	KDB_JVM_SQL_WS_URI=ws://127.0.0.1:17444/kdb go test -tags interop ./go/kdb/client/... -run JvmServerOverWebSocket -v
package client_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

func TestConnectAgainstRealJvmServerOverWebSocket(t *testing.T) {
	uri := os.Getenv("KDB_JVM_SQL_WS_URI")
	if uri == "" {
		t.Skip("KDB_JVM_SQL_WS_URI not set - see file doc comment for how to start a JVM server to test against")
	}
	namespace := os.Getenv("KDB_JVM_SQL_NAMESPACE")
	if namespace == "" {
		namespace = "demo/interop"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Connect(ctx, uri, "")
	if err != nil {
		t.Fatalf("connect to %s: %v", uri, err)
	}
	defer c.Close()

	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	// Unique per run: the SELECT below (deliberately unfiltered, see its comment) has to pick
	// this write out from whatever other rows a long-lived shared server has accumulated.
	marker := "go-ws-interop-" + docID
	commitHash, err := c.PutJSON(ctx, namespace, docID, []byte(`{"marker":"`+marker+`"}`))
	if err != nil {
		t.Fatalf("PutJSON over ws:// against a real JVM server: %v", err)
	}
	if commitHash == "" {
		t.Fatal("expected a non-empty commit hash")
	}

	// Not GetJSON: it sends wire.MsgDocumentGet (component 40's direct-document-get message),
	// which the Kotlin server has no counterpart for - kdb-wire's WireMessageType tops out at
	// SESSION_BEGIN_ACK(0x13), never adding DOCUMENT_GET/DOCUMENT_GET_RESULT/UPSERT/
	// UPSERT_RESULT (0x14-0x17), and SqlWireHost.dispatch has no case for them either. Sending
	// one today doesn't get a clean rejection: the JVM's decodeHeader throws
	// WireDecodeException("unknown message type"), which SqlWireListen.kt's per-frame
	// coroutineScope propagates uncaught, tearing down the whole connection (see
	// docs/kdb-finish-up-plan.md's Kotlin frame-isolation finding - the same failure mode, a
	// different unhandled-decode trigger). Verify the write via SqlExec's `_doc` column instead,
	// which Kotlin does support end-to-end (proven by kdb/interop's
	// TestGoClientAgainstRealJvmServer) - this still exercises the thing this test exists to
	// prove, Connect's ws:// dial path against a real JVM server, without depending on
	// unimplemented Kotlin functionality.
	//
	// No WHERE/parameter binding: decodeRows matches result columns to struct fields by
	// case-insensitive NAME only (no struct-tag support), so a literal `_doc` column can never
	// match an exported Go field (Go requires an exported field to start with an uppercase
	// letter) - `AS doc` sidesteps that. `id = ?` was also tried against this same server and
	// came back "invalid parameter wire object", so this scans the table and matches by content
	// instead - namespace `demo/interop` is small enough (interop-test scale, not production
	// data) for that to be cheap.
	var rows []struct{ Doc string }
	if err := c.Query(ctx, namespace, "SELECT _doc AS doc FROM users", nil, &rows); err != nil {
		t.Fatalf("SqlExec SELECT over ws:// against a real JVM server: %v", err)
	}
	found := false
	for _, r := range rows {
		if r.Doc == `{"marker":"`+marker+`"}` {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected a row containing marker %q among %d rows", marker, len(rows))
	}
}
