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
	commitHash, err := c.PutJSON(ctx, namespace, docID, []byte(`{"marker":"go-ws-interop"}`))
	if err != nil {
		t.Fatalf("PutJSON over ws:// against a real JVM server: %v", err)
	}
	if commitHash == "" {
		t.Fatal("expected a non-empty commit hash")
	}

	jsonBody, gotCommitHash, err := c.GetJSON(ctx, namespace, docID)
	if err != nil {
		t.Fatalf("GetJSON over ws:// against a real JVM server: %v", err)
	}
	if gotCommitHash != commitHash {
		t.Fatalf("expected commit hash %s, got %s", commitHash, gotCommitHash)
	}
	if string(jsonBody) == "" {
		t.Fatal("expected a non-empty document body")
	}
}
