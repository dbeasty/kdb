package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// wsTestNamespace matches the namespace startTestServer opens in client_test.go.
const wsTestNamespace = "app/data"

func newWsTestRuntime(t *testing.T) *server.KdbServerRuntime {
	t.Helper()
	embedded, err := embed.OpenMemoryRuntime("demo", wsTestNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	return server.NewKdbServerRuntime(embedded)
}

// TestClientOverWebSocket is the end-to-end proof that the WebSocket listener actually serves
// the SQL wire protocol, not merely that it completes a handshake: a real client SDK doing real
// document operations, over ws://, against the Go server.
//
// It is the test that could not exist while transport/ws's Listen returned HTTP 501 - and the
// one that matters most for the browser story, since ws:// is the only transport page
// JavaScript can open.
func TestClientOverWebSocket(t *testing.T) {
	runtime := newWsTestRuntime(t)

	listener, err := server.ListenSqlWireWS("ws://127.0.0.1:0/kdb", runtime)
	if err != nil {
		t.Fatalf("ws listen: %v", err)
	}
	defer listener.Close()

	uri := fmt.Sprintf("ws://%s/kdb", listener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := client.Connect(ctx, uri, "")
	if err != nil {
		t.Fatalf("connect over ws: %v", err)
	}
	defer c.Close()

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	id := docID.String()

	body := []byte(`{"name":"ada","score":42}`)
	commit, err := c.PutJSON(ctx, wsTestNamespace, id, body)
	if err != nil {
		t.Fatalf("PutJSON over ws: %v", err)
	}
	if commit == "" {
		t.Fatal("PutJSON returned an empty commit hash")
	}

	got, _, err := c.GetJSON(ctx, wsTestNamespace, id)
	if err != nil {
		t.Fatalf("GetJSON over ws: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("unmarshal round-tripped document: %v", err)
	}
	if decoded["name"] != "ada" {
		t.Fatalf("document did not round trip over ws: %s", got)
	}
}

// TestClientOverWebSocketConcurrentRequests exercises correlation-id matching over a transport
// that delivers whole messages: several requests in flight on one connection, each reply
// matched to its own caller.
func TestClientOverWebSocketConcurrentRequests(t *testing.T) {
	runtime := newWsTestRuntime(t)

	listener, err := server.ListenSqlWireWS("ws://127.0.0.1:0/kdb", runtime)
	if err != nil {
		t.Fatalf("ws listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := client.Connect(ctx, fmt.Sprintf("ws://%s/kdb", listener.Addr().String()), "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	const writers = 6
	ids := make([]string, writers)
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		u, err := codec.RandomUUID()
		if err != nil {
			t.Fatalf("uuid: %v", err)
		}
		ids[i] = u.String()
		go func(n int, id string) {
			_, err := c.Upsert(ctx, wsTestNamespace, id, []byte(fmt.Sprintf(`{"n":%d}`, n)))
			errs <- err
		}(i, ids[i])
	}
	for i := 0; i < writers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent upsert: %v", err)
		}
	}

	for i, id := range ids {
		got, _, err := c.GetJSON(ctx, wsTestNamespace, id)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(got, &decoded); err != nil {
			t.Fatalf("unmarshal %d: %v", i, err)
		}
		// A mismatch here is the correlation-id bug this test exists to catch: replies handed
		// to the wrong caller still decode, they are just answers to somebody else's question.
		if n, ok := decoded["n"].(float64); !ok || int(n) != i {
			t.Fatalf("document %d carries the wrong body: %s", i, got)
		}
	}
}
