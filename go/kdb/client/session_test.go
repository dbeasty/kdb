package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func syncReplicas(t *testing.T, from, to *server.KdbServerRuntime) {
	t.Helper()
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", to, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"}, Local: from.PeerNamespaces(),
	})
	if err != nil || res.Namespaces[0].Err != nil {
		t.Fatalf("sync: %v %+v", err, res)
	}
}

// TestSessionReadsItsOwnWritesOnAnotherReplica: a user writes through replica A, then - carrying
// the session token - reads through replica B. Until B has the write, B refuses with a retry-after
// rather than serving the older value; once it has it, the read sees the write. A read without the
// token gets whatever B holds.
func TestSessionReadsItsOwnWritesOnAnotherReplica(t *testing.T) {
	addrA, a := startTestServer(t)
	addrB, b := startTestServer(t)
	a.NodeID, _ = codec.RandomUUID()
	b.NodeID, _ = codec.RandomUUID()
	b.SessionWait = 100 * time.Millisecond
	ctx := context.Background()
	id, _ := codec.RandomUUID()

	onA := connectTestClient(t, addrA).NewSession()
	if _, err := onA.Upsert(ctx, "app/data", id.String(), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	syncReplicas(t, a, b)
	if _, err := onA.Upsert(ctx, "app/data", id.String(), []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}

	// The user moves to B with the token.
	bClient := connectTestClient(t, addrB)
	onB := bClient.NewSession()
	onB.Resume(onA.Token())
	_, _, err := onB.GetJSON(ctx, "app/data", id.String())
	var busy *client.BusyError
	if !errors.As(err, &busy) || busy.RetryAfterMs <= 0 {
		t.Fatalf("B has not got v2: the session read must be refused with a retry-after, got %v", err)
	}
	// Without the token, B serves what it has - the older value.
	if body, _, err := bClient.GetJSON(ctx, "app/data", id.String()); err != nil || string(body) != `{"v":1}` {
		t.Fatalf("a read without the token: %s %v", body, err)
	}

	syncReplicas(t, a, b)
	body, _, err := onB.GetJSON(ctx, "app/data", id.String())
	if err != nil || string(body) != `{"v":2}` {
		t.Fatalf("once B has the write, the session reads it: %s %v", body, err)
	}
}

// A replica that catches up while the read waits serves it, without the client retrying.
func TestSessionReadWaitsForAReplicaCatchingUp(t *testing.T) {
	addrA, a := startTestServer(t)
	addrB, b := startTestServer(t)
	a.NodeID, _ = codec.RandomUUID()
	b.NodeID, _ = codec.RandomUUID()
	b.SessionWait = 3 * time.Second
	ctx := context.Background()
	id, _ := codec.RandomUUID()
	onA := connectTestClient(t, addrA).NewSession()
	if _, err := onA.Upsert(ctx, "app/data", id.String(), []byte(`{"v":"new"}`)); err != nil {
		t.Fatal(err)
	}
	onB := connectTestClient(t, addrB).NewSession()
	onB.Resume(onA.Token())
	go func() {
		time.Sleep(150 * time.Millisecond)
		syncReplicas(t, a, b)
	}()
	start := time.Now()
	body, _, err := onB.GetJSON(ctx, "app/data", id.String())
	if err != nil || string(body) != `{"v":"new"}` {
		t.Fatalf("the read should wait for B to catch up: %s %v", body, err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("the read returned before B could have caught up")
	}
}

// Monotonic reads: a value a session has read is never followed by an older one from a replica
// that is behind.
func TestSessionReadsAreMonotonic(t *testing.T) {
	addrA, a := startTestServer(t)
	addrB, b := startTestServer(t)
	a.NodeID, _ = codec.RandomUUID()
	b.NodeID, _ = codec.RandomUUID()
	b.SessionWait = 50 * time.Millisecond
	ctx := context.Background()
	id, _ := codec.RandomUUID()
	writer := connectTestClient(t, addrA)
	if _, err := writer.Upsert(ctx, "app/data", id.String(), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	syncReplicas(t, a, b)
	if _, err := writer.Upsert(ctx, "app/data", id.String(), []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	reader := connectTestClient(t, addrA).NewSession()
	if body, _, err := reader.GetJSON(ctx, "app/data", id.String()); err != nil || string(body) != `{"v":2}` {
		t.Fatalf("read on A: %s %v", body, err)
	}
	onB := connectTestClient(t, addrB).NewSession()
	onB.Resume(reader.Token())
	var busy *client.BusyError
	if _, _, err := onB.GetJSON(ctx, "app/data", id.String()); !errors.As(err, &busy) {
		t.Fatalf("B would go back to v1; the session read must be refused, got %v", err)
	}
}
