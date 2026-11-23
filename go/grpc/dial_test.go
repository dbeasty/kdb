package kdbgrpc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	kdbgrpc "github.com/limidus/kdb/go/grpc"
	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// The point of Transport is that a client over gRPC is the *ordinary* client -
// same handshake, correlation, sessions and error classification - with only
// the bytes' path changed. So the test that matters is a real client.Client
// doing a real round trip over it, not a frame exchange that would prove only
// that the stream carries bytes (listen_test.go already covers that).
func TestTransportRoundTripsThroughOrdinaryClient(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("demo", testNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	ln, err := kdbgrpc.Listen("grpc://127.0.0.1:0", server.NewKdbServerRuntime(rt), kdbgrpc.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	c, err := client.ConnectWithOptions(ctx, ln.Addr().String(), "", client.ConnectOptions{
		Transport: kdbgrpc.NewTransport(),
	})
	if err != nil {
		t.Fatalf("connect over grpc transport: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	const docID = "11111111-1111-4111-8111-111111111111"
	if _, err := c.Upsert(ctx, testNamespace, docID, []byte(`{"name":"over-grpc"}`)); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	body, _, err := c.GetJSON(ctx, testNamespace, docID)
	if err != nil {
		t.Fatalf("GetJSON: %v", err)
	}
	if got, want := string(body), `"over-grpc"`; !contains(got, want) {
		t.Fatalf("GetJSON = %s, want it to contain %s", got, want)
	}
}

// A grpc:// scheme has to be accepted as readily as a bare host:port, since
// that is what a config file or flag will carry.
func TestTransportAcceptsGRPCScheme(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("demo", testNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	ln, err := kdbgrpc.Listen("grpc://127.0.0.1:0", server.NewKdbServerRuntime(rt), kdbgrpc.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	c, err := client.ConnectWithOptions(ctx, "grpc://"+ln.Addr().String(), "", client.ConnectOptions{
		Transport: kdbgrpc.NewTransport(),
	})
	if err != nil {
		t.Fatalf("connect with grpc:// scheme: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	const docID = "22222222-2222-4222-8222-222222222222"
	if _, err := c.Upsert(ctx, testNamespace, docID, []byte(`{"name":"scheme"}`)); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

// client.Client sends from whichever goroutine makes a request, relying on the transport's Send
// being safe for concurrent use (the TCP and WebSocket ones lock around the write). A gRPC
// stream is not: concurrent SendMsg calls race (go test -race reports it) and can interleave
// messages on the wire.
func TestTransportConcurrentRequests(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("demo", testNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	ln, err := kdbgrpc.Listen("grpc://127.0.0.1:0", server.NewKdbServerRuntime(rt), kdbgrpc.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	c, err := client.ConnectWithOptions(ctx, ln.Addr().String(), "", client.ConnectOptions{Transport: kdbgrpc.NewTransport()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	const docID = "33333333-3333-4333-8333-333333333333"
	if _, err := c.Upsert(ctx, testNamespace, docID, []byte(`{"name":"x"}`)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, _, err := c.GetJSON(ctx, testNamespace, docID); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent GetJSON: %v", err)
	}
}
