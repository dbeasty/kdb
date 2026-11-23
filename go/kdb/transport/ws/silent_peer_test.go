package ws

import (
	"net"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/transport/core"
)

func silentListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, c)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
		for _, c := range held {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// Both client-side handshake phases - TLS for wss://, the HTTP upgrade for either scheme - must
// give up within the connect timeout against a peer that accepted and never answered. Neither
// had a deadline: the dial timeout ends once TCP is up.
func TestConnectHandshakeIsBoundedByConnectTimeout(t *testing.T) {
	for _, scheme := range []string{"ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			addr := silentListener(t)
			opts := core.DefaultConnectOptions()
			opts.ConnectTimeoutMs = 200
			if scheme == "wss" {
				opts.TLS = &core.TransportTlsSettings{Enabled: true, InsecureSkipVerify: true}
			}
			result := make(chan error, 1)
			go func() {
				conn, err := NewTransport(opts).Connect(scheme + "://" + addr + "/kdb")
				if conn != nil {
					_ = conn.Close()
				}
				result <- err
			}()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("Connect succeeded against a silent peer")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Connect still blocked 5s into a 200ms connect timeout")
			}
		})
	}
}

// The handshake deadline must not outlive the handshake: an established connection that sits
// idle past the connect timeout still delivers the next frame.
func TestConnectHandshakeDeadlineIsClearedAfterUpgrade(t *testing.T) {
	server := startFakeWsServer(t).uri("/kdb")
	opts := core.DefaultConnectOptions()
	opts.ConnectTimeoutMs = 100
	conn, err := NewTransport(opts).Connect(server)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	time.Sleep(300 * time.Millisecond) // past the 100ms handshake deadline
	frame := wireShapedFrame([]byte("after idle"))
	if err := conn.Send(frame); err != nil {
		t.Fatalf("send after idle: %v", err)
	}
	select {
	case got, ok := <-conn.Incoming():
		if !ok {
			t.Fatal("connection closed after idling past the handshake deadline")
		}
		if len(got) != len(frame) {
			t.Fatalf("echo: %d bytes, want %d", len(got), len(frame))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the echo")
	}
}
