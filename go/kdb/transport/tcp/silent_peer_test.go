package tcp

import (
	"net"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/transport/core"
)

// silentListener accepts connections and never writes a byte - a black-holing load balancer, or
// a process that accepted and wedged.
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

// A tcps:// Connect to a peer that never answers the ClientHello must fail within the connect
// timeout. The TLS handshake used to run under context.Background(), so it waited forever.
func TestConnectTLSHandshakeIsBoundedByConnectTimeout(t *testing.T) {
	addr := silentListener(t)
	opts := core.DefaultConnectOptions()
	opts.ConnectTimeoutMs = 200
	opts.TLS = &core.TransportTlsSettings{Enabled: true, InsecureSkipVerify: true}
	result := make(chan error, 1)
	go func() {
		conn, err := NewTransport(opts).Connect("tcps://" + addr)
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Connect succeeded against a peer that never spoke TLS")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect still blocked 5s into a 200ms connect timeout")
	}
}
