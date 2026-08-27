package tcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// listenAndAcceptTLS mirrors listenAndAccept (socket_connection_test.go) but binds a tcps://
// listener using serverSettings.
func listenAndAcceptTLS(t *testing.T, serverSettings *core.TransportTlsSettings) (addr string, accepted <-chan stream.ConnectionHandle, cleanup func()) {
	t.Helper()
	opts := core.DefaultConnectOptions()
	opts.TLS = serverSettings
	transport := NewTransport(opts)
	ln, err := transport.ListenBound("tcps://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatalf("ListenBound: %v", err)
	}
	ch := make(chan stream.ConnectionHandle, 8)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = transport.Serve(ctx, ln, func(conn stream.ConnectionHandle) {
			ch <- conn
		})
	}()
	return fmt.Sprintf("tcps://%s", ln.Addr().String()), ch, func() {
		cancel()
		_ = ln.Close()
	}
}

func sendAndExpect(t *testing.T, client, server stream.ConnectionHandle, correlationID int) {
	t.Helper()
	wireCodec := wire.NewCodec(wire.EncodingJSON)
	msg := wire.PositionAckMessage{
		H:          wire.Header{MessageType: wire.MsgPositionAck, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: correlationID},
		Namespace:  "app/data",
		CommitHash: codec.Hash{},
	}
	frame, err := wireCodec.Encode(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := client.Send(frame); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case got, ok := <-server.Incoming():
		if !ok {
			t.Fatal("server Incoming() closed before delivering the frame")
		}
		gotMsg, err := wireCodec.Decode(got)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if gotMsg.Header().CorrelationID != correlationID {
			t.Fatalf("correlation id: got %d, want %d", gotMsg.Header().CorrelationID, correlationID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the frame to arrive over TLS")
	}
}

// TestTLSRoundTrip is the regression/feature test for docs/kdb-finish-up-plan.md's 2.1: a real
// client-server frame round trip over a tcps:// connection, server cert verified against a CA
// the client trusts (not InsecureSkipVerify/trustAll - the actual verification path).
func TestTLSRoundTrip(t *testing.T) {
	ca := newTestCA(t)
	server := ca.issueLeaf(t, "server", true)

	addr, accepted, cleanup := listenAndAcceptTLS(t, &core.TransportTlsSettings{
		Enabled:  true,
		CertFile: server.CertFile,
		KeyFile:  server.KeyFile,
	})
	defer cleanup()

	clientOpts := core.DefaultConnectOptions()
	clientOpts.TLS = &core.TransportTlsSettings{Enabled: true, CAFile: ca.certFile(t)}
	client, err := NewTransport(clientOpts).Connect(addr)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	var serverConn stream.ConnectionHandle
	select {
	case serverConn = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}
	defer serverConn.Close()

	sendAndExpect(t, client, serverConn, 1)
}

// TestTLSConnectRejectsUntrustedServerCert proves the client's CA verification is real, not a
// no-op: a client trusting a *different* CA than the one that signed the server's cert must fail
// to connect, rather than silently accepting whatever certificate the server presents.
func TestTLSConnectRejectsUntrustedServerCert(t *testing.T) {
	serverCA := newTestCA(t)
	server := serverCA.issueLeaf(t, "server", true)
	otherCA := newTestCA(t) // client will trust this one instead

	addr, _, cleanup := listenAndAcceptTLS(t, &core.TransportTlsSettings{
		Enabled:  true,
		CertFile: server.CertFile,
		KeyFile:  server.KeyFile,
	})
	defer cleanup()

	clientOpts := core.DefaultConnectOptions()
	clientOpts.TLS = &core.TransportTlsSettings{Enabled: true, CAFile: otherCA.certFile(t)}
	_, err := NewTransport(clientOpts).Connect(addr)
	if err == nil {
		t.Fatal("expected Connect to fail: client trusts a different CA than the one that signed the server cert")
	}
}

// TestTLSMutualAuthRequired is the mTLS test: a server configured with RequireClientAuth must
// reject a client that presents no certificate, and accept one that presents a certificate
// signed by the configured CA. Each case gets its own listener/accept channel - a shared one
// would risk the "no cert" case's TCP-level-accepted-but-doomed connection being dequeued by the
// "valid cert" case instead of its own (Accept() returns before the TLS handshake even runs, so
// a connection lands in the channel regardless of whether that handshake will go on to fail).
func TestTLSMutualAuthRequired(t *testing.T) {
	ca := newTestCA(t)
	server := ca.issueLeaf(t, "server", true)
	caFile := ca.certFile(t)
	serverSettings := &core.TransportTlsSettings{
		Enabled:           true,
		CertFile:          server.CertFile,
		KeyFile:           server.KeyFile,
		CAFile:            caFile,
		RequireClientAuth: true,
	}

	t.Run("no client certificate is rejected", func(t *testing.T) {
		addr, accepted, cleanup := listenAndAcceptTLS(t, serverSettings)
		defer cleanup()

		clientOpts := core.DefaultConnectOptions()
		clientOpts.TLS = &core.TransportTlsSettings{Enabled: true, CAFile: caFile} // no CertFile/KeyFile
		client, connectErr := NewTransport(clientOpts).Connect(addr)
		// TLS 1.3 can let the client consider its own handshake "complete" (having sent its
		// Finished message) before the server's rejection alert arrives back - so a nil
		// connectErr here doesn't necessarily mean the mTLS requirement wasn't enforced. Treat
		// either an immediate Connect error, or the connection dying/refusing to carry a frame
		// shortly after, as the expected rejection.
		if connectErr != nil {
			return
		}
		defer client.Close()
		sendErr := client.Send([]byte{0, 0, 0, 0})
		select {
		case _, ok := <-accepted:
			if ok && sendErr == nil {
				t.Fatal("expected the mTLS server to reject a client with no certificate, but it accepted and stayed usable")
			}
		case <-time.After(2 * time.Second):
			// Never accepted at all is also an acceptable rejection outcome.
		}
	})

	t.Run("valid client certificate is accepted", func(t *testing.T) {
		addr, accepted, cleanup := listenAndAcceptTLS(t, serverSettings)
		defer cleanup()

		clientCert := ca.issueLeaf(t, "client", false)
		clientOpts := core.DefaultConnectOptions()
		clientOpts.TLS = &core.TransportTlsSettings{
			Enabled:  true,
			CAFile:   caFile,
			CertFile: clientCert.CertFile,
			KeyFile:  clientCert.KeyFile,
		}
		client, err := NewTransport(clientOpts).Connect(addr)
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer client.Close()

		var serverConn stream.ConnectionHandle
		select {
		case serverConn = <-accepted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the server to accept the mTLS connection")
		}
		defer serverConn.Close()

		sendAndExpect(t, client, serverConn, 7)
	})
}

// TestTLSListenRequiresSettings and TestTLSConnectRequiresSettings are the regression tests for
// docs/kdb-finish-up-plan.md 2.1's core instruction: "make configured-but-unimplemented an
// error, never a silent plaintext fallback." A tcps:// URI with no (or disabled) TLS settings
// must fail loudly, not silently listen/connect in plaintext.
func TestTLSListenRequiresSettings(t *testing.T) {
	for name, opts := range map[string]core.TransportConnectOptions{
		"nil TLS": core.DefaultConnectOptions(),
		"disabled TLS": func() core.TransportConnectOptions {
			o := core.DefaultConnectOptions()
			o.TLS = &core.TransportTlsSettings{Enabled: false}
			return o
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewTransport(opts).ListenBound("tcps://127.0.0.1:0?bind=true")
			if err == nil {
				t.Fatal("expected ListenBound to fail for tcps:// with no usable TLS settings")
			}
		})
	}
}

func TestTLSConnectRequiresSettings(t *testing.T) {
	ca := newTestCA(t)
	server := ca.issueLeaf(t, "server", true)
	addr, _, cleanup := listenAndAcceptTLS(t, &core.TransportTlsSettings{
		Enabled:  true,
		CertFile: server.CertFile,
		KeyFile:  server.KeyFile,
	})
	defer cleanup()

	_, err := NewTransport(core.DefaultConnectOptions()).Connect(addr) // no TLS set at all
	if err == nil {
		t.Fatal("expected Connect to fail for tcps:// with no TLS settings configured")
	}
}
