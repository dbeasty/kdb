package ws

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/transport/core"
)

// selfSignedServerCert mints a minimal, short-lived, self-signed cert for 127.0.0.1/localhost -
// these wss:// tests use InsecureSkipVerify on the client side (real CA-chain verification is
// already covered thoroughly by kdb/transport/tcp's TLS tests; this file's job is to prove the
// wss:// client dial path itself - raw TCP, TLS handshake, then the RFC 6455 upgrade all still
// work once wrapped in TLS - not to re-prove certificate verification).
func selfSignedServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kdb-ws-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// startFakeWssServer is startFakeWsServer wrapped in a TLS listener - same hand-rolled RFC 6455
// handling, so this exercises the real client TLS-dial-then-upgrade path against a listener that
// isn't the production ws.Transport.Listen stub.
func startFakeWssServer(t *testing.T) *fakeWsServer {
	t.Helper()
	cert := selfSignedServerCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeWsServer{ln: ln, addr: ln.Addr().String()}
	go s.acceptLoop(t)
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// TestWSSConnectAndEchoRoundTrip is the feature test for docs/kdb-finish-up-plan.md 2.1's "wss://
// client" bullet: a real TLS handshake followed by a real RFC 6455 upgrade and frame echo,
// against a TLS-wrapped listener.
func TestWSSConnectAndEchoRoundTrip(t *testing.T) {
	server := startFakeWssServer(t)
	opts := core.DefaultConnectOptions()
	opts.TLS = &core.TransportTlsSettings{Enabled: true, InsecureSkipVerify: true}
	conn, err := NewTransport(opts).Connect(server.wssURI("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	payload := wireShapedFrame([]byte("hello over wss"))
	if err := conn.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case got := <-conn.Incoming():
		if !bytes.Equal(got, payload) {
			t.Fatalf("got %q, want %q", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for echo over wss")
	}
}

// TestWSSConnectRequiresSettings is kdb/transport/tcp's TestTLSConnectRequiresSettings, mirrored
// for ws: a wss:// URI with no TLS settings configured must fail loudly, never silently
// downgrade to a plaintext connection attempt.
func TestWSSConnectRequiresSettings(t *testing.T) {
	server := startFakeWssServer(t)
	_, err := NewTransport(core.DefaultConnectOptions()).Connect(server.wssURI("/kdb")) // no TLS set
	if err == nil {
		t.Fatal("expected Connect to fail for wss:// with no TLS settings configured")
	}
}

// wssURI is fakeWsServer.uri's wss:// counterpart, used when secure is set.
func (s *fakeWsServer) wssURI(path string) string {
	_, port, _ := net.SplitHostPort(s.addr)
	p, _ := strconv.Atoi(port)
	return fmt.Sprintf("wss://127.0.0.1:%d%s", p, path)
}
