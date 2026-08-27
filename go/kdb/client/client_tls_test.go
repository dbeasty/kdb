package client_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

// testCertFiles/newTestCA/issueLeaf duplicate kdb/transport/tcp's identically-named test helpers
// (unexported _test.go helpers can't be shared across packages) - see that package for the full
// rationale (ECDSA P-256, one-hour validity, self-signed CA).
type testCertFiles struct {
	CertFile string
	KeyFile  string
}

type testCA struct {
	cert *x509.Certificate
	der  []byte
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kdb-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return &testCA{cert: cert, der: der, key: key}
}

func (ca *testCA) certFile(t *testing.T) string {
	t.Helper()
	return writePEMFile(t, "ca-cert.pem", "CERTIFICATE", ca.der)
}

func (ca *testCA) issueLeaf(t *testing.T, commonName string, serverAuth bool) testCertFiles {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if serverAuth {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tmpl.DNSNames = []string{"localhost"}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert for %s: %v", commonName, err)
	}
	certFile := writePEMFile(t, commonName+"-cert.pem", "CERTIFICATE", der)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key for %s: %v", commonName, err)
	}
	keyFile := writePEMFile(t, commonName+"-key.pem", "EC PRIVATE KEY", keyDER)
	return testCertFiles{CertFile: certFile, KeyFile: keyFile}
}

func writePEMFile(t *testing.T, name, blockType string, der []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("pem encode %s: %v", path, err)
	}
	return path
}

func startTestServerTLS(t *testing.T, tlsSettings *core.TransportTlsSettings) (addr string, rt *server.KdbServerRuntime) {
	t.Helper()
	embedded, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	rt = server.NewKdbServerRuntime(embedded)
	ln, err := server.ListenSqlWireTLS("tcps://127.0.0.1:0?bind=true", rt, tlsSettings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return "tcps://" + ln.Addr().String(), rt
}

// TestConnectWithOptionsOverTLS is the client-SDK-level "Go↔Go TLS" test docs/kdb-finish-up-plan.md's
// 2.1 asks for: a real client.ConnectWithOptions dial, over a real tcps:// SQL wire listener,
// followed by an actual PutJSON round trip - the same shape as
// TestConnectPutJSONGetJSONRoundTrip, just over TLS instead of plaintext.
func TestConnectWithOptionsOverTLS(t *testing.T) {
	ca := newTestCA(t)
	serverCert := ca.issueLeaf(t, "server", true)
	addr, _ := startTestServerTLS(t, &core.TransportTlsSettings{
		Enabled:  true,
		CertFile: serverCert.CertFile,
		KeyFile:  serverCert.KeyFile,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := client.ConnectWithOptions(ctx, addr, "", client.ConnectOptions{
		TLS: &core.TransportTlsSettings{Enabled: true, CAFile: ca.certFile(t)},
	})
	if err != nil {
		t.Fatalf("ConnectWithOptions: %v", err)
	}
	defer c.Close()

	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutJSON(ctx, "app/data", docID, []byte(`{"over":"tls"}`)); err != nil {
		t.Fatalf("PutJSON over tcps:// failed: %v", err)
	}
	got, _, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatalf("GetJSON over tcps:// failed: %v", err)
	}
	if string(got) == "" {
		t.Fatal("expected a non-empty document back over tcps://")
	}
}

// TestConnectWithOptionsRejectsUntrustedTLSServer proves ConnectWithOptions' CA verification is
// real: connecting with a CA that didn't sign the server's certificate must fail.
func TestConnectWithOptionsRejectsUntrustedTLSServer(t *testing.T) {
	serverCA := newTestCA(t)
	serverCert := serverCA.issueLeaf(t, "server", true)
	otherCA := newTestCA(t)
	addr, _ := startTestServerTLS(t, &core.TransportTlsSettings{
		Enabled:  true,
		CertFile: serverCert.CertFile,
		KeyFile:  serverCert.KeyFile,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.ConnectWithOptions(ctx, addr, "", client.ConnectOptions{
		TLS: &core.TransportTlsSettings{Enabled: true, CAFile: otherCA.certFile(t)},
	})
	if err == nil {
		t.Fatal("expected ConnectWithOptions to fail: client trusts a different CA than the one that signed the server cert")
	}
}

// TestConnectRejectsWssScheme now that wss:// is a real (client-side) scheme, documents the
// remaining honest boundary: plain Connect (no TLS options - see ConnectOptions) against a
// wss:// URI must still fail with a clear "TLS settings required" message, not a confusing raw
// dial/handshake error, and never a silent plaintext attempt.
func TestConnectRejectsWssScheme(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.Connect(ctx, "wss://127.0.0.1:1/kdb", "")
	if err == nil {
		t.Fatal("expected wss:// with no TLS options to be rejected")
	}
	if !strings.Contains(err.Error(), "TLS settings") && !strings.Contains(err.Error(), "wss://") {
		t.Fatalf("expected a TLS-settings-required rejection message, got: %v", err)
	}
}

// TestConnectRejectsTcpsSchemeWithoutTLSOptions is tcps://'s counterpart to the wss:// case
// above - same "configured-but-unimplemented is an error, never silent plaintext" guarantee.
func TestConnectRejectsTcpsSchemeWithoutTLSOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.Connect(ctx, "tcps://127.0.0.1:1/kdb", "")
	if err == nil {
		t.Fatal("expected tcps:// with no TLS options to be rejected")
	}
	if !strings.Contains(err.Error(), "TLS settings") && !strings.Contains(err.Error(), "tcps://") {
		t.Fatalf("expected a TLS-settings-required rejection message, got: %v", err)
	}
}
