package tcp

import (
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
	"testing"
	"time"
)

// testCertFiles is a PEM cert+key pair on disk, for tests that need real files (BuildTLSConfig
// reads from paths, not in-memory bytes, matching how a real deployment configures --tls-cert).
type testCertFiles struct {
	CertFile string
	KeyFile  string
}

// testCA is a minimal self-signed CA (ECDSA P-256, one-hour validity - these certs only need to
// outlive one test process) that can mint leaf certs signed by it, for TLS round-trip and mTLS
// tests. Real production certs are out of scope here; this only needs to exercise
// core.TransportTlsSettings' actual load/verify path.
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

// issueLeaf mints a cert signed by ca, for serverAuth (the listener's own identity) or client
// auth (a certificate a client presents for mTLS).
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
