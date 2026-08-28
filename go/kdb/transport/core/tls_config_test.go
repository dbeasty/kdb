package core

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// selfSignedCertFiles generates a fresh self-signed ECDSA cert (one-hour validity - it only needs
// to outlive this test process) and writes the cert and key as PEM files under t.TempDir().
// BuildTLSConfig reads from file paths, not in-memory bytes, matching how a real deployment
// configures --tls-cert, so the tests need real files.
func selfSignedCertFiles(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kdb-core-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true, // lets the same file double as a CAFile in client-auth tests
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certFile = writeTestPEM(t, "cert.pem", "CERTIFICATE", der)
	keyFile = writeTestPEM(t, "key.pem", "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func writeTestPEM(t *testing.T, name, blockType string, der []byte) string {
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

// TestBuildTLSConfigNilAndDisabled: both a nil receiver and Enabled=false mean "no TLS
// configured" and must return (nil, nil) - the caller, not this function, decides whether that is
// acceptable for its URI scheme.
func TestBuildTLSConfigNilAndDisabled(t *testing.T) {
	var nilSettings *TransportTlsSettings
	for _, role := range []bool{true, false} {
		cfg, err := nilSettings.BuildTLSConfig(role)
		if cfg != nil || err != nil {
			t.Fatalf("nil settings (server=%v): got cfg=%v err=%v, want nil/nil", role, cfg, err)
		}
		cfg, err = (&TransportTlsSettings{Enabled: false, CertFile: "ignored.pem"}).BuildTLSConfig(role)
		if cfg != nil || err != nil {
			t.Fatalf("disabled settings (server=%v): got cfg=%v err=%v, want nil/nil", role, cfg, err)
		}
	}
}

// TestBuildTLSConfigServerRequiresCertAndKey: a TLS server with no certificate cannot complete
// any handshake, so enabling TLS server-side without cert+key must be a configuration error, not
// a config that fails at accept time.
func TestBuildTLSConfigServerRequiresCertAndKey(t *testing.T) {
	_, err := (&TransportTlsSettings{Enabled: true}).BuildTLSConfig(true)
	if err == nil {
		t.Fatal("server config without cert/key accepted")
	}
}

// TestBuildTLSConfigCertKeyMustBePaired: setting only one of CertFile/KeyFile is always a
// misconfiguration (a cert is useless without its private key and vice versa) and must fail
// loudly for both roles rather than being half-applied.
func TestBuildTLSConfigCertKeyMustBePaired(t *testing.T) {
	certFile, keyFile := selfSignedCertFiles(t)
	cases := []TransportTlsSettings{
		{Enabled: true, CertFile: certFile},
		{Enabled: true, KeyFile: keyFile},
	}
	for i, s := range cases {
		for _, server := range []bool{true, false} {
			s := s
			if _, err := s.BuildTLSConfig(server); err == nil {
				t.Fatalf("case %d (server=%v): lone cert or key accepted", i, server)
			}
		}
	}
}

// TestBuildTLSConfigBadFilePaths: nonexistent cert/key/CA paths must surface as errors at config
// build time - the operator typo'd a path and needs to hear about it before the first connection.
func TestBuildTLSConfigBadFilePaths(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.pem")
	certFile, keyFile := selfSignedCertFiles(t)

	if _, err := (&TransportTlsSettings{Enabled: true, CertFile: missing, KeyFile: missing}).BuildTLSConfig(true); err == nil {
		t.Fatal("missing cert/key files accepted")
	}
	if _, err := (&TransportTlsSettings{Enabled: true, CertFile: certFile, KeyFile: keyFile, CAFile: missing}).BuildTLSConfig(true); err == nil {
		t.Fatal("missing CA file accepted")
	}
}

// TestBuildTLSConfigRejectsGarbagePEM: files that exist but contain no parseable certificate
// (empty file, non-PEM junk) must be rejected. Silently building an empty cert pool would mean a
// server that rejects every client, or a client that trusts nothing, with no hint why.
func TestBuildTLSConfigRejectsGarbagePEM(t *testing.T) {
	dir := t.TempDir()
	junk := filepath.Join(dir, "junk.pem")
	if err := os.WriteFile(junk, []byte("this is not PEM data"), 0o600); err != nil {
		t.Fatalf("write junk file: %v", err)
	}

	certFile, keyFile := selfSignedCertFiles(t)
	if _, err := (&TransportTlsSettings{Enabled: true, CertFile: certFile, KeyFile: keyFile, CAFile: junk}).BuildTLSConfig(false); err == nil {
		t.Fatal("CA file with no certificates accepted")
	}
	if _, err := (&TransportTlsSettings{Enabled: true, CertFile: junk, KeyFile: keyFile}).BuildTLSConfig(true); err == nil {
		t.Fatal("garbage cert file accepted")
	}
	// A cert paired with the wrong key must also fail - LoadX509KeyPair checks they match.
	otherCert, _ := selfSignedCertFiles(t)
	if _, err := (&TransportTlsSettings{Enabled: true, CertFile: otherCert, KeyFile: keyFile}).BuildTLSConfig(true); err == nil {
		t.Fatal("cert paired with mismatched key accepted")
	}
}

// TestBuildTLSConfigRequireClientAuthNeedsCA: mTLS without a CA to verify client certs against is
// unenforceable - there would be nothing to check a presented certificate's signature with - so
// it must be rejected instead of quietly accepting any client.
func TestBuildTLSConfigRequireClientAuthNeedsCA(t *testing.T) {
	certFile, keyFile := selfSignedCertFiles(t)
	s := &TransportTlsSettings{Enabled: true, CertFile: certFile, KeyFile: keyFile, RequireClientAuth: true}
	if _, err := s.BuildTLSConfig(true); err == nil {
		t.Fatal("RequireClientAuth without CAFile accepted")
	}
}

// TestBuildTLSConfigServerSuccess: the happy server path with a real (self-signed) cert. Checks
// the certificate is actually loaded and that the ClientAuth mode maps correctly for each of the
// three server postures: no CA, CA without required auth, CA with required auth (mTLS).
func TestBuildTLSConfigServerSuccess(t *testing.T) {
	certFile, keyFile := selfSignedCertFiles(t)

	base := TransportTlsSettings{Enabled: true, CertFile: certFile, KeyFile: keyFile}
	cfg, err := base.BuildTLSConfig(true)
	if err != nil {
		t.Fatalf("server config with valid cert/key: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("Certificates: got %d, want 1", len(cfg.Certificates))
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Fatalf("ClientAuth without CA: got %v, want NoClientCert", cfg.ClientAuth)
	}

	withCA := base
	withCA.CAFile = certFile // self-signed cert doubles as its own CA
	cfg, err = withCA.BuildTLSConfig(true)
	if err != nil {
		t.Fatalf("server config with CA: %v", err)
	}
	if cfg.ClientCAs == nil {
		t.Fatal("ClientCAs not populated from CAFile")
	}
	if cfg.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("ClientAuth with CA but not required: got %v, want VerifyClientCertIfGiven", cfg.ClientAuth)
	}

	mtls := withCA
	mtls.RequireClientAuth = true
	cfg, err = mtls.BuildTLSConfig(true)
	if err != nil {
		t.Fatalf("mTLS server config: %v", err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("ClientAuth for mTLS: got %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
}

// TestBuildTLSConfigClientSuccess: the happy client paths. A client needs no certificate of its
// own; ServerName and InsecureSkipVerify must be carried through verbatim (ServerName drives SNI
// and hostname verification); a CAFile must land in RootCAs (trust anchors), never ClientCAs.
func TestBuildTLSConfigClientSuccess(t *testing.T) {
	cfg, err := (&TransportTlsSettings{Enabled: true, ServerName: "db.example.com"}).BuildTLSConfig(false)
	if err != nil {
		t.Fatalf("bare client config: %v", err)
	}
	if cfg == nil || cfg.ServerName != "db.example.com" {
		t.Fatalf("ServerName not carried through: %+v", cfg)
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify defaulted to true")
	}

	certFile, keyFile := selfSignedCertFiles(t)
	full := &TransportTlsSettings{
		Enabled:            true,
		InsecureSkipVerify: true,
		CAFile:             certFile,
		CertFile:           certFile,
		KeyFile:            keyFile,
	}
	cfg, err = full.BuildTLSConfig(false)
	if err != nil {
		t.Fatalf("full client config: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify not carried through")
	}
	if cfg.RootCAs == nil {
		t.Fatal("client CAFile did not populate RootCAs")
	}
	if cfg.ClientCAs != nil {
		t.Fatal("client config populated ClientCAs (server-side field)")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("client cert not loaded: got %d certificates", len(cfg.Certificates))
	}
}
