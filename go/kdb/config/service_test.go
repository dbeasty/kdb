package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func noEnv(string) (string, bool) { return "", false }
func noFlags(string) bool         { return false }
func envOf(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}
func flagsOf(set ...string) func(string) bool {
	m := map[string]bool{}
	for _, s := range set {
		m[s] = true
	}
	return func(k string) bool { return m[k] }
}

func TestResolveServiceDefaultsOnly(t *testing.T) {
	s, err := ResolveService(nil, noEnv, noFlags, ServiceSettings{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if s != DefaultServiceSettings() {
		t.Fatalf("defaults-only resolution diverged: %+v", s)
	}
}

func TestResolveServiceFileOverridesDefaults(t *testing.T) {
	ns := "file/ns"
	rbac := true
	drain := "5s"
	cert := "/etc/kdb/tls.crt"
	file := &ServiceFile{
		Namespace:    &ns,
		RBAC:         &rbac,
		DrainTimeout: &drain,
		TLS:          &ServiceTLSFile{CertFile: &cert},
	}
	s, err := ResolveService(file, noEnv, noFlags, ServiceSettings{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if s.Namespace != "file/ns" || !s.RBAC || s.DrainTimeout != 5*time.Second || s.TLSCert != cert {
		t.Fatalf("file layer not applied: %+v", s)
	}
	// A field the file did not set keeps its default.
	if s.SQLAddr != DefaultServiceSettings().SQLAddr {
		t.Fatalf("unset field lost its default: %q", s.SQLAddr)
	}
}

func TestResolveServiceEnvOverridesFile(t *testing.T) {
	ns := "file/ns"
	file := &ServiceFile{Namespace: &ns}
	env := envOf(map[string]string{
		"KDB_NAMESPACE":     "env/ns",
		"KDB_MEMORY":        "true",
		"KDB_DRAIN_TIMEOUT": "7s",
	})
	s, err := ResolveService(file, env, noFlags, ServiceSettings{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if s.Namespace != "env/ns" || !s.Memory || s.DrainTimeout != 7*time.Second {
		t.Fatalf("env layer not applied over file: %+v", s)
	}
}

func TestResolveServiceExplicitFlagBeatsEverything(t *testing.T) {
	ns := "file/ns"
	file := &ServiceFile{Namespace: &ns}
	env := envOf(map[string]string{"KDB_NAMESPACE": "env/ns"})
	flags := ServiceSettings{Namespace: "flag/ns"}
	s, err := ResolveService(file, env, flagsOf("namespace"), flags)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if s.Namespace != "flag/ns" {
		t.Fatalf("explicit flag did not win: %q", s.Namespace)
	}
}

func TestResolveServiceUnsetFlagDefaultDoesNotMaskFile(t *testing.T) {
	// The classic trap: --sql-addr's *default* value sitting in the parsed flag struct must not
	// override the file's value when the operator never typed the flag.
	addr := "tcp://10.0.0.5:9999?bind=true"
	file := &ServiceFile{SQLAddr: &addr}
	flags := DefaultServiceSettings() // what a parsed-but-untouched FlagSet yields
	s, err := ResolveService(file, noEnv, noFlags, flags)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if s.SQLAddr != addr {
		t.Fatalf("flag default masked the config file: %q", s.SQLAddr)
	}
}

func TestResolveServiceBadEnvValueErrors(t *testing.T) {
	env := envOf(map[string]string{"KDB_MEMORY_LIMIT_MB": "lots"})
	if _, err := ResolveService(nil, env, noFlags, ServiceSettings{}); err == nil {
		t.Fatal("bad KDB_MEMORY_LIMIT_MB should error, not be ignored")
	}
	env = envOf(map[string]string{"KDB_ABORT_AFTER": "sometime"})
	if _, err := ResolveService(nil, env, noFlags, ServiceSettings{}); err == nil {
		t.Fatal("bad KDB_ABORT_AFTER should error, not be ignored")
	}
}

func TestLoadServiceFileRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kdb-service.json")
	if err := os.WriteFile(path, []byte(`{"namespcae": "typo/ns"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadServiceFile(path); err == nil {
		t.Fatal("a typoed config key should fail loudly, not silently configure nothing")
	}
}

func TestLoadServiceFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kdb-service.json")
	body := `{
		"namespace": "prod/users",
		"dataDir": "/var/lib/kdb",
		"adminAddr": "127.0.0.1:9093",
		"drainTimeout": "10s",
		"tls": {"certFile": "/etc/kdb/tls.crt", "keyFile": "/etc/kdb/tls.key", "clientAuth": true}
	}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := LoadServiceFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	s, err := ResolveService(f, noEnv, noFlags, ServiceSettings{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if s.Namespace != "prod/users" || s.DataDir != "/var/lib/kdb" || s.AdminAddr != "127.0.0.1:9093" ||
		s.DrainTimeout != 10*time.Second || s.TLSCert != "/etc/kdb/tls.crt" || !s.TLSClientAuth {
		t.Fatalf("round trip lost fields: %+v", s)
	}
}
