package driver

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestParseFileURL(t *testing.T) {
	p, err := ParseURL("kdb://file///tmp/kdbdata/demo/users")
	if err != nil {
		t.Fatal(err)
	}
	if p.Mode != ModeFile {
		t.Fatalf("mode %v", p.Mode)
	}
	if p.Catalog != "demo" || p.NamespaceID != "demo/users" {
		t.Fatalf("ns: %+v", p)
	}
	// Absolute stays absolute - it used to come back as "tmp/kdbdata", relative to the cwd.
	if want := filepath.FromSlash("/tmp/kdbdata"); p.DataRoot != want {
		t.Fatalf("data root %q, want %s", p.DataRoot, want)
	}
	for _, raw := range []string{"kdb://file:///var/lib/kdb/app/users", "kdb://file:///var/lib/kdb/app/users?readOnly=true"} {
		p, err := ParseURL(raw)
		if err != nil {
			t.Fatal(err)
		}
		if p.DataRoot != filepath.FromSlash("/var/lib/kdb") || p.NamespaceID != "app/users" {
			t.Errorf("%s: root %q ns %q", raw, p.DataRoot, p.NamespaceID)
		}
	}
	rel, err := ParseDSN("file:data/app/users")
	if err != nil {
		t.Fatal(err)
	}
	if rel.DataRoot != "data" {
		t.Errorf("relative URL: root %q, want data", rel.DataRoot)
	}
}

// A Windows data root keeps its drive, however the URL spells it - including the file://<root>
// form the tests build from t.TempDir().
func TestParseFileURLWindowsDrive(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("drive letters are only volume names on Windows")
	}
	for _, raw := range []string{
		"kdb://file:///C:/kdb/data/app/users",
		`kdb://file://C:\kdb\data/app/users`,
		`kdb://file:C:\kdb\data\app\users`,
	} {
		p, err := ParseURL(raw)
		if err != nil {
			t.Fatal(err)
		}
		if p.DataRoot != `C:\kdb\data` || p.NamespaceID != "app/users" {
			t.Errorf("%s: root %q ns %q", raw, p.DataRoot, p.NamespaceID)
		}
	}
}

func TestParseMemorySemicolonParams(t *testing.T) {
	p, err := ParseDSN("memory:///demo/users;unique=true")
	if err != nil {
		t.Fatal(err)
	}
	if p.MemoryParams["unique"] != "true" {
		t.Fatalf("params %+v", p.MemoryParams)
	}
}
