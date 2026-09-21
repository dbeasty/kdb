package driver

import "testing"

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
	if p.DataRoot != "/tmp/kdbdata" {
		t.Fatalf("data root %q, want /tmp/kdbdata", p.DataRoot)
	}
	for _, raw := range []string{"kdb://file:///var/lib/kdb/app/users", "kdb://file:///var/lib/kdb/app/users?readOnly=true"} {
		p, err := ParseURL(raw)
		if err != nil {
			t.Fatal(err)
		}
		if p.DataRoot != "/var/lib/kdb" || p.NamespaceID != "app/users" {
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

func TestParseMemorySemicolonParams(t *testing.T) {
	p, err := ParseDSN("memory:///demo/users;unique=true")
	if err != nil {
		t.Fatal(err)
	}
	if p.MemoryParams["unique"] != "true" {
		t.Fatalf("params %+v", p.MemoryParams)
	}
}
