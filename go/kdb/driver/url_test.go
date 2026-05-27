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
