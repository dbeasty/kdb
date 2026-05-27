package driver

import (
	"context"
	"database/sql"
	"testing"
)

func TestDriverRegisters(t *testing.T) {
	EnsureRegistered()
	ClearMemoryRegistries()
	d, err := sql.Open("kdb", "memory:///demo/users?unique=true")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
}

func TestAcceptsMemoryURL(t *testing.T) {
	if err := AcceptsURL("kdb://memory:///demo/users"); err != nil {
		t.Fatal(err)
	}
	if err := AcceptsURL("postgres://localhost/db"); err == nil {
		t.Fatal("expected reject")
	}
}

func TestParseMemoryURL(t *testing.T) {
	p, err := ParseURL("kdb://memory:///myapp/users?unique=true")
	if err != nil {
		t.Fatal(err)
	}
	if p.Catalog != "myapp" || p.NamespaceID != "myapp/users" {
		t.Fatalf("parsed: %+v", p)
	}
	if p.MemoryParams["unique"] != "true" {
		t.Fatalf("params: %+v", p.MemoryParams)
	}
}

func TestConnectionPingAndSelectOne(t *testing.T) {
	ClearMemoryRegistries()
	db, err := Open("kdb://memory:///demo/users?unique=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow("SELECT 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("got %d", n)
	}
}

func TestCatalogFromConn(t *testing.T) {
	ClearMemoryRegistries()
	db, err := Open("kdb://memory:///myapp/users?unique=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dbConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()
	err = dbConn.Raw(func(driverConn any) error {
		c := driverConn.(*conn)
		if c.Catalog() != "myapp" {
			t.Fatalf("catalog %q", c.Catalog())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCloseConnection(t *testing.T) {
	ClearMemoryRegistries()
	db, err := Open("kdb://memory:///demo/users?unique=true")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err == nil {
		t.Fatal("expected ping failure after close")
	}
}
