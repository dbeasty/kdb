package server

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/wire"
)

// indexedRuntime is a test runtime with the index catalog the real service opens at startup.
func indexedRuntime(t *testing.T) *KdbServerRuntime {
	t.Helper()
	rt := newTestRuntime(t)
	if _, err := rt.OpenIndexes(stores.Options{}); err != nil {
		t.Fatal(err)
	}
	return rt
}

// `CREATE UNIQUE INDEX` used to succeed on any field and then allow duplicates, including
// concurrent ones: uniqueness is enforced at commit from the namespace schema's UNIQUE
// constraints, and an index does not take part. A statement that reports success and does
// nothing is the worst way for a constraint to fail, so it now says no.

func TestUniqueIndexOnASchemalessNamespaceIsRefused(t *testing.T) {
	rt := indexedRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	r := c.sqlExec(t, "app/data", sess.SessionID, `CREATE UNIQUE INDEX u_email ON docs (email) USING HASH`)
	if r.Error == nil {
		t.Fatal("a UNIQUE index nothing enforces was accepted")
	}
	if !strings.Contains(*r.Error, "UNIQUE") || !strings.Contains(*r.Error, "schema") {
		t.Fatalf("the error should say what to do instead, got %q", *r.Error)
	}
}

func TestPlainIndexOnASchemalessNamespaceStillWorks(t *testing.T) {
	rt := indexedRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	if r := c.sqlExec(t, "app/data", sess.SessionID, `CREATE INDEX by_user ON docs (userId) USING HASH`); r.Error != nil {
		t.Fatalf("a plain index must keep working: %s", *r.Error)
	}
}

func TestUniqueIndexOnAFieldTheSchemaDeclaresUniqueIsAccepted(t *testing.T) {
	rt := indexedRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	if r := c.sqlExec(t, "app/data", sess.SessionID, `CREATE TABLE users (email VARCHAR NOT NULL UNIQUE, name VARCHAR NOT NULL)`); r.Error != nil {
		t.Fatalf("create table: %s", *r.Error)
	}
	// The schema enforces it, so asking for the index form too is honest.
	if r := c.sqlExec(t, "app/data", sess.SessionID, `CREATE UNIQUE INDEX u_email ON users (email) USING HASH`); r.Error != nil {
		t.Fatalf("a UNIQUE index on a schema-unique field should be accepted: %s", *r.Error)
	}
}
