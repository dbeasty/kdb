package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/wire"
)

// `CREATE UNIQUE INDEX` on a schemaless namespace installs a unique constraint. Uniqueness is
// enforced at commit from the namespace schema (the UniqueKeyRegistry); the index is a lookup
// structure and takes no part. It used to be accepted and enforce nothing - duplicates, even 10
// concurrent ones, all went in. These run over a real listener.

// indexedRuntime is a test runtime with the index catalog the real service opens at startup.
func indexedRuntime(t *testing.T) *KdbServerRuntime {
	t.Helper()
	rt := newTestRuntime(t)
	if _, err := rt.OpenIndexes(stores.Options{}); err != nil {
		t.Fatal(err)
	}
	return rt
}

type uniqueFixture struct {
	t    *testing.T
	addr string
	c    *rawWireClient
	sess wire.SessionBeginAckMessage
}

func newUniqueFixture(t *testing.T) *uniqueFixture {
	t.Helper()
	rt := indexedRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	return &uniqueFixture{t: t, addr: addr, c: c, sess: c.sessionBegin(t, "app/data", "READ_COMMITTED")}
}

func (f *uniqueFixture) sql(text string) wire.SqlResultMessage {
	f.t.Helper()
	return f.c.sqlExec(f.t, "app/data", f.sess.SessionID, text)
}

func (f *uniqueFixture) put(body string) wire.UpsertResultMessage {
	f.t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		f.t.Fatal(err)
	}
	return f.c.upsert(f.t, "app/data", f.sess.SessionID, id.String(), body)
}

func TestUniqueIndexOnASchemalessNamespaceIsEnforced(t *testing.T) {
	f := newUniqueFixture(t)

	if r := f.sql(`CREATE UNIQUE INDEX u_email ON docs (email) USING HASH`); r.Error != nil {
		t.Fatalf("create unique index: %s", *r.Error)
	}
	if r := f.put(`{"email":"a@b.co","name":"first"}`); r.Error != nil {
		t.Fatalf("the first document must be accepted: %s", *r.Error)
	}

	dup := f.put(`{"email":"a@b.co","name":"second"}`)
	if dup.Error == nil {
		t.Fatal("a duplicate value went in")
	}
	if dup.ErrorCode == nil || *dup.ErrorCode != wire.ErrorCodeUniqueViolation {
		t.Fatalf("want UNIQUE_VIOLATION, got code %v (%s)", dup.ErrorCode, *dup.Error)
	}

	// Documents that do not carry the field claim nothing, so any number may omit it (SQL NULL
	// semantics: a missing value never collides).
	for i := 0; i < 3; i++ {
		if r := f.put(fmt.Sprintf(`{"title":"t%d"}`, i)); r.Error != nil {
			t.Fatalf("a document without the field must be accepted: %s", *r.Error)
		}
	}
}

func TestConcurrentInsertsOfOneUniqueValueLetExactlyOneIn(t *testing.T) {
	f := newUniqueFixture(t)
	if r := f.sql(`CREATE UNIQUE INDEX u_email ON docs (email) USING HASH`); r.Error != nil {
		t.Fatalf("create unique index: %s", *r.Error)
	}

	const racers = 12
	var wg sync.WaitGroup
	results := make([]wire.UpsertResultMessage, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// A connection each: the race is between independent clients, not one pipeline.
			c := dialRawWireClient(t, f.addr)
			c.handshake(t, wire.ClientSQL, "app/data")
			sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
			id, _ := codec.RandomUUID()
			results[i] = c.upsert(t, "app/data", sess.SessionID, id.String(), `{"email":"race@b.co"}`)
		}(i)
	}
	wg.Wait()

	won := 0
	for _, r := range results {
		if r.Error == nil {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("%d of %d concurrent inserts of the same value succeeded; exactly one may", won, racers)
	}
}

func TestUniqueIndexRefusesDataThatAlreadyViolatesIt(t *testing.T) {
	f := newUniqueFixture(t)
	f.put(`{"email":"dup@b.co","n":1}`)
	f.put(`{"email":"dup@b.co","n":2}`)

	r := f.sql(`CREATE UNIQUE INDEX u_email ON docs (email) USING HASH`)
	if r.Error == nil {
		t.Fatal("the constraint was installed over data that already violates it")
	}
	if !strings.Contains(*r.Error, "u_email") {
		t.Fatalf("the error should name the index, got %q", *r.Error)
	}
	// Nothing was left behind: no constraint (duplicates are still accepted) and no half-made
	// index (the name is free to use).
	if r := f.put(`{"email":"dup@b.co","n":3}`); r.Error != nil {
		t.Fatalf("a refused statement must not leave a constraint behind: %s", *r.Error)
	}
	if r := f.sql(`CREATE INDEX u_email ON docs (email) USING HASH`); r.Error != nil {
		t.Fatalf("a refused statement must not leave an index behind: %s", *r.Error)
	}
}

func TestCompoundUniqueConstraint(t *testing.T) {
	f := newUniqueFixture(t)
	if r := f.sql(`CREATE UNIQUE INDEX u_pair ON docs (entityType, entityId)`); r.Error != nil {
		t.Fatalf("compound unique: %s", *r.Error)
	}
	if r := f.put(`{"entityType":"task","entityId":"1"}`); r.Error != nil {
		t.Fatal(*r.Error)
	}
	if r := f.put(`{"entityType":"task","entityId":"2"}`); r.Error != nil {
		t.Fatalf("a different second part must be accepted: %s", *r.Error)
	}
	if r := f.put(`{"entityType":"project","entityId":"1"}`); r.Error != nil {
		t.Fatalf("a different first part must be accepted: %s", *r.Error)
	}
	if r := f.put(`{"entityType":"task","entityId":"1"}`); r.Error == nil {
		t.Fatal("the same pair went in twice")
	}
}

// A constraint-only schema is not a declared schema: the namespace must go on answering queries
// about fields nobody declared, or installing a constraint would break every query in the app.
func TestConstraintDoesNotMakeTheNamespaceRejectUndeclaredColumns(t *testing.T) {
	f := newUniqueFixture(t)
	if r := f.sql(`CREATE UNIQUE INDEX u_email ON docs (email) USING HASH`); r.Error != nil {
		t.Fatal(*r.Error)
	}
	f.put(`{"email":"a@b.co","status":"open","tags":["x"]}`)
	f.put(`{"email":"c@d.co","status":"done"}`)

	for _, q := range []string{
		`SELECT kdb_id FROM docs WHERE status = 'open'`,
		`SELECT kdb_id FROM docs WHERE email = 'c@d.co'`,
		`SELECT COUNT(*) FROM docs WHERE tags = 'x'`,
	} {
		if r := f.sql(q); r.Error != nil {
			t.Fatalf("%s: %s", q, *r.Error)
		}
	}
	if r := f.sql(`SELECT kdb_id FROM docs WHERE status = 'open'`); len(r.Rows) != 1 {
		t.Fatalf("expected the one open document, got %d rows", len(r.Rows))
	}
}

func TestPlainIndexOnASchemalessNamespaceStillWorks(t *testing.T) {
	f := newUniqueFixture(t)
	if r := f.sql(`CREATE INDEX by_user ON docs (userId) USING HASH`); r.Error != nil {
		t.Fatalf("a plain index must keep working: %s", *r.Error)
	}
	// ...and adds no constraint.
	f.put(`{"userId":"u1"}`)
	if r := f.put(`{"userId":"u1"}`); r.Error != nil {
		t.Fatalf("a plain index must not enforce uniqueness: %s", *r.Error)
	}
}

func TestUniqueIndexOnAFieldTheSchemaDeclaresUniqueIsAccepted(t *testing.T) {
	f := newUniqueFixture(t)
	if r := f.sql(`CREATE TABLE users (email VARCHAR NOT NULL UNIQUE, name VARCHAR NOT NULL)`); r.Error != nil {
		t.Fatalf("create table: %s", *r.Error)
	}
	// The schema already enforces it, so asking for the index form too is honest.
	if r := f.sql(`CREATE UNIQUE INDEX u_email ON users (email) USING HASH`); r.Error != nil {
		t.Fatalf("a UNIQUE index on a schema-unique field should be accepted: %s", *r.Error)
	}
}

func TestUniqueIndexOnADeclaredSchemaThatLacksTheConstraintIsRefused(t *testing.T) {
	f := newUniqueFixture(t)
	if r := f.sql(`CREATE TABLE users (email VARCHAR NOT NULL, name VARCHAR NOT NULL)`); r.Error != nil {
		t.Fatalf("create table: %s", *r.Error)
	}
	r := f.sql(`CREATE UNIQUE INDEX u_name ON users (name) USING HASH`)
	if r.Error == nil {
		t.Fatal("changing a declared schema must not be a side effect of CREATE INDEX")
	}
	if !strings.Contains(*r.Error, "UNIQUE") || !strings.Contains(*r.Error, "schema") {
		t.Fatalf("the error should say what to do instead, got %q", *r.Error)
	}
}
