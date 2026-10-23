package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// INSERT ... ON CONFLICT: an upsert on a declared unique constraint, decided under the write gate.
// These run over a real listener, with the constraint made the way an application makes it.

func newUpsertFixture(t *testing.T) *uniqueFixture {
	t.Helper()
	rt := indexedRuntime(t)
	// A compound constraint is declared on the schema (a composite BTREE cannot be UNIQUE).
	sch, err := rt.Schema().WithUniqueTuple([]string{"kind", "ref"}, codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.SetSchemaChecked(sch); err != nil {
		t.Fatal(err)
	}
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	f := &uniqueFixture{t: t, addr: addr, c: c, sess: c.sessionBegin(t, "app/data", "READ_COMMITTED")}
	if r := f.sql(`CREATE UNIQUE INDEX u_job ON docs (jobId) USING HASH`); r.Error != nil {
		t.Fatalf("create unique index: %s", *r.Error)
	}
	return f
}

func (f *uniqueFixture) ok(text string) wire.SqlResultMessage {
	f.t.Helper()
	r := f.sql(text)
	if r.Error != nil {
		f.t.Fatalf("%s: %s", text, *r.Error)
	}
	return r
}

func (f *uniqueFixture) refused(text, want string) {
	f.t.Helper()
	r := f.sql(text)
	if r.Error == nil {
		f.t.Fatalf("%s: accepted, want an error containing %q", text, want)
	}
	if !strings.Contains(*r.Error, want) {
		f.t.Fatalf("%s: %s, want an error containing %q", text, *r.Error, want)
	}
}

func (f *uniqueFixture) count(where string) string {
	f.t.Helper()
	return f.ok("SELECT COUNT(*) FROM docs WHERE " + where).Rows[0][0]
}

func decodeDoc(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	return m
}

const upsertJobSQL = `INSERT INTO docs (kdb_id, jobId, status, attempts, lastError) VALUES ('%s', 'j1', 'pending', 0, 'boom')
	ON CONFLICT (jobId) DO UPDATE SET status = 'pending', attempts = attempts + 1 REMOVE lastError
	RETURNING kdb_id, _doc`

// The first upsert inserts the row - with the kdb_id it names - and every later one updates that
// document in place: SET and REMOVE apply, fields neither names are kept, the row's own values
// are not written, and no second document appears.
func TestInsertOnConflictInsertsThenUpdates(t *testing.T) {
	f := newUpsertFixture(t)
	first := mustRandomUUID(t)
	r := f.ok(fmt.Sprintf(upsertJobSQL, first))
	if r.RowsAffected != 1 || len(r.Rows) != 1 || r.Rows[0][0] != first.String() {
		t.Fatalf("insert: affected %d, rows %v", r.RowsAffected, r.Rows)
	}
	if len(r.GeneratedIDs) != 1 || r.GeneratedIDs[0] != first.String() {
		t.Fatalf("insert: generated ids %v", r.GeneratedIDs)
	}
	if doc := decodeDoc(t, r.Rows[0][1]); doc["lastError"] != "boom" || doc["attempts"] != float64(0) {
		t.Fatalf("inserted %v: the row's values, all of them, are the document", doc)
	}
	if !r.ReadOnly {
		t.Fatal("the reply must say there is nothing left to commit")
	}
	f.ok(`UPDATE docs SET note = 'kept' WHERE jobId = 'j1'`)
	f.c.txCommit(t, "app/data", f.sess.SessionID)

	for i := 1; i <= 3; i++ {
		r = f.ok(fmt.Sprintf(upsertJobSQL, mustRandomUUID(t)))
		if r.RowsAffected != 1 || r.Rows[0][0] != first.String() || len(r.GeneratedIDs) != 0 {
			t.Fatalf("upsert %d: affected %d, rows %v, generated %v - want the first document updated", i, r.RowsAffected, r.Rows, r.GeneratedIDs)
		}
		doc := decodeDoc(t, r.Rows[0][1])
		if _, has := doc["lastError"]; has || doc["attempts"] != float64(i) || doc["note"] != "kept" {
			t.Fatalf("upsert %d returned %v", i, doc)
		}
	}
	if n := f.count("jobId = 'j1'"); n != "1" {
		t.Fatalf("%s documents hold jobId j1", n)
	}
}

// DO NOTHING leaves the holder as it is and reports no row; DO UPDATE ... WHERE that does not
// match the holder does the same - in neither case is the row inserted.
func TestInsertOnConflictDoNothingAndUnmatchedWhere(t *testing.T) {
	f := newUpsertFixture(t)
	f.ok(fmt.Sprintf(upsertJobSQL, mustRandomUUID(t)))
	for _, text := range []string{
		`INSERT INTO docs (jobId, status) VALUES ('j1', 'other') ON CONFLICT (jobId) DO NOTHING RETURNING kdb_id`,
		`INSERT INTO docs (jobId, status) VALUES ('j1', 'other') ON CONFLICT (jobId) DO UPDATE SET status = 'other' WHERE status = 'done' RETURNING kdb_id`,
	} {
		r := f.ok(text)
		if r.RowsAffected != 0 || len(r.Rows) != 0 || len(r.GeneratedIDs) != 0 {
			t.Fatalf("%s: affected %d, rows %v, generated %v", text, r.RowsAffected, r.Rows, r.GeneratedIDs)
		}
	}
	if n := f.count("jobId = 'j1' AND status = 'pending'"); n != "1" {
		t.Fatalf("%s pending j1 documents, want the untouched one", n)
	}
	if n := f.count("status = 'other'"); n != "0" {
		t.Fatalf("%s documents were written", n)
	}
	// Without RETURNING: the count alone.
	if r := f.ok(`INSERT INTO docs (jobId) VALUES ('j2') ON CONFLICT (jobId) DO NOTHING`); r.RowsAffected != 1 {
		t.Fatalf("a new key: affected %d", r.RowsAffected)
	}
}

// A compound constraint is named by its columns in any order; a row with a null part claims
// nothing, so it is inserted every time, as SQL's NULLs do not collide.
func TestInsertOnConflictCompoundAndNullParts(t *testing.T) {
	f := newUpsertFixture(t)
	up := `INSERT INTO docs (kind, ref, n) VALUES ('a', 'r1', 1) ON CONFLICT (ref, kind) DO UPDATE SET n = n + 1 RETURNING n`
	for want := 1; want <= 3; want++ {
		if r := f.ok(up); r.Rows[0][0] != fmt.Sprint(want) {
			t.Fatalf("pass %d returned n = %v", want, r.Rows)
		}
	}
	other := `INSERT INTO docs (kind, ref, n) VALUES ('b', 'r1', 1) ON CONFLICT (kind, ref) DO UPDATE SET n = n + 1 RETURNING n`
	if r := f.ok(other); r.Rows[0][0] != "1" {
		t.Fatalf("another tuple returned %v, want its own insert", r.Rows)
	}
	nulls := `INSERT INTO docs (kind, ref, n) VALUES ('a', NULL, 1) ON CONFLICT (kind, ref) DO UPDATE SET n = n + 1 RETURNING n`
	for i := 0; i < 2; i++ {
		if r := f.ok(nulls); r.Rows[0][0] != "1" || len(r.GeneratedIDs) != 1 {
			t.Fatalf("null part, pass %d: %v %v", i, r.Rows, r.GeneratedIDs)
		}
	}
	if n := f.count("kind = 'a'"); n != "3" {
		t.Fatalf("%s documents of kind a, want one keyed and two with a null ref", n)
	}
}

// Concurrent upserts of one key, each on its own connection: exactly one inserts, every other
// one updates that document, and none fails - the race a find-then-insert loses.
func TestInsertOnConflictConcurrentWritersShareOneDocument(t *testing.T) {
	f := newUpsertFixture(t)
	const writers, rounds = 8, 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	inserted, ids := 0, map[string]bool{}
	errs := make(chan string, writers*rounds)
	for w := 0; w < writers; w++ {
		c := dialRawWireClient(t, f.addr)
		c.handshake(t, wire.ClientSQL, "app/data")
		sess := c.sessionBegin(t, "app/data", "READ_COMMITTED").SessionID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				r := c.sqlExec(t, "app/data", sess, `INSERT INTO docs (jobId, n) VALUES ('hot', 1)
					ON CONFLICT (jobId) DO UPDATE SET n = n + 1 RETURNING kdb_id`)
				if r.Error != nil {
					errs <- *r.Error
					continue
				}
				mu.Lock()
				inserted += len(r.GeneratedIDs)
				ids[r.Rows[0][0]] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("an upsert failed: %s", e)
	}
	if inserted != 1 || len(ids) != 1 {
		t.Fatalf("%d inserts, %d distinct documents returned; want 1 and 1", inserted, len(ids))
	}
	if r := f.ok(`SELECT n FROM docs WHERE jobId = 'hot'`); len(r.Rows) != 1 || r.Rows[0][0] != fmt.Sprint(writers*rounds) {
		t.Fatalf("n = %v, want %d", r.Rows, writers*rounds)
	}
}

// What an upsert refuses, and why.
func TestInsertOnConflictRefusals(t *testing.T) {
	f := newUpsertFixture(t)
	held := mustRandomUUID(t)
	f.ok(fmt.Sprintf(upsertJobSQL, held))
	f.refused(`INSERT INTO docs (jobId, status) VALUES ('j9', 'x') ON CONFLICT (status) DO NOTHING`, "unique constraint")
	f.refused(`INSERT INTO docs (kind) VALUES ('a') ON CONFLICT (kind) DO NOTHING`, "unique constraint")
	f.refused(`INSERT INTO docs (jobId) VALUES ('j8'), ('j9') ON CONFLICT (jobId) DO NOTHING`, "exactly one VALUES row")
	f.refused(`INSERT INTO docs (jobId) VALUES ('j8') ON CONFLICT (jobId) DO UPDATE SET kdb_id = 'x'`, "kdb_id cannot be assigned")
	// A buffered INSERT may not name kdb_id: it would merge into whatever document has it.
	f.refused(fmt.Sprintf(`INSERT INTO docs (kdb_id, jobId) VALUES ('%s', 'j7')`, mustRandomUUID(t)), "kdb_id can be given only")
	// An upsert naming an id some document already has.
	f.refused(fmt.Sprintf(`INSERT INTO docs (kdb_id, jobId) VALUES ('%s', 'j6') ON CONFLICT (jobId) DO NOTHING`, held), "already exists")
	// A row colliding on a constraint other than the target is a violation, as in PostgreSQL.
	f.ok(`INSERT INTO docs (kind, ref) VALUES ('k', 'r') ON CONFLICT (kind, ref) DO NOTHING`)
	if r := f.sql(`INSERT INTO docs (jobId, kind, ref) VALUES ('j5', 'k', 'r') ON CONFLICT (jobId) DO NOTHING`); r.Error == nil {
		t.Fatal("a row colliding on (kind, ref) was accepted")
	}
	// It commits on its own, so not in the middle of an open transaction.
	f.ok(`UPDATE docs SET touched = true WHERE jobId = 'j1'`)
	f.refused(`INSERT INTO docs (jobId) VALUES ('j4') ON CONFLICT (jobId) DO NOTHING`, "commits on its own")
	f.c.txRollback(t, "app/data", f.sess.SessionID)
	if n := f.count("jobId = 'j5' OR jobId = 'j4' OR jobId = 'j6'"); n != "0" {
		t.Fatalf("%s refused rows were written", n)
	}
}

// A plain INSERT is buffered on the session as before, with fresh ids.
func TestPlainInsertIsStillBuffered(t *testing.T) {
	f := newUpsertFixture(t)
	r := f.ok(`INSERT INTO docs (jobId) VALUES ('p1')`)
	if r.ReadOnly || len(r.GeneratedIDs) != 1 {
		t.Fatalf("plain insert: read-only %v, generated %v", r.ReadOnly, r.GeneratedIDs)
	}
	if _, err := codec.ParseUUID(r.GeneratedIDs[0]); err != nil {
		t.Fatal(err)
	}
	f.c.txCommit(t, "app/data", f.sess.SessionID)
	if n := f.count("jobId = 'p1'"); n != "1" {
		t.Fatalf("%s documents after commit", n)
	}
}
