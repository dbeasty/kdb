package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// An upsert is a merge patch: it names the keys it changes and the stored document keeps the
// rest. The indexes used to be fed the patch rather than the stored document, so a partial write
// that left an indexed field out removed the document from that index - while the document still
// had the field. `WHERE g = 'a'` then missed a row whose g was 'a'.
func TestPartialWriteKeepsTheDocumentInTheIndexOfFieldsItLeftOut(t *testing.T) {
	f := newUniqueFixture(t)
	if r := f.sql(`CREATE INDEX by_g ON docs (g) USING HASH`); r.Error != nil {
		t.Fatal(*r.Error)
	}
	if r := f.sql(`CREATE INDEX by_n ON docs (n) USING BTREE`); r.Error != nil {
		t.Fatal(*r.Error)
	}
	id, _ := codec.RandomUUID()
	put := func(body string) {
		t.Helper()
		if r := f.c.upsert(t, "app/data", f.sess.SessionID, id.String(), body); r.Error != nil {
			t.Fatalf("%s: %s", body, *r.Error)
		}
	}
	count := func(where string) int {
		t.Helper()
		r := f.sql(`SELECT kdb_id FROM docs WHERE ` + where)
		if r.Error != nil {
			t.Fatalf("%s: %s", where, *r.Error)
		}
		return len(r.Rows)
	}

	put(`{"g":"a","n":5,"h":1}`)
	put(`{"h":2}`) // touches neither indexed field
	if got := count(`g = 'a'`); got != 1 {
		t.Fatalf("g = 'a' after a write that did not name g: %d rows, want 1", got)
	}
	if got := count(`n > 3`); got != 1 {
		t.Fatalf("n > 3 after a write that did not name n: %d rows, want 1", got)
	}

	put(`{"g":"b"}`) // changes one indexed field, leaves the other
	if got := count(`g = 'a'`); got != 0 {
		t.Fatalf("the old value still matches: %d rows", got)
	}
	if got := count(`g = 'b'`); got != 1 {
		t.Fatalf("g = 'b': %d rows, want 1", got)
	}
	if got := count(`n = 5`); got != 1 {
		t.Fatalf("n = 5 after a write that changed only g: %d rows, want 1", got)
	}

	put(`{"g":null}`) // a merge patch's null removes the field
	if got := count(`g = 'b'`); got != 0 {
		t.Fatalf("a removed field still matches: %d rows", got)
	}
	if got := count(`n = 5`); got != 1 {
		t.Fatalf("n = 5 after removing g: %d rows, want 1", got)
	}
}
