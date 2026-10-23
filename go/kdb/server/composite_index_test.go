package server

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
)

// compositePair is two runtimes holding the same documents, one with a composite BTREE on
// (_c, status, updatedAt) and one without, so every answer the walk gives can be checked against
// the ordinary path's.
type compositePair struct {
	t           *testing.T
	with, bare  *rawWireClient
	ws, bs      string
	withRuntime *KdbServerRuntime
	ids         []codec.UUID
}

func newCompositePair(t *testing.T, docs []string) *compositePair {
	t.Helper()
	p := &compositePair{t: t}
	rtWith, rtBare := indexedRuntime(t), indexedRuntime(t)
	p.ids = make([]codec.UUID, len(docs))
	for i, body := range docs {
		p.ids[i] = mustRandomUUID(t)
		for _, rt := range []*KdbServerRuntime{rtWith, rtBare} {
			if _, err := rt.Upsert("app/data", p.ids[i], body, auth.Principal{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	p.with, p.bare = listenForTest(t, rtWith), listenForTest(t, rtBare)
	p.withRuntime = rtWith
	p.ws = p.with.sessionBegin(t, "app/data", "READ_COMMITTED").SessionID
	p.bs = p.bare.sessionBegin(t, "app/data", "READ_COMMITTED").SessionID
	for _, ddl := range []string{
		"CREATE INDEX c_status_updated ON t (_c, status, updatedAt) USING BTREE",
		"CREATE INDEX c_status_n ON t (_c, status, n) USING BTREE",
	} {
		if r := p.with.sqlExec(t, "app/data", p.ws, ddl); r.Error != nil {
			t.Fatalf("%s: %s", ddl, *r.Error)
		}
	}
	// The bare side still has an index on _c, so its answers come from the ordinary indexed path.
	if r := p.bare.sqlExec(t, "app/data", p.bs, "CREATE INDEX c_hash ON t (_c) USING HASH"); r.Error != nil {
		t.Fatalf("bare index: %s", *r.Error)
	}
	return p
}

func (p *compositePair) rows(c *rawWireClient, session, sqlText string) [][]string {
	p.t.Helper()
	r := c.sqlExec(p.t, "app/data", session, sqlText)
	if r.Error != nil {
		p.t.Fatalf("%s: %s", sqlText, *r.Error)
	}
	return r.Rows
}

func (p *compositePair) plan(sqlText string) string {
	p.t.Helper()
	return p.rows(p.with, p.ws, "EXPLAIN "+sqlText)[0][0]
}

// same runs sqlText on both sides and requires identical rows, and returns the composite side's plan.
func (p *compositePair) same(sqlText string) string {
	p.t.Helper()
	got, want := p.rows(p.with, p.ws, sqlText), p.rows(p.bare, p.bs, sqlText)
	if !reflect.DeepEqual(got, want) {
		p.t.Fatalf("%s:\n with the composite index %v\n without it                %v", sqlText, got, want)
	}
	return p.plan(sqlText)
}

// queueDocs is a qtask-shaped job collection with every irregular case the walk must not miss: an
// absent and a null sort field, an array status (which `status = 'pending'` matches), an object
// sort value, tied timestamps, and documents of other collections and statuses.
func queueDocs(rng *rand.Rand, n int) []string {
	statuses := []string{"pending", "pending", "pending", "done", "running"}
	var docs []string
	for i := 0; i < n; i++ {
		c := "jobs"
		if i%5 == 0 {
			c = "other"
		}
		ts := fmt.Sprintf("2026-10-%02dT%02d:%02d:00Z", 1+rng.Intn(20), rng.Intn(24), rng.Intn(4)*15) // many ties
		docs = append(docs, fmt.Sprintf(`{"_c":%q,"status":%q,"updatedAt":%q,"attempts":%d,"n":%d}`,
			c, statuses[rng.Intn(len(statuses))], ts, rng.Intn(5), rng.Intn(1000)))
	}
	docs = append(docs,
		`{"_c":"jobs","status":"pending","attempts":0,"n":1}`,                                    // no updatedAt: NULL, sorts first
		`{"_c":"jobs","status":"pending","updatedAt":null,"attempts":0,"n":2}`,                   // null updatedAt
		`{"_c":"jobs","status":["pending","x"],"updatedAt":"2026-09-01T00:00:00Z","attempts":0}`, // array status
		`{"_c":"jobs","status":"pending","updatedAt":{"t":1},"attempts":0}`,                      // object: JSON
		`{"_c":"jobs","updatedAt":"2026-09-02T00:00:00Z","attempts":0}`,                          // no status
		`{"_c":"jobs","status":null,"updatedAt":"2026-09-03T00:00:00Z","attempts":0}`,            // null status
	)
	return docs
}

// The composite walk answers exactly what the ordinary path answers - ascending and descending,
// with and without a residual, at several limits and offsets, through every irregular document -
// and it is the plan that ran.
func TestCompositeIndexOrderedReadsMatchTheOrdinaryPath(t *testing.T) {
	p := newCompositePair(t, queueDocs(rand.New(rand.NewSource(7)), 400))
	for _, dir := range []string{"ASC", "DESC"} {
		for _, where := range []string{
			"_c = 'jobs' AND status = 'pending'",
			"status = 'pending' AND _c = 'jobs' AND attempts < 3",
			"_c = 'jobs' AND status = 'done'",
			"_c = 'nothing' AND status = 'pending'",
		} {
			for _, page := range []string{"LIMIT 1", "LIMIT 7", "LIMIT 50", "LIMIT 5 OFFSET 3", "LIMIT 1000"} {
				q := fmt.Sprintf("SELECT kdb_id, updatedAt FROM t WHERE %s ORDER BY updatedAt %s, kdb_id %s", where, dir, page)
				if plan := p.same(q); !strings.Contains(plan, "index:c_status_updated+order(updatedAt") {
					t.Fatalf("%s ran %q, not the composite walk", q, plan)
				}
			}
		}
	}
}

// Rows tied on the ORDER BY are a set, not a sequence: both paths must agree on which rows make
// the page, whatever order each breaks the ties in.
func TestCompositeIndexTiesAreTheSameRows(t *testing.T) {
	var docs []string
	for i := 0; i < 60; i++ {
		docs = append(docs, fmt.Sprintf(`{"_c":"jobs","status":"pending","updatedAt":"2026-10-0%dT00:00:00Z"}`, 1+i%3))
	}
	p := newCompositePair(t, docs)
	for _, dir := range []string{"ASC", "DESC"} {
		q := "SELECT kdb_id FROM t WHERE _c = 'jobs' AND status = 'pending' ORDER BY updatedAt " + dir + " LIMIT 20"
		got, want := p.rows(p.with, p.ws, q), p.rows(p.bare, p.bs, q)
		if len(got) != 20 || len(want) != 20 {
			t.Fatalf("%s: %d and %d rows", q, len(got), len(want))
		}
		for _, rows := range [][][]string{got, want} {
			first := p.rows(p.with, p.ws, "SELECT updatedAt FROM t WHERE kdb_id = '"+rows[0][0]+"'")[0][0]
			if dir == "ASC" && !strings.HasPrefix(first, "2026-10-01") || dir == "DESC" && !strings.HasPrefix(first, "2026-10-03") {
				t.Fatalf("%s: first row sorts at %s", q, first)
			}
		}
	}
}

// Where the ordered field mixes numeric key types under the prefix, byte order is not SQL's: the
// walk declines and the ordinary path answers. One numeric type is walked.
func TestCompositeIndexDeclinesMixedNumericTypes(t *testing.T) {
	docs := []string{}
	for i := 0; i < 30; i++ {
		docs = append(docs, fmt.Sprintf(`{"_c":"ints","status":"s","n":%d}`, i))
		docs = append(docs, fmt.Sprintf(`{"_c":"mixed","status":"s","n":%d}`, i))
		docs = append(docs, fmt.Sprintf(`{"_c":"mixed","status":"s","n":%d.5}`, i))
	}
	p := newCompositePair(t, docs)
	qi := "SELECT kdb_id, n FROM t WHERE _c = 'ints' AND status = 's' ORDER BY n DESC, kdb_id LIMIT 5"
	if plan := p.same(qi); !strings.Contains(plan, "index:c_status_n") {
		t.Fatalf("ints ran %q, want the composite walk", plan)
	}
	qm := "SELECT kdb_id, n FROM t WHERE _c = 'mixed' AND status = 's' ORDER BY n, kdb_id LIMIT 5"
	if plan := p.same(qm); strings.Contains(plan, "index:c_status_n") {
		t.Fatalf("mixed int and float ran the composite walk (%q)", plan)
	}
}

// A queue claim through the composite index takes the same jobs, in the same order, as the
// ordinary path, and the index follows every claim's write.
func TestCompositeIndexServesQueueClaims(t *testing.T) {
	p := newCompositePair(t, queueDocs(rand.New(rand.NewSource(11)), 200))
	claim := "UPDATE t SET status = 'running' WHERE _c = 'jobs' AND status = 'pending' ORDER BY updatedAt, kdb_id LIMIT 1 RETURNING kdb_id"
	for i := 0; i < 25; i++ {
		got, want := p.rows(p.with, p.ws, claim), p.rows(p.bare, p.bs, claim)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("claim %d: %v with the composite index, %v without", i, got, want)
		}
	}
	rest := "SELECT kdb_id FROM t WHERE _c = 'jobs' AND status = 'pending' ORDER BY updatedAt, kdb_id LIMIT 1000"
	p.same(rest)
}

// Single-field lookups never pick a composite index: its keys are tuples, so an equality on its
// first field would find nothing.
func TestCompositeIndexIsNotUsedForSingleFieldLookups(t *testing.T) {
	p := newCompositePair(t, queueDocs(rand.New(rand.NewSource(3)), 100))
	p.same("SELECT COUNT(*) FROM t WHERE _c = 'jobs'")
	p.same("SELECT kdb_id FROM t WHERE _c = 'other' ORDER BY kdb_id")
}
