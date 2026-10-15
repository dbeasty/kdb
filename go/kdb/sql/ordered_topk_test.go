package sql

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// storeProvider is an IndexProvider over real index stores (the same MemoryStore the server's
// registry holds), with the ordered key-group capability served the way the server serves it.
type storeProvider struct {
	byField map[string]index.Store // field -> HASH or BTREE store
}

func (p *storeProvider) FullTextSearch(QueryContext, string, string, int) ([]index.RankedResult, bool, error) {
	return nil, false, nil
}

func (p *storeProvider) VectorSearch(QueryContext, string, []float32, int) ([]index.RankedResult, bool, error) {
	return nil, false, nil
}

func cellIndexKey(c Cell) (index.Key, bool) {
	switch v := c.(type) {
	case CellString:
		return index.StringKey{Value: v.Value}, true
	case CellLong:
		return index.Int64Key{Value: v.Value}, true
	case CellDouble:
		return index.Float64Key{Value: v.Value}, true
	case CellBool:
		return index.BoolKey{Value: v.Value}, true
	}
	return nil, false
}

func (p *storeProvider) ExactLookup(ctx QueryContext, field string, value Cell) ([]codec.UUID, bool, error) {
	s, ok := p.byField[field]
	if !ok {
		return nil, false, nil
	}
	k, ok := cellIndexKey(value)
	if !ok {
		return nil, false, nil
	}
	ids, err := s.Lookup(k, ctx.AtCommit)
	if err != nil {
		return nil, false, nil
	}
	return ids, true, nil
}

func (p *storeProvider) RangeLookup(ctx QueryContext, field string, low, high Cell, _, _ bool) ([]codec.UUID, bool, error) {
	s, ok := p.byField[field]
	if !ok || s.Descriptor().Type != index.IndexTypeBTree {
		return nil, false, nil
	}
	var from, to index.Key
	if low != nil {
		if from, ok = cellIndexKey(low); !ok {
			return nil, false, nil
		}
	}
	if high != nil {
		if to, ok = cellIndexKey(high); !ok {
			return nil, false, nil
		}
	}
	ids, err := s.Range(from, to, ctx.AtCommit, 0, true)
	if err != nil {
		return nil, false, nil
	}
	return ids, true, nil
}

func (p *storeProvider) IndexKeyGroups(ctx QueryContext, field string, kind IndexKeyKind, after Cell, maxIDs int) ([]IndexKeyGroup, bool, error) {
	s, ok := p.byField[field]
	if !ok {
		return nil, false, nil
	}
	return IndexKeyGroupsFromStore(s, ctx, field, kind, after, maxIDs)
}

func (p *storeProvider) UnindexedDocs(ctx QueryContext, field string, ids []codec.UUID) ([]codec.UUID, bool, error) {
	s, ok := p.byField[field]
	if !ok {
		return nil, false, nil
	}
	return UnindexedDocsFromStore(s, ctx, field, ids)
}

// orderedFixture is a namespace with indexes kept up to date commit by commit.
type orderedFixture struct {
	t        *testing.T
	ns       string
	d        *dag.InMemoryCommitDag
	store    *mem.InMemoryStorageAdapter
	provider *storeProvider
	commits  []codec.Hash
}

func newOrderedFixture(t *testing.T, ns string, indexes map[string]index.IndexType) *orderedFixture {
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	f := &orderedFixture{t: t, ns: ns, d: d, store: mem.NewInMemoryStorageAdapter(), provider: &storeProvider{byField: map[string]index.Store{}}}
	for field, typ := range indexes {
		desc := index.Descriptor{IndexID: index.DerivedIndexID(ns, field, typ), NamespaceID: ns, FieldName: field, Fields: []string{field}, Type: typ}
		f.provider.byField[field] = index.NewMemoryStore(desc, d)
	}
	return f
}

// commit writes (whole-document replace) and deletes, then files the documents in every index.
func (f *orderedFixture) commit(writes map[codec.UUID]string, deletes []codec.UUID) {
	t := f.t
	var ops []document.Op
	for id, body := range writes {
		ops = append(ops, document.WriteOp{DocID: id, Patch: body, Replace: true})
	}
	for _, id := range deletes {
		ops = append(ops, document.DeleteOp{DocID: id})
	}
	parent, _ := f.d.Head()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{ID: txID, BaseVersion: parent, Operations: ops, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	res, err := transaction.NewEngine(transaction.ConflictPolicyStrict, nil).Commit(tx, f.d, f.store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(transaction.ResultSuccess); !ok {
		t.Fatalf("commit: %+v", res)
	}
	head, _ := f.d.Head()
	for _, s := range f.provider.byField {
		ds := s.(index.DocumentStore)
		for id, body := range writes {
			if err := ds.PutDocument(id, head, body); err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range deletes {
			if err := s.Delete(id, head); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.commits = append(f.commits, head)
}

func (f *orderedFixture) run(oracle bool, q string, at *codec.Hash) (QueryResult, *ExecStats, error) {
	eng := NewEngineWithIndexes(f.store, f.d, f.provider).(*defaultEngine)
	eng.executor.scanIDsOnly = oracle // the oracle reads and sorts every candidate
	st := &ExecStats{}
	res, err := eng.Execute(q, QueryContext{NamespaceID: f.ns, Schema: schema.None(), Stats: st, AtCommit: at})
	return res, st, err
}

// randomA is a value for the ORDER BY column drawn from every case the ordered top-k must get
// right: absent, null, booleans (sorting as 0/1 among the numbers), ints and doubles that tie
// across types, -0, strings (including numeric-looking ones), arrays (sorting as JSON, but filed
// under their elements), one-element arrays, empty arrays, objects and nested shapes.
func randomA(r *rand.Rand, mix int) string {
	switch r.Intn(mix) {
	case 0:
		return ""
	case 1:
		return `"a":null,`
	case 2:
		return fmt.Sprintf(`"a":%v,`, r.Intn(2) == 0)
	case 3:
		return fmt.Sprintf(`"a":%d.0,`, r.Intn(4))
	case 4:
		return fmt.Sprintf(`"a":%d.5,`, r.Intn(6)-2)
	case 5:
		return fmt.Sprintf(`"a":"s%d",`, r.Intn(6))
	case 6:
		return `"a":-0.0,`
	case 7:
		return fmt.Sprintf(`"a":"%d",`, r.Intn(3))
	case 8:
		return fmt.Sprintf(`"a":[%d,"s%d"],`, r.Intn(4), r.Intn(3))
	case 9:
		return fmt.Sprintf(`"a":[%d],`, r.Intn(4))
	case 10:
		return `"a":[],`
	case 11:
		return fmt.Sprintf(`"a":{"x":%d},`, r.Intn(3))
	case 12:
		return fmt.Sprintf(`"a":[{"x":1},%v,null],`, r.Intn(2) == 0)
	case 13:
		return `"a":[[1],2],`
	default:
		// The common case: a scalar int, often tied.
		return fmt.Sprintf(`"a":%d,`, r.Intn(12)-3)
	}
}

func randomDoc(r *rand.Rand, i, mix int) string {
	b := ""
	if r.Intn(6) != 0 {
		b = fmt.Sprintf(`"b":%d,`, r.Intn(4))
	}
	return fmt.Sprintf(`{%s%s"g":"g%d","n":%d}`, randomA(r, mix), b, r.Intn(3), i)
}

// U7, second half: an ORDER BY ... LIMIT served from a BTREE on the first ORDER BY column returns
// exactly what reading and sorting every candidate returns, for every mix of value types, both
// directions, several sort keys, OFFSET, WHERE clauses served by an index or not, updates and
// deletes, and reads at an older commit - and it is actually taken where it applies.
func TestBTreeTopKMatchesTheFullSort(t *testing.T) {
	orders := []string{"a", "a DESC", "a, b DESC", "a, n DESC", "a, b, n", "b, a", "b", "b, n DESC", "b DESC, a", "a ASC, g DESC"}
	wheres := []string{
		"",
		" WHERE g = 'g1'",
		" WHERE n >= 50",
		" WHERE g IN ('g0', 'g2') AND n < 300",
		" WHERE g = 'g1' AND b > 1",
		" WHERE a > 2",
		" WHERE a >= 's2'",
		" WHERE n BETWEEN 10 AND 250 AND g <> 'g0'",
		" WHERE g = 'g2' AND (b = 1 OR a IS NULL)",
	}
	limits := []string{"LIMIT 1", "LIMIT 7", "LIMIT 25 OFFSET 10", "LIMIT 0", "LIMIT 500", "LIMIT 3 OFFSET 100", "LIMIT 40"}
	taken := 0
	total := 0
	for seed := int64(1); seed <= 6; seed++ {
		r := rand.New(rand.NewSource(seed))
		// Seeds alternate between every value shape and mostly plain ints (where the path is
		// taken the most).
		mix := 15
		if seed%2 == 0 {
			mix = 40
		}
		f := newOrderedFixture(t, fmt.Sprintf("app/btree-topk-%d", seed), map[string]index.IndexType{
			"a": index.IndexTypeBTree, "b": index.IndexTypeBTree, "n": index.IndexTypeBTree, "g": index.IndexTypeHash,
		})
		n := 300 + r.Intn(200)
		ids := make([]codec.UUID, n)
		writes := map[codec.UUID]string{}
		for i := range ids {
			ids[i], _ = codec.RandomUUID()
			writes[ids[i]] = randomDoc(r, i, mix)
		}
		f.commit(writes, nil)
		// A second commit rewrites and deletes some, so the index state has history.
		writes = map[codec.UUID]string{}
		var deletes []codec.UUID
		for i, id := range ids {
			switch r.Intn(10) {
			case 0:
				writes[id] = randomDoc(r, i, mix)
			case 1:
				deletes = append(deletes, id)
			}
		}
		f.commit(writes, deletes)
		ats := []*codec.Hash{nil, &f.commits[0]}
		for _, at := range ats {
			for _, order := range orders {
				for _, where := range wheres {
					for _, lim := range limits {
						q := fmt.Sprintf("SELECT n, a, b FROM t%s ORDER BY %s %s", where, order, lim)
						want, wantSt, werr := f.run(true, q, at)
						got, st, gerr := f.run(false, q, at)
						if fmt.Sprint(werr) != fmt.Sprint(gerr) {
							t.Fatalf("seed %d at %v: %s: error %v, full sort %v", seed, at, q, gerr, werr)
						}
						if !reflect.DeepEqual(got.Rows, want.Rows) {
							t.Fatalf("seed %d at %v: %s: ordered top-k differs from the full sort (plan %s)\n got  %v\n want %v", seed, at, q, got.Plan, got.Rows, want.Rows)
						}
						total++
						if strings.Contains(got.Plan, "+order(") {
							taken++
							if st.DocsRead > wantSt.DocsRead {
								t.Fatalf("%s: read %d documents, the full sort %d", q, st.DocsRead, wantSt.DocsRead)
							}
						}
					}
				}
			}
		}
	}
	// Not vacuous: a good share of the queries took the ordered path.
	if taken < total/10 {
		t.Fatalf("ordered top-k taken for %d of %d queries", taken, total)
	}
	t.Logf("ordered top-k taken for %d of %d queries", taken, total)
}

// The ordered top-k reads about LIMIT+OFFSET documents, not every candidate: candidates with
// distinct keys, a page of 25 out of ~1000 reads exactly 25.
func TestBTreeTopKReadsOnlyThePage(t *testing.T) {
	f := newOrderedFixture(t, "app/btree-topk-page", map[string]index.IndexType{"due": index.IndexTypeBTree, "u": index.IndexTypeHash})
	r := rand.New(rand.NewSource(3))
	writes := map[codec.UUID]string{}
	perm := r.Perm(3000)
	for i := 0; i < 3000; i++ {
		id, _ := codec.RandomUUID()
		writes[id] = fmt.Sprintf(`{"u":"u%d","due":"2026-%05d","n":%d}`, i%3, perm[i], i)
	}
	f.commit(writes, nil)
	for _, q := range []string{
		"SELECT n, due FROM t WHERE u = 'u1' ORDER BY due LIMIT 20 OFFSET 5",
		"SELECT n, due FROM t WHERE u = 'u1' ORDER BY due, n DESC LIMIT 25",
	} {
		want, wantSt, err := f.run(true, q, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, st, err := f.run(false, q, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("%s: rows differ", q)
		}
		if !strings.Contains(got.Plan, "+order(due)") {
			t.Fatalf("%s: plan %q, want the ordered top-k", q, got.Plan)
		}
		if st.DocsRead != 25 || wantSt.DocsRead != 1000 {
			t.Fatalf("%s: read %d documents (full sort %d), want 25 (1000)", q, st.DocsRead, wantSt.DocsRead)
		}
	}
	// DESC, and a numeric key beyond 2^53, are not served: same rows, every candidate read.
	q := "SELECT n FROM t WHERE u = 'u1' ORDER BY due DESC LIMIT 20"
	got, st, _ := f.run(false, q, nil)
	want, _, _ := f.run(true, q, nil)
	if !reflect.DeepEqual(got.Rows, want.Rows) || strings.Contains(got.Plan, "+order(") || st.DocsRead != 1000 {
		t.Fatalf("%s: plan %q read %d", q, got.Plan, st.DocsRead)
	}
}

// A walk that gives up part way (a key past 2^53) hands what it read to the streaming top-k,
// which reads no document twice.
func TestBTreeTopKFallbackReadsEachDocumentOnce(t *testing.T) {
	f := newOrderedFixture(t, "app/btree-topk-big", map[string]index.IndexType{"v": index.IndexTypeBTree, "u": index.IndexTypeHash})
	writes := map[codec.UUID]string{}
	for i := 0; i < 200; i++ {
		id, _ := codec.RandomUUID()
		v := fmt.Sprint(i)
		if i%50 == 7 {
			v = "9007199254740993" // 2^53 + 1
		}
		writes[id] = fmt.Sprintf(`{"u":"u%d","v":%s,"n":%d}`, i%2, v, i)
	}
	f.commit(writes, nil)
	q := "SELECT n, v FROM t WHERE u = 'u1' ORDER BY v LIMIT 98"
	want, _, err := f.run(true, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, st, err := f.run(false, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("rows differ:\n got  %v\n want %v", got.Rows, want.Rows)
	}
	if strings.Contains(got.Plan, "+order(") || st.DocsRead != 100 {
		t.Fatalf("plan %q read %d documents, want the fallback reading each of 100 once", got.Plan, st.DocsRead)
	}
}
