package sql

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// countingStore counts point fetches, to show the full scan no longer fetches each row twice.
type countingStore struct {
	*mem.InMemoryStorageAdapter
	gets int
}

func (c *countingStore) GetDocument(ns string, id codec.UUID, at codec.Hash) (*document.Document, error) {
	c.gets++
	return c.InMemoryStorageAdapter.GetDocument(ns, id, at)
}

func fetchOnceFixture(t *testing.T) (*countingStore, *dag.InMemoryCommitDag, string) {
	t.Helper()
	ns := "app/fetch-once"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := &countingStore{InMemoryStorageAdapter: mem.NewInMemoryStorageAdapter()}
	var ops []document.Op
	for i := 0; i < 300; i++ {
		id, _ := codec.RandomUUID()
		body := fmt.Sprintf(`{"n":%d,"grp":"g%d","tag":%q,"nested":{"v":%d}}`, i, i%7, []string{"a", "b", "c"}[i%3], i%11)
		if i%13 == 0 {
			body = fmt.Sprintf(`{"n":%d,"grp":null}`, i)
		}
		ops = append(ops, document.WriteOp{DocID: id, Patch: body})
	}
	parent, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{ID: txID, BaseVersion: parent, Operations: ops, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	res, err := transaction.NewEngine(transaction.ConflictPolicyStrict, nil).Commit(tx, d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(transaction.ResultSuccess); !ok {
		t.Fatalf("seed commit: %+v", res)
	}
	return store, d, ns
}

// B3: the full scan passes the documents it resolved straight through instead of re-fetching each
// by id. Every query shape returns exactly what the fetch-by-id path returned.
func TestFullScanFetchOnceReturnsIdenticalResults(t *testing.T) {
	store, d, ns := fetchOnceFixture(t)
	queries := []string{
		"SELECT * FROM t",
		"SELECT kdb_id FROM t LIMIT 5",
		"SELECT kdb_id FROM t LIMIT 7 OFFSET 290",
		"SELECT kdb_id FROM t LIMIT 10 OFFSET 400",
		"SELECT n FROM t WHERE n > 100",
		"SELECT n FROM t WHERE grp = 'g3' LIMIT 4",
		"SELECT n, tag FROM t WHERE tag = 'b' AND n < 50 ORDER BY n DESC",
		"SELECT n FROM t WHERE grp IS NULL",
		"SELECT grp, COUNT(*) FROM t GROUP BY grp ORDER BY grp",
		"SELECT COUNT(*) FROM t WHERE n >= 10",
		"SELECT DISTINCT tag FROM t ORDER BY tag",
		"SELECT n FROM t ORDER BY n LIMIT 3 OFFSET 2",
		"SELECT n FROM t WHERE nested.v = 3 ORDER BY n",
		"SELECT n FROM t WHERE n = -1",
	}
	run := func(idsOnly bool, q string) (QueryResult, error) {
		eng := NewEngine(store, d).(*defaultEngine)
		eng.executor.scanIDsOnly = idsOnly
		return eng.Execute(q, QueryContext{NamespaceID: ns, Schema: schema.None(), Stats: &ExecStats{}})
	}
	for _, q := range queries {
		want, werr := run(true, q)
		store.gets = 0
		got, gerr := run(false, q)
		if fmt.Sprint(werr) != fmt.Sprint(gerr) {
			t.Fatalf("%s: error %v, fetch-by-id gave %v", q, gerr, werr)
		}
		if !reflect.DeepEqual(got.Rows, want.Rows) || !reflect.DeepEqual(got.Columns, want.Columns) || got.Plan != want.Plan {
			t.Fatalf("%s: results differ\n got  %d rows %v\n want %d rows %v", q, len(got.Rows), got.Plan, len(want.Rows), want.Plan)
		}
		if got.Plan == "fullscan" && store.gets != 0 {
			t.Fatalf("%s: full scan still fetched %d documents by id", q, store.gets)
		}
	}
	// The row budget still bounds a residual scan exactly as before.
	for _, idsOnly := range []bool{true, false} {
		eng := NewEngine(store, d).(*defaultEngine)
		eng.executor.scanIDsOnly = idsOnly
		_, err := eng.Execute("SELECT n FROM t WHERE n > 1000", QueryContext{NamespaceID: ns, Schema: schema.None(), RowBudget: 50})
		if _, ok := err.(*ScanRowBudgetExceededError); !ok {
			t.Fatalf("idsOnly=%v: want ScanRowBudgetExceededError, got %v", idsOnly, err)
		}
	}
}
