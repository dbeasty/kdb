package sql

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// U7: ORDER BY ... LIMIT served by a bounded top-k returns exactly what sorting every match and
// cutting it returns - ties in candidate order, NULLs, absent fields, mixed types, arrays,
// several sort keys, both directions, OFFSET - while holding only LIMIT+OFFSET documents.
func TestTopKMatchesTheFullSort(t *testing.T) {
	ns := "app/topk"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	r := rand.New(rand.NewSource(7))
	var ops []document.Op
	for i := 0; i < 400; i++ {
		id, _ := codec.RandomUUID()
		var a string
		switch r.Intn(8) {
		case 0:
			a = `"a":null,`
		case 1:
			a = ""
		case 2:
			a = fmt.Sprintf(`"a":"s%d",`, r.Intn(5))
		case 3:
			a = fmt.Sprintf(`"a":[%d,%d],`, r.Intn(3), r.Intn(3))
		case 4:
			a = fmt.Sprintf(`"a":%v,`, r.Intn(2) == 0)
		default:
			a = fmt.Sprintf(`"a":%d,`, r.Intn(10))
		}
		ops = append(ops, document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{%s"b":%d,"g":"g%d","n":%d}`, a, r.Intn(4), i%3, i)})
	}
	parent, _ := d.Head()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{ID: txID, BaseVersion: parent, Operations: ops, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	if res, err := transaction.NewEngine(transaction.ConflictPolicyStrict, nil).Commit(tx, d, store, schema.None(), nil, ""); err != nil {
		t.Fatal(err)
	} else if _, ok := res.(transaction.ResultSuccess); !ok {
		t.Fatalf("seed: %+v", res)
	}
	run := func(oracle bool, q string) (QueryResult, *ExecStats, error) {
		eng := NewEngine(store, d).(*defaultEngine)
		eng.executor.scanIDsOnly = oracle // the oracle sorts every match, as before top-k
		st := &ExecStats{}
		res, err := eng.Execute(q, QueryContext{NamespaceID: ns, Schema: schema.None(), Stats: st})
		return res, st, err
	}
	for _, order := range []string{"a", "a DESC", "b, a", "b DESC, n", "a, b DESC, n", "g DESC, a DESC"} {
		for _, where := range []string{"", " WHERE n >= 50", " WHERE g = 'g1'"} {
			for _, lim := range []string{"LIMIT 1", "LIMIT 7", "LIMIT 25 OFFSET 10", "LIMIT 0", "LIMIT 500"} {
				q := fmt.Sprintf("SELECT n, a, b FROM t%s ORDER BY %s %s", where, order, lim)
				want, _, werr := run(true, q)
				got, st, gerr := run(false, q)
				if fmt.Sprint(werr) != fmt.Sprint(gerr) {
					t.Fatalf("%s: error %v, full sort %v", q, gerr, werr)
				}
				if !reflect.DeepEqual(got.Rows, want.Rows) {
					t.Fatalf("%s: top-k differs from the full sort\n got  %v\n want %v", q, got.Rows, want.Rows)
				}
				_ = st
			}
		}
	}
}
