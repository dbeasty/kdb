package sql_test

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

func TestCreateTableAndInsert(t *testing.T) {
	fx := newFixture(t)
	created, err := fx.sql.Execute(`CREATE TABLE users (
		userId VARCHAR NOT NULL,
		status VARCHAR NOT NULL
	)`, sql.QueryContext{NamespaceID: fx.ns, Schema: schema.None()})
	if err != nil {
		t.Fatal(err)
	}
	if created.AppliedSchema == nil || created.AppliedSchema.IsNone() {
		t.Fatal("expected schema")
	}
	dml, err := fx.executeDML(`INSERT INTO users (userId, status) VALUES ('u1', 'active')`, *created.AppliedSchema)
	if err != nil {
		t.Fatal(err)
	}
	if dml.RowsAffected != 1 || len(dml.GeneratedIDs) != 1 {
		t.Fatalf("insert: %+v", dml)
	}
}

func TestMultiRowInsert(t *testing.T) {
	fx := newFixture(t)
	created, err := fx.sql.Execute("CREATE TABLE t (id VARCHAR NOT NULL)", sql.QueryContext{NamespaceID: fx.ns, Schema: schema.None()})
	if err != nil {
		t.Fatal(err)
	}
	dml, err := fx.executeDML("INSERT INTO t (id) VALUES ('a'), ('b')", *created.AppliedSchema)
	if err != nil {
		t.Fatal(err)
	}
	if dml.RowsAffected != 2 {
		t.Fatalf("rows: %d", dml.RowsAffected)
	}
}

func TestCountAfterCreate(t *testing.T) {
	fx := newFixture(t)
	created, err := fx.sql.Execute("CREATE TABLE t (id VARCHAR NOT NULL)", sql.QueryContext{NamespaceID: fx.ns, Schema: schema.None()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fx.executeDML("INSERT INTO t (id) VALUES ('x')", *created.AppliedSchema); err != nil {
		t.Fatal(err)
	}
	count, err := fx.sql.Execute("SELECT COUNT(*) AS n FROM t", sql.QueryContext{NamespaceID: fx.ns, Schema: *created.AppliedSchema})
	if err != nil {
		t.Fatal(err)
	}
	if len(count.Rows) != 1 {
		t.Fatalf("rows: %d", len(count.Rows))
	}
	cell, ok := count.Rows[0].Values[0].(sql.CellLong)
	if !ok || cell.Value != 1 {
		t.Fatalf("count: %v", count.Rows[0].Values[0])
	}
}

// SELECT on an unknown column must fail as a normal error, not panic - Execute runs directly in
// the wire connection's goroutine, and an unrecovered panic there took the whole server down for
// every other connection, not just this one query.
func TestSelectUnknownColumnReturnsError(t *testing.T) {
	fx := newFixture(t)
	created, err := fx.sql.Execute("CREATE TABLE t (id VARCHAR NOT NULL)", sql.QueryContext{NamespaceID: fx.ns, Schema: schema.None()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fx.sql.Execute("SELECT nosuchcolumn FROM t", sql.QueryContext{NamespaceID: fx.ns, Schema: *created.AppliedSchema})
	if err == nil {
		t.Fatal("expected an error for an unknown column, got nil")
	}
	var planningErr *sql.PlanningError
	if !errors.As(err, &planningErr) {
		t.Fatalf("expected *sql.PlanningError, got %T: %v", err, err)
	}
}

func TestParserSelectInsertCreate(t *testing.T) {
	p := sql.DefaultParser{}
	for _, q := range []string{
		"SELECT id FROM t WHERE id = 'x'",
		"INSERT INTO t (id) VALUES ('a')",
		"CREATE TABLE t (id VARCHAR NOT NULL)",
	} {
		if _, err := p.Parse(q); err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
	}
}

type fixture struct {
	engine transaction.Engine
	sql    sql.Engine
	ns     string
	dag    *dag.InMemoryCommitDag
	store  *mem.InMemoryStorageAdapter
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ns := "app/sql"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	return &fixture{
		engine: transaction.NewEngine(transaction.ConflictPolicyStrict, nil),
		sql:    sql.NewEngine(store, d),
		ns:     ns,
		dag:    d,
		store:  store,
	}
}

func (fx *fixture) executeDML(sqlText string, sch schema.KdbSchema) (sql.DMLResult, error) {
	dml, err := fx.sql.ExecuteDML(sqlText, sql.QueryContext{NamespaceID: fx.ns, Schema: sch})
	if err != nil {
		return sql.DMLResult{}, err
	}
	if len(dml.Operations) == 0 {
		return dml, nil
	}
	parent, err := fx.dag.Head()
	if err != nil {
		return sql.DMLResult{}, err
	}
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: parent, Operations: dml.Operations,
		Timestamp: codec.TimestampNow(), AuthorNodeID: author,
	}
	res, err := fx.engine.Commit(tx, fx.dag, fx.store, sch, nil, "")
	if err != nil {
		return sql.DMLResult{}, err
	}
	if _, ok := res.(transaction.ResultSuccess); !ok {
		return sql.DMLResult{}, err
	}
	return dml, nil
}
