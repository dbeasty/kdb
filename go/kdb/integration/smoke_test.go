package integration

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

func TestFullStackMemory(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("demo", "demo/users", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	d, ok := rt.DAG.(*dag.InMemoryCommitDag)
	if !ok {
		t.Fatal("expected InMemoryCommitDag")
	}
	engine := sql.NewEngine(rt.Storage, d)
	ctx := sql.QueryContext{NamespaceID: rt.DefaultNamespace, Schema: schema.None()}
	created, err := engine.Execute("CREATE TABLE users (id VARCHAR NOT NULL, name VARCHAR NOT NULL)", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if created.AppliedSchema == nil {
		t.Fatal("expected schema")
	}
	sch := *created.AppliedSchema
	_, err = engine.ExecuteDML("INSERT INTO users (id, name) VALUES ('1', 'alice')", sql.QueryContext{
		NamespaceID: rt.DefaultNamespace,
		Schema:      sch,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := engine.Execute("SELECT COUNT(*) AS n FROM users", sql.QueryContext{
		NamespaceID: rt.DefaultNamespace,
		Schema:      sch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected count row")
	}
}
