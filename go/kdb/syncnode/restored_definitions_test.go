package syncnode

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/sql"
)

func execSQL(t *testing.T, rt *server.KdbServerRuntime, q string) {
	t.Helper()
	ctx := sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Schema()}
	if _, err := rt.SQLEngine().Execute(q, ctx); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// U5: a namespace's definitions - its indexes and unique constraints - travel with a backup
// (MetaStore.View) and a restored namespace adopts them (AdoptView): its indexes exist and are
// built from the restored documents, and its constraint is enforced, without the application
// declaring any of them again.
func TestRestoredNamespaceAdoptsItsDefinitions(t *testing.T) {
	const ns = "qtask/docs"
	source := newTestNode(t, ns, Config{})
	if _, err := source.primary.OpenIndexes(stores.Options{}); err != nil {
		t.Fatal(err)
	}
	if err := source.node.Start(); err != nil {
		t.Fatal(err)
	}
	execSQL(t, source.primary, `CREATE INDEX by_c ON docs (_c) USING HASH`)
	execSQL(t, source.primary, `CREATE INDEX by_user ON docs (userId) USING HASH`)
	execSQL(t, source.primary, `CREATE INDEX by_due ON docs (dueDate) USING BTREE`)
	execSQL(t, source.primary, `CREATE UNIQUE INDEX uniq_email ON docs (email)`)
	defs, err := source.node.Meta().View(func(n string) bool { return n == ns })
	if err != nil || len(defs) == 0 {
		t.Fatalf("definitions to back up: %d (%v)", len(defs), err)
	}

	// The restore: the same documents, no definitions.
	restored := newTestNode(t, ns, Config{})
	p, err := restored.primary.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		restored.put(t, ns, mustID(t), fmt.Sprintf(`{"_c":"users","userId":"u%d","email":"e%d@x","dueDate":"2026-01-%02d"}`, i%5, i, i%28+1))
	}
	if err := restored.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.node.Meta().AdoptView(defs); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, d := range p.Registry().Descriptors() {
		names[d.IndexName()] = true
	}
	for _, want := range []string{"by_c", "by_user", "by_due"} {
		if !names[want] {
			t.Fatalf("index %s not restored (have %v)", want, names)
		}
	}
	ctx := sql.QueryContext{NamespaceID: ns, Schema: restored.primary.Schema()}
	res, err := restored.primary.SQLEngine().Execute(`SELECT kdb_id FROM docs WHERE userId = 'u3'`, ctx)
	if err != nil || len(res.Rows) != 10 || !strings.HasPrefix(res.Plan, "index:") {
		t.Fatalf("restored index: %d rows via %q (%v), want 10 via an index", len(res.Rows), res.Plan, err)
	}
	// The unique constraint came back too.
	if _, err := restored.primary.Upsert(ns, mustID(t), `{"_c":"users","email":"e7@x"}`, auth.Principal{}); err == nil {
		t.Fatal("a duplicate email was accepted: the restored unique constraint is not enforced")
	}
}
