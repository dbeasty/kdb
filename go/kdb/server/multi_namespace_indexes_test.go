package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
)

func createTextIndex(t *testing.T, p *RegistryIndexProvider, ns, name string) {
	t.Helper()
	if err := p.CreateIndex(sql.StmtCreateIndex{
		Name: name, Table: "t", Fields: []sql.IndexField{{Path: "title", Weight: 1}}, Using: "FULLTEXT",
	}, sql.QueryContext{NamespaceID: ns}); err != nil {
		t.Fatalf("CreateIndex %s on %s: %v", name, ns, err)
	}
}

func indexNames(p *RegistryIndexProvider) map[string]bool {
	out := map[string]bool{}
	for _, d := range p.registry.Descriptors() {
		out[d.Options["index_name"]] = true
	}
	return out
}

// Two namespaces of one catalog under one host each keep their own index definitions. Index
// state used to live per *catalog*, so the second namespace to open loaded the first one's
// catalog.json and overwrote it on save.
func TestNamespacesOfOneCatalogKeepTheirOwnIndexes(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "bank/accounts", "bank/ledger")
	acct, _ := set.Get("bank/accounts")
	ledger, _ := set.Get("bank/ledger")
	pa, err := acct.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	pl, err := ledger.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	createTextIndex(t, pa, "bank/accounts", "accounts_text")
	createTextIndex(t, pl, "bank/ledger", "ledger_text")
	if got := indexNames(pa); !got["accounts_text"] || got["ledger_text"] {
		t.Errorf("bank/accounts indexes %v", got)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	for ns, want := range map[string]string{"bank/accounts": "accounts_text", "bank/ledger": "ledger_text"} {
		cat, err := index.LoadCatalog(filepath.Join(root, "ns", ns, "index"))
		if err != nil {
			t.Fatalf("%s catalog: %v", ns, err)
		}
		if cat.NamespaceID != ns || len(cat.Indexes) != 1 || cat.Indexes[0].Options["index_name"] != want {
			t.Errorf("%s catalog on disk: %+v", ns, cat)
		}
	}

	host2, set2 := fileSet(t, root, "bank/accounts", "bank/ledger")
	defer host2.Close()
	for ns, want := range map[string]string{"bank/accounts": "accounts_text", "bank/ledger": "ledger_text"} {
		rt, _ := set2.Get(ns)
		p, err := rt.OpenIndexes(stores.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if got := indexNames(p); len(got) != 1 || !got[want] {
			t.Errorf("%s after reopen: %v, want only %s", ns, got, want)
		}
	}
}

// A deployment that created indexes before they moved keeps them: the catalog-level directory is
// still used by the namespace its catalog.json names, and by no other.
func TestLegacyCatalogLevelIndexDirStaysWithItsNamespace(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "bank/accounts", "bank/ledger")
	acct, _ := set.Get("bank/accounts")
	ledger, _ := set.Get("bank/ledger")
	legacy := filepath.Join(root, "bank", "index")
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := index.SaveCatalog(legacy, index.Catalog{NamespaceID: "bank/accounts"}); err != nil {
		t.Fatal(err)
	}
	if got := acct.indexDir(); got != legacy {
		t.Errorf("the namespace that owns the legacy directory moved off it: %s", got)
	}
	if got := ledger.indexDir(); got == legacy {
		t.Error("another namespace of the catalog was handed the legacy directory")
	}
	host.Close()
}

// A namespace opened beside the primary draws on the primary's memory budget, and releasing it
// does not stop the guard the primary still depends on.
func TestSharedGovernance(t *testing.T) {
	primary := newTestRuntime(t)
	primary.SetMemoryLimit(testBudget, 0.85)
	t.Cleanup(func() { primary.memGuard.Stop() })
	sec := newTestRuntime(t)
	sec.ShareGovernanceWith(primary)
	if sec.Admission() == nil || sec.Admission() != primary.Admission() {
		t.Fatal("the secondary does not share the primary's grant pool")
	}
	primary.memGuard.Stop()
	for i := 0; i < sampleWindow; i++ {
		primary.memGuard.observe(float64(testBudget) * 0.99)
	}
	if sec.MemoryZone() != primary.MemoryZone() || sec.MemoryZone() < ZoneHigh {
		t.Errorf("secondary zone %v, primary %v", sec.MemoryZone(), primary.MemoryZone())
	}
	sec.Release()
	if primary.memGuard != sec.memGuard {
		t.Fatal("guard not shared")
	}
}
