package server

import (
	"encoding/json"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
)

func chainOf(kind string) peersync.ResolutionChain {
	return peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: kind}}}
}

// openInto opens ns in memory into node's set, as an opener would, with its definitions applied.
func openInto(t *testing.T, n metaNode, ns string) *KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(ns), ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	srv.NodeID = n.data.NodeID
	n.store.ApplyTo(srv)
	if err := n.data.Namespaces.Add(srv); err != nil {
		t.Fatal(err)
	}
	return srv
}

func hashOf(rt *KdbServerRuntime) string { return rt.ResolutionChainOf().Hash() }

// TestPatternDefinitionsResolveMostSpecificFirst (G4): one definition per pattern covers every
// matching namespace, open now or later; a namespace's own definition overrides; among patterns
// the more specific wins.
func TestPatternDefinitionsResolveMostSpecificFirst(t *testing.T) {
	n := newMetaNode(t)
	u1 := openInto(t, n, "app/u/1")
	lw, fm, q := chainOf(peersync.RuleLastWrite), chainOf(peersync.RuleFieldMerge), chainOf(peersync.RuleQueue)
	if err := n.store.SetResolution("app/**", q); err != nil {
		t.Fatal(err)
	}
	if err := n.store.SetResolution("app/u/*", lw); err != nil {
		t.Fatal(err)
	}
	if hashOf(u1) != lw.Hash() {
		t.Fatalf("app/u/1 should take app/u/* (more literal segments) over app/**")
	}
	if hashOf(n.data) != q.Hash() {
		t.Fatal("app/data should take app/**")
	}
	u2 := openInto(t, n, "app/u/2") // opened after the pattern was set
	if hashOf(u2) != lw.Hash() {
		t.Fatal("a namespace opened later should get its pattern's chain")
	}
	if err := n.store.SetResolution("app/u/2", fm); err != nil {
		t.Fatal(err)
	}
	if hashOf(u2) != fm.Hash() || hashOf(u1) != lw.Hash() {
		t.Fatal("a namespace's own chain overrides the pattern, for that namespace only")
	}
	// The pattern changes; the override holds.
	if err := n.store.SetResolution("app/u/*", q); err != nil {
		t.Fatal(err)
	}
	if hashOf(u1) != q.Hash() || hashOf(u2) != fm.Hash() {
		t.Fatalf("after changing the pattern: u1 %s u2 %s", hashOf(u1), hashOf(u2))
	}
	// The CLI's reader resolves the same way.
	if c, err := ReadResolutionChain(n.meta.Runtime, "app/u/7"); err != nil || c.Hash() != q.Hash() {
		t.Fatalf("ReadResolutionChain for an unopened namespace under a pattern: %v %v", c, err)
	}
}

// TestPatternHome: a home rule by pattern applies to matching namespaces; a namespace's own
// assignment overrides it.
func TestPatternHome(t *testing.T) {
	n := newMetaNode(t)
	m1 := openInto(t, n, "app/m/1")
	if _, err := n.store.AssignHome("app/m/*", "cloud-node", "tcp://cloud"); err != nil {
		t.Fatal(err)
	}
	if h, ok := m1.HomeOf(); !ok || h.Node != "cloud-node" {
		t.Fatalf("app/m/1 should be homed by the pattern: %+v", h)
	}
	if _, err := n.store.AssignHome("app/m/1", "phone-a", "tcp://phone"); err != nil {
		t.Fatal(err)
	}
	if h, ok := m1.HomeOf(); !ok || h.Node != "phone-a" {
		t.Fatalf("the namespace's own assignment should override: %+v", h)
	}
	m2 := openInto(t, n, "app/m/2")
	if h, ok := m2.HomeOf(); !ok || h.Node != "cloud-node" {
		t.Fatalf("app/m/2 should still follow the pattern: %+v", h)
	}
}

// TestPatternValidation: a wildcard must be a whole segment.
func TestPatternValidation(t *testing.T) {
	n := newMetaNode(t)
	for _, bad := range []string{"app/u*", "app//x", "/app", ""} {
		if err := n.store.SetResolution(bad, chainOf(peersync.RuleLastWrite)); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
}

// TestViewShowsPatternsAndOnlyGrantedNamespaces: the view a scoped peer gets holds every pattern
// and exactly the definitions of namespaces it may see - never another user's.
func TestViewShowsPatternsAndOnlyGrantedNamespaces(t *testing.T) {
	n := newMetaNode(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(n.store.SetResolution("app/u/*", chainOf(peersync.RuleLastWrite)))
	must(n.store.SetResolution("app/u/1", chainOf(peersync.RuleFieldMerge)))
	must(n.store.SetResolution("app/u/2", chainOf(peersync.RuleQueue)))
	view, err := n.store.View(func(ns string) bool { return ns == "app/u/1" })
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range view {
		var md metaDoc
		_ = json.Unmarshal([]byte(d.Body), &md)
		names = append(names, md.Namespace)
	}
	if len(view) != 2 || !containsStr(names, "app/u/*") || !containsStr(names, "app/u/1") || containsStr(names, "app/u/2") {
		t.Fatalf("view: %v", names)
	}
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
