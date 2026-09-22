package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/script"
)

const keepLocalProc = `function main(c) { return { take: "local" }; }`

// TestProcedureReplicates: a procedure defined on A reaches B through the metadata namespace,
// byte for byte - so both nodes compute the same source hash, which is what a chain pins.
func TestProcedureReplicates(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "keepLocal", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	if got := a.data.ProcedureHash("keepLocal"); got != script.SourceHash(keepLocalProc) {
		t.Fatalf("A did not apply its own procedure: %q", got)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the procedure", func() bool {
		return b.data.ProcedureHash("keepLocal") == script.SourceHash(keepLocalProc)
	})
	if src, _ := b.data.ProcedureSource("keepLocal"); src != keepLocalProc {
		t.Fatalf("B holds different source: %q", src)
	}
}

// TestProcedureRedefinitionReplicates: a changed procedure replaces the old one everywhere, and
// the hash changes with it - a node still on the old revision must be able to tell.
func TestProcedureRedefinitionReplicates(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	const v2 = `function main(c) { return { take: "remote" }; }`
	if err := a.store.SetProcedure("app/data", "pick", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the first revision", func() bool { return b.data.ProcedureHash("pick") != "" })
	first := b.data.ProcedureHash("pick")

	if err := a.store.SetProcedure("app/data", "pick", v2); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the second revision", func() bool { return b.data.ProcedureHash("pick") != first })
	if src, _ := b.data.ProcedureSource("pick"); src != v2 {
		t.Fatalf("B holds %q", src)
	}
}

// TestProcedureDropReplicates: dropping is a tombstone, not a deleted document - B has to learn
// that the procedure is gone, which a missing document could not tell it apart from one it never
// received.
func TestProcedureDropReplicates(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "keepLocal", keepLocalProc); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to apply the procedure", func() bool { return b.data.ProcedureHash("keepLocal") != "" })

	if err := a.store.DropProcedure("app/data", "keepLocal"); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.data.ProcedureSource("keepLocal"); ok {
		t.Fatal("A still holds the dropped procedure")
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to drop the procedure", func() bool { return b.data.ProcedureHash("keepLocal") == "" })
}

// TestBrokenProcedureIsRefusedAtDefinition: source that will not compile never becomes a
// definition. Refusing it here means no node ever has to decide what to do with a procedure that
// cannot run in the middle of a merge.
func TestBrokenProcedureIsRefusedAtDefinition(t *testing.T) {
	a := newMetaNode(t)
	err := a.store.SetProcedure("app/data", "broken", `function main( {`)
	if !errors.Is(err, script.ErrCompile) {
		t.Fatalf("got %v", err)
	}
	if _, ok := a.data.ProcedureSource("broken"); ok {
		t.Fatal("the broken procedure was stored anyway")
	}
}

// TestProcedureNeedsAName: the document id is derived from the name, so an unnamed procedure
// would collide with every other unnamed one in the namespace.
func TestProcedureNeedsAName(t *testing.T) {
	a := newMetaNode(t)
	if err := a.store.SetProcedure("app/data", "", keepLocalProc); err == nil || !strings.Contains(err.Error(), "needs a name") {
		t.Fatalf("got %v", err)
	}
}

// TestProcedureNamesAreListed: an operator can see what a namespace defines.
func TestProcedureNamesAreListed(t *testing.T) {
	a := newMetaNode(t)
	for _, n := range []string{"zeta", "alpha"} {
		if err := a.store.SetProcedure("app/data", n, keepLocalProc); err != nil {
			t.Fatal(err)
		}
	}
	got := a.data.ProcedureNames()
	if len(got) != 2 || got[0] != "alpha" || got[1] != "zeta" {
		t.Fatalf("got %v", got)
	}
}
