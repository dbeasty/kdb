package peersync

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// keepLowerBody is a symmetric Choose: it keeps whichever side's body sorts first and forks the
// other. Symmetric because it looks only at the two values, never at which one is "local" - so
// both nodes settle the conflict the same way, as a chain rule must.
func keepLowerBody(t *testing.T) func(codec.UUID, document.Op, document.Op) (Settlement, bool) {
	t.Helper()
	return func(id codec.UUID, l, r document.Op) (Settlement, bool) {
		lb, rb := opBody(l), opBody(r)
		if lb == nil || rb == nil {
			return Settlement{}, false
		}
		keep, losing := lb, rb
		if *rb < *lb {
			keep, losing = rb, lb
		}
		f, ok := ForkOf(id, losing, codec.Hash{})
		if !ok {
			return Settlement{}, false
		}
		return Settlement{Body: keep, Fork: f}, true
	}
}

func TestForkIDIsAFunctionOfWhatItKeeps(t *testing.T) {
	doc := newUUID(t)
	if ForkID(doc, `{"a":1}`) != ForkID(doc, `{"a":1}`) {
		t.Fatal("the same losing content must fork to the same id, or a recurring conflict forks forever")
	}
	if ForkID(doc, `{"a":1}`) == ForkID(doc, `{"a":2}`) {
		t.Fatal("different content must fork to different ids")
	}
	if ForkID(doc, `{"a":1}`) == ForkID(newUUID(t), `{"a":1}`) {
		t.Fatal("the same content split from two documents must not collide")
	}
}

func TestForkOfAddsTheBackReference(t *testing.T) {
	doc := newUUID(t)
	body := `{"text":"theirs"}`
	f, ok := ForkOf(doc, &body, codec.Hash{})
	if !ok {
		t.Fatal("a plain object should fork")
	}
	if !strings.Contains(f.Body, `"conflictOf":{"id":"`+doc.String()+`"}`) {
		t.Fatalf("no back-reference: %s", f.Body)
	}
	if !strings.Contains(f.Body, `"text":"theirs"`) {
		t.Fatalf("the losing content was not kept: %s", f.Body)
	}
}

func TestForkOfRefusesWhatItCannotKeep(t *testing.T) {
	doc := newUUID(t)
	if _, ok := ForkOf(doc, nil, codec.Hash{}); ok {
		t.Fatal("a deleted side has no content to fork")
	}
	for _, body := range []string{`[1,2]`, `"a string"`, `{not json`} {
		b := body
		if _, ok := ForkOf(doc, &b, codec.Hash{}); ok {
			t.Fatalf("%s cannot carry a back-reference, so it must not fork", body)
		}
	}
}

// TestForkKeepsBothSidesInOneMerge: the merge writes the kept side to the document and the other
// to a new one, and both nodes build the same merge - the extra document included.
func TestForkKeepsBothSidesInOneMerge(t *testing.T) {
	f := newChainFixture(t, "app/fork-both", `{"text":"draft"}`, `{"text":"mine"}`, `{"text":"theirs"}`)
	out, kept := f.resolveBothWays(t, ResolutionOptions{Choose: keepLowerBody(t)})
	if out.Kind != OutcomeMerged {
		t.Fatalf("expected a merge, got %v", out.Kind)
	}
	if kept != `{"text":"mine"}` {
		t.Fatalf("the kept side should be the lower body, got %s", kept)
	}
	forkID := ForkID(f.doc, `{"text":"theirs"}`)
	for name, s := range map[string]side{"local": f.local, "remote": f.remote} {
		doc, err := s.storage.GetDocument(f.ns, forkID, out.MergeCommit.DocumentTreeHash)
		if err != nil || doc == nil {
			t.Fatalf("%s has no fork document: %v %v", name, doc, err)
		}
		if !strings.Contains(doc.JSON, `"text":"theirs"`) || !strings.Contains(doc.JSON, f.doc.String()) {
			t.Fatalf("%s fork holds %s", name, doc.JSON)
		}
	}
}

// TestForkDoesNotOverwriteWhatIsAlreadyThere: once a fork id exists on either head, the merge
// leaves it alone. Otherwise a second merge over the same conflict would put the original losing
// content back over whatever has happened to the fork since - an edit, or a deletion.
func TestForkDoesNotOverwriteWhatIsAlreadyThere(t *testing.T) {
	f := newChainFixture(t, "app/fork-existing", `{"text":"draft"}`, `{"text":"mine"}`, `{"text":"theirs"}`)
	forkID := ForkID(f.doc, `{"text":"theirs"}`)
	// Someone has already worked on the fork copy: it exists at the local head with its own
	// content. Written on both sides so the two heads still differ only over f.doc.
	edited := `{"text":"theirs, since edited"}`
	localC := writeDocBy(t, f.local, f.ns, f.localC.Hash, forkID, edited, f.localNode)
	mergeInto(t, f.remote, f.ns, localC)
	out, err := ResolveDivergence(f.local.dag, f.local.storage, f.ns, localC.Hash, f.remoteC.Hash, ResolutionOptions{Choose: keepLowerBody(t)})
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != OutcomeMerged {
		t.Fatalf("expected a merge, got %v", out.Kind)
	}
	doc, err := f.local.storage.GetDocument(f.ns, forkID, out.MergeCommit.DocumentTreeHash)
	if err != nil || doc == nil {
		t.Fatalf("the fork document vanished: %v %v", doc, err)
	}
	if doc.JSON != edited {
		t.Fatalf("the merge overwrote the edited fork with %s", doc.JSON)
	}
}

// TestSettlementCanDeleteTheDocument: sometimes the answer to two conflicting writes is that
// neither belongs there. Taking a side cannot say that when neither side is a delete.
func TestSettlementCanDeleteTheDocument(t *testing.T) {
	f := newChainFixture(t, "app/fork-delete", `{"v":0}`, `{"v":"local"}`, `{"v":"remote"}`)
	drop := func(codec.UUID, document.Op, document.Op) (Settlement, bool) { return Settlement{}, true }
	out, err := ResolveDivergence(f.local.dag, f.local.storage, f.ns, f.localC.Hash, f.remoteC.Hash, ResolutionOptions{Choose: drop})
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != OutcomeMerged {
		t.Fatalf("expected a merge, got %v", out.Kind)
	}
	doc, err := f.local.storage.GetDocument(f.ns, f.doc, out.MergeCommit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if doc != nil {
		t.Fatalf("the document survived the merge: %s", doc.JSON)
	}
}
