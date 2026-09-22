package script

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

func localConflict(t *testing.T) transaction.DocumentConflict {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return transaction.DocumentConflict{
		DocID:       id,
		BaseDoc:     &document.Document{ID: id, JSON: `{"qty":1}`},
		ExistingDoc: &document.Document{ID: id, JSON: `{"qty":5}`},
		IncomingDoc: &document.Document{ID: id, JSON: `{"qty":9}`},
	}
}

func TestLocalResolverTakesASide(t *testing.T) {
	r := ConflictResolver{Source: `function main(c) { return { take: c.local.qty > c.remote.qty ? "local" : "remote" }; }`}
	got, err := r.Resolve(localConflict(t))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JSON != `{"qty":9}` {
		t.Fatalf("got %v", got)
	}
}

func TestLocalResolverReturnsABuiltDocument(t *testing.T) {
	r := ConflictResolver{Source: `function main(c) { return { doc: { qty: c.local.qty + c.remote.qty } }; }`}
	c := localConflict(t)
	got, err := r.Resolve(c)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JSON != `{"qty":14}` || got.ID != c.DocID {
		t.Fatalf("got %+v", got)
	}
}

// TestLocalResolverDeferLeavesTheConflictReported: nil with no error is what the engine reads as
// "report it", which is what defer means here.
func TestLocalResolverDeferLeavesTheConflictReported(t *testing.T) {
	r := ConflictResolver{Source: `function main(c) { return { defer: true }; }`}
	got, err := r.Resolve(localConflict(t))
	if got != nil || err != nil {
		t.Fatalf("got %v %v", got, err)
	}
}

// TestLocalResolverRefusesWhatALocalCommitCannotDo: fork and delete need a merge commit to put
// the extra document or the deletion in.
func TestLocalResolverRefusesWhatALocalCommitCannotDo(t *testing.T) {
	for _, src := range []string{
		`function main(c) { return { fork: { keep: "local" } }; }`,
		`function main(c) { return { delete: true }; }`,
	} {
		r := ConflictResolver{Source: src}
		if _, err := r.Resolve(localConflict(t)); !errors.Is(err, ErrOutput) {
			t.Fatalf("%s: got %v", src, err)
		}
	}
}

func TestLocalResolverRefusesToTakeADeletedSide(t *testing.T) {
	c := localConflict(t)
	c.IncomingDoc = nil
	r := ConflictResolver{Source: `function main(c) { return { take: "remote" }; }`}
	if _, err := r.Resolve(c); !errors.Is(err, ErrOutput) {
		t.Fatalf("got %v", err)
	}
}

func TestLocalResolverPassesTheFailureOn(t *testing.T) {
	r := ConflictResolver{Source: `function main(c) { throw new Error("nope"); }`}
	if _, err := r.Resolve(localConflict(t)); !errors.Is(err, ErrRuntime) {
		t.Fatalf("got %v", err)
	}
}

func TestLocalResolverSatisfiesTheEngineInterface(t *testing.T) {
	var _ transaction.ConflictResolver = ConflictResolver{Source: `function main() { return { defer: true }; }`}
}
