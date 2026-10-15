package transaction_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// A merge re-applies the merged branch's field-level changes over the primary, not its whole
// stored documents: a logged write is the whole document (operationsAsStored), and replaying that
// as a patch overwrote every field the primary changed concurrently. The expected documents are
// the same literals as Kotlin's MergeFieldChangesTest, so both engines must produce them byte for byte.
func TestMergeAppliesTheBranchesFieldChanges(t *testing.T) {
	type side []document.Op
	write := func(id codec.UUID, patch string) document.Op { return document.WriteOp{DocID: id, Patch: patch} }
	replace := func(id codec.UUID, body string) document.Op {
		return document.WriteOp{DocID: id, Patch: body, Replace: true}
	}
	cases := []struct {
		name           string
		base           string // "" = the document does not exist at the ancestor
		primary, other func(codec.UUID) side
		want           string // "" = the document is absent after the merge
	}{
		{"disjoint fields keep both sides", `{"a":1,"b":1}`,
			func(id codec.UUID) side { return side{write(id, `{"b":3}`)} },
			func(id codec.UUID) side { return side{write(id, `{"a":2}`)} },
			`{"a":2,"b":3}`},
		{"a key the branch removed goes, the primary's change stays", `{"a":1,"b":1,"c":1}`,
			func(id codec.UUID) side { return side{write(id, `{"c":2}`)} },
			func(id codec.UUID) side { return side{replace(id, `{"a":1,"c":1}`)} },
			`{"a":1,"c":2}`},
		{"the same field on both sides takes the branch's value", `{"a":1}`,
			func(id codec.UUID) side { return side{write(id, `{"a":3}`)} },
			func(id codec.UUID) side { return side{write(id, `{"a":2}`)} },
			`{"a":2}`},
		{"a branch edit to a document the primary deleted keeps only the edit", `{"a":1,"b":1}`,
			func(id codec.UUID) side { return side{document.DeleteOp{DocID: id}} },
			func(id codec.UUID) side { return side{write(id, `{"a":2}`)} },
			`{"a":2}`},
		{"a document the branch created arrives whole", "",
			func(id codec.UUID) side { return nil },
			func(id codec.UUID) side { return side{write(id, `{"n":1,"m":"x"}`)} },
			`{"n":1,"m":"x"}`},
		{"a document the branch deleted is gone", `{"a":1}`,
			func(id codec.UUID) side { return side{write(id, `{"b":2}`)} },
			func(id codec.UUID) side { return side{document.DeleteOp{DocID: id}} },
			""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns := "app/merge-fields"
			f := newMergeFixture(t, ns)
			id, _ := codec.RandomUUID()
			base := f.base
			if tc.base != "" {
				base = f.commitOn(t, base, write(id, tc.base))
			}
			left, right := base, base
			if ops := tc.primary(id); len(ops) > 0 {
				left = f.commitOn(t, base, ops...)
			}
			if ops := tc.other(id); len(ops) > 0 {
				right = f.commitOn(t, base, ops...)
			}
			res, err := f.engine.Merge(left, right, f.dag, f.store, schema.None(), "merge")
			if err != nil {
				t.Fatal(err)
			}
			success, ok := res.(transaction.ResultSuccess)
			if !ok {
				t.Fatalf("merge: %#v", res)
			}
			got, err := f.store.GetDocument(ns, id, success.Commit.DocumentTreeHash)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.want == "" && got != nil:
				t.Fatalf("document should be absent, got %s", got.JSON)
			case tc.want != "" && (got == nil || got.JSON != tc.want):
				t.Fatalf("merged document = %v, want %s", got, tc.want)
			}
		})
	}
}
