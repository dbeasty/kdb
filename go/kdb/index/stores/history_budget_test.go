package stores_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
)

// historyRegistry is a registry of n HASH indexes over fields f0..f(n-1), each document carrying
// all of them, and an n-commit write history: every commit rewrites one document's every field.
func historyRegistry(t *testing.T, n int) (*index.Registry, *dag.InMemoryCommitDag, codec.Hash) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag("ns/hist")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	r := index.NewRegistry("ns/hist", d, stores.NewFactory(d, stores.Options{}))
	for i := 0; i < n; i++ {
		id, _ := codec.RandomUUID()
		f := fmt.Sprintf("f%d", i)
		desc := index.Descriptor{IndexID: id, NamespaceID: "ns/hist", FieldName: f, Fields: []string{f}, Type: index.IndexTypeHash,
			Options: map[string]string{index.OptionIndexName: "i_" + f}}
		if _, err := r.Add(desc); err != nil {
			t.Fatal(err)
		}
	}
	return r, d, head
}

func historyBytes(r *index.Registry) (n int64) {
	for _, s := range r.Stores() {
		if hs, ok := s.(interface{ HistoryBytes() int64 }); ok {
			n += hs.HistoryBytes()
		}
	}
	return n
}

// writeCommits appends commits, each rewriting one of docs documents' every field, and returns
// the commit hashes in order.
func writeCommits(t *testing.T, r *index.Registry, d *dag.InMemoryCommitDag, head codec.Hash, docs []codec.UUID, fields, commits int, round int) []codec.Hash {
	t.Helper()
	var out []codec.Hash
	for c := 0; c < commits; c++ {
		head = appendCommit(t, d, head)
		body := "{"
		for f := 0; f < fields; f++ {
			if f > 0 {
				body += ","
			}
			body += fmt.Sprintf(`"f%d":"v%d-%d"`, f, round, c%17)
		}
		body += "}"
		if _, err := r.ApplyWrite(docs[c%len(docs)], head, body); err != nil {
			t.Fatal(err)
		}
		// What the server's commit path does (RegistryIndexProvider.applyCommit, repairHeads):
		// the rebuild check is what moves every store's head to the new commit.
		r.MarkCommit(head)
		r.NeedingRebuild()
		out = append(out, head)
	}
	return out
}

// With a budget, the indexes' history stays within it together - one shared window of commits -
// reads inside the window are still answered by every index, reads past it fall back
// (ErrHistoryUnavailable), and the window settles instead of ratcheting down to its floor.
func TestRegistryHistoryStaysWithinItsBudget(t *testing.T) {
	const indexes, fields = 12, 12
	r, d, head := historyRegistry(t, indexes)
	budget := int64(2 << 20)
	r.SetHistoryBudget(func() int64 { return budget })
	docs := make([]codec.UUID, 50)
	for i := range docs {
		docs[i], _ = codec.RandomUUID()
	}
	commits := writeCommits(t, r, d, head, docs, fields, 20_000, 1)

	if used := historyBytes(r); used > budget*5/4 {
		t.Fatalf("history holds %d bytes against a %d-byte budget", used, budget)
	}
	w := r.HistoryWindow()
	if w <= 256 || w >= index.DefaultHistoryEvents {
		t.Fatalf("window %d commits: expected it to settle between the floor and the default", w)
	}
	// A commit inside the window: every index answers it.
	inside := commits[len(commits)-w/2]
	for _, s := range r.Stores() {
		if _, err := s.Lookup(index.StringKey{Value: "v1-0"}, &inside); err != nil {
			t.Fatalf("%s at a commit %d back, inside the %d-commit window: %v", s.Descriptor().IndexName(), w/2, w, err)
		}
	}
	// One long past it: no index holds it any more, and says so.
	past := commits[len(commits)-3*w]
	for _, s := range r.Stores() {
		if _, err := s.Lookup(index.StringKey{Value: "v1-0"}, &past); !errors.Is(err, index.ErrHistoryUnavailable) {
			t.Fatalf("%s at a commit %d back, past the window: got %v, want ErrHistoryUnavailable", s.Descriptor().IndexName(), 3*w, err)
		}
	}

	// Steady state: more of the same writes do not keep shrinking the window.
	writeCommits(t, r, d, commits[len(commits)-1], docs, fields, 5_000, 2)
	if w2 := r.HistoryWindow(); w2 < w*3/4 {
		t.Fatalf("window went from %d to %d commits under the same load", w, w2)
	}
}

// Without a budget each index keeps its own window, as before.
func TestRegistryHistoryWithoutABudgetIsPerIndex(t *testing.T) {
	r, d, head := historyRegistry(t, 3)
	docs := []codec.UUID{}
	for i := 0; i < 10; i++ {
		id, _ := codec.RandomUUID()
		docs = append(docs, id)
	}
	writeCommits(t, r, d, head, docs, 3, 2_000, 1)
	if w := r.HistoryWindow(); w != 0 {
		t.Fatalf("window %d with no budget set", w)
	}
}

// Deferred, the check only runs when its owner asks (the server: after the write gate is
// released). Until then history is not trimmed; once it runs it is back within the budget.
func TestRegistryHistoryCheckCanBeDeferred(t *testing.T) {
	r, d, head := historyRegistry(t, 12)
	r.DeferHistoryBounding()
	budget := int64(1 << 20)
	r.SetHistoryBudget(func() int64 { return budget })
	docs := make([]codec.UUID, 50)
	for i := range docs {
		docs[i], _ = codec.RandomUUID()
	}
	commits := writeCommits(t, r, d, head, docs, 12, 8_000, 1)
	if used := historyBytes(r); used <= budget {
		t.Fatalf("history %d bytes within the %d budget before any check ran", used, budget)
	}
	for i := 0; i < 8; i++ { // each check shrinks the window by at most what one pass trims
		r.BoundHistoryIfDue()
		commits = writeCommits(t, r, d, commits[len(commits)-1], docs, 12, 32, 2)
	}
	r.BoundHistoryIfDue()
	if used := historyBytes(r); used > budget*5/4 {
		t.Fatalf("history %d bytes against a %d budget after the deferred checks", used, budget)
	}
}
