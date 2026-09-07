package embed_test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

const retentionDocID = "11111111-1111-4111-8111-111111111111"

// smallBudgetRuntime opens a runtime whose retention budgets are small
// enough that writing a few megabytes of versions is guaranteed to evict
// history, so these tests exercise the load-back paths rather than
// accidentally finding everything still in memory.
func smallBudgetRuntime(tb testing.TB, root string) *embed.EmbeddedKdbRuntime {
	tb.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		tb.Fatal(err)
	}
	return rt
}

// writeVersions rewrites one document versions times, each version
// distinguishable by its "rev" field, and returns the commit hash and the
// exact JSON written for each.
func writeVersions(tb testing.TB, rt *embed.EmbeddedKdbRuntime, versions int) ([]codec.Hash, []string) {
	tb.Helper()
	hashes := make([]codec.Hash, 0, versions)
	texts := make([]string, 0, versions)
	for i := 0; i < versions; i++ {
		b, err := json.Marshal(map[string]any{
			"id":   retentionDocID,
			"rev":  i,
			"blob": strings.Repeat(string(rune('a'+i%26)), 50_000),
		})
		if err != nil {
			tb.Fatal(err)
		}
		res, err := embed.PutJSONDocument(rt, "bench/matches", string(b))
		if err != nil {
			tb.Fatal(err)
		}
		hashes = append(hashes, res.Commit)
		texts = append(texts, string(b))
	}
	return hashes, texts
}

// TestHistoricalReadSurvivesVersionEviction is the guarantee that makes
// bounding the in-memory version store acceptable: a version evicted under
// the budget is still readable at the commit that wrote it, because it is
// re-read from the delta log on demand.
func TestHistoricalReadSurvivesVersionEviction(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	commits, texts := writeVersions(t, rt, 120)
	defer rt.Close()

	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}

	// Read the oldest versions - the ones certain to have been evicted.
	for _, i := range []int{0, 1, 7, 40} {
		commit, err := rt.DAG.GetCommitOrThrow(commits[i])
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		got, err := rt.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("version %d: document not found at its own commit - an evicted version was not loaded back", i)
		}
		if got.JSON != texts[i] {
			t.Fatalf("version %d: read back the wrong content\n got %.80s\nwant %.80s", i, got.JSON, texts[i])
		}
	}
	// Without this the test would still pass against an engine that never
	// evicts anything - it would just be reading memory and proving
	// nothing about the path it exists to cover.
	if e, ok := rt.Storage.(*engine.ServerEngine); ok && e.ColdDocumentLoads() == 0 {
		t.Fatal("no version was loaded back from the delta log, so eviction never happened")
	}
}

// TestHistoricalReadSurvivesReopen is the same guarantee across a restart,
// where every version arrives through replay rather than through the write
// path, and nothing was ever resident to begin with.
func TestHistoricalReadSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	commits, texts := writeVersions(t, rt, 120)
	rt.Close()

	reopened := smallBudgetRuntime(t, root)
	defer reopened.Close()

	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 5, 60, 119} {
		commit, err := reopened.DAG.GetCommitOrThrow(commits[i])
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		got, err := reopened.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
		if err != nil {
			t.Fatalf("version %d: %v", i, err)
		}
		if got == nil || got.JSON != texts[i] {
			t.Fatalf("version %d: wrong content after reopen", i)
		}
	}
	if e, ok := reopened.Storage.(*engine.ServerEngine); ok && e.ColdDocumentLoads() == 0 {
		t.Fatal("no version was loaded back from the delta log, so eviction never happened")
	}
}

// TestWalkWithOperationsLoadsEvictedOperations covers the DAG half. It
// also pins down the trap that motivates having two walk methods at all:
// plain Walk hands back commits whose operations the budget dropped, so
// anything that reads operations must go through WalkWithOperations.
func TestWalkWithOperationsLoadsEvictedOperations(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	_, texts := writeVersions(t, rt, 120)
	rt.Close()

	reopened := smallBudgetRuntime(t, root)
	defer reopened.Close()

	d, ok := underlyingDag(reopened)
	if !ok {
		t.Skip("runtime DAG is not an *InMemoryCommitDag")
	}
	head, err := reopened.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}

	plain := reopened.DAG.Walk(head, nil, 1<<20)
	stripped := 0
	for _, e := range plain {
		if full, ok := e.(dag.FullEntry); ok && len(full.Commit.Operations) == 0 {
			stripped++
		}
	}
	if stripped == 0 {
		t.Fatal("no commit had its operations evicted, so this test is not exercising the load-back path")
	}

	hydrated, err := d.WalkWithOperations(head, nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	written := make(map[string]bool, len(texts))
	for _, text := range texts {
		written[text] = true
	}
	seen := 0
	for _, e := range hydrated {
		full, ok := e.(dag.FullEntry)
		if !ok {
			continue
		}
		for _, op := range full.Commit.Operations {
			w, isWrite := op.(document.WriteOp)
			if !isWrite {
				continue
			}
			if !written[w.Patch] {
				t.Fatalf("commit %s carries a patch that was never written", full.Commit.Hash.Hex())
			}
			seen++
		}
	}
	if seen < stripped {
		t.Fatalf("WalkWithOperations returned %d write operations but %d commits had been stripped", seen, stripped)
	}
}

// TestOpenMemoryIsBoundedByLiveData is the property the whole change
// exists for: how much memory a namespace costs to hold open must follow
// its live data, not the number of times that data has been rewritten.
//
// Asserted as a ratio between two histories of the same live document
// rather than as an absolute megabyte figure, so it does not become a
// tuning tripwire: before this change the same comparison was linear in
// the rewrite count (16x the history cost 16x the memory).
func TestOpenMemoryIsBoundedByLiveData(t *testing.T) {
	measure := func(rewrites int) float64 {
		root := t.TempDir()
		rt := smallBudgetRuntime(t, root)
		writeVersions(t, rt, rewrites)
		rt.Close()

		reopened := smallBudgetRuntime(t, root)
		defer reopened.Close()
		return liveHeapMB(reopened)
	}
	short := measure(40)
	long := measure(320)
	if short <= 0 {
		t.Skip("could not measure heap")
	}
	if ratio := long / short; ratio > 3 {
		t.Fatalf("8x the history cost %.1fx the memory (%.2f MB -> %.2f MB); "+
			"retention is tracking history again rather than live data", ratio, short, long)
	}
}

// liveHeapMB is the heap still held with rt reachable, measured after a
// collection so it reports retention rather than whatever the allocator
// had not yet swept.
func liveHeapMB(rt *embed.EmbeddedKdbRuntime) float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	runtime.KeepAlive(rt)
	return float64(m.HeapAlloc) / (1024 * 1024)
}

func underlyingDag(rt *embed.EmbeddedKdbRuntime) (*dag.InMemoryCommitDag, bool) {
	switch d := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		return d, true
	case interface{ Delegate() *dag.InMemoryCommitDag }:
		return d.Delegate(), true
	default:
		_ = fmt.Sprint(d)
		return nil, false
	}
}

// TestHeadKeepsItsOperationsWhenLargerThanTheBudget guards the ordering
// that makes operation eviction safe on the write path.
//
// A commit is stored before the branch head advances to it, so during that
// window nothing marks it as a retention root. A commit whose own
// operations exceed the budget would therefore evict itself on the way in,
// and the head snapshot published straight afterwards - the one the
// lock-free read path and the commit-stream notifier both read - would
// describe a commit that wrote nothing.
func TestHeadKeepsItsOperationsWhenLargerThanTheBudget(t *testing.T) {
	root := t.TempDir()
	rt := smallBudgetRuntime(t, root)
	defer rt.Close()

	// One document several times the 256KB commit-operations budget, so
	// the commit carrying it cannot fit and would be evicted immediately.
	body, err := json.Marshal(map[string]any{
		"id":   retentionDocID,
		"blob": strings.Repeat("z", 2_000_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, "bench/matches", string(body)); err != nil {
		t.Fatal(err)
	}

	_, head, ok, err := rt.DAG.HeadCommit()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no head commit")
	}
	if len(head.Operations) == 0 {
		t.Fatal("the head commit was published with no operations - it evicted its own before becoming the branch head")
	}
	w, isWrite := head.Operations[0].(document.WriteOp)
	if !isWrite || w.Patch != string(body) {
		t.Fatal("the head commit's operation is not the document that was written")
	}
}
