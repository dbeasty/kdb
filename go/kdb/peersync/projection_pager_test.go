package peersync

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/wire"
)

// commitOps applies ops to s's storage and DAG as one commit on main.
func commitOps(t testing.TB, s side, ns string, ops []document.Op) document.Commit {
	t.Helper()
	for _, op := range ops {
		var err error
		switch o := op.(type) {
		case document.WriteOp:
			err = s.storage.PutDocument(ns, document.Document{ID: o.DocID, JSON: o.Patch})
		case document.DeleteOp:
			err = s.storage.DeleteDocument(ns, o.DocID)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	head, _ := s.dag.Head()
	parent, err := s.dag.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := s.storage.CommitTree(ns, parent.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	s.dag.PutDocumentTree(tree)
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	c, err := s.dag.AppendCommitDetached(document.Transaction{
		ID: txID, BaseVersion: head, Operations: ops, Timestamp: nextTimestamp(), AuthorNodeID: author,
	}, head, tree, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.dag.SetHead(mainBranch, c.Hash); err != nil {
		t.Fatal(err)
	}
	return c
}

// allPages pages one projection transfer with pager, returning every page.
func allPages(t *testing.T, pager func(after string) (wire.ProjectPageMessage, error)) []wire.ProjectPageMessage {
	t.Helper()
	var pages []wire.ProjectPageMessage
	after := ""
	for i := 0; ; i++ {
		if i > 10_000 {
			t.Fatal("projection transfer does not end")
		}
		p, err := pager(after)
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, p)
		if p.Done {
			return pages
		}
		after = p.Next
	}
}

// TestProjectionPagersMatchTheOriginals: the seeking reset pager and the tree-diff delta pager
// serve exactly the pages the original walk-and-sort and replay pagers served, over random
// histories of writes, rewrites and deletes, for a filter that documents move in and out of, and
// at several page sizes. Bodies are never rewritten to an earlier value: that is the one case
// where the two delta pagers legitimately differ (replay resends the document, a diff sees no
// change).
func TestProjectionPagersMatchTheOriginals(t *testing.T) {
	for _, f := range []string{
		"region = 'EU'",
		// id ranges, which the seeking pager turns into a seek and a stop
		"region = 'EU' AND kdb_id >= '4' AND kdb_id < 'b'",
		"kdb_id BETWEEN '2a' AND '9f-'",
		"'c' <= kdb_id",
		"kdb_id > '0000' AND kdb_id <= 'ffff' AND region = 'US'",
		"kdb_id = 'nothing'",
	} {
		t.Run(f, func(t *testing.T) { testProjectionPagersMatch(t, f) })
	}
}

func testProjectionPagersMatch(t *testing.T, filterSQL string) {
	const ns = "app/projection-pagers"
	filter, err := sql.ParseFilter(filterSQL)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(3))
	s := newSide(t, ns)
	env := sideEnv(s, ns)
	walker := env.Storage.(storage.TreeWalker)
	ids := make([]codec.UUID, 400)
	for i := range ids {
		// Seeded, so a failure reproduces: the pages depend on where ids fall.
		ids[i] = codec.UUID{MSB: rng.Int63() - rng.Int63(), LSB: rng.Int63() - rng.Int63()}
	}
	version := 0
	write := func(id codec.UUID) document.Op {
		version++
		region := []string{"EU", "US"}[rng.Intn(2)]
		return document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{"region":%q,"v":%d,"pad":"%0*d"}`, region, version, rng.Intn(200), 0)}
	}
	var commits []document.Commit
	for round := 0; round < 30; round++ {
		var ops []document.Op
		for k := 0; k < 1+rng.Intn(40); k++ {
			id := ids[rng.Intn(len(ids))]
			if rng.Intn(5) == 0 {
				ops = append(ops, document.DeleteOp{DocID: id})
			} else {
				ops = append(ops, write(id))
			}
		}
		commits = append(commits, commitOps(t, s, ns, ops))
	}
	canRead := func(id codec.UUID) bool { return id.LSB%7 != 0 } // some documents are not readable
	matches := func(d document.Document) bool {
		return sql.EvalPredicate(filter, d, schema.None(), nil) && canRead(d.ID)
	}
	pager := func(at document.Commit, after string, maxBytes int) projectionPager {
		return projectionPager{
			env: env, filter: filter, matches: matches, after: after, maxBytes: maxBytes,
			page: wire.ProjectPageMessage{Namespace: ns, AtHex: at.Hash.Hex(), Done: true},
		}
	}
	for _, maxBytes := range []int{1, 300, 4096, 1 << 20} {
		for _, at := range []document.Commit{commits[5], commits[29]} {
			seek := allPages(t, func(after string) (wire.ProjectPageMessage, error) {
				pp := pager(at, after, maxBytes)
				p, ok, err := pp.resetSeek(walker, at)
				if err == nil && !ok {
					t.Fatal("resetSeek fell back")
				}
				return p, err
			})
			scan := allPages(t, func(after string) (wire.ProjectPageMessage, error) {
				pp := pager(at, after, maxBytes)
				return pp.resetScan(walker, at)
			})
			if !reflect.DeepEqual(seek, scan) {
				t.Fatalf("reset at %d bytes: seek served %+v\nscan served %+v", maxBytes, seek, scan)
			}
		}
		for _, r := range [][2]int{{0, 1}, {3, 17}, {10, 29}, {29, 29}} {
			from, at := commits[r[0]], commits[r[1]]
			diff := allPages(t, func(after string) (wire.ProjectPageMessage, error) {
				pp := pager(at, after, maxBytes)
				p, ok, err := pp.deltaDiff(walker, from, at)
				if err == nil && !ok {
					t.Fatal("deltaDiff fell back")
				}
				return p, err
			})
			replay := allPages(t, func(after string) (wire.ProjectPageMessage, error) {
				pp := pager(at, after, maxBytes)
				return pp.deltaReplay(at.Hash, from)
			})
			// The two delta pagers send the same changes but may split them differently: replay
			// passes over ops the diff never sees (a document created and deleted inside the range),
			// so it can find a page full one op later and end with an empty page. Both are valid
			// transfers, so compare what they deliver, and that every page keeps to the budget.
			if dw, rw := deltaContent(diff), deltaContent(replay); !reflect.DeepEqual(dw, rw) {
				t.Fatalf("delta %v at %d bytes: diff delivered %+v\nreplay delivered %+v", r, maxBytes, dw, rw)
			}
			for _, pages := range [][]wire.ProjectPageMessage{diff, replay} {
				checkPageBudget(t, pages, maxBytes)
			}
		}
	}
}

// TestProjectionDeltaCostsTheChangeNotTheBacklog: a long-offline projection's catch-up over a
// rewrite-heavy backlog - 2,000 commits rewriting 5 documents - costs the diff pager the 5
// documents, while the replay pager walks every commit; and the diff pager's cost does not grow
// with the backlog.
func TestProjectionDeltaCostsTheChangeNotTheBacklog(t *testing.T) {
	const ns = "app/projection-cost"
	filter, err := sql.ParseFilter("TRUE")
	if err != nil {
		t.Fatal(err)
	}
	s := newSide(t, ns)
	env := sideEnv(s, ns)
	walker := env.Storage.(storage.TreeWalker)
	ids := make([]codec.UUID, 5)
	for i := range ids {
		ids[i], _ = codec.RandomUUID()
	}
	from := commitOps(t, s, ns, []document.Op{document.WriteOp{DocID: ids[0], Patch: `{"v":0}`}})
	var mid, at document.Commit
	for i := 1; i <= 2000; i++ {
		at = commitOps(t, s, ns, []document.Op{document.WriteOp{DocID: ids[i%5], Patch: fmt.Sprintf(`{"v":%d}`, i)}})
		if i == 200 {
			mid = at
		}
	}
	matches := func(d document.Document) bool { return sql.EvalPredicate(filter, d, schema.None(), nil) }
	pager := func(to document.Commit) projectionPager {
		return projectionPager{env: env, filter: filter, matches: matches, maxBytes: 1 << 20,
			page: wire.ProjectPageMessage{Namespace: ns, AtHex: to.Hash.Hex(), Done: true}}
	}
	diffAllocs := func(to document.Commit) float64 {
		return testing.AllocsPerRun(5, func() {
			pp := pager(to)
			if _, ok, err := pp.deltaDiff(walker, from, to); err != nil || !ok {
				t.Fatal(ok, err)
			}
		})
	}
	replayAllocs := testing.AllocsPerRun(5, func() {
		pp := pager(at)
		if _, err := pp.deltaReplay(at.Hash, from); err != nil {
			t.Fatal(err)
		}
	})
	short, long := diffAllocs(mid), diffAllocs(at)
	t.Logf("allocations: diff over 200 commits %.0f, over 2,000 %.0f; replay over 2,000 %.0f", short, long, replayAllocs)
	if long > 2*short {
		t.Errorf("the diff pager's cost grew with the backlog: %.0f allocations over 200 commits, %.0f over 2,000", short, long)
	}
	if long*10 > replayAllocs {
		t.Errorf("the diff pager (%.0f allocations) is not well under replay (%.0f)", long, replayAllocs)
	}
}

// TestProjectionIDRangeSeeks: a filter bounded to an id range reads that range, not the namespace.
func TestProjectionIDRangeSeeks(t *testing.T) {
	r := filterIDRange(mustFilter(t, "kdb_id >= '8' AND kdb_id < '9' AND region = 'EU'"))
	if r.lo != "8" || r.hi != "9" || r.hiInclusive {
		t.Fatalf("range %+v", r)
	}
	start, ok := r.seekFrom()
	if !ok || start.String() != "7fffffff-ffff-ffff-ffff-ffffffffffff" {
		t.Fatalf("seek from %v %v", start, ok)
	}
	if !r.beyond("90000000-0000-0000-0000-000000000000") || r.beyond("8fffffff-ffff-ffff-ffff-ffffffffffff") {
		t.Fatal("beyond")
	}
	if got := filterIDRange(mustFilter(t, "kdb_id >= 'X'")); got.lo != "" {
		t.Fatalf("a non-id literal must not bound the walk: %+v", got)
	}
	if _, ok := filterIDRange(mustFilter(t, "kdb_id >= '0'")).seekFrom(); ok {
		t.Fatal("the smallest bound starts at the beginning")
	}
}

func mustFilter(t *testing.T, s string) sql.Expr {
	t.Helper()
	e, err := sql.ParseFilter(s)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// deltaContent is what a delta transfer delivers, page boundaries aside.
func deltaContent(pages []wire.ProjectPageMessage) (out struct {
	Writes  []wire.SnapshotDoc
	Deletes []string
}) {
	for _, p := range pages {
		out.Writes = append(out.Writes, p.Writes...)
		out.Deletes = append(out.Deletes, p.Deletes...)
	}
	return out
}

// checkPageBudget: a page goes over maxBytes by at most the one entry that crossed it, and only
// the last page is Done.
func checkPageBudget(t *testing.T, pages []wire.ProjectPageMessage, maxBytes int) {
	t.Helper()
	for i, p := range pages {
		size, largest := 0, 0
		for _, w := range p.Writes {
			size += len(w.Body)
			largest = max(largest, len(w.Body))
		}
		size += 40 * len(p.Deletes)
		if len(p.Deletes) > 0 {
			largest = max(largest, 40)
		}
		if size > maxBytes+largest {
			t.Fatalf("page %d holds %d bytes, over the %d budget by more than one entry", i, size, maxBytes)
		}
		if p.Done != (i == len(pages)-1) {
			t.Fatalf("page %d of %d: Done=%v", i, len(pages), p.Done)
		}
	}
}
