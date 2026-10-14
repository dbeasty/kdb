package peersync

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/wire"
)

// seedBulk writes n documents of about bodyLen bytes to ns in one commit and moves main to it.
func seedBulk(t testing.TB, s side, ns string, n, bodyLen int) document.Commit {
	t.Helper()
	pad := strings.Repeat("x", bodyLen)
	ops := make([]document.Op, 0, n)
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(`{"i":%d,"pad":%q}`, i, pad)
		if err := s.storage.PutDocument(ns, document.Document{ID: id, JSON: body}); err != nil {
			t.Fatal(err)
		}
		ops = append(ops, document.WriteOp{DocID: id, Patch: body})
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
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	c, err := s.dag.AppendCommitDetached(document.Transaction{
		ID: txID, BaseVersion: head, Operations: ops, Timestamp: nextTimestamp(), AuthorNodeID: author,
	}, head, tree, nil, "bulk")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.dag.SetHead(mainBranch, c.Hash); err != nil {
		t.Fatal(err)
	}
	return c
}

func sideEnv(s side, ns string) IngestEnv {
	return IngestEnv{DAG: s.dag, Storage: s.storage, NamespaceID: ns, ApplyToStorage: true}
}

// TestSnapshotPagesMatchScan: the seeking pager serves exactly the pages the original
// walk-and-sort pager served - every page of a full transfer, at several sizes and page sizes,
// and for arbitrary cursors, including ones that are not a canonical id (which fall back to the
// original algorithm).
func TestSnapshotPagesMatchScan(t *testing.T) {
	sizes := []int{0, 1, 7, 300, 2_500}
	if testing.Short() {
		sizes = sizes[:4] // 2,500 runs nightly, or without -short
	}
	for _, n := range sizes {
		ns := fmt.Sprintf("app/snap-pages-%d", n)
		s := newSide(t, ns)
		var at codec.Hash
		if n > 0 {
			at = seedBulk(t, s, ns, n, 40).Hash
		} else {
			at, _ = s.dag.Head()
		}
		env := sideEnv(s, ns)
		commit, _ := env.DAG.GetCommitOrThrow(at)
		state, err := stateAt(env, env.Storage.(storage.TreeWalker), commit)
		if err != nil {
			t.Fatal(err)
		}
		if state.tree == nil {
			t.Fatal("the memory adapter resolves trees: the seeking pager should be in use")
		}
		for _, pageBytes := range []int{1, 700, 9_000, 1 << 20} {
			after, pages, docs := "", 0, 0
			for {
				got, err := snapshotPage(env, at, after, pageBytes)
				if err != nil {
					t.Fatal(err)
				}
				want, err := snapshotPageScan(env, state, commit, after, pageBytes)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("n=%d page=%d after=%q: seek and scan pages differ\nseek: %d docs done=%v next=%q total=%d\nscan: %d docs done=%v next=%q total=%d",
						n, pageBytes, after, len(got.Docs), got.Done, got.Next, got.Total, len(want.Docs), want.Done, want.Next, want.Total)
				}
				pages++
				docs += len(got.Docs)
				if got.Done {
					break
				}
				after = got.Next
			}
			if docs != n {
				t.Fatalf("n=%d page=%d: a full transfer served %d documents in %d pages", n, pageBytes, docs, pages)
			}
		}
		// Cursors a peer could send: inside the tree, between ids, past either end, and ones that
		// are not canonical ids at all.
		cursors := []string{"", "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff", "zzz", "8", "ABCDEF01-0000-0000-0000-000000000000"}
		for i := 0; i < 20; i++ {
			id, _ := codec.RandomUUID()
			cursors = append(cursors, id.String())
		}
		if n > 0 {
			first, _ := snapshotPage(env, at, "", 1<<20)
			for i := 0; i < len(first.Docs); i += max(1, len(first.Docs)/10) {
				cursors = append(cursors, first.Docs[i].DocID, strings.ToUpper(first.Docs[i].DocID))
			}
		}
		for _, c := range cursors {
			got, err := snapshotPage(env, at, c, 2_000)
			if err != nil {
				t.Fatal(err)
			}
			want, err := snapshotPageScan(env, state, commit, c, 2_000)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("n=%d cursor %q: seek and scan pages differ", n, c)
			}
		}
	}
}

// pageAllocBytes is the bytes allocated serving one page of 20 documents from the middle of the
// namespace, averaged over several pages.
func pageAllocBytes(t *testing.T, env IngestEnv, at codec.Hash, page func(after string) (wire.SnapshotPageMessage, error), cursor string) uint64 {
	t.Helper()
	const runs = 10
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		p, err := page(cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Docs) == 0 {
			t.Fatal("empty page")
		}
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / runs
}

// TestSnapshotPageCostIsThePage: what the host allocates to serve one page does not grow with the
// namespace. The original pager is measured alongside to show the test can tell the difference:
// it collected and sorted every id after the cursor, per page.
func TestSnapshotPageCostIsThePage(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation measurement over a 40,000-document namespace; runs nightly, or without -short")
	}
	type cost struct{ seek, scan uint64 }
	var costs []cost
	for _, n := range []int{4_000, 40_000} {
		ns := fmt.Sprintf("app/snap-cost-%d", n)
		s := newSide(t, ns)
		at := seedBulk(t, s, ns, n, 20).Hash
		env := sideEnv(s, ns)
		commit, _ := env.DAG.GetCommitOrThrow(at)
		state, err := stateAt(env, env.Storage.(storage.TreeWalker), commit)
		if err != nil {
			t.Fatal(err)
		}
		// A cursor a third of the way in: the scan still has two thirds of the namespace to sort.
		all, _ := snapshotPage(env, at, "", 1<<30)
		cursor := all.Docs[n/3].DocID
		const pageBytes = 20 * 40
		seek := pageAllocBytes(t, env, at, func(c string) (wire.SnapshotPageMessage, error) { return snapshotPage(env, at, c, pageBytes) }, cursor)
		scan := pageAllocBytes(t, env, at, func(c string) (wire.SnapshotPageMessage, error) {
			return snapshotPageScan(env, state, commit, c, pageBytes)
		}, cursor)
		costs = append(costs, cost{seek, scan})
		t.Logf("n=%d: a page allocates %d B seeking, %d B scanning", n, seek, scan)
	}
	if costs[1].seek > 2*costs[0].seek+4096 {
		t.Fatalf("seeking: a page allocated %d B at 4k documents and %d B at 40k - it grows with the namespace", costs[0].seek, costs[1].seek)
	}
	if costs[1].scan < 5*costs[0].scan {
		t.Fatalf("the scan reference did not grow with the namespace (%d B, %d B): the measurement is not sensitive", costs[0].scan, costs[1].scan)
	}
}

// gateNode is a LocalNode whose write gate is a mutex, recording when it is held and what
// Advanced was told.
type gateNode struct {
	mu        sync.Mutex
	held      atomic.Bool
	exclusive atomic.Int32
	advanced  func(AdvanceStep) error
}

func (g *gateNode) Exclusive(fn func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.exclusive.Add(1)
	g.held.Store(true)
	defer g.held.Store(false)
	return fn()
}

func (g *gateNode) Advanced(step AdvanceStep) error {
	if g.advanced != nil {
		return g.advanced(step)
	}
	return nil
}

// TestInstallSnapshotTransfersOutsideTheGate: the pages are fetched with the write gate free, the
// gate is taken once, to install, and Advanced still sees every installed document (streamed).
func TestInstallSnapshotTransfersOutsideTheGate(t *testing.T) {
	ns := "app/snap-gate"
	remote := newSide(t, ns)
	root := seedBulk(t, remote, ns, 500, 100)
	remoteEnv := sideEnv(remote, ns)
	local := newSide(t, ns)
	node := &gateNode{}
	applied := map[codec.UUID]string{}
	node.advanced = func(step AdvanceStep) error {
		if len(step.Applied) != 0 {
			t.Error("a snapshot's applied set should stream (AppliedPages), not be held in Applied")
		}
		return step.EachApplied(func(ops []document.Op) error {
			for _, op := range ops {
				w := op.(document.WriteOp)
				applied[w.DocID] = w.Patch
			}
			return nil
		})
	}
	env := sideEnv(local, ns)
	env.Node = node
	env.StagingDir = t.TempDir()
	pages, pagesUnderGate := 0, 0
	installed, err := InstallSnapshot(env, func(after string) (wire.SnapshotPageMessage, error) {
		pages++
		if node.held.Load() {
			pagesUnderGate++
		}
		return snapshotPage(remoteEnv, root.Hash, after, 3_000)
	})
	if err != nil {
		t.Fatal(err)
	}
	if installed.Hash != root.Hash || mustHead(t, local) != root.Hash {
		t.Fatal("the snapshot was not installed at the remote head")
	}
	if pages < 10 || pagesUnderGate != 0 || node.exclusive.Load() != 1 {
		t.Fatalf("%d pages, %d fetched under the gate, gate taken %d times: want many pages, none under the gate, one gate hold", pages, pagesUnderGate, node.exclusive.Load())
	}
	if len(applied) != 500 {
		t.Fatalf("Advanced saw %d documents, want 500", len(applied))
	}
	for _, op := range root.Operations {
		w := op.(document.WriteOp)
		if applied[w.DocID] != w.Patch {
			t.Fatalf("Advanced saw the wrong body for %s", w.DocID)
		}
		got, err := local.storage.GetDocument(ns, w.DocID, root.DocumentTreeHash)
		if err != nil || got == nil || got.JSON != w.Patch {
			t.Fatalf("document %s not installed: %v", w.DocID, err)
		}
	}
	if entries, _ := readDirNames(env.StagingDir); len(entries) != 0 {
		t.Fatalf("the staging file was left behind: %v", entries)
	}
}

// TestInstallSnapshotYieldsToALocalWrite: a local write made while the pages are in flight is
// not blocked by the transfer - it completes - and it wins: the install then finds the namespace
// has history of its own and refuses with ErrNotEmpty, leaving the local write and nothing of the
// snapshot. Exactly the outcome had the local write landed before the bootstrap began.
func TestInstallSnapshotYieldsToALocalWrite(t *testing.T) {
	ns := "app/snap-local-write"
	remote := newSide(t, ns)
	root := seedBulk(t, remote, ns, 300, 100)
	remoteEnv := sideEnv(remote, ns)
	local := newSide(t, ns)
	node := &gateNode{}
	env := sideEnv(local, ns)
	env.Node = node
	env.StagingDir = t.TempDir()
	localDoc := newUUID(t)
	var localCommit document.Commit
	pages := 0
	_, err := InstallSnapshot(env, func(after string) (wire.SnapshotPageMessage, error) {
		pages++
		if pages == 3 {
			// A local writer, on another goroutine, taking the gate as every writer does. It
			// would deadlock here if the install held the gate across the transfer.
			done := make(chan error, 1)
			go func() {
				done <- node.Exclusive(func() error {
					localCommit = writeAtHead(t, local, ns, localDoc, `{"local":true}`)
					return nil
				})
			}()
			if err := <-done; err != nil {
				return wire.SnapshotPageMessage{}, err
			}
		}
		return snapshotPage(remoteEnv, root.Hash, after, 3_000)
	})
	if !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("expected ErrNotEmpty once a local write landed mid-transfer, got %v", err)
	}
	if pages < 4 {
		t.Fatalf("only %d pages: the local write did not land mid-transfer", pages)
	}
	if mustHead(t, local) != localCommit.Hash {
		t.Fatal("main is not the local write")
	}
	n := 0
	if err := local.storage.(storage.TreeWalker).WalkTree(ns, localCommit.DocumentTreeHash, func(id codec.UUID, _ codec.Hash) bool {
		n++
		if id != localDoc {
			t.Errorf("a snapshot document %s is in the local tree", id)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("local tree holds %d documents, want the local write alone", n)
	}
	if local.dag.HasCommit(root.Hash) {
		t.Fatal("the refused snapshot's root is in the DAG")
	}
}

// treeOnlyStorage is a storage adapter that builds trees but keeps no bodies: what a namespace's
// live state costs in the tree alone, so a test can see what an install holds beyond it.
type treeOnlyStorage struct {
	*mem.InMemoryStorageAdapter
	mu      sync.Mutex
	live    document.DocumentTree
	pending map[codec.UUID]*codec.Hash // nil: delete
}

func newTreeOnlyStorage() *treeOnlyStorage {
	return &treeOnlyStorage{InMemoryStorageAdapter: mem.NewInMemoryStorageAdapter(), live: document.EmptyDocumentTree(), pending: map[codec.UUID]*codec.Hash{}}
}

func (s *treeOnlyStorage) PutDocument(_ string, doc document.Document) error {
	h, err := doc.ContentHash()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[doc.ID] = &h
	return nil
}

func (s *treeOnlyStorage) DeleteDocument(_ string, id codec.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[id] = nil
	return nil
}

func (s *treeOnlyStorage) DiscardPending(string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.pending)
	return nil
}

func (s *treeOnlyStorage) CommitTree(string, codec.Hash) (document.DocumentTree, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, h := range s.pending {
		var err error
		if h == nil {
			s.live, err = s.live.Without(id)
		} else {
			s.live, err = s.live.With(id, *h)
		}
		if err != nil {
			return document.DocumentTree{}, err
		}
	}
	clear(s.pending)
	return s.live, nil
}

func liveHeap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestInstallSnapshotMemoryIsBounded: an install holds a page, not the namespace. Measured with
// forced GCs: the live heap while pages are in flight stays within a few pages whatever the
// namespace's size, and what is live when Advanced runs - after every document is applied - is
// the tree alone, not the bodies (the install used to hold every body twice there, as its
// applied set and as the ids it wrote).
func TestInstallSnapshotMemoryIsBounded(t *testing.T) {
	const bodyLen, pageBytes = 2_000, 64 << 10
	for _, n := range []int{2_000, 8_000} {
		ns := fmt.Sprintf("app/snap-mem-%d", n)
		remote := newSide(t, ns)
		root := seedBulk(t, remote, ns, n, bodyLen)
		remoteEnv := sideEnv(remote, ns)
		local := newSide(t, ns)
		local.storage = newTreeOnlyStorage()
		node := &gateNode{}
		env := sideEnv(local, ns)
		env.Node = node
		env.StagingDir = t.TempDir()
		var atAdvance uint64
		node.advanced = func(AdvanceStep) error { atAdvance = liveHeap(); return nil }
		base := liveHeap()
		var peak uint64
		pages := 0
		if _, err := InstallSnapshot(env, func(after string) (wire.SnapshotPageMessage, error) {
			pages++
			if pages%4 == 0 {
				peak = max(peak, liveHeap())
			}
			return snapshotPage(remoteEnv, root.Hash, after, pageBytes)
		}); err != nil {
			t.Fatal(err)
		}
		// The remote side and its commit are part of base: keep them so, or the collector frees
		// their bodies mid-install and the difference comes out negative.
		runtime.KeepAlive(root)
		runtime.KeepAlive(remoteEnv)
		transfer := int64(peak) - int64(base)
		installed := int64(atAdvance) - int64(base)
		bodies := int64(n * bodyLen)
		t.Logf("n=%d (%d MB of bodies): live heap +%d KB at peak of transfer, +%d KB at Advanced (%d B/doc)",
			n, bodies>>20, transfer>>10, installed>>10, installed/int64(n))
		if transfer > 2<<20 {
			t.Fatalf("n=%d: transfer held %d KB live: more than a few pages", n, transfer>>10)
		}
		// The tree costs a few hundred bytes a document; a body is 2,000.
		if installed > int64(n)*600 {
			t.Fatalf("n=%d: %d B/doc live once installed: bodies are being held", n, installed/int64(n))
		}
	}
}

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out, nil
}
