package peersync

import (
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/mem"
)

// streamingStorage streams a tree's bodies the way a log would hand them over: in no particular
// order, some more than once, and some not at all (skip) - those the host must read the ordinary
// way.
type streamingStorage struct {
	*mem.InMemoryStorageAdapter
	skip func(codec.UUID) bool
}

func (s *streamingStorage) StreamTreeBodies(ns string, tree document.DocumentTree, fn func(codec.UUID, string) error) error {
	var docs []document.Document
	var err error
	tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		var d *document.Document
		if d, err = s.GetDocument(ns, id, tree.TreeHash); err != nil {
			return false
		}
		if !s.skip(id) {
			docs = append(docs, *d)
		}
		return true
	})
	if err != nil {
		return err
	}
	docs = append(docs, docs[:len(docs)/5]...) // written twice in the log
	rand.New(rand.NewSource(1)).Shuffle(len(docs), func(i, j int) { docs[i], docs[j] = docs[j], docs[i] })
	for _, d := range docs {
		if err := fn(d.ID, d.JSON); err != nil {
			return err
		}
	}
	return nil
}

// TestSnapshotServedFromSortedCopy: a large snapshot's pages come from the sorted copy of its
// bodies - merged across many runs, with duplicates collapsed and what the copy lacks read from
// storage - and are exactly the pages point reads serve. Paging forward reuses one cursor rather
// than re-merging the runs from the start for every page.
func TestSnapshotServedFromSortedCopy(t *testing.T) {
	defer func(docs, run int) { snapshotSortMinDocs, snapshotRunBytes = docs, run }(snapshotSortMinDocs, snapshotRunBytes)
	snapshotSortMinDocs, snapshotRunBytes = 1, 4_000 // ~10 documents a run

	ns := "app/snap-sorted"
	remote := newTestNamespaces(t, ns)
	seedHistory(t, remote, ns, 300)
	r := remote.side(ns)
	r.storage = &streamingStorage{InMemoryStorageAdapter: r.storage.(*mem.InMemoryStorageAdapter), skip: func(id codec.UUID) bool { return id.String()[3]%7 == 0 }}
	remote.sides[ns] = r
	env, err := remote.Env(ns, false)
	if err != nil {
		t.Fatal(err)
	}
	head := mustHead(t, r)
	commit, err := r.dag.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	tree, ok, err := r.storage.(storage.TreeResolver).TreeAt(commit.DocumentTreeHash)
	if err != nil || !ok {
		t.Fatal(err)
	}
	src := sortedBodiesFor(env, head, tree)
	if src == nil {
		t.Fatal("no sorted copy for a snapshot above the threshold")
	}
	defer src.close()
	<-src.done
	if !src.ready || len(src.runs) < 10 {
		t.Fatalf("copy ready=%v with %d runs, want many", src.ready, len(src.runs))
	}

	// What point reads serve, for comparison: the same pages from storage with no copy.
	plain := env
	plain.Storage = r.storage.(*streamingStorage).InMemoryStorageAdapter
	for _, pageBytes := range []int{1, 900, 5_000, 1 << 20} {
		after, pages := "", 0
		for {
			got, err := snapshotPage(env, head, after, pageBytes)
			if err != nil {
				t.Fatal(err)
			}
			want, err := snapshotPage(plain, head, after, pageBytes)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Docs) != len(want.Docs) || got.Done != want.Done || got.Next != want.Next {
				t.Fatalf("page %d at %q: %d docs done=%v next=%q, want %d docs done=%v next=%q",
					pages, after, len(got.Docs), got.Done, got.Next, len(want.Docs), want.Done, want.Next)
			}
			for i := range got.Docs {
				if got.Docs[i] != want.Docs[i] {
					t.Fatalf("page %d doc %d: %+v, want %+v", pages, i, got.Docs[i], want.Docs[i])
				}
			}
			pages++
			if got.Done {
				break
			}
			after = got.Next
		}
		if pageBytes == 1 && pages < 100 {
			t.Fatalf("only %d pages at one document a page", pages)
		}
	}
	// Four page sizes, each paged front to back: a cursor each, not one per page.
	if src.cursors > 4 {
		t.Fatalf("%d cursors made for 4 forward passes", src.cursors)
	}
}

// TestSnapshotInstallFromSortedCopy: end to end, a node bootstraps from a host serving its pages
// from the copy, and ends with the host's state.
func TestSnapshotInstallFromSortedCopy(t *testing.T) {
	defer func(docs, run int) { snapshotSortMinDocs, snapshotRunBytes = docs, run }(snapshotSortMinDocs, snapshotRunBytes)
	snapshotSortMinDocs, snapshotRunBytes = 1, 8_000
	// The host's copy is built in the background and can outlive the sync; let it finish before
	// the test's directories go.
	defer waitSnapshotBuilds()

	ns := "app/snap-sorted-install"
	remote := newTestNamespaces(t, ns)
	seedHistory(t, remote, ns, 200)
	r := remote.side(ns)
	r.storage = &streamingStorage{InMemoryStorageAdapter: r.storage.(*mem.InMemoryStorageAdapter), skip: func(id codec.UUID) bool { return id.String()[5]%5 == 0 }}
	remote.sides[ns] = r
	local := newTestNamespaces(t, ns)
	v2Hub(t, "hub-snap-sorted", newHost(remote), nil)
	// The first page starts the copy and is served without it; the rest race its build.
	res := syncV2(t, "hub-snap-sorted", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true, PageBytes: 500})
	if res.Namespaces[0].Snapshot == "" {
		t.Fatal("no snapshot installed")
	}
	if mustHead(t, local.side(ns)) != mustHead(t, r) {
		t.Fatal("local main is not the host's head")
	}
}

// waitSnapshotBuilds waits for every sorted copy being built to finish.
func waitSnapshotBuilds() {
	snapshotSources.Lock()
	var pending []*sortedBodies
	for _, src := range snapshotSources.m {
		pending = append(pending, src)
	}
	snapshotSources.Unlock()
	for _, src := range pending {
		<-src.done
	}
}
