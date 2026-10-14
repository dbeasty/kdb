package dag

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

type mapSetStore struct {
	mu sync.Mutex
	m  map[codec.Hash][]byte
}

func (s *mapSetStore) WriteAttachmentSet(h codec.Hash, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[h] = append([]byte(nil), b...)
	return nil
}

func (s *mapSetStore) ReadAttachmentSet(h codec.Hash) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[h]
	return b, ok, nil
}

// versionOp puts version i of doc's one attachment, under a name of its own so every version's
// set is distinct.
func versionOp(doc codec.UUID, i int) document.AttachmentPutOp {
	return document.AttachmentPutOp{DocID: doc, AttachmentID: attID, Name: fmt.Sprintf("v%d", i),
		Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h(byte(i)), Size: int64(i), Mime: "x/y"}
}

// A DAG whose set pool is backed by a store keeps it within its budget across a long attachment
// history, and every commit's table still reads its own versions - rebuilt after the cache is
// dropped too, from sets read back and checked against the roots the commits recorded.
func TestDagSetPoolBoundedAcrossHistory(t *testing.T) {
	const budget = 16 << 10
	b := newDagBuilder(t)
	store := &mapSetStore{m: map[codec.Hash][]byte{}}
	if err := b.dag.SetAttachmentSetStore(store, budget); err != nil {
		t.Fatal(err)
	}
	docs := []codec.UUID{{MSB: 1}, {MSB: 2}, {MSB: 3}, {MSB: 4}}
	type point struct {
		commit codec.Hash
		table  document.AttachmentTable // as read then: its sets are evicted long before the end
		want   map[codec.UUID]string    // doc -> attachment name at the commit
	}
	var points []point
	current := map[codec.UUID]string{}
	parent := b.root
	pool := b.dag.AttachmentSetPool()
	for i := 0; i < 1200; i++ {
		doc := docs[i%len(docs)]
		c := b.attachCommit("v", parent, versionOp(doc, i))
		parent = c.Hash
		current[doc] = fmt.Sprintf("v%d", i)
		if st := pool.Stats(); st.Bytes > 2*budget {
			t.Fatalf("after %d commits the pool holds %d bytes (budget %d)", i+1, st.Bytes, budget)
		}
		if i%97 == 0 {
			want := map[codec.UUID]string{}
			for d, n := range current {
				want[d] = n
			}
			points = append(points, point{c.Hash, tableAt(t, b.dag, c.Hash), want})
		}
	}
	if st := pool.Stats(); st.Spills == 0 {
		t.Fatalf("no set was spilled: %+v", st)
	}
	check := func(held bool) {
		t.Helper()
		for _, p := range points {
			tab := p.table
			if !held {
				tab = tableAt(t, b.dag, p.commit)
			}
			for d, name := range p.want {
				if a, ok := aget(t, tab, d, attID); !ok || a.Name != name {
					t.Fatalf("commit %s doc %s: %+v, want %s", p.commit.Hex()[:8], d, a, name)
				}
			}
		}
	}
	// Tables held since: their sets come back from the store.
	check(true)
	if pool.Stats().Loads == 0 {
		t.Fatal("no set was read back from the store")
	}
	// Asked for again, from the cache or rebuilt from history.
	check(false)
	b.dag.ForgetAttachmentTablesExcept(nil)
	check(false)
}

// When the store gives back something else than a commit's table names, reading that commit fails
// - from the table, or from a rebuild that cannot reproduce the recorded root - and never answers
// with other attachments.
func TestDagSetStoreDisagreementIsAnError(t *testing.T) {
	b := newDagBuilder(t)
	store := &mapSetStore{m: map[codec.Hash][]byte{}}
	if err := b.dag.SetAttachmentSetStore(store, 1); err != nil { // evict all but the newest
		t.Fatal(err)
	}
	doc := codec.UUID{MSB: 9}
	old := b.attachCommit("old", b.root, versionOp(doc, 1))
	b.attachCommit("new", old.Hash, versionOp(doc, 2))
	oldSet, _ := tableAt(t, b.dag, old.Hash).SetHash(doc)
	if _, ok := store.m[oldSet]; !ok {
		t.Fatal("the old set was not spilled")
	}
	forged := document.AppendAttachmentSet(nil, document.AttachmentSet{{ID: attID, Name: "forged",
		AttachmentRef: document.AttachmentRef{Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h(7)}}})
	store.m[oldSet] = forged
	if _, _, err := tableAt(t, b.dag, old.Hash).Get(doc, attID); !errors.Is(err, document.ErrAttachmentSetCorrupt) {
		t.Fatalf("reading a commit whose set was forged: %v, want ErrAttachmentSetCorrupt", err)
	}
	delete(store.m, oldSet)
	if _, _, err := tableAt(t, b.dag, old.Hash).Get(doc, attID); !errors.Is(err, document.ErrAttachmentSetMissing) {
		t.Fatalf("reading a commit whose set is gone: %v, want ErrAttachmentSetMissing", err)
	}
	// A table rebuilt from history makes its sets again, from the operations, and needs no store.
	b.dag.ForgetAttachmentTablesExcept(nil)
	if a, ok := aget(t, tableAt(t, b.dag, old.Hash), doc, attID); !ok || a.Name != "v1" {
		t.Fatalf("rebuilt from history: %+v", a)
	}
}

// Seeded tables and transactions' tables join the DAG's pool, so what is derived from them is
// bounded too.
func TestSeededTablesJoinTheDagPool(t *testing.T) {
	b := newDagBuilder(t)
	store := &mapSetStore{m: map[codec.Hash][]byte{}}
	if err := b.dag.SetAttachmentSetStore(store, 1<<20); err != nil {
		t.Fatal(err)
	}
	seed, _ := document.EmptyAttachmentTable().Put(codec.UUID{MSB: 1}, document.Attachment{ID: attID, Name: "s",
		AttachmentRef: document.AttachmentRef{Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h(1)}})
	if err := b.dag.SeedAttachmentTable(seed); err != nil {
		t.Fatal(err)
	}
	for _, p := range b.dag.PinnedAttachmentTables() {
		if p.Pool() != b.dag.AttachmentSetPool() {
			t.Fatal("a seeded table kept a pool of its own")
		}
	}
}

// CommitsAbove is the set difference of the heads' and the stops' ancestries, with or without
// generation numbers, on a history with branches and merges.
func TestCommitsAboveIsTheAncestryDifference(t *testing.T) {
	for _, pruning := range []bool{false, true} {
		t.Run(fmt.Sprintf("pruning=%v", pruning), func(t *testing.T) {
			b := newDagBuilder(t)
			b.dag.SetGraphSettings(GraphSettings{AncestryPruning: pruning})
			r := rand.New(rand.NewSource(3))
			all := []codec.Hash{b.root}
			for i := 0; i < 120; i++ {
				p := all[len(all)-1-r.Intn(min(len(all), 6))]
				if i%7 == 6 {
					q := all[r.Intn(len(all))]
					if q != p && !b.dag.IsAncestor(q, p) && !b.dag.IsAncestor(p, q) {
						all = append(all, b.merge(fmt.Sprintf("m%d", i), p, q))
						continue
					}
				}
				all = append(all, b.commit(fmt.Sprintf("c%d", i), p))
			}
			pick := func(n int) []codec.Hash {
				out := make([]codec.Hash, n)
				for i := range out {
					out[i] = all[r.Intn(len(all))]
				}
				return out
			}
			for trial := 0; trial < 40; trial++ {
				heads, stops := pick(1+r.Intn(3)), pick(r.Intn(3))
				if trial == 0 {
					stops = nil
				}
				want := map[codec.Hash]bool{}
				for _, h := range heads {
					for a := range b.dag.AncestorSet(h) {
						want[a] = true
					}
				}
				for _, s := range stops {
					for a := range b.dag.AncestorSet(s) {
						delete(want, a)
					}
				}
				got := b.dag.CommitsAbove(heads, stops)
				gotSet := map[codec.Hash]bool{}
				for _, h := range got {
					if gotSet[h] {
						t.Fatalf("CommitsAbove returned %s twice", b.name(h))
					}
					gotSet[h] = true
				}
				if len(gotSet) != len(want) {
					t.Fatalf("trial %d: %d commits, want %d (%v vs %v)", trial, len(gotSet), len(want), names(b, got), keys(b, want))
				}
				for h := range want {
					if !gotSet[h] {
						t.Fatalf("trial %d: missing %s", trial, b.name(h))
					}
				}
			}
		})
	}
}

func names(b *dagBuilder, hs []codec.Hash) []string {
	out := b.names_(hs)
	sort.Strings(out)
	return out
}

func keys(b *dagBuilder, m map[codec.Hash]bool) []string {
	var hs []codec.Hash
	for h := range m {
		hs = append(hs, h)
	}
	return names(b, hs)
}

// The history floor changes when history is cut below - a stub - and not when commits are added
// above.
func TestHistoryFloorChangesOnlyBelow(t *testing.T) {
	b := newDagBuilder(t)
	a := b.commit("a", b.root)
	c := b.commit("c", a)
	floor := b.dag.HistoryFloor()
	b.commit("d", c)
	if b.dag.HistoryFloor() != floor {
		t.Fatal("a commit above changed the floor")
	}
	if _, err := b.dag.StubCommit(a, "archive"); err != nil {
		t.Fatal(err)
	}
	if b.dag.HistoryFloor() == floor {
		t.Fatal("stubbing a commit left the floor as it was")
	}
}
