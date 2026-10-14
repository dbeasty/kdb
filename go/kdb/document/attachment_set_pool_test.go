package document

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// memSetStore is an AttachmentSetStore in a map, counting its traffic.
type memSetStore struct {
	mu            sync.Mutex
	m             map[codec.Hash][]byte
	reads, writes int
	fail          error
}

func newMemSetStore() *memSetStore { return &memSetStore{m: map[codec.Hash][]byte{}} }

func (s *memSetStore) WriteAttachmentSet(h codec.Hash, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.writes++
	s.m[h] = append([]byte(nil), b...)
	return nil
}

func (s *memSetStore) ReadAttachmentSet(h codec.Hash) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	b, ok := s.m[h]
	return b, ok, nil
}

// version is the attachment a document has at version i of a long history.
func version(i int) Attachment {
	return Attachment{ID: codec.UUID{MSB: 1, LSB: 1}, Name: fmt.Sprintf("v%05d.bin", i),
		AttachmentRef: AttachmentRef{Scheme: AttachmentHashSchemeChunked1, ContentHash: attachHash(byte(i)), Size: int64(i), Mime: "x/y"}}
}

// However many versions a table goes through, a store-backed pool keeps about its budget in
// memory; every version stays readable, the evicted ones read back from the store.
func TestSetPoolStaysWithinItsBudget(t *testing.T) {
	const budget = 8 << 10
	store := newMemSetStore()
	pool := NewAttachmentSetPool(store, budget)
	docs := []codec.UUID{{MSB: 1}, {MSB: 2}, {MSB: 3}}
	tab := pool.Empty()
	type kept struct {
		tab  AttachmentTable
		want map[codec.UUID]AttachmentSet
	}
	var history []kept
	var err error
	maxSets := 0
	for i := 0; i < 3000; i++ {
		doc := docs[i%len(docs)]
		if tab, err = tab.Put(doc, version(i)); err != nil {
			t.Fatal(err)
		}
		st := pool.Stats()
		if st.Bytes > budget+setSize(AttachmentSet{version(i)}) {
			t.Fatalf("after %d versions the pool holds %d bytes, over its %d budget", i+1, st.Bytes, budget)
		}
		maxSets = max(maxSets, st.Sets)
		if i%250 == 0 {
			want := map[codec.UUID]AttachmentSet{}
			for _, d := range docs {
				s, err := tab.List(d)
				if err != nil {
					t.Fatal(err)
				}
				want[d] = s
			}
			history = append(history, kept{tab, want})
		}
	}
	st := pool.Stats()
	if st.Evictions == 0 || st.Spills == 0 {
		t.Fatalf("nothing was evicted: %+v", st)
	}
	if maxSets > budget/int(setSize(AttachmentSet{version(0)})) {
		t.Fatalf("the pool held up to %d sets, more than its budget allows", maxSets)
	}
	// Every kept version reads back as it was, most of them from the store.
	loadsBefore := st.Loads
	for _, k := range history {
		for d, want := range k.want {
			got, err := k.tab.List(d)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("doc %s read back %v, want %v", d, got, want)
			}
		}
	}
	if pool.Stats().Loads == loadsBefore {
		t.Fatal("no old version was read back from the store")
	}
	// A set read back is not written out again when evicted once more: only the ones never
	// written yet (in memory now) may be.
	writes, resident := store.writes, pool.Stats().Sets
	for _, k := range history {
		if err := k.tab.Walk(func(codec.UUID, Attachment) bool { return true }); err != nil {
			t.Fatal(err)
		}
	}
	if store.writes-writes > resident {
		t.Fatalf("re-reading old versions wrote %d sets again", store.writes-writes)
	}
}

// Without a store, nothing is evicted: there would be nowhere to read it back from.
func TestSetPoolWithoutAStoreKeepsEverything(t *testing.T) {
	pool := NewAttachmentSetPool(nil, 1)
	tab := pool.Empty()
	var err error
	for i := 0; i < 100; i++ {
		if tab, err = tab.Put(codec.UUID{MSB: 1}, version(i)); err != nil {
			t.Fatal(err)
		}
	}
	if st := pool.Stats(); st.Sets != 100 || st.Evictions != 0 {
		t.Fatalf("pool without a store: %+v", st)
	}
}

// A set the store does not have, or has wrong, is an error naming it - never an empty or a
// different set.
func TestSetPoolFailsLoudlyWhenTheStoreDisagrees(t *testing.T) {
	doc := codec.UUID{MSB: 7}
	setup := func(t *testing.T) (*memSetStore, AttachmentTable, codec.Hash) {
		store := newMemSetStore()
		pool := NewAttachmentSetPool(store, 1) // evict everything but the newest
		old, err := pool.Empty().Put(doc, att(1, "a", 1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := old.Put(doc, att(1, "b", 2)); err != nil {
			t.Fatal(err)
		}
		h, _ := old.SetHash(doc)
		if pool.Contains(h) {
			t.Fatal("the old set was not evicted")
		}
		return store, old, h
	}
	t.Run("missing", func(t *testing.T) {
		store, old, h := setup(t)
		delete(store.m, h)
		if _, err := old.List(doc); !errors.Is(err, ErrAttachmentSetMissing) {
			t.Fatalf("List: %v, want ErrAttachmentSetMissing", err)
		}
		if _, _, err := old.Get(doc, att(1, "", 0).ID); !errors.Is(err, ErrAttachmentSetMissing) {
			t.Fatalf("Get: %v", err)
		}
		if err := old.Walk(func(codec.UUID, Attachment) bool { return true }); !errors.Is(err, ErrAttachmentSetMissing) {
			t.Fatalf("Walk: %v", err)
		}
		if _, err := old.Put(doc, att(2, "c", 3)); !errors.Is(err, ErrAttachmentSetMissing) {
			t.Fatalf("Put on a table whose set is missing: %v", err)
		}
	})
	t.Run("another set", func(t *testing.T) {
		store, old, h := setup(t)
		store.m[h] = AppendAttachmentSet(nil, AttachmentSet{att(1, "forged", 9)})
		if _, err := old.List(doc); !errors.Is(err, ErrAttachmentSetCorrupt) {
			t.Fatalf("List: %v, want ErrAttachmentSetCorrupt", err)
		}
	})
	t.Run("garbage", func(t *testing.T) {
		store, old, h := setup(t)
		store.m[h] = []byte{1, 5, 0}
		if _, err := old.List(doc); !errors.Is(err, ErrAttachmentSetCorrupt) {
			t.Fatalf("List: %v, want ErrAttachmentSetCorrupt", err)
		}
	})
	t.Run("store write fails", func(t *testing.T) {
		store := newMemSetStore()
		store.fail = errors.New("disk full")
		pool := NewAttachmentSetPool(store, 1)
		tab, err := pool.Empty().Put(doc, att(1, "a", 1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tab.Put(doc, att(1, "b", 2)); err == nil {
			t.Fatal("a put whose eviction could not be written succeeded")
		}
		if got, err := tab.List(doc); err != nil || len(got) != 1 || got[0].Name != "a" {
			t.Fatalf("the unwritten set was lost: %v %v", got, err)
		}
	})
}

func TestAttachmentSetEncodingRoundTrips(t *testing.T) {
	set := AttachmentSet{att(1, "a.png", 1), att(2, "é ☃", 2), att(3, "", 3)}
	set[2].Name, set[2].Mime = "c", ""
	got, err := ParseAttachmentSet(AppendAttachmentSet(nil, set))
	if err != nil || !reflect.DeepEqual(got, set) {
		t.Fatalf("round trip: %v %v", got, err)
	}
	if got, err := ParseAttachmentSet(AppendAttachmentSet(nil, nil)); err != nil || len(got) != 0 {
		t.Fatalf("empty set: %v %v", got, err)
	}
	for name, bad := range map[string]AttachmentSet{
		"out of order":  {att(2, "a", 1), att(1, "b", 2)},
		"repeated name": {att(1, "a", 1), att(2, "a", 2)},
	} {
		if _, err := ParseAttachmentSet(AppendAttachmentSet(nil, bad)); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	enc := AppendAttachmentSet(nil, set)
	if _, err := ParseAttachmentSet(enc[:len(enc)-1]); err == nil {
		t.Fatal("truncated set accepted")
	}
}

// A table built in one pool moves to another with the same root, and reads the same.
func TestAttachmentTableInPool(t *testing.T) {
	doc := codec.UUID{MSB: 1}
	src, _ := EmptyAttachmentTable().Put(doc, att(1, "a", 1))
	dst := NewAttachmentSetPool(newMemSetStore(), 0)
	moved, err := src.InPool(dst)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Pool() != dst || *moved.Root() != *src.Root() || moved.Len() != 1 {
		t.Fatalf("moved table: %+v", moved)
	}
	if a, ok := get(t, moved, doc, att(1, "", 0).ID); !ok || a.Name != "a" {
		t.Fatal("moved table reads differently")
	}
	if same, _ := moved.InPool(dst); same.Pool() != dst {
		t.Fatal("moving to its own pool changed it")
	}
}

// While tracking, the sets written to the store are reported for two collections, so one that is
// abandoned after asking forgets none.
func TestSetPoolReportsWrittenSetsForTwoCollections(t *testing.T) {
	store := newMemSetStore()
	pool := NewAttachmentSetPool(store, 1)
	pool.TrackWritten(true)
	doc := codec.UUID{MSB: 1}
	tab := pool.Empty()
	put := func(i int) codec.Hash {
		var err error
		if tab, err = tab.Put(doc, version(i)); err != nil {
			t.Fatal(err)
		}
		h, _ := tab.SetHash(doc)
		return h
	}
	first := put(0)
	put(1) // evicts first
	if w := pool.TakeWritten(); len(w) != 1 {
		t.Fatalf("first collection saw %d written sets, want 1", len(w))
	}
	if _, ok := pool.TakeWritten()[first]; !ok {
		t.Fatal("the second collection forgot a set written before the first")
	}
	if _, ok := pool.TakeWritten()[first]; ok {
		t.Fatal("a third collection still keeps it")
	}
}
