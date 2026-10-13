package fulltext

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
)

type ftWorld struct {
	t     *testing.T
	rng   *rand.Rand
	d     *dag.InMemoryCommitDag
	store *Store
	ref   *refStore
	docs  []codec.UUID
	main  []codec.Hash
	side  []codec.Hash

	historical, served, unavailable int
}

var ftVocab = strings.Fields("alpha beta gamma delta deploy staging release rollback cache index query vector merge commit branch tree node")

func newFTWorld(t *testing.T, seed int64, window int) *ftWorld {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(fmt.Sprintf("ns/ft-%d", seed))
	if err != nil {
		t.Fatal(err)
	}
	desc := index.Descriptor{FieldName: "title", Fields: []string{"title", "body"}, Type: index.IndexTypeFullText,
		Options: map[string]string{index.OptionWeights: "title=3"}}
	s, err := NewFullTextStore(desc, d, Options{HistoryEvents: window})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := newRefStore(desc, d, Options{})
	if err != nil {
		t.Fatal(err)
	}
	g, _ := d.Head()
	w := &ftWorld{t: t, rng: rand.New(rand.NewSource(seed)), d: d, store: s, ref: ref, main: []codec.Hash{g}}
	for i := 0; i < 16; i++ {
		w.docs = append(w.docs, codec.UUID{MSB: w.rng.Int63(), LSB: w.rng.Int63()})
	}
	return w
}

func (w *ftWorld) tx(parent codec.Hash) document.Transaction {
	return document.Transaction{ID: codec.UUID{MSB: w.rng.Int63(), LSB: 3}, BaseVersion: parent, Timestamp: codec.TimestampNow()}
}

func (w *ftWorld) tip() codec.Hash { return w.main[len(w.main)-1] }

func (w *ftWorld) appendMain() codec.Hash {
	c, err := w.d.AppendCommit(w.tx(w.tip()), w.tip(), document.EmptyDocumentTree(), nil, "")
	if err != nil {
		w.t.Fatal(err)
	}
	w.main = append(w.main, c.Hash)
	return c.Hash
}

func (w *ftWorld) appendSide() codec.Hash {
	tip := w.tip()
	base := w.main[len(w.main)-1-w.rng.Intn(min(len(w.main), 5))]
	c, err := w.d.AppendCommitDetached(w.tx(base), base, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		w.t.Fatal(err)
	}
	if err := w.d.SetHead("main", tip); err != nil {
		w.t.Fatal(err)
	}
	w.side = append(w.side, c.Hash)
	return c.Hash
}

func (w *ftWorld) merge() codec.Hash {
	if len(w.side) == 0 {
		return w.appendMain()
	}
	c, err := w.d.AppendMergeCommit(w.tx(w.tip()), w.tip(), w.side[w.rng.Intn(len(w.side))], document.EmptyDocumentTree(), nil, "")
	if err != nil {
		w.t.Fatal(err)
	}
	w.main = append(w.main, c.Hash)
	return c.Hash
}

func (w *ftWorld) words(n int) string {
	out := make([]string, n)
	for i := range out {
		out[i] = ftVocab[w.rng.Intn(len(ftVocab))]
	}
	return strings.Join(out, " ")
}

func (w *ftWorld) write(commit codec.Hash) {
	doc := w.docs[w.rng.Intn(len(w.docs))]
	if w.rng.Intn(6) == 0 {
		_ = w.store.Delete(doc, commit)
		_ = w.ref.Delete(doc, commit)
		return
	}
	text := fmt.Sprintf(`{"title":%q,"body":%q}`, w.words(1+w.rng.Intn(3)), w.words(w.rng.Intn(8)))
	if err := w.store.PutDocument(doc, commit, text); err != nil {
		w.t.Fatal(err)
	}
	if err := w.ref.PutDocument(doc, commit, text); err != nil {
		w.t.Fatal(err)
	}
}

func (w *ftWorld) search() {
	head, _ := w.d.Head()
	var at *codec.Hash
	switch r := w.rng.Intn(10); {
	case r < 4:
	case r < 7:
		c := w.main[len(w.main)-1-w.rng.Intn(min(len(w.main), 5))]
		at = &c
	case r < 9 || len(w.side) == 0:
		c := w.main[w.rng.Intn(len(w.main))]
		at = &c
	default:
		c := w.side[w.rng.Intn(len(w.side))]
		at = &c
	}
	historical := at != nil && *at != head
	if historical {
		w.historical++
	}
	q := w.words(1 + w.rng.Intn(2))
	if w.rng.Intn(4) == 0 {
		q = `"` + w.words(2) + `" ` + w.words(1)
	}
	got, err := w.store.Search(q, at, w.rng.Intn(4))
	if errors.Is(err, index.ErrHistoryUnavailable) {
		w.unavailable++
		if !historical && !w.store.stats().Broken {
			w.t.Fatalf("search %q at head unavailable without the store being broken", q)
		}
		return
	}
	if err != nil {
		w.t.Fatal(err)
	}
	if historical {
		w.served++
	}
	var want []index.RankedResult
	if at == nil {
		want, _ = w.ref.Search(q, nil, 0)
	} else {
		want, _ = w.ref.Search(q, at, 0)
	}
	// Same limit on both.
	if l := len(got); l < len(want) && l > 0 {
		want = want[:l]
	} else if len(got) == 0 && len(want) > 0 {
		w.t.Fatalf("search %q: got nothing, want %v", q, want)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		w.t.Fatalf("search %q at %v (historical %v, stats %+v):\n got  %v\n want %v", q, at, historical, w.store.stats(), got, want)
	}
	gs, err1 := w.store.Stats(at)
	ws, _ := w.ref.Stats(at)
	if err1 == nil && fmt.Sprint(gs) != fmt.Sprint(ws) {
		w.t.Fatalf("stats at %v: got %v want %v", at, gs, ws)
	}
}

func (w *ftWorld) run(n int, sideEvents bool) {
	for i := 0; i < n; i++ {
		switch r := w.rng.Intn(100); {
		case r < 45:
			c := w.appendMain()
			for j := 0; j < 1+w.rng.Intn(2); j++ {
				w.write(c)
			}
		case r < 50:
			s := w.appendSide()
			if sideEvents {
				w.write(s)
			}
		case r < 53:
			w.write(w.merge())
		case r < 56:
			w.write(w.main[len(w.main)-1-w.rng.Intn(min(len(w.main), 6))])
		default:
			w.search()
		}
		if w.store.stats().Broken {
			if !w.store.NeedsRebuild() {
				w.t.Fatal("broken store does not report needing a rebuild")
			}
			_ = w.store.Clear()
			_ = w.ref.Clear()
		}
	}
}

func TestFullTextMatchesReference(t *testing.T) {
	for _, window := range []int{-1, 6, 30, 150} {
		for seed := int64(1); seed <= 10; seed++ {
			for _, side := range []bool{false, true} {
				w := newFTWorld(t, seed*100+int64(window), window)
				w.run(1000, side)
				if window < 0 && w.unavailable > 0 {
					t.Fatalf("unbounded store reported %d searches unavailable", w.unavailable)
				}
				if st := w.store.stats(); window > 0 && st.Events > window+window/4+1 {
					t.Fatalf("window %d: %d events retained", window, st.Events)
				}
				if seed == 1 {
					t.Logf("window %d side %v: %d of %d historical searches served, %d unavailable", window, side, w.served, w.historical, w.unavailable)
				}
			}
		}
	}
}

// TestFullTextPostingsBoundedByWindow: rewriting the same documents forever keeps postings
// proportional to the live corpus plus the window, not to the number of writes.
func TestFullTextPostingsBoundedByWindow(t *testing.T) {
	w := newFTWorld(t, 7, 64)
	var peak int
	for i := 0; i < 20000; i++ {
		c := w.appendMain()
		w.write(c)
		if i%5 == 0 {
			if _, err := w.store.Search("alpha", nil, 3); err != nil {
				t.Fatal(err)
			}
		}
		if st := w.store.stats(); i > 1000 && st.Postings > peak {
			peak = st.Postings
		}
	}
	// 16 docs live plus <= 80 retained events, each version at most ~11 distinct terms, twice
	// over for dead postings awaiting compaction.
	if peak > 2*(16+80)*11*2 {
		t.Fatalf("postings peaked at %d", peak)
	}
}

func TestFullTextSnapshotAfterDrop(t *testing.T) {
	w := newFTWorld(t, 9, 16)
	for i := 0; i < 200; i++ {
		w.write(w.appendMain())
	}
	snap, err := w.store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewFullTextStore(w.store.desc, w.d, Options{HistoryEvents: 16})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	for _, q := range ftVocab {
		got, err := restored.Search(q, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := w.ref.Search(q, nil, 0)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("restored head search %q: got %v want %v", q, got, want)
		}
	}
	old := w.main[50]
	if _, err := restored.Search("alpha", &old, 0); !errors.Is(err, index.ErrHistoryUnavailable) {
		t.Fatalf("head-only restore answered a historical search: %v", err)
	}
}

func TestFullTextConcurrentSearchAndWrite(t *testing.T) {
	w := newFTWorld(t, 13, 32)
	var mu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < 1500; i++ {
			mu.Lock()
			c := w.appendMain()
			doc := w.docs[w.rng.Intn(len(w.docs))]
			text := fmt.Sprintf(`{"title":%q}`, w.words(3))
			mu.Unlock()
			if err := w.store.PutDocument(doc, c, text); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				c := w.main[len(w.main)-1-rng.Intn(min(len(w.main), 6))]
				mu.Unlock()
				if _, err := w.store.Search("alpha beta", &c, 5); err != nil && !errors.Is(err, index.ErrHistoryUnavailable) {
					t.Error(err)
					return
				}
				if _, err := w.store.Search("gamma", nil, 5); err != nil {
					t.Error(err)
					return
				}
			}
		}(r)
	}
	wg.Wait()
}
