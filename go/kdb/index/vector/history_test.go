package vector

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
)

const histDims = 4

type vWorld struct {
	t     *testing.T
	rng   *rand.Rand
	d     *dag.InMemoryCommitDag
	store *Store
	ref   *refStore
	docs  []codec.UUID
	main  []codec.Hash
	side  []codec.Hash
	// exactCompare: results must equal the reference's exactly (exact search, or HNSW with no
	// compaction, where both graphs hold the same nodes in the same order). Otherwise each HNSW
	// result is checked against the exact oracle instead.
	exactCompare bool

	historical, served, unavailable int
}

func newVWorld(t *testing.T, seed int64, window, threshold int) *vWorld {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(fmt.Sprintf("ns/v-%d", seed))
	if err != nil {
		t.Fatal(err)
	}
	desc := index.Descriptor{FieldName: "e", Fields: []string{"e"}, Type: index.IndexTypeVector,
		Options: map[string]string{index.OptionDimensions: strconv.Itoa(histDims), index.OptionM: "4", index.OptionEfConstruction: "16"}}
	s, err := NewVectorStore(desc, d, Options{HistoryEvents: window, ExactThreshold: threshold})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := newRefStore(desc, d, Options{ExactThreshold: threshold})
	if err != nil {
		t.Fatal(err)
	}
	g, _ := d.Head()
	w := &vWorld{t: t, rng: rand.New(rand.NewSource(seed)), d: d, store: s, ref: ref, main: []codec.Hash{g},
		exactCompare: threshold > 0 || window < 0}
	for i := 0; i < 40; i++ {
		w.docs = append(w.docs, codec.UUID{MSB: w.rng.Int63(), LSB: w.rng.Int63()})
	}
	return w
}

func (w *vWorld) tx(parent codec.Hash) document.Transaction {
	return document.Transaction{ID: codec.UUID{MSB: w.rng.Int63(), LSB: 4}, BaseVersion: parent, Timestamp: codec.TimestampNow()}
}

func (w *vWorld) tip() codec.Hash { return w.main[len(w.main)-1] }

func (w *vWorld) appendMain() codec.Hash {
	c, err := w.d.AppendCommit(w.tx(w.tip()), w.tip(), document.EmptyDocumentTree(), nil, "")
	if err != nil {
		w.t.Fatal(err)
	}
	w.main = append(w.main, c.Hash)
	return c.Hash
}

func (w *vWorld) appendSide() codec.Hash {
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

func (w *vWorld) merge() codec.Hash {
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

func (w *vWorld) vec() []float32 {
	v := make([]float32, histDims)
	for i := range v {
		v[i] = float32(w.rng.Intn(9)-4) / 4
	}
	return v
}

func (w *vWorld) write(commit codec.Hash) {
	doc := w.docs[w.rng.Intn(len(w.docs))]
	if w.rng.Intn(6) == 0 {
		_ = w.store.Delete(doc, commit)
		_ = w.ref.Delete(doc, commit)
		return
	}
	parts := make([]string, histDims)
	for i, f := range w.vec() {
		parts[i] = strconv.FormatFloat(float64(f), 'f', -1, 32)
	}
	text := `{"e":[` + strings.Join(parts, ",") + `]}`
	if err := w.store.PutDocument(doc, commit, text); err != nil {
		w.t.Fatal(err)
	}
	if err := w.ref.PutDocument(doc, commit, text); err != nil {
		w.t.Fatal(err)
	}
}

func (w *vWorld) search() {
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
	q, k := w.vec(), 1+w.rng.Intn(6)
	got, err := w.store.NearestNeighbours(q, k, at)
	if errors.Is(err, index.ErrHistoryUnavailable) {
		w.unavailable++
		if !historical && !w.store.stats().Broken {
			w.t.Fatal("head search unavailable without the store being broken")
		}
		return
	}
	if err != nil {
		w.t.Fatal(err)
	}
	if historical {
		w.served++
	}
	gotN, _ := w.store.LiveCount(at)
	wantN, _ := w.ref.LiveCount(at)
	if gotN != wantN {
		w.t.Fatalf("live count at %v: got %d want %d", at, gotN, wantN)
	}
	if w.exactCompare {
		want, _ := w.ref.NearestNeighbours(q, k, at)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			w.t.Fatalf("knn at %v: got %v want %v (stats %+v)", at, got, want, w.store.stats())
		}
		return
	}
	// Approximate: every hit must be a vector visible at the cutoff, with its exact score.
	oracle, _ := w.ref.ExactNearestNeighbours(q, len(w.docs), at)
	score := make(map[codec.UUID]float32, len(oracle))
	for _, r := range oracle {
		score[r.DocID] = r.Score
	}
	for _, h := range got {
		s, ok := score[h.DocID]
		if !ok || s != h.Score {
			w.t.Fatalf("knn at %v: hit %v not visible (or wrong score); oracle %v", at, h, oracle)
		}
	}
}

func (w *vWorld) run(n int, sideEvents bool) {
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

func TestVectorMatchesReference(t *testing.T) {
	seeds := int64(6)
	if testing.Short() {
		seeds = 2 // all 6 run nightly, or without -short
	}
	for _, window := range []int{-1, 8, 40} {
		for _, threshold := range []int{1 << 20, -1} {
			for seed := int64(1); seed <= seeds; seed++ {
				for _, side := range []bool{false, true} {
					w := newVWorld(t, seed*10+int64(window), window, threshold)
					w.run(700, side)
					if window < 0 && w.unavailable > 0 {
						t.Fatalf("unbounded store reported %d searches unavailable", w.unavailable)
					}
					if st := w.store.stats(); window > 0 && st.Events > window+window/4+1 {
						t.Fatalf("window %d: %d events retained", window, st.Events)
					}
					if seed == 1 {
						t.Logf("window %d threshold %d side %v: %d of %d historical searches served, %d unavailable",
							window, threshold, side, w.served, w.historical, w.unavailable)
					}
				}
			}
		}
	}
}

// TestVectorGraphBoundedByWindow: rewriting the same documents forever keeps the node list (and
// so the HNSW graph) proportional to the live vectors plus the window.
func TestVectorGraphBoundedByWindow(t *testing.T) {
	w := newVWorld(t, 3, 32, -1)
	peak := 0
	for i := 0; i < 5000; i++ {
		w.write(w.appendMain())
		if i%10 == 0 {
			if _, err := w.store.NearestNeighbours(w.vec(), 3, nil); err != nil {
				t.Fatal(err)
			}
		}
		if n := w.store.stats().Nodes; n > peak {
			peak = n
		}
	}
	// 40 live plus at most 40 retained events' nodes and prevs, twice over before a compaction.
	if peak > 2*(40+2*40)+64 {
		t.Fatalf("node list peaked at %d", peak)
	}
}

// TestVectorInsertsAfterGraphBuilt: once a search has built the HNSW graph, further puts are
// inserted into it incrementally. The graph's node slice used to be refreshed only by searches,
// so the second put after a search indexed past its end and panicked.
func TestVectorInsertsAfterGraphBuilt(t *testing.T) {
	w := newVWorld(t, 21, 0, -1)
	for i := 0; i < 20; i++ {
		w.write(w.appendMain())
	}
	if _, err := w.store.NearestNeighbours(w.vec(), 3, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		c := w.appendMain()
		if err := w.store.PutDocument(w.docs[i], c, `{"e":[1,0,0,0]}`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.store.NearestNeighbours([]float32{1, 0, 0, 0}, 3, nil); err != nil {
		t.Fatal(err)
	}
}
