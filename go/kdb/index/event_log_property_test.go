package index

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// propWorld drives one commit DAG with the bounded event log and the pre-bounding reference
// side by side, feeding both the same events and comparing every read.
type propWorld struct {
	t    *testing.T
	rng  *rand.Rand
	d    *dag.InMemoryCommitDag
	log  *eventLog
	ref  *refEventLog
	docs []codec.UUID

	main []codec.Hash // main-branch commits, oldest first
	side []codec.Hash // commits off the main branch

	reads, unavailable, historical, historicalServed int
}

func newPropWorld(t *testing.T, seed int64, window int) *propWorld {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(fmt.Sprintf("ns/prop-%d", seed))
	if err != nil {
		t.Fatal(err)
	}
	g, _ := d.Head()
	w := &propWorld{
		t: t, rng: rand.New(rand.NewSource(seed)), d: d,
		log: newEventLog(d, MemoryOptions{HistoryEvents: window}), ref: newRefEventLog(d),
		main: []codec.Hash{g},
	}
	for i := 0; i < 24; i++ {
		w.docs = append(w.docs, codec.UUID{MSB: w.rng.Int63() - w.rng.Int63(), LSB: w.rng.Int63()})
	}
	return w
}

func (w *propWorld) tx(parent codec.Hash) document.Transaction {
	return document.Transaction{
		ID: codec.UUID{MSB: w.rng.Int63(), LSB: w.rng.Int63()}, BaseVersion: parent,
		Timestamp: codec.TimestampNow(), AuthorNodeID: codec.UUID{MSB: 7, LSB: 7},
	}
}

func (w *propWorld) tip() codec.Hash { return w.main[len(w.main)-1] }

func (w *propWorld) appendMain() codec.Hash {
	c, err := w.d.AppendCommit(w.tx(w.tip()), w.tip(), document.EmptyDocumentTree(), nil, "")
	if err != nil {
		w.t.Fatal(err)
	}
	w.main = append(w.main, c.Hash)
	return c.Hash
}

// appendSide adds a commit off a recent main commit without moving the main branch.
func (w *propWorld) appendSide() codec.Hash {
	tip := w.tip()
	base := w.main[len(w.main)-1-w.rng.Intn(min(len(w.main), 6))]
	if len(w.side) > 0 && w.rng.Intn(2) == 0 {
		base = w.side[len(w.side)-1]
	}
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

func (w *propWorld) merge() codec.Hash {
	if len(w.side) == 0 {
		return w.appendMain()
	}
	other := w.side[w.rng.Intn(len(w.side))]
	c, err := w.d.AppendMergeCommit(w.tx(w.tip()), w.tip(), other, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		w.t.Fatal(err)
	}
	w.main = append(w.main, c.Hash)
	return c.Hash
}

func (w *propWorld) randKey() Key {
	switch w.rng.Intn(4) {
	case 0:
		return StringKey{Value: fmt.Sprintf("s%02d", w.rng.Intn(12))}
	case 1:
		return CompositeKey{Parts: []Key{StringKey{Value: fmt.Sprintf("c%d", w.rng.Intn(3))}, Int64Key{Value: int64(w.rng.Intn(4))}}}
	default:
		return Int64Key{Value: int64(w.rng.Intn(30) - 10)}
	}
}

// write is what MemoryStore's prepared put does: a delete, then the document's new keys.
func (w *propWorld) write(commit codec.Hash) {
	doc := w.docs[w.rng.Intn(len(w.docs))]
	w.log.delete(doc, commit)
	w.ref.delete(doc, commit)
	if w.rng.Intn(6) == 0 {
		return // a document leaving the index
	}
	n := 1
	if w.rng.Intn(5) == 0 {
		n = 1 + w.rng.Intn(3) // multikey
	}
	for i := 0; i < n; i++ {
		e := Entry{DocID: doc, Key: w.randKey(), CommitHash: commit}
		w.log.put(e)
		w.ref.put(e)
	}
}

func (w *propWorld) writes(commit codec.Hash) {
	for i := 0; i < 1+w.rng.Intn(3); i++ {
		w.write(commit)
	}
}

func (w *propWorld) cutoffForRead() (codec.Hash, bool) {
	head, _ := w.d.Head()
	switch r := w.rng.Intn(10); {
	case r < 4:
		return head, false
	case r < 7:
		return w.main[len(w.main)-1-w.rng.Intn(min(len(w.main), 5))], true
	case r < 9 || len(w.side) == 0:
		return w.main[w.rng.Intn(len(w.main))], true
	default:
		return w.side[w.rng.Intn(len(w.side))], true
	}
}

func (w *propWorld) read() {
	cutoff, historical := w.cutoffForRead()
	head, _ := w.d.Head()
	if cutoff == head {
		historical = false
	}
	w.reads++
	if historical {
		w.historical++
	}
	var got, want any
	var err error
	desc := ""
	switch w.rng.Intn(3) {
	case 0:
		k := w.randKey()
		desc = fmt.Sprintf("lookup %v", k)
		var ids []codec.UUID
		ids, err = w.log.lookup(k, &cutoff)
		got, want = ids, w.ref.lookup(k, cutoff)
	case 1:
		k := w.randKey()
		desc = fmt.Sprintf("count %v", k)
		var n int
		n, err = w.log.lookupCount(k, &cutoff)
		got, want = n, w.ref.lookupCount(k, cutoff)
	default:
		var from, to Key
		if w.rng.Intn(4) > 0 {
			from = Int64Key{Value: int64(w.rng.Intn(30) - 12)}
		}
		if w.rng.Intn(4) > 0 {
			to = Int64Key{Value: int64(w.rng.Intn(30) - 8)}
		}
		if w.rng.Intn(6) == 0 {
			from, to = StringKey{Value: "s03"}, CompositeKey{Parts: []Key{StringKey{Value: "c1"}}}
		}
		limit := 0
		if w.rng.Intn(2) == 0 {
			limit = 1 + w.rng.Intn(8)
		}
		asc := w.rng.Intn(3) > 0
		desc = fmt.Sprintf("range %v..%v limit %d asc %v", from, to, limit, asc)
		var ids []codec.UUID
		ids, err = w.log.rangeScan(from, to, &cutoff, limit, asc)
		got, want = ids, w.ref.rangeScan(from, to, cutoff, limit, asc)
	}
	if errors.Is(err, ErrHistoryUnavailable) {
		w.unavailable++
		if !historical && !w.log.stats().Broken {
			w.t.Fatalf("%s at head: unavailable without the state being broken", desc)
		}
		return
	}
	if err != nil {
		w.t.Fatalf("%s: %v", desc, err)
	}
	if historical {
		w.historicalServed++
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		w.t.Fatalf("%s at %s (historical=%v, stats %+v):\n got  %v\n want %v", desc, cutoff.Hex()[:8], historical, w.log.stats(), got, want)
	}
}

// run performs n random operations. sideEvents allows events under commits off the main branch
// (which a server never logs), the one thing that can force the bounded log to give up.
func (w *propWorld) run(n int, sideEvents bool) {
	for i := 0; i < n; i++ {
		switch r := w.rng.Intn(100); {
		case r < 45:
			w.writes(w.appendMain())
		case r < 50:
			s := w.appendSide()
			if sideEvents {
				w.writes(s)
			}
		case r < 54:
			c := w.merge()
			w.writes(c)
		case r < 57:
			// A late event for a commit the head passed some time ago.
			w.writes(w.main[len(w.main)-1-w.rng.Intn(min(len(w.main), 8))])
		case r < 60:
			w.appendMain()
		default:
			w.read()
		}
		if w.log.stats().Broken {
			// What the server does: rebuild. Both sides start over from nothing.
			if !w.log.needsRebuild() {
				w.t.Fatal("broken log does not report needing a rebuild")
			}
			w.log.clear()
			w.ref.clear()
		}
	}
}

func TestEventLogMatchesReferenceUnbounded(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		w := newPropWorld(t, seed, -1)
		w.run(1500, true)
		if w.unavailable != 0 {
			t.Fatalf("seed %d: an unbounded log reported %d reads unavailable", seed, w.unavailable)
		}
	}
}

func TestEventLogMatchesReferenceBounded(t *testing.T) {
	for _, window := range []int{8, 40, 200} {
		for seed := int64(1); seed <= 12; seed++ {
			for _, side := range []bool{false, true} {
				w := newPropWorld(t, seed*1000+int64(window), window)
				w.run(1500, side)
				st := w.log.stats()
				if st.Events > window+window/4+1 {
					t.Fatalf("window %d: %d events retained", window, st.Events)
				}
				if seed == 1 {
					t.Logf("window %d side %v: %d of %d historical reads served from the window, %d unavailable",
						window, side, w.historicalServed, w.historical, w.unavailable)
				}
			}
		}
	}
}

// TestEventLogRecentHistoryServedFromWindow: in the server's pattern (every event under the new
// head, a read after each commit) every commit whose events are still in the window is answered
// from it, and every older one is reported unavailable rather than answered wrongly.
func TestEventLogRecentHistoryServedFromWindow(t *testing.T) {
	const window = 64
	w := newPropWorld(t, 99, window)
	for i := 0; i < 400; i++ {
		c := w.appendMain()
		w.write(c)
		if _, err := w.log.lookup(StringKey{Value: "s01"}, &c); err != nil {
			t.Fatal(err)
		}
	}
	served := 0
	for back := 0; back < len(w.main); back++ {
		c := w.main[len(w.main)-1-back]
		got, err := w.log.rangeScan(nil, nil, &c, 0, true)
		if errors.Is(err, ErrHistoryUnavailable) {
			if back < window*3/8 {
				t.Fatalf("commit %d back unavailable; the window covers it", back)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if want := w.ref.rangeScan(nil, nil, c, 0, true); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("commit %d back: got %v want %v", back, got, want)
		}
		served++
	}
	if served < window*3/8 {
		t.Fatalf("only %d historical commits served", served)
	}
	if !w.log.stats().Dropped {
		t.Fatal("expected history to have been dropped")
	}
}

// TestEventLogMemoryBoundedByWindow: retained events, steps and keys stop growing with writes.
func TestEventLogMemoryBoundedByWindow(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/bounded")
	l := newEventLog(d, MemoryOptions{HistoryEvents: 512})
	rng := rand.New(rand.NewSource(3))
	head, _ := d.Head()
	docs := make([]codec.UUID, 200)
	for i := range docs {
		docs[i] = codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}
	}
	var heaps []uint64
	for round := 0; round < 4; round++ {
		for i := 0; i < 20000; i++ {
			c, err := d.AppendCommit(document.Transaction{ID: codec.UUID{MSB: rng.Int63(), LSB: 1}, BaseVersion: head}, head, document.EmptyDocumentTree(), nil, "")
			if err != nil {
				t.Fatal(err)
			}
			head = c.Hash
			doc := docs[rng.Intn(len(docs))]
			l.delete(doc, head)
			// A fresh key every write: without key compaction the key table would grow forever.
			l.put(Entry{DocID: doc, Key: Int64Key{Value: int64(round*20000 + i)}, CommitHash: head})
			if i%7 == 0 {
				if _, err := l.rangeScan(nil, nil, nil, 5, true); err != nil {
					t.Fatal(err)
				}
			}
		}
		st := l.stats()
		if st.Events > 512+128 || st.Steps > 512 || st.Keys > 4*len(docs)+2048 {
			t.Fatalf("round %d: log not bounded: %+v", round, st)
		}
		if st.Live != len(docs) && round > 0 {
			t.Fatalf("round %d: %d live keys, want %d", round, st.Live, len(docs))
		}
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		heaps = append(heaps, ms.HeapAlloc)
	}
	runtime.KeepAlive(l)
	// The DAG grows (20k commits a round); the log must not add to that growth. Compare against
	// the same DAG growth without the log: a commit costs well under 2 KB, so allow that.
	perRound := (int64(heaps[3]) - int64(heaps[1])) / 2
	if perRound > 20000*2048 {
		t.Fatalf("heap grows %d bytes per 20k writes", perRound)
	}
}

// TestEventLogBrokenAfterGraphChangeAfterDrop: once history is dropped, a head that moves to a
// commit that does not descend from it cannot be followed; the log must say so - every read
// unavailable, NeedsRebuild true - rather than answer from a stale state.
func TestEventLogBrokenAfterGraphChangeAfterDrop(t *testing.T) {
	w := newPropWorld(t, 5, 16)
	for i := 0; i < 100; i++ {
		w.writes(w.appendMain())
		w.read()
	}
	if !w.log.stats().Dropped {
		t.Fatal("expected dropped history")
	}
	// Rewind the branch to an old commit and write on top of it.
	if err := w.d.SetHead("main", w.main[10]); err != nil {
		t.Fatal(err)
	}
	w.main = w.main[:11]
	w.writes(w.appendMain())
	if _, err := w.log.lookup(StringKey{Value: "s01"}, nil); !errors.Is(err, ErrHistoryUnavailable) {
		t.Fatalf("lookup after rewind: err %v, want ErrHistoryUnavailable", err)
	}
	if !w.log.needsRebuild() {
		t.Fatal("NeedsRebuild false after rewind")
	}
	w.log.clear()
	w.log.put(Entry{DocID: w.docs[0], Key: StringKey{Value: "s01"}, CommitHash: w.tip()})
	if ids, err := w.log.lookup(StringKey{Value: "s01"}, nil); err != nil || len(ids) != 1 {
		t.Fatalf("after rebuild: %v %v", ids, err)
	}
}

func TestEventLogSnapshotRoundTrip(t *testing.T) {
	for _, window := range []int{-1, 16} {
		w := newPropWorld(t, 11, window)
		for i := 0; i < 200; i++ {
			w.writes(w.appendMain())
			if i%3 == 0 {
				w.read()
			}
		}
		snap, err := w.log.snapshotBytes()
		if err != nil {
			t.Fatal(err)
		}
		restored := newEventLog(w.d, MemoryOptions{HistoryEvents: window})
		if err := restored.restoreSnapshotBytes(snap); err != nil {
			t.Fatal(err)
		}
		head := w.tip()
		got, err := restored.bucketsCopy(&head)
		if err != nil {
			t.Fatal(err)
		}
		want := w.ref.bucketsCopy(head)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("window %d restored head buckets differ:\n got  %v\n want %v", window, got, want)
		}
		old := w.main[len(w.main)-50]
		ids, err := restored.rangeScan(nil, nil, &old, 0, true)
		if window < 0 {
			if err != nil || fmt.Sprint(ids) != fmt.Sprint(w.ref.rangeScan(nil, nil, old, 0, true)) {
				t.Fatalf("complete restore lost history: %v %v", ids, err)
			}
		} else if !errors.Is(err, ErrHistoryUnavailable) {
			t.Fatalf("head-state restore answered a historical read: %v %v", ids, err)
		}
	}
}

// TestEventLogReadsFormatOneSnapshots: snapshots written before format 2 restore the full log.
func TestEventLogReadsFormatOneSnapshots(t *testing.T) {
	w := newPropWorld(t, 12, -1)
	for i := 0; i < 60; i++ {
		w.writes(w.appendMain())
	}
	old := w.ref.snapshotBytes()
	restored := newEventLog(w.d, MemoryOptions{})
	if err := restored.restoreSnapshotBytes(old); err != nil {
		t.Fatal(err)
	}
	for _, c := range []codec.Hash{w.tip(), w.main[20], w.main[40]} {
		got, err := restored.rangeScan(nil, nil, &c, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		if want := w.ref.rangeScan(nil, nil, c, 0, true); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("format 1 restore at %s: got %v want %v", c.Hex()[:8], got, want)
		}
	}
}

// TestEventLogConcurrentReadersAndWriters is for -race: writers appending commits and events
// while readers read at head and at recent commits.
func TestEventLogConcurrentReadersAndWriters(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/concurrent")
	s := NewMemoryStoreWithOptions(Descriptor{FieldName: "k", Type: IndexTypeBTree}, d, MemoryOptions{HistoryEvents: 64})
	var commitsMu sync.Mutex
	g, _ := d.Head()
	commits := []codec.Hash{g}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(1))
		for i := 0; i < 3000; i++ {
			commitsMu.Lock()
			parent := commits[len(commits)-1]
			c, err := d.AppendCommit(document.Transaction{ID: codec.UUID{MSB: rng.Int63(), LSB: 2}, BaseVersion: parent}, parent, document.EmptyDocumentTree(), nil, "")
			if err != nil {
				commitsMu.Unlock()
				t.Error(err)
				return
			}
			commits = append(commits, c.Hash)
			commitsMu.Unlock()
			doc := codec.UUID{MSB: int64(rng.Intn(50)), LSB: 1}
			if err := s.PutDocument(doc, c.Hash, fmt.Sprintf(`{"k":%d}`, rng.Intn(20))); err != nil {
				t.Error(err)
				return
			}
		}
		close(stop)
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r + 10)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				commitsMu.Lock()
				c := commits[len(commits)-1-rng.Intn(min(len(commits), 10))]
				commitsMu.Unlock()
				if _, err := s.Range(Int64Key{Value: 3}, Int64Key{Value: 12}, &c, 0, true); err != nil && !errors.Is(err, ErrHistoryUnavailable) {
					t.Error(err)
					return
				}
				if _, err := s.Lookup(Int64Key{Value: 5}, nil); err != nil {
					t.Error(err)
					return
				}
				if _, err := s.LookupCount(Int64Key{Value: 7}, &c); err != nil && !errors.Is(err, ErrHistoryUnavailable) {
					t.Error(err)
					return
				}
			}
		}(r)
	}
	wg.Wait()
}

// TestKeyEncodingOrderMatchesCompareKeys: the byte order the B-tree uses is CompareKeys' order
// for every pair CompareKeys tells apart, and decoding gives the key back.
func TestKeyEncodingOrderMatchesCompareKeys(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	var keys []Key
	strs := []string{"", "a", "a\x00", "a\x00b", "a\x01", "ab", "b", "\x00", "\xff", "a|b", "a,b", "a\\b"}
	for _, s := range strs {
		keys = append(keys, StringKey{Value: s})
	}
	for i := 0; i < 200; i++ {
		switch rng.Intn(9) {
		case 0:
			keys = append(keys, BoolKey{Value: rng.Intn(2) == 0})
		case 1:
			keys = append(keys, Int32Key{Value: int32(rng.Int63() - rng.Int63())})
		case 2:
			keys = append(keys, Int64Key{Value: rng.Int63() - rng.Int63()})
		case 3:
			keys = append(keys, Float64Key{Value: rng.NormFloat64() * 1e6})
		case 4:
			keys = append(keys, TimestampKey{EpochMillis: rng.Int63() - rng.Int63()})
		case 5:
			keys = append(keys, UUIDKey{ID: codec.UUID{MSB: rng.Int63() - rng.Int63(), LSB: rng.Int63() - rng.Int63()}})
		case 6:
			v := make([]float32, rng.Intn(3))
			for j := range v {
				v[j] = float32(rng.NormFloat64())
			}
			keys = append(keys, NewVectorKey(v))
		case 7:
			keys = append(keys, CompositeKey{Parts: []Key{StringKey{Value: strs[rng.Intn(len(strs))]}, Int64Key{Value: int64(rng.Intn(3))}}})
		default:
			keys = append(keys, CompositeKey{Parts: []Key{StringKey{Value: strs[rng.Intn(len(strs))]}}})
		}
	}
	keys = append(keys, NullKey{}, Float64Key{Value: 1}, Float64Key{Value: -1}, Int64Key{Value: 0})
	for _, a := range keys {
		ea := appendKeyEnc(nil, a)
		back, rest, err := decodeKeyEnc(ea)
		if err != nil || len(rest) != 0 || KeyString(back) != KeyString(a) {
			t.Fatalf("round trip %v: got %v rest %d err %v", a, back, len(rest), err)
		}
		for _, b := range keys {
			eb := appendKeyEnc(nil, b)
			want := CompareKeys(a, b)
			got := slices.Compare(ea, eb)
			if (KeyString(a) == KeyString(b)) != (got == 0) {
				t.Fatalf("identity of %v and %v: KeyString equal %v, encodings equal %v", a, b, KeyString(a) == KeyString(b), got == 0)
			}
			if want != 0 && (want < 0) != (got < 0) {
				t.Fatalf("order of %v and %v: CompareKeys %d, bytes %d", a, b, want, got)
			}
		}
	}
}
