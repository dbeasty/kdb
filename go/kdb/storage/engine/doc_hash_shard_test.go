package engine

import (
	"runtime"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func TestShardedDocByHashStore_ConcurrentPutGet(t *testing.T) {
	s := newShardedDocByHashStore(0)
	const n = 2000
	hashes := make([]codec.Hash, n)
	docs := make([]document.Document, n)
	for i := range docs {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		doc, err := document.FromJSONWithID(id, `{"v":1}`)
		if err != nil {
			t.Fatal(err)
		}
		h, err := doc.ContentHash()
		if err != nil {
			t.Fatal(err)
		}
		docs[i] = doc
		hashes[i] = h
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range docs {
		i := i
		go func() {
			defer wg.Done()
			s.Put(hashes[i], docs[i])
		}()
	}
	wg.Wait()

	for i := range docs {
		got, ok := s.Get(hashes[i])
		if !ok {
			t.Fatalf("Get(%s): missing after concurrent Put", hashes[i].Hex())
		}
		if got.JSON != docs[i].JSON {
			t.Fatalf("Get(%s) JSON = %q, want %q", hashes[i].Hex(), got.JSON, docs[i].JSON)
		}
	}
}

// A pin taken before its version is stored carries across when it is stored; a placeholder
// released before then disappears; and a stored version becomes evictable only when its last pin
// goes.
func TestDocHashPinsAcrossPlaceholderAndLoad(t *testing.T) {
	s := newShardedDocByHashStore(0)
	var h codec.Hash
	h.Bytes[0] = 9
	s.Pin(h)
	s.Pin(h)
	if _, ok := s.Get(h); ok {
		t.Fatal("a placeholder is visible to Get")
	}
	s.Put(h, document.Document{JSON: "{}"})
	sh := s.shardFor(h)
	if e := sh.docs[h]; e.pins != 2 || e.el != nil {
		t.Fatalf("stored version lost its pins: %+v", e)
	}
	if _, left := sh.pinned[h]; left {
		t.Fatal("placeholder pin left behind after the version was stored")
	}
	s.Unpin(h)
	s.Unpin(h)
	if e := sh.docs[h]; e.pins != 0 || e.el == nil {
		t.Fatal("version with no pins is not evictable")
	}

	var p codec.Hash
	p.Bytes[0] = 10
	s.Pin(p)
	s.Unpin(p)
	if _, left := s.shardFor(p).pinned[p]; left {
		t.Fatal("released placeholder still held")
	}
}

// What a namespace's live tree costs in this store right after an open: a pin per document and
// nothing loaded. It was ~190 B a document as a full entry; a placeholder must stay small.
func TestDocHashPlaceholderPinsAreSmall(t *testing.T) {
	const n = 200_000
	before := heapNow()
	s := newShardedDocByHashStore(0)
	for i := 0; i < n; i++ {
		var h codec.Hash
		h.Bytes[0], h.Bytes[1], h.Bytes[2], h.Bytes[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
		s.Pin(h)
	}
	per := float64(heapNow()-before) / n
	runtime.KeepAlive(s)
	if per > 90 {
		t.Fatalf("a placeholder pin costs %.0f bytes", per)
	}
	t.Logf("placeholder pin: %.0f bytes", per)
}

func heapNow() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}
