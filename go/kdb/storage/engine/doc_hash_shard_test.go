package engine

import (
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func TestShardedDocByHashStore_ConcurrentPutGet(t *testing.T) {
	s := newShardedDocByHashStore()
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
