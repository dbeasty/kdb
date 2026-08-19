package engine

import (
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func TestShardedDocStore_ConcurrentPutGetDelete(t *testing.T) {
	s := newShardedDocStore()
	const n = 2000
	ids := make([]codec.UUID, n)
	for i := range ids {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for _, id := range ids {
		id := id
		go func() {
			defer wg.Done()
			doc, err := document.FromJSONWithID(id, `{"v":1}`)
			if err != nil {
				t.Errorf("FromJSONWithID: %v", err)
				return
			}
			s.Put(doc)
		}()
	}
	wg.Wait()

	if got := len(s.Snapshot()); got != n {
		t.Fatalf("Snapshot len=%d, want %d", got, n)
	}
	for _, id := range ids {
		if _, ok := s.Get(id); !ok {
			t.Fatalf("Get(%s): missing after concurrent Put", id.String())
		}
	}

	wg.Add(n)
	for _, id := range ids {
		id := id
		go func() {
			defer wg.Done()
			s.Delete(id)
		}()
	}
	wg.Wait()

	if got := len(s.Snapshot()); got != 0 {
		t.Fatalf("Snapshot len=%d after deleting all, want 0", got)
	}
}
