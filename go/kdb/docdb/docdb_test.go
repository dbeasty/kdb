package docdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

func open(t *testing.T, dir string) *DB {
	t.Helper()
	opts := Options{}
	opts.Runtime.Storage.Durability = storage.DurabilityAsync
	db, err := Open(dir, "app/docs", opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func id(t *testing.T) codec.UUID {
	u, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func fields(t *testing.T, body string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("stored body %q: %v", body, err)
	}
	return m
}

func TestPutGetMergeDelete(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	doc := id(t)
	if _, found, err := db.Get(doc); err != nil || found {
		t.Fatalf("absent document: found=%v err=%v", found, err)
	}
	if _, err := db.Put(doc, `{"a":1,"b":2}`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Merge(doc, `{"b":3,"c":4}`); err != nil {
		t.Fatal(err)
	}
	body, found, err := db.Get(doc)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got := fields(t, body); got["a"] != 1.0 || got["b"] != 3.0 || got["c"] != 4.0 {
		t.Fatalf("after merge: %s", body)
	}
	// Put replaces the whole document: fields it does not mention are gone.
	if _, err := db.Put(doc, `{"z":true}`); err != nil {
		t.Fatal(err)
	}
	if body, _, _ := db.Get(doc); body != `{"z":true}` {
		t.Fatalf("after put: %s, want the body exactly as put", body)
	}
	if _, err := db.Delete(doc); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := db.Get(doc); found {
		t.Fatal("deleted document still found")
	}
	if _, err := db.Delete(doc); err != nil {
		t.Fatalf("deleting an absent document: %v", err)
	}
	for _, bad := range []string{`[1]`, `{"a":`, `"x"`} {
		if _, err := db.Put(doc, bad); err == nil {
			t.Fatalf("Put(%q) accepted", bad)
		}
		if _, err := db.Merge(doc, bad); err == nil {
			t.Fatalf("Merge(%q) accepted", bad)
		}
	}
}

// Many goroutines writing at once: every merge of a different field of one document survives
// (the merge applies at the gate, to the document as it then is), and puts and reads of other
// documents interleave freely.
func TestConcurrentWriters(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	shared := id(t)
	const writers, perWriter = 16, 25
	docs := make([]codec.UUID, writers)
	for i := range docs {
		docs[i] = id(t)
	}
	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter*3)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := db.Merge(shared, fmt.Sprintf(`{"w%d_%d":%d}`, w, i, i)); err != nil {
					errs <- err
				}
				if _, err := db.Put(docs[w], fmt.Sprintf(`{"w":%d,"i":%d}`, w, i)); err != nil {
					errs <- err
				}
				if _, _, err := db.Get(docs[(w+1)%writers]); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	body, found, err := db.Get(shared)
	if err != nil || !found {
		t.Fatalf("shared document: found=%v err=%v", found, err)
	}
	if got := len(fields(t, body)); got != writers*perWriter {
		t.Fatalf("shared document kept %d of %d fields", got, writers*perWriter)
	}
	for w, d := range docs {
		body, _, _ := db.Get(d)
		if got := fields(t, body); got["w"] != float64(w) || got["i"] != float64(perWriter-1) {
			t.Fatalf("document %d holds %s, want its last put", w, body)
		}
	}
}

// What was written is there after a close and reopen, and a closed DB refuses calls.
func TestReopenAndClose(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	doc := id(t)
	if _, err := db.Put(doc, `{"kept":1}`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, _, err := db.Get(doc); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close: %v, want ErrClosed", err)
	}
	if _, err := db.Put(doc, `{}`); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close: %v, want ErrClosed", err)
	}
	db = open(t, dir)
	defer db.Close()
	if body, found, err := db.Get(doc); err != nil || !found || body != `{"kept":1}` {
		t.Fatalf("after reopen: %q found=%v err=%v", body, found, err)
	}
}

// Closing while writers are busy waits for them; none sees a closed runtime underneath.
func TestCloseWhileWriting(t *testing.T) {
	db := open(t, t.TempDir())
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doc := id(t)
			for i := 0; ; i++ {
				if _, err := db.Merge(doc, fmt.Sprintf(`{"i":%d}`, i)); err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("write failed with %v, want ErrClosed", err)
					}
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		if _, err := db.Merge(id(t), `{"x":1}`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
