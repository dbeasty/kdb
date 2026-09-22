package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func putJSONNode(t *testing.T) *KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	srv.NodeID = mustRandomUUID(t)
	return srv
}

// PutJSON replaces the whole document: a field the new body leaves out is gone.
func TestPutJSONReplacesTheWholeDocument(t *testing.T) {
	srv := putJSONNode(t)
	id := mustRandomUUID(t)
	if _, err := srv.Upsert("app/data", id, `{"a":1,"b":2}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.PutJSON("app/data", id, `{"a":3}`, nil, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	body, _, _, _ := srv.GetDocument("app/data", id)
	if !strings.Contains(body, `"a":3`) || strings.Contains(body, `"b"`) {
		t.Fatalf("want exactly {\"a\":3}, got %s", body)
	}
}

// A stale expectation fails with the current hash and changes nothing; a fresh one succeeds.
func TestPutJSONPreconditions(t *testing.T) {
	srv := putJSONNode(t)
	id := mustRandomUUID(t)
	if _, err := srv.PutJSON("app/data", id, `{"v":1}`, &Expect{Absent: true}, auth.Principal{}); err != nil {
		t.Fatalf("create when absent: %v", err)
	}
	var failed *PreconditionFailedError
	if _, err := srv.PutJSON("app/data", id, `{"v":0}`, &Expect{Absent: true}, auth.Principal{}); !errors.As(err, &failed) {
		t.Fatalf("create over an existing document must fail, got %v", err)
	}
	_, hash, found, err := srv.ReadForUpdate("app/data", id)
	if err != nil || !found || hash == "" {
		t.Fatalf("read for update: %v %v %q", found, err, hash)
	}
	if _, err := srv.Upsert("app/data", id, `{"v":2}`, auth.Principal{}); err != nil { // someone else writes
		t.Fatal(err)
	}
	if _, err := srv.PutJSON("app/data", id, `{"v":10}`, &Expect{ContentHash: hash}, auth.Principal{}); !errors.As(err, &failed) {
		t.Fatalf("a stale hash must fail, got %v", err)
	}
	if failed.Current == "" || failed.Current == hash {
		t.Fatalf("the failure should name the current hash: %+v", failed)
	}
	if body, _, _, _ := srv.GetDocument("app/data", id); !strings.Contains(body, `"v":2`) {
		t.Fatalf("a failed precondition changed the document: %s", body)
	}
	if _, err := srv.PutJSON("app/data", id, `{"v":10}`, &Expect{ContentHash: failed.Current}, auth.Principal{}); err != nil {
		t.Fatalf("retry with the current hash: %v", err)
	}
}

// Concurrent read-modify-writes through PutJSON with retry lose no update.
func TestPutJSONReadModifyWriteLosesNoUpdates(t *testing.T) {
	srv := putJSONNode(t)
	id := mustRandomUUID(t)
	if _, err := srv.PutJSON("app/data", id, `{"n":0}`, nil, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	const writers, each = 8, 10
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				for {
					body, hash, _, err := srv.ReadForUpdate("app/data", id)
					if err != nil {
						t.Error(err)
						return
					}
					n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(strings.ReplaceAll(body, " ", ""), `{"n":`), "}"))
					_, err = srv.PutJSON("app/data", id, fmt.Sprintf(`{"n":%d}`, n+1), &Expect{ContentHash: hash}, auth.Principal{})
					var failed *PreconditionFailedError
					if errors.As(err, &failed) {
						continue
					}
					if err != nil {
						t.Error(err)
						return
					}
					break
				}
			}
		}()
	}
	wg.Wait()
	body, _, _, _ := srv.GetDocument("app/data", id)
	if want := fmt.Sprintf(`"n":%d`, writers*each); !strings.Contains(strings.ReplaceAll(body, " ", ""), want) {
		t.Fatalf("lost updates: %s, want %s", body, want)
	}
}

// A peer merge landing between the read and the write is seen, not overwritten; a merge that
// touched only other documents does not get in the way.
func TestPutJSONSeesAPeerMergeBetweenReadAndWrite(t *testing.T) {
	a, b := putJSONNode(t), putJSONNode(t)
	doc, other := mustRandomUUID(t), mustRandomUUID(t)
	if _, err := a.Upsert("app/data", doc, `{"owner":"a"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	syncTo(t, a, b)

	_, hash, _, _ := a.ReadForUpdate("app/data", doc)
	if _, err := b.Upsert("app/data", other, `{"unrelated":true}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	syncTo(t, a, b) // A takes B's unrelated write
	if _, err := a.PutJSON("app/data", doc, `{"owner":"a","n":1}`, &Expect{ContentHash: hash}, auth.Principal{}); err != nil {
		t.Fatalf("an unrelated merge must not fail the precondition: %v", err)
	}

	syncTo(t, a, b) // B has A's write, so B's next change is a clean descendant of it
	_, hash, _, _ = a.ReadForUpdate("app/data", doc)
	if _, err := b.Upsert("app/data", doc, `{"owner":"b"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	syncTo(t, a, b) // B's change to the same document reaches A
	var failed *PreconditionFailedError
	if _, err := a.PutJSON("app/data", doc, `{"owner":"a","n":2}`, &Expect{ContentHash: hash}, auth.Principal{}); !errors.As(err, &failed) {
		t.Fatalf("a merge that changed the document must fail the precondition, got %v", err)
	}
}
