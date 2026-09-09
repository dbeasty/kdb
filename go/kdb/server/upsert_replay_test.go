package server_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// A shallow-merge upsert must survive a restart.
//
// Upsert is documented as a shallow root-level merge: "the wire UPSERT and SQL's SET _doc apply the
// supplied body as a shallow root-level merge, so a key the new body omits keeps its stored value"
// (README, Document identity). So a body naming fewer keys than the stored document is not a
// misuse - it is the documented way to update one field.
//
// The two paths that have to agree about that body read it differently:
//
//   - the write path merges it over what is stored
//     (transaction/default_engine.go: baseDoc.Merge(o.Patch)),
//   - replay treats it as the entire document
//     (embed/delta_replay.go: FromJSONWithID(o.DocID, o.Patch) then PutDocument),
//
// and the commit records the operation as given, not the merged result. If they disagree, the tree
// replay rebuilds hashes differently from the one the commit recorded, and every read at head then
// resolves a tree that no longer exists.
//
// This uses nothing but the public write path: no control plane, no SQL, no new code.
func TestUpsertMergeSurvivesRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")

	var liveKeys string
	var docID string
	func() {
		rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer rt.Close()
		srv := server.NewKdbServerRuntime(rt)

		put, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a","x":1}`)
		if err != nil {
			t.Fatal(err)
		}
		docID = put.DocID.String()

		// The documented merge: name only y, and x is meant to survive.
		if _, err := srv.Upsert("demo/users", put.DocID, `{"y":2}`, auth.Principal{ID: "test"}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		body, _, found, err := srv.GetDocument("demo/users", put.DocID)
		if err != nil || !found {
			t.Fatalf("reading back the upsert: found=%v err=%v", found, err)
		}
		liveKeys = keysIn(t, body)
		if liveKeys != "{id,x,y}" {
			t.Fatalf("the merge itself did not behave as documented: got %s, want {id,x,y}", liveKeys)
		}
	}()

	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rt.Close()
	srv := server.NewKdbServerRuntime(rt)

	id, err := codec.UUIDFromString(docID)
	if err != nil {
		t.Fatal(err)
	}
	body, _, found, err := srv.GetDocument("demo/users", id)
	if err != nil {
		t.Fatalf("reading after restart: %v", err)
	}
	if !found {
		t.Fatalf(
			"the document is gone after a restart.\n"+
				"Before it, the merge read back as %s. The commit is still in the log, and head still "+
				"names the same document tree - but replay rebuilt that tree by treating the merge "+
				"patch as a whole document, so it hashes differently from the tree the commit "+
				"recorded, and every read at head now resolves a tree that was never written.",
			liveKeys)
	}
	if got := keysIn(t, body); got != liveKeys {
		t.Fatalf("the document changed shape across a restart: live %s, after restart %s", liveKeys, got)
	}
}

func keysIn(t *testing.T, body string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	out := "{"
	for i, k := range keys {
		if i > 0 {
			out += ","
		}
		out += k
	}
	return out + "}"
}
