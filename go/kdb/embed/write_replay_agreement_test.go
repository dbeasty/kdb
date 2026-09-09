package embed_test

import (
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// NOTE ON COVERAGE: PutJSONDocument writes through the storage adapter directly, so this test
// exercises the *replace* path on both sides and passes trivially. The path that merges is the
// transaction engine (KdbServerRuntime.Commit), which is what the wire layer and the control plane
// use - see TestEngineWriteAndReplayAgree in the control package, which drives that one over a
// file-backed runtime and reopens it.
//
// A write and a replay of that write must produce the same document. They are two readings of the
// same WriteOp, and the two paths read it differently:
//
//   - the live write path merges the patch over whatever is stored
//     (transaction/default_engine.go: baseDoc.Merge(o.Patch)),
//   - replay treats the patch as the whole document
//     (embed/delta_replay.go: FromJSONWithID(o.DocID, o.Patch) then PutDocument).
//
// Those agree only when every patch happens to be a complete document. This checks the case where
// one is not: a second write that names fewer keys than the first.
func TestWriteAndReplayAgreeOnAPartialPatch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")

	var live string
	func() {
		rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer rt.Close()

		// Both writes address the same document: a natural-key "id" derives a stable UUID.
		if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a","x":1}`); err != nil {
			t.Fatal(err)
		}
		// Names only y. Under merge semantics x survives; read as a whole document, x is gone.
		res, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a","y":2}`)
		if err != nil {
			t.Fatal(err)
		}
		body, err := readDocBody(rt, res.DocID.String())
		if err != nil {
			t.Fatal(err)
		}
		live = renderKeys(body)
	}()

	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rt.Close()

	// Derive the id the way the writer did rather than enumerating the tree: after a reopen the
	// head tree is not resident, and resolving it is a separate concern from this test.
	docID, _, err := document.ResolveID(`{"id":"a"}`)
	if err != nil {
		t.Fatal(err)
	}
	body, err := readDocBody(rt, docID.String())
	if err != nil {
		t.Fatal(err)
	}
	replayed := renderKeys(body)

	if live != replayed {
		t.Fatalf(
			"the same document reads differently before and after a reopen.\n"+
				"  live (write path, merges the patch):      %s\n"+
				"  replayed (treats the patch as the whole):  %s\n"+
				"A commit records the patch it was given, so a write naming fewer keys than the "+
				"document already has is reconstructed as a smaller document than the one that was "+
				"acknowledged.", live, replayed)
	}
}

// readDocBody lives in crash_truncation_test.go: both tests read a document at head the same way,
// and one helper in the package is one definition of what "the document at head" means.

func renderKeys(body map[string]any) string {
	if body == nil {
		return "<absent>"
	}
	keys := make([]string, 0, len(body))
	for k := range body {
		keys = append(keys, k)
	}
	sortStrings(keys)
	out := "{"
	for i, k := range keys {
		if i > 0 {
			out += ","
		}
		out += k
	}
	return out + "}"
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
