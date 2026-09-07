package embed_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// growingStore writes one document that gains an entry per rewrite, under
// the given strategy, and returns the data directory's size along with
// each version's commit and text.
func growingStore(t *testing.T, s storage.HistoryStrategy, rewrites int) (int64, []codec.Hash, []string) {
	t.Helper()
	root := t.TempDir()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.HistoryStrategy = s
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	type doc struct {
		ID     string           `json:"id"`
		Events []map[string]any `json:"events"`
	}
	d := doc{ID: retentionDocID}
	blob := strings.Repeat("x", 3000)
	commits := make([]codec.Hash, 0, rewrites)
	texts := make([]string, 0, rewrites)
	for i := 0; i < rewrites; i++ {
		d.Events = append(d.Events, map[string]any{"seq": i, "blob": blob})
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		res, err := embed.PutJSONDocument(rt, "bench/matches", string(b))
		if err != nil {
			t.Fatal(err)
		}
		commits = append(commits, res.Commit)
		texts = append(texts, string(b))
	}
	rt.Close()
	return dataDirBytes(t, root), commits, texts
}

// TestObjectsStrategyStoresEachVersionOnce is the property this exists
// for: choosing the objects strategy must not put a second copy of every
// version on disk.
//
// It used to. The strategy stored each version's text under its content
// hash so a historical read could find it, which meant the same bytes
// lived in the delta log and again in the object store - a 29.5% larger
// data directory on the workload below. What is stored now is where in the
// log the version is: seventeen bytes instead of the document. The journal
// stays the one copy, which is what a repair reads and what a peer
// receives, and there is nothing to reconcile it against.
func TestObjectsStrategyStoresEachVersionOnce(t *testing.T) {
	const rewrites = 200
	replayBytes, _, _ := growingStore(t, storage.HistoryStrategyReplay, rewrites)
	objectsBytes, _, _ := growingStore(t, storage.HistoryStrategyObjects, rewrites)

	overhead := float64(objectsBytes) / float64(replayBytes)
	t.Logf("data directory: replay %.2f MB, objects %.2f MB (%.0f%% larger)",
		float64(replayBytes)/(1024*1024), float64(objectsBytes)/(1024*1024), (overhead-1)*100)

	// What is left is per-entry overhead in the SSTable that holds the tree
	// objects and the location records, not the versions themselves.
	// Storing the versions again measured 1.30 on this workload.
	if overhead > 1.15 {
		t.Fatalf("the objects strategy uses %.2fx the disk of replay; a second copy of each version "+
			"is being stored rather than a pointer to the one in the log", overhead)
	}
}

// TestLocationsResolveHistoricalReads checks the read path the locations
// exist to serve: every version readable at the commit that wrote it, on a
// namespace where none of them are resident any more.
func TestLocationsResolveHistoricalReads(t *testing.T) {
	root := t.TempDir()
	rt := tinyTreeBudgetRuntime(t, root, storage.HistoryStrategyObjects)
	commits, texts := writeVersions(t, rt, 120)
	rt.Close()

	re := tinyTreeBudgetRuntime(t, root, storage.HistoryStrategyObjects)
	defer re.Close()
	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 1, 59, 118, 119} {
		got := readAt(t, re, commits[i], docID)
		if got == nil {
			t.Fatalf("version %d: not readable at its own commit", i)
		}
		if got.JSON != texts[i] {
			t.Fatalf("version %d: wrong content", i)
		}
	}
}
