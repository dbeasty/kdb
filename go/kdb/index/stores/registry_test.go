package stores_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/schema"
)

func mustUUID(t *testing.T, s string) codec.UUID {
	t.Helper()
	id, err := codec.UUIDFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func hashDesc(t *testing.T) index.Descriptor {
	return index.Descriptor{
		IndexID: mustUUID(t, "00000000-0000-4000-8000-000000000101"), NamespaceID: "ns/reg",
		FieldName: "status", Fields: []string{"status"}, Type: index.IndexTypeHash,
		Options: map[string]string{index.OptionIndexName: "tasks_status"},
	}
}

func textDesc(t *testing.T) index.Descriptor {
	return index.Descriptor{
		IndexID: mustUUID(t, "00000000-0000-4000-8000-000000000102"), NamespaceID: "ns/reg",
		FieldName: "title", Fields: []string{"title", "description"}, Type: index.IndexTypeFullText,
		Options: map[string]string{index.OptionIndexName: "tasks_text", index.OptionWeights: "title=3"},
	}
}

func vecDesc(t *testing.T) index.Descriptor {
	return index.Descriptor{
		IndexID: mustUUID(t, "00000000-0000-4000-8000-000000000103"), NamespaceID: "ns/reg",
		FieldName: "embedding", Fields: []string{"embedding"}, Type: index.IndexTypeVector,
		Options: map[string]string{index.OptionIndexName: "tasks_vec", index.OptionDimensions: "3", index.OptionMetric: "cosine"},
	}
}

func newRegistry(t *testing.T) (*index.Registry, *dag.InMemoryCommitDag, codec.Hash) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag("ns/reg")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	r := index.NewRegistry("ns/reg", d, stores.NewFactory(d, stores.Options{}))
	for _, desc := range []index.Descriptor{hashDesc(t), textDesc(t), vecDesc(t)} {
		if _, err := r.Add(desc); err != nil {
			t.Fatal(err)
		}
	}
	return r, d, head
}

// TestFactoryDispatchesOnIndexType: each descriptor type must produce its own implementation,
// since the registry is what the commit path and the planner both go through.
func TestFactoryDispatchesOnIndexType(t *testing.T) {
	r, _, _ := newRegistry(t)
	for _, tc := range []struct {
		name string
		typ  index.IndexType
	}{
		{"tasks_status", index.IndexTypeHash},
		{"tasks_text", index.IndexTypeFullText},
		{"tasks_vec", index.IndexTypeVector},
	} {
		s, ok := r.ByName(tc.name)
		if !ok {
			t.Fatalf("%s not registered", tc.name)
		}
		if s.Descriptor().Type != tc.typ {
			t.Errorf("%s has type %s, want %s", tc.name, s.Descriptor().Type, tc.typ)
		}
	}
}

// TestResolveByNameOrFirstField is the SQL rule (§9.1): MATCH and SIMILARITY accept either an
// index name or the field the index leads with.
func TestResolveByNameOrFirstField(t *testing.T) {
	r, _, _ := newRegistry(t)
	if _, ok := r.Resolve("tasks_text", index.IndexTypeFullText); !ok {
		t.Error("resolve by name failed")
	}
	if _, ok := r.Resolve("title", index.IndexTypeFullText); !ok {
		t.Error("resolve by first field failed")
	}
	if _, ok := r.Resolve("description", index.IndexTypeFullText); ok {
		t.Error("a non-leading field must not resolve")
	}
	if _, ok := r.Resolve("tasks_text", index.IndexTypeVector); ok {
		t.Error("the type must match")
	}
}

// TestApplyWriteFansOutToEveryStore: one document update feeds the hash, full-text and vector
// indexes at once, each extracting its own key from the same JSON.
func TestApplyWriteFansOutToEveryStore(t *testing.T) {
	r, _, head := newRegistry(t)
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	body := `{"status":"open","title":"deploy staging","description":"ship it","embedding":[1,0,0]}`
	hints, err := r.ApplyWrite(docID, head, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(hints) != 3 {
		t.Fatalf("got %d hints, want one per index", len(hints))
	}

	hash, _ := r.ByName("tasks_status")
	ids, err := hash.Lookup(index.StringKey{Value: "open"}, nil)
	if err != nil || len(ids) != 1 || ids[0] != docID {
		t.Errorf("hash lookup gave %v (err %v)", ids, err)
	}
	text, _ := r.ByName("tasks_text")
	hits, err := text.Search("deploy", nil, 0)
	if err != nil || len(hits) != 1 || hits[0].DocID != docID {
		t.Errorf("full-text search gave %v (err %v)", hits, err)
	}
	vec, _ := r.ByName("tasks_vec")
	near, err := vec.NearestNeighbours([]float32{1, 0, 0}, 5, nil)
	if err != nil || len(near) != 1 || near[0].DocID != docID {
		t.Errorf("vector search gave %v (err %v)", near, err)
	}
}

// TestPrepareRejectsBeforeAnyStoreMutates is the §10 contract: a document whose vector has the
// wrong dimension fails validation, and because validation precedes every apply, no index -
// not even the hash index that would have accepted the document - is left holding it.
func TestPrepareRejectsBeforeAnyStoreMutates(t *testing.T) {
	r, _, head := newRegistry(t)
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	body := `{"status":"open","title":"deploy","embedding":[1,0]}`

	_, err := r.PrepareWrite(docID, body)
	if err == nil {
		t.Fatal("expected the wrong-length vector to fail preparation")
	}
	var mismatch *index.DimensionMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error %v is not a dimension mismatch", err)
	}

	hash, _ := r.ByName("tasks_status")
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, nil); len(ids) != 0 {
		t.Errorf("the hash index must not hold the rejected document: %v", ids)
	}
	text, _ := r.ByName("tasks_text")
	if hits, _ := text.Search("deploy", nil, 0); len(hits) != 0 {
		t.Errorf("the full-text index must not hold the rejected document: %v", hits)
	}
	if _, ok := r.Head(); ok {
		t.Error("a rejected write must not advance the registry head")
	}

	// The same document with a correct vector applies to everything.
	if _, err := r.ApplyWrite(docID, head, `{"status":"open","title":"deploy","embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, nil); len(ids) != 1 {
		t.Errorf("after the valid write the hash index holds it: %v", ids)
	}
}

// TestApplyDeleteRemovesFromEveryStore, and an as-of read before the tombstone still sees the
// document in each.
func TestApplyDeleteRemovesFromEveryStore(t *testing.T) {
	r, d, genesis := newRegistry(t)
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if _, err := r.ApplyWrite(docID, genesis, `{"status":"open","title":"deploy","embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	second := appendCommit(t, d, genesis)
	if _, err := r.ApplyDelete(docID, second); err != nil {
		t.Fatal(err)
	}

	hash, _ := r.ByName("tasks_status")
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, nil); len(ids) != 0 {
		t.Errorf("hash index at head: %v", ids)
	}
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, &genesis); len(ids) != 1 {
		t.Errorf("hash index as of genesis must still see it: %v", ids)
	}
	text, _ := r.ByName("tasks_text")
	if hits, _ := text.Search("deploy", nil, 0); len(hits) != 0 {
		t.Errorf("full-text at head: %v", hits)
	}
	if hits, _ := text.Search("deploy", &genesis, 0); len(hits) != 1 {
		t.Errorf("full-text as of genesis: %v", hits)
	}
	vec, _ := r.ByName("tasks_vec")
	if near, _ := vec.NearestNeighbours([]float32{1, 0, 0}, 5, nil); len(near) != 0 {
		t.Errorf("vector at head: %v", near)
	}
}

// TestUpdatedKeyLeavesTheOldBucket: a document whose indexed value changed must not still be
// findable under its previous key at head.
func TestUpdatedKeyLeavesTheOldBucket(t *testing.T) {
	r, d, genesis := newRegistry(t)
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if _, err := r.ApplyWrite(docID, genesis, `{"status":"open","title":"a","embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	second := appendCommit(t, d, genesis)
	if _, err := r.ApplyWrite(docID, second, `{"status":"done","title":"a","embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	hash, _ := r.ByName("tasks_status")
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, nil); len(ids) != 0 {
		t.Errorf("the old key must be empty at head: %v", ids)
	}
	if ids, _ := hash.Lookup(index.StringKey{Value: "done"}, nil); len(ids) != 1 {
		t.Errorf("the new key holds the document: %v", ids)
	}
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, &genesis); len(ids) != 1 {
		t.Errorf("as of genesis the old key still holds it: %v", ids)
	}
}

// TestMultikeyArrayFieldIndexesEveryElement: an array value files the document under each of
// its elements, which is what makes `tags = 'x'` a membership test (§2).
func TestMultikeyArrayFieldIndexesEveryElement(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/multikey")
	head, _ := d.Head()
	r := index.NewRegistry("ns/multikey", d, stores.NewFactory(d, stores.Options{}))
	desc := index.Descriptor{
		IndexID: mustUUID(t, "00000000-0000-4000-8000-000000000201"), FieldName: "tags",
		Fields: []string{"tags"}, Type: index.IndexTypeHash,
		Options: map[string]string{index.OptionIndexName: "tags_hash"},
	}
	store, err := r.Add(desc)
	if err != nil {
		t.Fatal(err)
	}
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if _, err := r.ApplyWrite(docID, head, `{"tags":["ops","staging"]}`); err != nil {
		t.Fatal(err)
	}
	for _, tag := range []string{"ops", "staging"} {
		ids, err := store.Lookup(index.StringKey{Value: tag}, nil)
		if err != nil || len(ids) != 1 {
			t.Errorf("lookup %q gave %v (err %v)", tag, ids, err)
		}
	}
}

// TestRebuildFromScanReindexesAtHead is the §6.5 recovery path: a store cleared (or opened
// stale) is refilled from a head scan and answers as though it had been maintained.
func TestRebuildFromScanReindexesAtHead(t *testing.T) {
	r, _, head := newRegistry(t)
	docs := map[string]string{
		"00000000-0000-4000-8000-000000000001": `{"status":"open","title":"deploy staging","embedding":[1,0,0]}`,
		"00000000-0000-4000-8000-000000000002": `{"status":"done","title":"write notes","embedding":[0,1,0]}`,
	}
	stats, err := r.RebuildFromScan(func(yield func(codec.UUID, string) bool) error {
		for id, body := range docs {
			if !yield(mustUUID(t, id), body) {
				return nil
			}
		}
		return nil
	}, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 2 || stats.Skipped != 0 {
		t.Fatalf("stats = %+v, want 2 documents and no skips", stats)
	}
	text, _ := r.ByName("tasks_text")
	if hits, _ := text.Search("deploy", nil, 0); len(hits) != 1 {
		t.Errorf("full-text after rebuild: %v", hits)
	}
	hash, _ := r.ByName("tasks_status")
	if ids, _ := hash.Lookup(index.StringKey{Value: "done"}, nil); len(ids) != 1 {
		t.Errorf("hash after rebuild: %v", ids)
	}
	if h, ok := r.Head(); !ok || h != head {
		t.Error("a rebuild advances the registry head to the scanned commit")
	}
}

// TestRebuildSkipsUnindexableDocuments: a stored document with a bad vector cannot reject a
// commit that already happened, so the rebuild leaves it out and reports it instead of failing.
func TestRebuildSkipsUnindexableDocuments(t *testing.T) {
	r, _, head := newRegistry(t)
	stats, err := r.RebuildFromScan(func(yield func(codec.UUID, string) bool) error {
		yield(mustUUID(t, "00000000-0000-4000-8000-000000000001"), `{"status":"open","title":"fine","embedding":[1,0,0]}`)
		yield(mustUUID(t, "00000000-0000-4000-8000-000000000002"), `{"status":"open","title":"bad vector","embedding":[9]}`)
		return nil
	}, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 2 || stats.Skipped != 1 || stats.FirstError == nil {
		t.Fatalf("stats = %+v, want one skipped document with an error", stats)
	}
	vec, _ := r.ByName("tasks_vec")
	if near, _ := vec.NearestNeighbours([]float32{1, 0, 0}, 5, nil); len(near) != 1 {
		t.Errorf("only the valid vector is indexed: %v", near)
	}
	text, _ := r.ByName("tasks_text")
	if hits, _ := text.Search("bad", nil, 0); len(hits) != 1 {
		t.Errorf("the document is still indexed by the stores that could take it: %v", hits)
	}
}

// TestSaveAllAndOpenRestoresFreshSnapshots: catalog plus snapshots round-trip through a data
// directory, and a registry opened at the same head needs no rebuild.
func TestSaveAllAndOpenRestoresFreshSnapshots(t *testing.T) {
	r, d, head := newRegistry(t)
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if _, err := r.ApplyWrite(docID, head, `{"status":"open","title":"deploy staging","embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "index")
	if err := r.SaveAll(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, index.CatalogFileName)); err != nil {
		t.Fatalf("catalog not written: %v", err)
	}

	reopened, report, err := stores.Open(dir, "ns/reg", d, stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !report.CatalogFound {
		t.Fatal("catalog not found on open")
	}
	if len(report.Stale) != 0 {
		t.Fatalf("snapshots taken at the current head must open fresh, stale: %v", report.Stale)
	}
	if len(report.Fresh) != 3 {
		t.Fatalf("got %d fresh stores, want 3", len(report.Fresh))
	}
	text, ok := reopened.ByName("tasks_text")
	if !ok {
		t.Fatal("tasks_text missing after reopen")
	}
	if hits, _ := text.Search("deploy", nil, 0); len(hits) != 1 || hits[0].DocID != docID {
		t.Errorf("the reopened full-text index answers the query: %v", hits)
	}
	hash, _ := reopened.ByName("tasks_status")
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, nil); len(ids) != 1 {
		t.Errorf("the reopened hash index answers the lookup: %v", ids)
	}
}

// TestOpenReportsStaleSnapshotsAfterANewCommit: the head moved past the snapshot, so every
// store is reported stale and must be rebuilt by scan before it is trusted (§6.5).
func TestOpenReportsStaleSnapshotsAfterANewCommit(t *testing.T) {
	r, d, head := newRegistry(t)
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if _, err := r.ApplyWrite(docID, head, `{"status":"open","title":"deploy","embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "index")
	if err := r.SaveAll(dir); err != nil {
		t.Fatal(err)
	}
	newHead := appendCommit(t, d, head)

	reopened, report, err := stores.Open(dir, "ns/reg", d, stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Stale) != 3 || len(report.Fresh) != 0 {
		t.Fatalf("after a new commit every snapshot is stale: fresh %d, stale %d", len(report.Fresh), len(report.Stale))
	}
	text, _ := reopened.ByName("tasks_text")
	if hits, _ := text.Search("deploy", nil, 0); len(hits) != 0 {
		t.Errorf("a stale store opens empty, awaiting rebuild: %v", hits)
	}
	if _, err := reopened.RebuildFromScan(func(yield func(codec.UUID, string) bool) error {
		yield(docID, `{"status":"open","title":"deploy","embedding":[1,0,0]}`)
		return nil
	}, newHead, report.StaleIDs()); err != nil {
		t.Fatal(err)
	}
	if hits, _ := text.Search("deploy", nil, 0); len(hits) != 1 {
		t.Errorf("after the rebuild the index answers again: %v", hits)
	}
}

// TestOpenOnAnEmptyDirectoryYieldsAnEmptyRegistry - a fresh namespace has no catalog, which
// is not an error.
func TestOpenOnAnEmptyDirectoryYieldsAnEmptyRegistry(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/empty")
	r, report, err := stores.Open(filepath.Join(t.TempDir(), "missing"), "ns/empty", d, stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.CatalogFound {
		t.Error("no catalog should have been found")
	}
	if len(r.Descriptors()) != 0 {
		t.Errorf("registry should be empty: %v", r.Descriptors())
	}
}

// TestDuplicateIndexRegistrationIsRejected: two indexes cannot share a name, an id, or a
// (first field, type) pair, since all three are lookup keys.
func TestDuplicateIndexRegistrationIsRejected(t *testing.T) {
	r, _, _ := newRegistry(t)
	if _, err := r.Add(hashDesc(t)); err == nil {
		t.Error("re-adding the same descriptor must fail")
	}
	other := hashDesc(t)
	other.IndexID = mustUUID(t, "00000000-0000-4000-8000-000000000199")
	if _, err := r.Add(other); err == nil {
		t.Error("a duplicate index name must fail")
	}
	sameField := other
	sameField.Options = map[string]string{index.OptionIndexName: "another_name"}
	if _, err := r.Add(sameField); err == nil {
		t.Error("a second hash index on the same field must fail")
	}
}

// TestDescriptorsFromSchemaInferTypes mirrors Kotlin's inferIndexType: ordered numeric and
// timestamp fields become BTREE indexes, everything else HASH, and only indexed fields count.
func TestDescriptorsFromSchemaInferTypes(t *testing.T) {
	fields := []schema.Field{
		schema.MustField("title", schema.StringType{}, false, true, false),
		schema.MustField("priority", schema.Int32Type{}, false, true, false),
		schema.MustField("score", schema.Float64Type{}, false, true, false),
		schema.MustField("due", schema.TimestampType{}, false, true, false),
		schema.MustField("count", schema.Int64Type{}, false, true, false),
		schema.MustField("flag", schema.BoolType{}, false, true, false),
		schema.MustField("notes", schema.StringType{}, false, false, false),
	}
	sch, err := schema.Build(fields, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	descs := index.DescriptorsFromSchema("ns/schema", sch)
	if len(descs) != 6 {
		t.Fatalf("got %d descriptors, want one per indexed field (6)", len(descs))
	}
	want := map[string]index.IndexType{
		"title": index.IndexTypeHash, "priority": index.IndexTypeBTree, "score": index.IndexTypeBTree,
		"due": index.IndexTypeBTree, "count": index.IndexTypeBTree, "flag": index.IndexTypeHash,
	}
	for _, d := range descs {
		if d.Type != want[d.FieldName] {
			t.Errorf("%s got %s, want %s", d.FieldName, d.Type, want[d.FieldName])
		}
		if d.NamespaceID != "ns/schema" {
			t.Errorf("%s namespace = %q", d.FieldName, d.NamespaceID)
		}
		if d.Options[index.OptionFieldType] == "" {
			t.Errorf("%s carries no field_type option", d.FieldName)
		}
		if d.FieldName == "notes" {
			t.Error("an unindexed field must not produce a descriptor")
		}
	}
	// The derived id is stable, so a catalog entry survives a restart.
	again := index.DescriptorsFromSchema("ns/schema", sch)
	for i := range descs {
		if descs[i].IndexID != again[i].IndexID {
			t.Errorf("%s: derived index id is not stable", descs[i].FieldName)
		}
	}
}

// TestSchemaDerivedTimestampFieldProducesTypedKeys: the field_type option is what makes an
// RFC 3339 string index as a TimestampKey, so a range query over it orders chronologically
// rather than lexicographically.
func TestSchemaDerivedTimestampFieldProducesTypedKeys(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/typed")
	head, _ := d.Head()
	r := index.NewRegistry("ns/typed", d, stores.NewFactory(d, stores.Options{}))
	sch, err := schema.Build([]schema.Field{
		schema.MustField("due", schema.TimestampType{}, false, true, false),
	}, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	descs := index.DescriptorsFromSchema("ns/typed", sch)
	store, err := r.Add(descs[0])
	if err != nil {
		t.Fatal(err)
	}
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if _, err := r.ApplyWrite(docID, head, `{"due":"2026-09-05T10:00:00Z"}`); err != nil {
		t.Fatal(err)
	}
	ts, err := codec.TimestampFromISO8601("2026-09-05T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	ids, err := store.Lookup(index.TimestampKey{EpochMillis: ts.EpochMillis}, nil)
	if err != nil || len(ids) != 1 {
		t.Fatalf("timestamp lookup gave %v (err %v)", ids, err)
	}
}

// TestApplyHintsReplicatesUpdates: a follower applies the hints a leader produced and ends up
// with the same index content (§10).
func TestApplyHintsReplicatesUpdates(t *testing.T) {
	leader, d, head := newRegistry(t)
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	hints, err := leader.ApplyWrite(docID, head, `{"status":"open","title":"deploy","embedding":[1,0,0]}`)
	if err != nil {
		t.Fatal(err)
	}

	follower := index.NewRegistry("ns/reg", d, stores.NewFactory(d, stores.Options{}))
	for _, desc := range []index.Descriptor{hashDesc(t), textDesc(t), vecDesc(t)} {
		if _, err := follower.Add(desc); err != nil {
			t.Fatal(err)
		}
	}
	if err := follower.ApplyHints(hints); err != nil {
		t.Fatal(err)
	}
	hash, _ := follower.ByName("tasks_status")
	if ids, _ := hash.Lookup(index.StringKey{Value: "open"}, nil); len(ids) != 1 {
		t.Errorf("replicated hash lookup: %v", ids)
	}
	text, _ := follower.ByName("tasks_text")
	if hits, _ := text.Search("deploy", nil, 0); len(hits) != 1 {
		t.Errorf("replicated full-text search: %v", hits)
	}
	vec, _ := follower.ByName("tasks_vec")
	if near, _ := vec.NearestNeighbours([]float32{1, 0, 0}, 5, nil); len(near) != 1 {
		t.Errorf("replicated vector search: %v", near)
	}
}

func appendCommit(t *testing.T, d *dag.InMemoryCommitDag, parent codec.Hash) codec.Hash {
	t.Helper()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{ID: txID, BaseVersion: parent, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	c, err := d.AppendCommit(tx, parent, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return c.Hash
}
