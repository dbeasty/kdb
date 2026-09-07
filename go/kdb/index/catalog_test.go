package index_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
)

// TestSnapshotPreservesEventOrder guards the ordering bug a per-kind snapshot layout invites:
// emitting every put before every delete replays a document's own update backwards (the
// delete of the old key lands after the put of the new one), so the restored index has
// pruned exactly the entry it just wrote.
func TestSnapshotPreservesEventOrder(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("ns/snapshot-order")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	eng := index.NewVersionedEngine(d)
	docID, _ := codec.RandomUUID()
	oldKey := index.StringKey{Value: "open"}
	newKey := index.StringKey{Value: "done"}

	// The shape every re-index has: drop the previous key, then file the new one.
	if err := eng.Put(index.Entry{DocID: docID, Key: oldKey, CommitHash: head}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Delete(docID, head); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put(index.Entry{DocID: docID, Key: newKey, CommitHash: head}); err != nil {
		t.Fatal(err)
	}

	snap, err := eng.SnapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	restored := index.NewVersionedEngine(d)
	if err := restored.RestoreSnapshotBytes(snap); err != nil {
		t.Fatal(err)
	}
	ids, err := restored.Lookup(newKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != docID {
		t.Fatalf("restored lookup of the current key gave %v, want the document", ids)
	}
	if ids, _ := restored.Lookup(oldKey, nil); len(ids) != 0 {
		t.Errorf("the superseded key must stay empty after restore: %v", ids)
	}
}

// TestCatalogRoundTripPreservesOptions: the catalog is what makes indexes survive a restart
// (§9.2), so every descriptor field - options included - must come back unchanged.
func TestCatalogRoundTripPreservesOptions(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/catalog")
	head, _ := d.Head()
	id1, _ := codec.RandomUUID()
	id2, _ := codec.RandomUUID()
	cat := index.Catalog{
		NamespaceID: "ns/catalog",
		Indexes: []index.Descriptor{
			{
				IndexID: id1, NamespaceID: "ns/catalog", FieldName: "title",
				Fields: []string{"title", "description", "tags"}, Type: index.IndexTypeFullText,
				SchemaVersion: 3, CreatedAtHash: head,
				Options: map[string]string{index.OptionIndexName: "tasks_text", index.OptionWeights: "title=3,tags=2"},
			},
			{
				IndexID: id2, NamespaceID: "ns/catalog", FieldName: "embedding",
				Fields: []string{"embedding"}, Type: index.IndexTypeVector, Unique: false,
				CreatedAtHash: head,
				Options: map[string]string{
					index.OptionIndexName: "tasks_vec", index.OptionDimensions: "768",
					index.OptionMetric: "cosine", index.OptionM: "16",
					index.OptionEfConstruction: "200", index.OptionEfSearch: "64",
				},
			},
		},
	}
	dir := t.TempDir()
	if err := index.SaveCatalog(dir, cat); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, index.CatalogFileName)); err != nil {
		t.Fatalf("catalog.json not written: %v", err)
	}
	got, err := index.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.NamespaceID != cat.NamespaceID || len(got.Indexes) != 2 {
		t.Fatalf("loaded %+v", got)
	}
	byID := map[codec.UUID]index.Descriptor{}
	for _, d := range got.Indexes {
		byID[d.IndexID] = d
	}
	for _, want := range cat.Indexes {
		g, ok := byID[want.IndexID]
		if !ok {
			t.Fatalf("index %s missing after reload", want.IndexID)
		}
		if g.Type != want.Type || g.FieldName != want.FieldName || g.SchemaVersion != want.SchemaVersion {
			t.Errorf("index %s: got %+v, want %+v", want.IndexID, g, want)
		}
		if g.CreatedAtHash != want.CreatedAtHash {
			t.Errorf("index %s: createdAt hash lost", want.IndexID)
		}
		if len(g.Fields) != len(want.Fields) {
			t.Errorf("index %s: fields %v, want %v", want.IndexID, g.Fields, want.Fields)
		}
		for k, v := range want.Options {
			if g.Options[k] != v {
				t.Errorf("index %s: option %s = %q, want %q", want.IndexID, k, g.Options[k], v)
			}
		}
	}
}

// TestCatalogBytesAreDeterministic: the file is a function of its content, so an unchanged
// catalog rewrites byte-identically rather than churning on map order.
func TestCatalogBytesAreDeterministic(t *testing.T) {
	id1, _ := codec.RandomUUID()
	id2, _ := codec.RandomUUID()
	cat := index.Catalog{NamespaceID: "ns/det", Indexes: []index.Descriptor{
		{IndexID: id1, FieldName: "a", Type: index.IndexTypeHash, Options: map[string]string{"z": "1", "a": "2", "m": "3"}},
		{IndexID: id2, FieldName: "b", Type: index.IndexTypeBTree, Options: map[string]string{"b": "1"}},
	}}
	first, err := index.MarshalCatalog(cat)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		again, err := index.MarshalCatalog(cat)
		if err != nil {
			t.Fatal(err)
		}
		if string(again) != string(first) {
			t.Fatalf("catalog bytes changed between marshals:\n%s\n%s", first, again)
		}
	}
	// Reordering the input must not change the file either: indexes are sorted by id.
	swapped := index.Catalog{NamespaceID: "ns/det", Indexes: []index.Descriptor{cat.Indexes[1], cat.Indexes[0]}}
	other, err := index.MarshalCatalog(swapped)
	if err != nil {
		t.Fatal(err)
	}
	if string(other) != string(first) {
		t.Error("catalog bytes depend on the order the descriptors were listed in")
	}
}

// TestSaveCatalogIsAtomic: the write goes through a temp file and a rename, so a reader never
// sees a half-written catalog and the directory holds no leftovers.
func TestSaveCatalogIsAtomic(t *testing.T) {
	dir := t.TempDir()
	id, _ := codec.RandomUUID()
	cat := index.Catalog{NamespaceID: "ns/atomic", Indexes: []index.Descriptor{{IndexID: id, FieldName: "a", Type: index.IndexTypeHash}}}
	for i := 0; i < 3; i++ {
		if err := index.SaveCatalog(dir, cat); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != index.CatalogFileName {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("directory holds %v, want only %s", names, index.CatalogFileName)
	}
}

// TestLoadCatalogRejectsCorruptFiles: a garbled or future-versioned catalog is an error the
// caller can see, not a silently empty index set.
func TestLoadCatalogRejectsCorruptFiles(t *testing.T) {
	for name, content := range map[string]string{
		"not json":       "{not json",
		"wrong version":  `{"formatVersion":99,"namespaceId":"x","indexes":[]}`,
		"bad index type": `{"formatVersion":1,"namespaceId":"x","indexes":[{"indexId":"00000000-0000-4000-8000-000000000001","type":"QUANTUM"}]}`,
		"bad uuid":       `{"formatVersion":1,"namespaceId":"x","indexes":[{"indexId":"nope","type":"HASH"}]}`,
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, index.CatalogFileName), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := index.LoadCatalog(dir); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// TestIndexTypeNamesRoundTrip: the catalog stores types by name, so the mapping both ways is
// part of the on-disk contract.
func TestIndexTypeNamesRoundTrip(t *testing.T) {
	for _, typ := range []index.IndexType{index.IndexTypeHash, index.IndexTypeBTree, index.IndexTypeFullText, index.IndexTypeVector} {
		got, err := index.ParseIndexType(typ.String())
		if err != nil || got != typ {
			t.Errorf("%s round-tripped to %v (err %v)", typ, got, err)
		}
	}
	if _, err := index.ParseIndexType("unknown"); err == nil {
		t.Error("an unknown type name must be an error")
	}
}

// TestFieldWeightsParseFromOptions: weights default to 1 and only name indexed fields, since
// a typo in an index definition would otherwise silently score a field at the wrong weight.
func TestFieldWeightsParseFromOptions(t *testing.T) {
	desc := index.Descriptor{
		FieldName: "title", Fields: []string{"title", "description", "tags"},
		Options: map[string]string{index.OptionWeights: "title=3,tags=2"},
	}
	w, err := desc.FieldWeights()
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != 3 || w[0] != 3 || w[1] != 1 || w[2] != 2 {
		t.Fatalf("weights = %v, want [3 1 2]", w)
	}
	bad := desc
	bad.Options = map[string]string{index.OptionWeights: "nosuchfield=2"}
	if _, err := bad.FieldWeights(); err == nil {
		t.Error("a weight naming an unindexed field must be an error")
	}
	malformed := desc
	malformed.Options = map[string]string{index.OptionWeights: "title"}
	if _, err := malformed.FieldWeights(); err == nil {
		t.Error("a malformed weight entry must be an error")
	}
}

// TestManifestMismatchIsRejected: restoring one index's snapshot into another would quietly
// serve the wrong postings, so the manifest identifies its index.
func TestManifestMismatchIsRejected(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/manifest")
	head, _ := d.Head()
	idA, _ := codec.RandomUUID()
	idB, _ := codec.RandomUUID()
	storeA := index.NewMemoryStore(index.Descriptor{IndexID: idA, FieldName: "a", Type: index.IndexTypeHash}, d)
	docID, _ := codec.RandomUUID()
	if err := storeA.Put(index.Entry{DocID: docID, Key: index.StringKey{Value: "k"}, CommitHash: head}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "idx")
	if err := index.SaveStoreSnapshot(dir, storeA, head); err != nil {
		t.Fatal(err)
	}
	storeB := index.NewMemoryStore(index.Descriptor{IndexID: idB, FieldName: "b", Type: index.IndexTypeHash}, d)
	if _, err := index.LoadStoreSnapshot(dir, storeB); err == nil {
		t.Error("loading another index's snapshot must fail")
	}
	// The rightful owner still loads it.
	reopened := index.NewMemoryStore(index.Descriptor{IndexID: idA, FieldName: "a", Type: index.IndexTypeHash}, d)
	if _, err := index.LoadStoreSnapshot(dir, reopened); err != nil {
		t.Fatalf("the owning index must load its own snapshot: %v", err)
	}
}

// TestReadManifestOnAnEmptyDirectoryReportsNoSnapshot, which is how open distinguishes "never
// persisted" (rebuild) from a real I/O failure.
func TestReadManifestOnAnEmptyDirectoryReportsNoSnapshot(t *testing.T) {
	if _, err := index.ReadManifest(t.TempDir()); err != index.ErrNoSnapshot {
		t.Fatalf("got %v, want index.ErrNoSnapshot", err)
	}
}

func appendCatalogCommit(t *testing.T, d *dag.InMemoryCommitDag, parent codec.Hash) codec.Hash {
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

// TestStoreSnapshotManifestNamesItsHead: staleness detection is a comparison against the DAG
// head, so the manifest must record the commit the snapshot was taken at.
func TestStoreSnapshotManifestNamesItsHead(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/head")
	head, _ := d.Head()
	id, _ := codec.RandomUUID()
	store := index.NewMemoryStore(index.Descriptor{IndexID: id, FieldName: "a", Type: index.IndexTypeHash}, d)
	dir := filepath.Join(t.TempDir(), "idx")
	if err := index.SaveStoreSnapshot(dir, store, head); err != nil {
		t.Fatal(err)
	}
	m, err := index.ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.HeadCommitHex != head.Hex() {
		t.Errorf("manifest head = %s, want %s", m.HeadCommitHex, head.Hex())
	}
	newHead := appendCatalogCommit(t, d, head)
	if m.HeadCommitHex == newHead.Hex() {
		t.Error("the manifest must not follow the DAG head; that is what makes it detectably stale")
	}
}
