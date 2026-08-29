package transaction_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Builder and Merge had no tests. Builder is the public way to assemble a transaction, and
// Merge is what runs when two branches are brought back together - the operation whose
// behaviour on a divergent history peer-sync depends on.

// fixedHash builds a well-formed hash with every byte set to fill, for the cases that need a
// hash that is syntactically valid but belongs to nothing.
func fixedHash(t *testing.T, fill byte) codec.Hash {
	t.Helper()
	h, err := codec.HashFromHex(strings.Repeat(fmt.Sprintf("%02x", fill), 32))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// ------------------------------------------------------------------------------ Builder

func TestBuilderCollectsOpsInOrder(t *testing.T) {
	docA, _ := codec.RandomUUID()
	docB, _ := codec.RandomUUID()
	blob := fixedHash(t, 0xab)

	b := &transaction.Builder{NamespaceID: "ns"}
	b.Write(docA, `{"v":1}`).Delete(docB).FileWrite("assets/logo.png", blob)

	tx, err := b.Build(codec.TimestampFromEpochMicros(1_700_000_000_000_000))
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Operations) != 3 {
		t.Fatalf("got %d ops, want 3", len(tx.Operations))
	}
	// Order matters: a write followed by a delete of the same document is not the same
	// transaction as the reverse, so the builder must not reorder.
	if w, ok := tx.Operations[0].(document.WriteOp); !ok || w.DocID != docA || w.Patch != `{"v":1}` {
		t.Errorf("op 0: %#v", tx.Operations[0])
	}
	if d, ok := tx.Operations[1].(document.DeleteOp); !ok || d.DocID != docB {
		t.Errorf("op 1: %#v", tx.Operations[1])
	}
	if f, ok := tx.Operations[2].(document.FileWriteOp); !ok || f.Path != "assets/logo.png" || f.BlobHash != blob {
		t.Errorf("op 2: %#v", tx.Operations[2])
	}
}

func TestBuilderWriteDocumentUsesTheDocumentsOwnIDAndBody(t *testing.T) {
	doc, err := document.FromJSON(`{"title":"hello"}`)
	if err != nil {
		t.Fatal(err)
	}
	b := &transaction.Builder{NamespaceID: "ns"}
	b.WriteDocument(doc)

	tx, err := b.Build(codec.TimestampFromEpochMicros(1))
	if err != nil {
		t.Fatal(err)
	}
	w, ok := tx.Operations[0].(document.WriteOp)
	if !ok {
		t.Fatalf("op is %T, want WriteOp", tx.Operations[0])
	}
	if w.DocID != doc.ID || w.Patch != doc.JSON {
		t.Fatalf("WriteDocument did not carry the document's own id and body: %#v", w)
	}
}

func TestBuilderCarriesBaseVersionAndAuthorAndMintsAnID(t *testing.T) {
	base := fixedHash(t, 0x11)
	author, _ := codec.RandomUUID()
	b := &transaction.Builder{NamespaceID: "ns", BaseVersion: base, AuthorNodeID: author}

	first, err := b.Build(codec.TimestampFromEpochMicros(5))
	if err != nil {
		t.Fatal(err)
	}
	if first.BaseVersion != base || first.AuthorNodeID != author {
		t.Fatalf("base/author not carried: %#v", first)
	}
	if first.Timestamp.EpochMicros() != 5 {
		t.Fatalf("timestamp is %d, want 5", first.Timestamp.EpochMicros())
	}

	// Each Build is a distinct transaction, so it gets its own id even from the same builder -
	// two transactions sharing an id would be treated as replays of each other.
	second, err := b.Build(codec.TimestampFromEpochMicros(6))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatal("two Builds produced the same transaction id")
	}
}

func TestBuilderBuildWithZeroTimestampUsesNow(t *testing.T) {
	b := &transaction.Builder{NamespaceID: "ns"}
	tx, err := b.Build(codec.Timestamp{})
	if err != nil {
		t.Fatal(err)
	}
	const y2020Micros = 1_577_836_800_000_000
	if tx.Timestamp.EpochMicros() < y2020Micros {
		t.Fatalf("a zero timestamp was not replaced with now: %d", tx.Timestamp.EpochMicros())
	}
}

// Build snapshots the ops rather than aliasing the builder's slice: a transaction already built
// must not change because the builder was used again afterwards.
func TestBuilderBuildSnapshotsOps(t *testing.T) {
	docA, _ := codec.RandomUUID()
	docB, _ := codec.RandomUUID()
	b := &transaction.Builder{NamespaceID: "ns"}
	b.Write(docA, `{"v":1}`)

	built, err := b.Build(codec.TimestampFromEpochMicros(1))
	if err != nil {
		t.Fatal(err)
	}
	b.Write(docB, `{"v":2}`)

	if len(built.Operations) != 1 {
		t.Fatalf("a built transaction grew to %d ops after the builder was reused", len(built.Operations))
	}
}

// The builder guards its slice with a mutex, so concurrent appends must neither race nor lose
// an operation.
func TestBuilderIsSafeForConcurrentAppends(t *testing.T) {
	b := &transaction.Builder{NamespaceID: "ns"}
	const writers = 8
	const each = 50

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				id, _ := codec.RandomUUID()
				b.Write(id, `{}`)
			}
		}()
	}
	wg.Wait()

	tx, err := b.Build(codec.TimestampFromEpochMicros(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Operations) != writers*each {
		t.Fatalf("got %d ops, want %d - an append was lost", len(tx.Operations), writers*each)
	}
}

func TestBuilderSchemaMigrationEncodesTheMigration(t *testing.T) {
	migrationID, _ := codec.RandomUUID()
	field, err := schema.NewField("added", schema.StringType{}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	m := schema.SchemaMigration{
		MigrationID: migrationID,
		FromVersion: 1,
		ToVersion:   2,
		Steps:       []schema.MigrationStep{schema.AddFieldStep{Field: field}},
		Description: "add a field",
	}

	b := &transaction.Builder{NamespaceID: "ns"}
	if err := b.SchemaMigration(m); err != nil {
		t.Fatal(err)
	}
	tx, err := b.Build(codec.TimestampFromEpochMicros(1))
	if err != nil {
		t.Fatal(err)
	}
	op, ok := tx.Operations[0].(document.SchemaMigrationOp)
	if !ok {
		t.Fatalf("op is %T, want SchemaMigrationOp", tx.Operations[0])
	}
	if op.MigrationID != migrationID {
		t.Fatalf("migration id is %s, want %s", op.MigrationID, migrationID)
	}
	if op.MigrationPayload == "" {
		t.Fatal("migration payload is empty")
	}
	// The payload has to decode back to the same migration, or a peer replaying this op gets a
	// different schema than the one that was committed.
	back, err := transaction.DecodeMigration(op.MigrationPayload)
	if err != nil {
		t.Fatalf("payload does not decode: %v", err)
	}
	if back.MigrationID != migrationID || back.FromVersion != 1 || back.ToVersion != 2 {
		t.Fatalf("decoded migration differs: %+v", back)
	}
	if len(back.Steps) != 1 {
		t.Fatalf("decoded %d steps, want 1", len(back.Steps))
	}
}

// ------------------------------------------------------------------------------ Merge

// mergeFixture builds two divergent branches over one shared base and returns their heads.
type mergeFixture struct {
	dag    *dag.InMemoryCommitDag
	store  *mem.InMemoryStorageAdapter
	engine transaction.Engine
	base   codec.Hash
}

func newMergeFixture(t *testing.T, ns string) *mergeFixture {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	base, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	return &mergeFixture{
		dag:    d,
		store:  mem.NewInMemoryStorageAdapter(),
		engine: transaction.NewEngine(transaction.ConflictPolicyStrict, nil),
		base:   base,
	}
}

// commitOn replays a transaction onto an explicit target, so a test can build a second branch
// without the main branch head getting in the way.
func (f *mergeFixture) commitOn(t *testing.T, target codec.Hash, ops ...document.Op) codec.Hash {
	t.Helper()
	res, err := f.engine.Replay(newTx(target, ops...), f.dag, f.store, schema.None(), target, "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := res.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected success building the fixture, got %T", res)
	}
	return success.Commit.Hash
}

func TestMergeBringsTheOtherBranchesWritesIntoTheResult(t *testing.T) {
	f := newMergeFixture(t, "app/merge")
	docLeft, _ := codec.RandomUUID()
	docRight, _ := codec.RandomUUID()

	left := f.commitOn(t, f.base, document.WriteOp{DocID: docLeft, Patch: `{"side":"left"}`})
	right := f.commitOn(t, f.base, document.WriteOp{DocID: docRight, Patch: `{"side":"right"}`})

	res, err := f.engine.Merge(left, right, f.dag, f.store, schema.None(), "merge right into left")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := res.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected a successful merge, got %T", res)
	}

	// The merge commit records both branch heads as parents, which is what lets a peer that has
	// only one side discover the other.
	mergeCommit, ok := f.dag.GetCommit(success.Commit.Hash)
	if !ok {
		t.Fatal("merge commit not in the dag")
	}
	if len(mergeCommit.ParentHashes) != 2 {
		t.Fatalf("merge commit has %d parents, want 2", len(mergeCommit.ParentHashes))
	}
	if mergeCommit.ParentHashes[0] != left || mergeCommit.ParentHashes[1] != right {
		t.Fatal("merge commit's parents are not the two branch heads")
	}

	// Both sides' documents must be readable from the merged tree.
	tree, ok := f.dag.GetDocumentTree(success.Commit.DocumentTreeHash)
	if !ok {
		t.Fatal("merged tree not in the dag")
	}
	for _, id := range []codec.UUID{docLeft, docRight} {
		if !tree.Contains(id) {
			t.Errorf("document %s is missing from the merged tree", id)
		}
	}
}

func TestMergeOfDisjointHistoriesReportsNoMergeBase(t *testing.T) {
	f := newMergeFixture(t, "app/disjoint")
	docID, _ := codec.RandomUUID()
	left := f.commitOn(t, f.base, document.WriteOp{DocID: docID, Patch: `{"v":1}`})

	// A hash from another namespace's history entirely: there is no common ancestor to merge
	// against, and that has to be an error rather than a merge against nothing.
	other := newMergeFixture(t, "app/other")
	stranger := other.commitOn(t, other.base, document.WriteOp{DocID: docID, Patch: `{"v":2}`})

	if _, err := f.engine.Merge(left, stranger, f.dag, f.store, schema.None(), "impossible"); err == nil {
		t.Fatal("merging disjoint histories was accepted")
	}
}

func TestMergeOfAnAlreadyMergedBranchIsAnEmptyMerge(t *testing.T) {
	f := newMergeFixture(t, "app/fastforward")
	docID, _ := codec.RandomUUID()

	base := f.base
	behind := f.commitOn(t, base, document.WriteOp{DocID: docID, Patch: `{"v":1}`})
	ahead := f.commitOn(t, behind, document.WriteOp{DocID: docID, Patch: `{"v":2}`})

	// Merging an ancestor into its own descendant contributes nothing new; it must still
	// produce a well-formed result rather than failing or losing the tip's content.
	res, err := f.engine.Merge(ahead, behind, f.dag, f.store, schema.None(), "merge an ancestor")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := res.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected success, got %T", res)
	}
	tree, ok := f.dag.GetDocumentTree(success.Commit.DocumentTreeHash)
	if !ok {
		t.Fatal("merged tree missing")
	}
	if !tree.Contains(docID) {
		t.Fatal("the tip's document was lost by merging its own ancestor")
	}
}

// ------------------------------------------------------------------------------ Validate

func TestValidateReportsSchemaViolationsWithoutCommitting(t *testing.T) {
	f := newMergeFixture(t, "app/validate")
	field, err := schema.NewField("count", schema.Int32Type{}, true, false, false)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := schema.Build([]schema.Field{field}, 1, codec.TimestampFromEpochMicros(1), "")
	if err != nil {
		t.Fatal(err)
	}

	docID, _ := codec.RandomUUID()
	tx := newTx(f.base, document.WriteOp{DocID: docID, Patch: `{"count":"not a number"}`})

	violations, err := f.engine.Validate(tx, f.dag, f.store, sch)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) == 0 {
		t.Fatal("a document violating its schema was reported as valid")
	}

	// Validate is a dry run: the head must not have moved and the document must not exist.
	head, err := f.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != f.base {
		t.Fatal("Validate advanced the branch head")
	}
}

func TestValidateAcceptsAConformingDocument(t *testing.T) {
	f := newMergeFixture(t, "app/validate-ok")
	field, err := schema.NewField("count", schema.Int32Type{}, true, false, false)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := schema.Build([]schema.Field{field}, 1, codec.TimestampFromEpochMicros(1), "")
	if err != nil {
		t.Fatal(err)
	}

	docID, _ := codec.RandomUUID()
	tx := newTx(f.base, document.WriteOp{DocID: docID, Patch: `{"count":7}`})

	violations, err := f.engine.Validate(tx, f.dag, f.store, sch)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("a conforming document was reported as violating: %+v", violations)
	}
}

func TestValidateRejectsAnUnknownBaseVersion(t *testing.T) {
	f := newMergeFixture(t, "app/validate-base")
	unknown := fixedHash(t, 0xee)
	docID, _ := codec.RandomUUID()
	tx := newTx(unknown, document.WriteOp{DocID: docID, Patch: `{}`})

	if _, err := f.engine.Validate(tx, f.dag, f.store, schema.None()); err == nil {
		t.Fatal("a transaction based on an unknown commit was validated")
	}
}
