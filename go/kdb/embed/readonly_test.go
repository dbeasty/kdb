//go:build unix

package embed_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// writeDoc commits one document through a writable runtime, returning the new head.
func writeDoc(t *testing.T, rt *embed.EmbeddedKdbRuntime, json string) codec.Hash {
	t.Helper()
	head, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	docID, _ := codec.RandomUUID()
	txID, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID:          txID,
		BaseVersion: head,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: json}},
		Timestamp:   codec.TimestampNow(),
	}
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	persisting, ok := rt.DAG.(*embed.PersistingCommitDAG)
	if !ok {
		t.Fatalf("expected a file-backed runtime's PersistingCommitDAG, got %T", rt.DAG)
	}
	res, err := engine.Commit(tx, persisting.Delegate(), rt.Storage, rt.Schema, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := res.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("commit did not succeed: %T", res)
	}
	if err := persisting.Persist(success.Commit); err != nil {
		t.Fatal(err)
	}
	return success.Commit.Hash
}

// TestReadOnlyRuntimeRefusesWritesAndSeesCommittedData is the read-replica contract: a second
// runtime can attach to a data directory a writer already holds, read what the writer has made
// durable, and refuse every write with a reason that names the actual cause.
func TestReadOnlyRuntimeRefusesWritesAndSeesCommittedData(t *testing.T) {
	dataRoot := t.TempDir()

	writer, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	writeDoc(t, writer, `{"marker":"visible-to-reader"}`)

	reader, err := embed.OpenReadOnlyFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatalf("a reader could not attach alongside the writer: %v", err)
	}
	defer reader.Close()

	if !reader.ReadOnly {
		t.Fatal("the read-only runtime does not report itself as read-only")
	}
	if err := reader.AssertWritable(); !errors.Is(err, embed.ErrReadOnly) {
		t.Fatalf("AssertWritable should refuse a read-only runtime, got %v", err)
	}

	head, err := reader.DAG.Head()
	if err != nil {
		t.Fatalf("reader could not resolve a head: %v", err)
	}
	commit, ok := reader.DAG.GetCommit(head)
	if !ok {
		t.Fatal("reader's head names no commit")
	}
	found := false
	err = reader.Storage.ScanDocuments("app/data", commit.DocumentTreeHash, 64, func(batch []document.Document) error {
		for _, d := range batch {
			if strings.Contains(d.JSON, "visible-to-reader") {
				found = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("the reader could not see the writer's committed document")
	}
}

// TestSeveralReadersMayAttachAtOnce: shared means shared. If a second reader were excluded, the
// mode would be a slower exclusive lock rather than a read replica.
func TestSeveralReadersMayAttachAtOnce(t *testing.T) {
	dataRoot := t.TempDir()
	writer, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	writeDoc(t, writer, `{"marker":"seed"}`)
	writer.Close()

	var readers []*embed.EmbeddedKdbRuntime
	for i := 0; i < 3; i++ {
		r, err := embed.OpenReadOnlyFileRuntime(dataRoot, "demo", "app/data", schema.None())
		if err != nil {
			t.Fatalf("reader %d could not attach: %v", i, err)
		}
		readers = append(readers, r)
	}
	for _, r := range readers {
		defer r.Close()
	}
}

// TestSecondWriterStillExcluded: splitting the attach lock from the writer lock must not weaken
// the guarantee that made the directory lock exist. Readers multiply; writers do not.
func TestSecondWriterStillExcluded(t *testing.T) {
	dataRoot := t.TempDir()
	first, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if second, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None()); err == nil {
		second.Close()
		t.Fatal("a second writer opened a directory the first is writing to")
	}

	// A reader, by contrast, attaches fine alongside that same live writer.
	reader, err := embed.OpenReadOnlyFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatalf("a reader was excluded by a live writer: %v", err)
	}
	reader.Close()
}

// TestMaintenanceLockExcludesEveryone pins LockDataDir's contract, which the split must not
// dilute: kdb-inspect's repair/restore paths rewrite segments in place, so they need the whole
// directory to themselves - readers included, since a reader mid-scan would see the rewrite.
func TestMaintenanceLockExcludesEveryone(t *testing.T) {
	dataRoot := t.TempDir()
	seed, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	writeDoc(t, seed, `{"marker":"seed"}`)
	seed.Close()

	release, err := embed.LockDataDir(dataRoot)
	if err != nil {
		t.Fatal(err)
	}

	if rt, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None()); err == nil {
		rt.Close()
		t.Fatal("a writer opened a directory held for maintenance")
	}
	if rt, err := embed.OpenReadOnlyFileRuntime(dataRoot, "demo", "app/data", schema.None()); err == nil {
		rt.Close()
		t.Fatal("a reader opened a directory held for maintenance")
	}

	release()

	// And both work again once maintenance is done.
	rt, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatalf("writer could not reopen after maintenance released: %v", err)
	}
	rt.Close()
}

// TestMaintenanceLockBlockedByALiveReader is the other direction: maintenance must not be able
// to start while anything still has the directory open.
func TestMaintenanceLockBlockedByALiveReader(t *testing.T) {
	dataRoot := t.TempDir()
	seed, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	writeDoc(t, seed, `{"marker":"seed"}`)
	seed.Close()

	reader, err := embed.OpenReadOnlyFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if release, err := embed.LockDataDir(dataRoot); err == nil {
		release()
		t.Fatal("maintenance took the directory while a reader had it open")
	}
}

// TestReaderRefreshPicksUpNewCommits: a reader's view is a snapshot as of its open, and Refresh
// is how it advances. Without this a reader would be permanently frozen at whatever it first saw.
func TestReaderRefreshPicksUpNewCommits(t *testing.T) {
	dataRoot := t.TempDir()
	writer, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	writeDoc(t, writer, `{"marker":"first"}`)

	reader, err := embed.OpenReadOnlyFileRuntime(dataRoot, "demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	before, err := reader.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}

	writeDoc(t, writer, `{"marker":"second"}`)
	if err := writer.Storage.Flush("app/data"); err != nil {
		t.Fatal(err)
	}
	writer.Close()

	if err := reader.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	after, err := reader.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	if after == before {
		t.Fatal("Refresh did not advance the reader past the commit it opened at")
	}
}
