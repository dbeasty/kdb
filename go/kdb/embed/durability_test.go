package embed

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// TestDurabilityModesEndToEnd exercises the whole newly-connected path -
// config-level durability -> FileRuntimeOptions -> StorageEngineConfig ->
// PersistingCommitDAG's commit log - by the only measure that matters: does a
// commit survive reopening the data directory?
//
// Async is checked after a clean Close (which drains the queue), not
// immediately after the write: acknowledging before the flush is exactly what
// distinguishes it from Sync, so asserting durability mid-flight would be
// asserting the opposite of the mode's contract.
func TestDurabilityModesEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		name          string
		durability    storage.Durability
		wantPersisted bool
	}{
		{"sync", storage.DurabilitySync, true},
		{"async survives a clean close", storage.DurabilityAsync, true},
		{"memory-only keeps nothing", storage.DurabilityMemoryOnly, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			ns := "app/data"
			opts := FileRuntimeOptions{Storage: StorageOptions{Durability: tc.durability}}

			rt, err := OpenFileRuntimeWithOptions(dataDir, "app", ns, schema.None(), opts)
			if err != nil {
				t.Fatalf("OpenFileRuntimeWithOptions: %v", err)
			}
			docID, err := codec.RandomUUID()
			if err != nil {
				t.Fatal(err)
			}
			commit, err := PutJSONDocument(rt, ns, `{"id":"`+docID.String()+`","v":"written"}`)
			if err != nil {
				t.Fatalf("PutJSONDocument: %v", err)
			}
			rt.Close()

			reopened, err := OpenFileRuntimeWithOptions(dataDir, "app", ns, schema.None(), opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			head, err := reopened.DAG.Head()
			if err != nil {
				t.Fatal(err)
			}

			persisted := head == commit.Commit
			if persisted != tc.wantPersisted {
				t.Fatalf("commit persisted across reopen = %v, want %v (head=%s, commit=%s)",
					persisted, tc.wantPersisted, head.Hex(), commit.Commit.Hex())
			}
			if !tc.wantPersisted {
				return
			}
			headCommit, ok := reopened.DAG.GetCommit(head)
			if !ok {
				t.Fatalf("head commit %s missing after reopen", head.Hex())
			}
			doc, err := reopened.Storage.GetDocumentOrThrow(ns, docID, headCommit.DocumentTreeHash)
			if err != nil {
				t.Fatalf("document missing after reopen: %v", err)
			}
			if doc.JSON == "" {
				t.Fatal("document round-tripped empty")
			}
		})
	}
}

// TestCompressionCodecIsConfigurableAndReadable: writing a namespace with
// compression off and reopening it must work, since the v2 page format records
// the codec per frame rather than relying on the reader being configured to
// match.
func TestCompressionCodecIsConfigurableAndReadable(t *testing.T) {
	dataDir := t.TempDir()
	ns := "app/data"
	none := storage.CompressionNone
	zstd := storage.CompressionZSTD

	rt, err := OpenFileRuntimeWithOptions(dataDir, "app", ns, schema.None(),
		FileRuntimeOptions{Storage: StorageOptions{Compression: &none}})
	if err != nil {
		t.Fatalf("open with compression=none: %v", err)
	}
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	commit, err := PutJSONDocument(rt, ns, `{"id":"`+docID.String()+`","v":"uncompressed"}`)
	if err != nil {
		t.Fatalf("PutJSONDocument: %v", err)
	}
	rt.Close()

	// Reopened with the opposite codec configured - the frames say what they are.
	reopened, err := OpenFileRuntimeWithOptions(dataDir, "app", ns, schema.None(),
		FileRuntimeOptions{Storage: StorageOptions{Compression: &zstd}})
	if err != nil {
		t.Fatalf("reopen with compression=zstd: %v", err)
	}
	defer reopened.Close()
	head, err := reopened.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != commit.Commit {
		t.Fatalf("head after reopen = %s, want %s - a segment written under one codec must stay readable under another",
			head.Hex(), commit.Commit.Hex())
	}
}
