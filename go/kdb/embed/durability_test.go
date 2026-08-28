package embed

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
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
		syncMode      storio.SyncMode
		intervalMS    int64
		wantPersisted bool
	}{
		{"sync", storage.DurabilitySync, storio.SyncModeFull, 0, true},
		{"sync + fast sync mode", storage.DurabilitySync, storio.SyncModeFast, 0, true},
		{"async survives a clean close", storage.DurabilityAsync, storio.SyncModeFull, 0, true},
		{"async + fast sync mode + 100ms interval", storage.DurabilityAsync, storio.SyncModeFast, 100, true},
		// An interval far longer than the test proves Close's drain-and-flush
		// stands on its own - durability on a clean shutdown must never depend
		// on the interval timer having happened to fire.
		{"async hour-long interval still drains on close", storage.DurabilityAsync, storio.SyncModeFast, 3_600_000, true},
		{"memory-only keeps nothing", storage.DurabilityMemoryOnly, storio.SyncModeFull, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			ns := "app/data"
			opts := FileRuntimeOptions{Storage: StorageOptions{
				Durability:              tc.durability,
				SyncMode:                tc.syncMode,
				AsyncSyncIntervalMillis: tc.intervalMS,
			}}

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
