package embed

import (
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// opsLoaderLog writes perSegment commits into each of segments delta segments and returns a reader
// over them with the commits in log order.
func opsLoaderLog(t testing.TB, segments, perSegment int) (storage.DeltaSegmentReader, []document.Commit) {
	t.Helper()
	const ns = "test/opsidx"
	shim := storio.NewInMemoryPlatformIO()
	cfg := storage.StorageEngineConfig{CompressionCodec: storage.CompressionNone, IOShim: shim}
	var all []document.Commit
	var parents []codec.Hash
	for seq := 0; seq < segments; seq++ {
		id, _ := codec.RandomUUID()
		w := delta.NewDefaultWriter(ns, id, int64(seq), shim, cfg)
		// Appended in batches: the in-memory shim copies a segment on every append, so one
		// append per commit is quadratic in the segment - seconds here, past CI's ten-minute
		// test timeout on a slower runner.
		var batch []storage.DeltaRecord
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if _, err := w.AppendBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
		for i := 0; i < perSegment; i++ {
			tx, _ := codec.RandomUUID()
			author, _ := codec.RandomUUID()
			doc, _ := codec.RandomUUID()
			c := document.Commit{ParentHashes: parents, NamespaceID: ns, TransactionID: tx, Timestamp: codec.TimestampNow(),
				AuthorNodeID: author, Operations: []document.Op{document.WriteOp{DocID: doc, Patch: `{"x":1}`}},
				DocumentTreeHash: document.EmptyDocumentTree().TreeHash, Message: "idx"}
			h, err := document.ComputeCommitHash(c)
			if err != nil {
				t.Fatal(err)
			}
			c.Hash = h
			payload, err := c.ToPayloadBytes()
			if err != nil {
				t.Fatal(err)
			}
			batch = append(batch, storage.DeltaRecord{CommitHash: h, NamespaceID: ns, CommitPayload: payload})
			if len(batch) == 1000 {
				flush()
			}
			all = append(all, c)
			parents = []codec.Hash{h}
		}
		flush()
		if seq < segments-1 {
			if _, err := w.Seal(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return delta.NewDefaultReader(ns, shim, cfg), all
}

// The index finds every commit's frame across segments, and the frame it names holds that commit.
func TestCommitOpsLoaderIndexFindsEveryFrame(t *testing.T) {
	r, all := opsLoaderLog(t, 3, 200)
	l := newCommitOpsLoader(r)
	for i, c := range all {
		ops, err := l.load(c.Hash)
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		if len(ops) != 1 {
			t.Fatalf("commit %d: %d operations, want 1", i, len(ops))
		}
	}
	if len(l.segments) != 3 {
		t.Fatalf("segment table holds %d segments, want 3", len(l.segments))
	}
}

// What the index keeps per commit, once built: pointer-free locations, well under the ~225 bytes
// per commit a whole DeltaSegmentRef per entry cost.
func TestCommitOpsLoaderIndexBytesPerCommit(t *testing.T) {
	r, all := opsLoaderLog(t, 4, 25_000)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	l := newCommitOpsLoader(r)
	if err := l.refresh(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	perCommit := float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(len(all))
	runtime.KeepAlive(l)
	t.Logf("index: %.0f bytes per commit over %d commits", perCommit, len(all))
	if perCommit > 120 {
		t.Fatalf("index holds %.0f bytes per commit, want under 120", perCommit)
	}
}
