package embed

import (
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// writeCountingShim counts the writes that reach a segment.
type writeCountingShim struct {
	storage.PlatformIOShim
	mu     sync.Mutex
	writes int
}

func (s *writeCountingShim) AppendToSegment(name string, b []byte) (int64, error) {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.PlatformIOShim.AppendToSegment(name, b)
}

// TestCommitLogBatchesWritesAndReportsEveryOffset: records queued faster than they are written go
// out a drained batch per write, not one write each - and every record is still reported at the
// (sequence, offset) its frame is actually at, which is what readers of the log rely on.
func TestCommitLogBatchesWritesAndReportsEveryOffset(t *testing.T) {
	const ns = "test/batch"
	shim := &writeCountingShim{PlatformIOShim: storio.NewInMemoryPlatformIO()}
	cfg := storage.StorageEngineConfig{CompressionCodec: storage.CompressionZSTD, IOShim: shim}
	id, _ := codec.RandomUUID()
	w := delta.NewDefaultWriter(ns, id, 0, shim, cfg)
	c := newCommitLogWriter(w, storage.DurabilityAsync, 0)

	type loc struct{ seq, off int64 }
	var mu sync.Mutex
	at := map[codec.Hash]loc{}
	c.onPersisted = func(tree codec.Hash, seq, off int64) {
		mu.Lock()
		at[tree] = loc{seq, off}
		mu.Unlock()
	}
	const n = 2000
	var waits []func() error
	byTree := map[codec.Hash]codec.Hash{} // tree hash -> commit hash
	var parent []codec.Hash
	for i := 0; i < n; i++ {
		tx, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		doc, _ := codec.RandomUUID()
		cm := document.Commit{ParentHashes: parent, NamespaceID: ns, TransactionID: tx, Timestamp: codec.TimestampNow(),
			AuthorNodeID: author, Operations: []document.Op{document.WriteOp{DocID: doc, Patch: `{"x":1}`}},
			DocumentTreeHash: document.EmptyDocumentTree().TreeHash, Message: "batch"}
		h, err := document.ComputeCommitHash(cm)
		if err != nil {
			t.Fatal(err)
		}
		cm.Hash = h
		payload, err := cm.ToPayloadBytes()
		if err != nil {
			t.Fatal(err)
		}
		tree := codec.Hash{Bytes: h.Bytes} // any distinct non-zero key will do
		tree.Bytes[0] ^= 0xff
		byTree[tree] = h
		wait, err := c.EnqueueAsync(storage.DeltaRecord{CommitHash: h, NamespaceID: ns, CommitPayload: payload}, tree)
		if err != nil {
			t.Fatal(err)
		}
		waits = append(waits, wait)
		parent = []codec.Hash{h}
	}
	for _, wait := range waits {
		if err := wait(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if shim.writes >= n {
		t.Fatalf("%d writes for %d records: nothing was batched", shim.writes, n)
	}
	if len(at) != n {
		t.Fatalf("%d records reported, want %d", len(at), n)
	}
	r := delta.NewDefaultReader(ns, shim, cfg)
	for tree, l := range at {
		got, err := r.ReadCommitAt(storage.DeltaSegmentRef{NamespaceID: ns, SequenceNumber: l.seq}, l.off)
		if err != nil {
			t.Fatalf("reading the frame reported at %d/%d: %v", l.seq, l.off, err)
		}
		if got.Hash != byTree[tree] {
			t.Fatalf("frame at %d/%d holds %s, want %s", l.seq, l.off, got.Hash.Hex()[:8], byTree[tree].Hex()[:8])
		}
	}
	t.Logf("%d records in %d writes", n, shim.writes)
}
