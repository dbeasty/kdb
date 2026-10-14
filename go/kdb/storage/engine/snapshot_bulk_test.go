package engine_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

// ApplySnapshotBatch builds the tree PutDocument and CommitTree build, batch by batch, whether a
// batch's ids arrive in order (the one-pass path) or not, and every body reads back.
func TestApplySnapshotBatchBuildsCommitTreesTree(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	var docs []document.Document
	for i := 0; i < 2000; i++ {
		docs = append(docs, document.Document{ID: codec.UUID{MSB: r.Int63(), LSB: r.Int63()}, JSON: fmt.Sprintf(`{"i":%d}`, i)})
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].ID.String() < docs[j].ID.String() })
	for _, shuffled := range []bool{false, true} {
		cfg := storage.StorageEngineConfig{IOShim: io.NewInMemoryPlatformIO(), MemtableFlushBytes: 8 << 10}
		bulk := engine.NewServerEngine("ns", cfg, nil)
		plain := engine.NewServerEngine("ns", storage.StorageEngineConfig{}, nil)
		for start := 0; start < len(docs); start += 300 {
			batch := append([]document.Document(nil), docs[start:min(start+300, len(docs))]...)
			if shuffled {
				r.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
			}
			got, err := bulk.ApplySnapshotBatch("ns", batch)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range batch {
				_ = plain.PutDocument("ns", d)
			}
			want, err := plain.CommitTree("ns", codec.Hash{})
			if err != nil {
				t.Fatal(err)
			}
			if got.TreeHash != want.TreeHash || got.Size() != want.Size() {
				t.Fatalf("shuffled=%v after %d: tree %s (%d), want %s (%d)", shuffled, start, got.TreeHash.Hex(), got.Size(), want.TreeHash.Hex(), want.Size())
			}
		}
		tree := bulk.LiveTree()
		for _, d := range docs {
			got, err := bulk.GetDocument("ns", d.ID, tree.TreeHash)
			if err != nil || got == nil || got.JSON != d.JSON {
				t.Fatalf("shuffled=%v: document %s read back %v, %v", shuffled, d.ID, got, err)
			}
		}
		if err := bulk.MaterializeLiveBodies(); err != nil || !bulk.LiveBodiesDurable() {
			t.Fatalf("shuffled=%v: bodies not durable: %v", shuffled, err)
		}
		_ = bulk.Close()
		_ = plain.Close()
	}
}
