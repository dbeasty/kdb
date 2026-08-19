package mem

import (
	"fmt"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/storage"
)

// InMemoryStorageAdapter keeps blobs and committed document trees keyed
// by hashes. Blob/document storage is sharded (blob_shard.go) and
// per-namespace pending writes are independently locked
// (pending_shard.go) so different content hashes / different namespaces
// no longer serialize behind one mutex - see the gap-fix notes in
// docs/benchmarks/phases-1-6-summary.md. trees stays behind its own
// single mutex: it's keyed by commit count, not document count, and is
// read-mostly, so it was never the contention source the blob/doc maps
// were.
type InMemoryStorageAdapter struct {
	cap storage.CapabilitySet

	blobStore *shardedBlobStore
	pending   *pendingByNamespace

	treesMu sync.Mutex
	trees   map[codec.Hash]document.DocumentTree
}

// NewInMemoryStorageAdapter returns a volatile in-memory storage adapter.
func NewInMemoryStorageAdapter() *InMemoryStorageAdapter {
	a := &InMemoryStorageAdapter{
		cap:       storage.MemoryCapabilities,
		blobStore: newShardedBlobStore(),
		pending:   newPendingByNamespace(),
		trees:     make(map[codec.Hash]document.DocumentTree),
	}
	empty := document.EmptyDocumentTree()
	a.trees[empty.TreeHash] = empty
	return a
}

func (a *InMemoryStorageAdapter) Capabilities() storage.CapabilitySet { return a.cap }

func (a *InMemoryStorageAdapter) GetDocument(namespaceID string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error) {
	a.treesMu.Lock()
	tree, ok := a.trees[atCommit]
	a.treesMu.Unlock()
	if !ok {
		return nil, nil
	}
	h, ok := tree.HashFor(docID)
	if !ok {
		return nil, nil
	}
	d, ok := a.blobStore.GetDocByBlob(h)
	if !ok {
		return nil, nil
	}
	cp := d
	return &cp, nil
}

func (a *InMemoryStorageAdapter) GetDocumentOrThrow(namespaceID string, docID codec.UUID, atCommit codec.Hash) (document.Document, error) {
	d, err := a.GetDocument(namespaceID, docID, atCommit)
	if err != nil {
		return document.Document{}, err
	}
	if d == nil {
		return document.Document{}, storage.NewDocumentNotFoundError(
			"missing document "+docID.String(), namespaceID, docID, atCommit)
	}
	return *d, nil
}

func (a *InMemoryStorageAdapter) GetDocuments(namespaceID string, docIDs []codec.UUID, atCommit codec.Hash) ([]*document.Document, error) {
	out := make([]*document.Document, len(docIDs))
	for i, id := range docIDs {
		d, err := a.GetDocument(namespaceID, id, atCommit)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

func (a *InMemoryStorageAdapter) ScanDocuments(namespaceID string, atCommit codec.Hash, batchSize int, onBatch func([]document.Document) error) error {
	if batchSize <= 0 {
		batchSize = 256
	}
	a.treesMu.Lock()
	tree, ok := a.trees[atCommit]
	a.treesMu.Unlock()
	if !ok {
		return nil
	}
	buf := make([]document.Document, 0, batchSize)
	for _, h := range tree.Entries {
		d, ok := a.blobStore.GetDocByBlob(h)
		if !ok {
			continue
		}
		buf = append(buf, d)
		if len(buf) >= batchSize {
			if err := onBatch(append([]document.Document(nil), buf...)); err != nil {
				return err
			}
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		return onBatch(buf)
	}
	return nil
}

func (a *InMemoryStorageAdapter) PutDocument(namespaceID string, doc document.Document) error {
	a.pending.put(namespaceID, doc)
	return nil
}

func (a *InMemoryStorageAdapter) DeleteDocument(namespaceID string, docID codec.UUID) error {
	a.pending.delete(namespaceID, docID)
	return nil
}

// DiscardPending drops any PutDocument/DeleteDocument calls made since
// the last CommitTree for namespaceID, restoring the last-committed
// visible state. Used to roll back a transaction whose write phase
// failed partway through.
func (a *InMemoryStorageAdapter) DiscardPending(namespaceID string) error {
	a.pending.discardAll(namespaceID)
	return nil
}

func (a *InMemoryStorageAdapter) CommitTree(namespaceID string, parentTreeHash codec.Hash) (document.DocumentTree, error) {
	lockWaitStart := time.Now()
	a.treesMu.Lock()
	defer a.treesMu.Unlock()
	metrics.Default.Record(metrics.StageLockWait, time.Since(lockWaitStart))
	rebuildStart := time.Now()
	defer func() {
		metrics.Default.Record(metrics.StageTreeRebuild, time.Since(rebuildStart))
	}()
	base, ok := a.trees[parentTreeHash]
	if !ok {
		return document.DocumentTree{}, fmt.Errorf("missing parent tree %s", parentTreeHash.Hex())
	}
	puts, dels := a.pending.takeAndClear(namespaceID)

	// Applied incrementally on top of base (With/Without are O(delta) via
	// DocumentTree's persistent trie) rather than copying base.Entries
	// into a fresh map and calling BuildDocumentTree from scratch - see
	// document/document_tree_trie.go and the Phase 3 gap-fix notes in
	// docs/benchmarks/phases-1-6-summary.md.
	built := base
	if dels != nil {
		for id := range dels {
			var err error
			built, err = built.Without(id)
			if err != nil {
				return document.DocumentTree{}, err
			}
		}
	}
	if puts != nil {
		for id, doc := range puts {
			if err := a.blobStore.RememberDocument(doc); err != nil {
				return document.DocumentTree{}, err
			}
			h, err := doc.ContentHash()
			if err != nil {
				return document.DocumentTree{}, err
			}
			built, err = built.With(id, h)
			if err != nil {
				return document.DocumentTree{}, err
			}
		}
	}
	a.trees[built.TreeHash] = built
	return built, nil
}

func (a *InMemoryStorageAdapter) Flush(namespaceID string) error { return nil }

func (a *InMemoryStorageAdapter) ReadBlob(contentHash codec.Hash) ([]byte, error) {
	return a.blobStore.ReadBlob(contentHash), nil
}

func (a *InMemoryStorageAdapter) WriteBlob(bytes []byte) (codec.Hash, error) {
	sum := document.SHA256Digest(bytes)
	h, err := codec.HashFromBytes(sum[:])
	if err != nil {
		return codec.Hash{}, err
	}
	a.blobStore.WriteBlob(h, append([]byte(nil), bytes...))
	return h, nil
}

func (a *InMemoryStorageAdapter) IngestDeltaSegment(segment storage.DeltaSegmentRef) error {
	return fmt.Errorf("memory adapter cannot ingest delta segment %s", segment.SegmentID.String())
}
