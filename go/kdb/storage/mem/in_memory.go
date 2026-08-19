package mem

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// InMemoryStorageAdapter keeps blobs and committed document trees keyed by hashes.
type InMemoryStorageAdapter struct {
	cap storage.CapabilitySet

	mu             sync.Mutex
	blobs          map[codec.Hash][]byte
	docsByBlob     map[codec.Hash]document.Document
	trees          map[codec.Hash]document.DocumentTree
	pendingPuts    map[string]map[codec.UUID]document.Document
	pendingDeletes map[string]map[codec.UUID]struct{}
}

// NewInMemoryStorageAdapter returns a volatile in-memory storage adapter.
func NewInMemoryStorageAdapter() *InMemoryStorageAdapter {
	a := &InMemoryStorageAdapter{
		cap:            storage.MemoryCapabilities,
		blobs:          make(map[codec.Hash][]byte),
		docsByBlob:     make(map[codec.Hash]document.Document),
		trees:          make(map[codec.Hash]document.DocumentTree),
		pendingPuts:    make(map[string]map[codec.UUID]document.Document),
		pendingDeletes: make(map[string]map[codec.UUID]struct{}),
	}
	empty := document.EmptyDocumentTree()
	a.trees[empty.TreeHash] = empty
	return a
}

func (a *InMemoryStorageAdapter) Capabilities() storage.CapabilitySet { return a.cap }

func (a *InMemoryStorageAdapter) GetDocument(namespaceID string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	tree, ok := a.trees[atCommit]
	if !ok {
		return nil, nil
	}
	h, ok := tree.HashFor(docID)
	if !ok {
		return nil, nil
	}
	d, ok := a.docsByBlob[h]
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
	a.mu.Lock()
	tree, ok := a.trees[atCommit]
	a.mu.Unlock()
	if !ok {
		return nil
	}
	buf := make([]document.Document, 0, batchSize)
	for _, h := range tree.Entries {
		a.mu.Lock()
		d, ok := a.docsByBlob[h]
		a.mu.Unlock()
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
	a.mu.Lock()
	defer a.mu.Unlock()
	m := a.pendingPuts[namespaceID]
	if m == nil {
		m = make(map[codec.UUID]document.Document)
		a.pendingPuts[namespaceID] = m
	}
	dm := a.pendingDeletes[namespaceID]
	if dm == nil {
		dm = make(map[codec.UUID]struct{})
		a.pendingDeletes[namespaceID] = dm
	}
	delete(dm, doc.ID)
	m[doc.ID] = doc
	return nil
}

func (a *InMemoryStorageAdapter) DeleteDocument(namespaceID string, docID codec.UUID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	pm := a.pendingPuts[namespaceID]
	if pm == nil {
		pm = make(map[codec.UUID]document.Document)
		a.pendingPuts[namespaceID] = pm
	}
	delete(pm, docID)
	dm := a.pendingDeletes[namespaceID]
	if dm == nil {
		dm = make(map[codec.UUID]struct{})
		a.pendingDeletes[namespaceID] = dm
	}
	dm[docID] = struct{}{}
	return nil
}

func (a *InMemoryStorageAdapter) CommitTree(namespaceID string, parentTreeHash codec.Hash) (document.DocumentTree, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	base, ok := a.trees[parentTreeHash]
	if !ok {
		return document.DocumentTree{}, fmt.Errorf("missing parent tree %s", parentTreeHash.Hex())
	}
	dels := a.pendingDeletes[namespaceID]
	puts := a.pendingPuts[namespaceID]
	delete(a.pendingDeletes, namespaceID)
	delete(a.pendingPuts, namespaceID)

	next := make(map[codec.UUID]codec.Hash, len(base.Entries))
	for id, h := range base.Entries {
		next[id] = h
	}
	if dels != nil {
		for id := range dels {
			delete(next, id)
		}
	}
	if puts != nil {
		for id, doc := range puts {
			if err := a.rememberDocumentLocked(doc); err != nil {
				return document.DocumentTree{}, err
			}
			h, err := doc.ContentHash()
			if err != nil {
				return document.DocumentTree{}, err
			}
			next[id] = h
		}
	}
	built, err := document.BuildDocumentTree(next)
	if err != nil {
		return document.DocumentTree{}, err
	}
	a.trees[built.TreeHash] = built
	return built, nil
}

func (a *InMemoryStorageAdapter) DiscardPending(namespaceID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.pendingPuts, namespaceID)
	delete(a.pendingDeletes, namespaceID)
	return nil
}

func (a *InMemoryStorageAdapter) Flush(namespaceID string) error { return nil }

func (a *InMemoryStorageAdapter) ReadBlob(contentHash codec.Hash) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.blobs[contentHash]
	if b == nil {
		return nil, nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (a *InMemoryStorageAdapter) WriteBlob(bytes []byte) (codec.Hash, error) {
	sum := document.SHA256Digest(bytes)
	h, err := codec.HashFromBytes(sum[:])
	if err != nil {
		return codec.Hash{}, err
	}
	a.mu.Lock()
	a.blobs[h] = append([]byte(nil), bytes...)
	a.mu.Unlock()
	return h, nil
}

func (a *InMemoryStorageAdapter) IngestDeltaSegment(segment storage.DeltaSegmentRef) error {
	return fmt.Errorf("memory adapter cannot ingest delta segment %s", segment.SegmentID.String())
}

func (a *InMemoryStorageAdapter) rememberDocumentLocked(doc document.Document) error {
	h, err := doc.ContentHash()
	if err != nil {
		return err
	}
	a.blobs[h] = []byte(doc.JSON)
	a.docsByBlob[h] = doc
	return nil
}
