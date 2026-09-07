package engine

import (
	"fmt"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// ErrUnknownNamespace is returned by a MultiplexAdapter asked for a namespace it does not route.
type ErrUnknownNamespace struct{ NamespaceID string }

func (e *ErrUnknownNamespace) Error() string {
	return fmt.Sprintf("kdb: no storage registered for namespace %q", e.NamespaceID)
}

// MultiplexAdapter presents several single-namespace engines as one storage.Adapter, dispatching
// on the namespaceID that every method of the interface already carries.
//
// It works because ServerEngine *is* a namespace: it ignores the namespaceID parameter in every
// implementation, since the engine, its WAL, its memtable and its delta log are all keyed to one
// namespace's files. That unused parameter is the seam this type occupies, so nothing about
// storage.Adapter had to change to support several namespaces behind one handle.
//
// Use it where a caller needs one adapter spanning namespaces - SQL over several spaces, or a
// transaction that touches more than one - rather than one handle per namespace. Where a caller
// only ever touches one namespace, give it that namespace's engine directly; this adds a map
// lookup and a lock for nothing.
//
// Safe for concurrent use, including registration: a host opens and closes namespaces while
// queries are running against the others.
type MultiplexAdapter struct {
	mu     sync.RWMutex
	routes map[string]storage.Adapter
	// order preserves registration order so Capabilities and the blob methods pick a stable
	// member rather than whichever one Go's map iteration happens to yield.
	order []string
}

// NewMultiplexAdapter returns an adapter with no routes. Every call fails with
// *ErrUnknownNamespace until something is registered.
func NewMultiplexAdapter() *MultiplexAdapter {
	return &MultiplexAdapter{routes: make(map[string]storage.Adapter)}
}

// Register routes namespaceID to a. Re-registering a namespace replaces its route.
func (m *MultiplexAdapter) Register(namespaceID string, a storage.Adapter) {
	if a == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, existed := m.routes[namespaceID]; !existed {
		m.order = append(m.order, namespaceID)
	}
	m.routes[namespaceID] = a
}

// Unregister drops a namespace's route. Calls naming it afterwards fail with
// *ErrUnknownNamespace, which is the point: a closed namespace must not quietly resolve
// somewhere else.
func (m *MultiplexAdapter) Unregister(namespaceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.routes, namespaceID)
	for i, id := range m.order {
		if id == namespaceID {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
}

// Namespaces lists the routed namespaces, sorted.
func (m *MultiplexAdapter) Namespaces() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := append([]string(nil), m.order...)
	sort.Strings(out)
	return out
}

// route resolves one namespace, or reports that it is not routed.
//
// An unknown namespace is an error and never a fallback. Routing it to some other engine would
// write one namespace's documents into another's files - silent, permanent, and invisible until
// something read the wrong namespace back. The private predecessor of this type panicked here
// instead, which is defensible in a two-namespace registry built once at startup and not in a
// type whose routes change while a server is running: a panic on a storage call takes down every
// other connection too.
func (m *MultiplexAdapter) route(namespaceID string) (storage.Adapter, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if a, ok := m.routes[namespaceID]; ok {
		return a, nil
	}
	return nil, &ErrUnknownNamespace{NamespaceID: namespaceID}
}

// first is the stable member the namespace-less methods fall back to.
func (m *MultiplexAdapter) first() (storage.Adapter, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, id := range m.order {
		if a, ok := m.routes[id]; ok {
			return a, nil
		}
	}
	return nil, &ErrUnknownNamespace{NamespaceID: "(none registered)"}
}

// Capabilities is the conservative intersection of the members' capabilities: a caller must not
// be told a multiplexed handle supports something one of the engines behind it does not.
//
// In practice every namespace under one host is opened by the same factory with the same target,
// so this is unanimous and the intersection changes nothing. It is computed anyway because the
// one configuration where it would matter - mixing engines of different targets - is exactly the
// one where taking an arbitrary member's answer would be wrong.
//
// IndexRetentionDefault is a mode rather than a capability, so it is taken from the first member
// rather than intersected; there is no meaningful "conservative" value to pick between two.
func (m *MultiplexAdapter) Capabilities() storage.CapabilitySet {
	m.mu.RLock()
	ids := append([]string(nil), m.order...)
	routes := make([]storage.Adapter, 0, len(ids))
	for _, id := range ids {
		if a, ok := m.routes[id]; ok {
			routes = append(routes, a)
		}
	}
	m.mu.RUnlock()

	if len(routes) == 0 {
		return storage.CapabilitySet{}
	}
	out := routes[0].Capabilities()
	for _, a := range routes[1:] {
		c := a.Capabilities()
		out.PersistsDeltaLog = out.PersistsDeltaLog && c.PersistsDeltaLog
		out.PersistsAcrossReload = out.PersistsAcrossReload && c.PersistsAcrossReload
		out.SupportsGpuBulkRead = out.SupportsGpuBulkRead && c.SupportsGpuBulkRead
		out.SupportsDirectDeltaIngest = out.SupportsDirectDeltaIngest && c.SupportsDirectDeltaIngest
		if c.MaxEnlistments != nil && (out.MaxEnlistments == nil || *c.MaxEnlistments < *out.MaxEnlistments) {
			out.MaxEnlistments = c.MaxEnlistments
		}
	}
	return out
}

func (m *MultiplexAdapter) GetDocument(namespaceID string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error) {
	a, err := m.route(namespaceID)
	if err != nil {
		return nil, err
	}
	return a.GetDocument(namespaceID, docID, atCommit)
}

func (m *MultiplexAdapter) GetDocumentOrThrow(namespaceID string, docID codec.UUID, atCommit codec.Hash) (document.Document, error) {
	a, err := m.route(namespaceID)
	if err != nil {
		return document.Document{}, err
	}
	return a.GetDocumentOrThrow(namespaceID, docID, atCommit)
}

func (m *MultiplexAdapter) GetDocuments(namespaceID string, docIDs []codec.UUID, atCommit codec.Hash) ([]*document.Document, error) {
	a, err := m.route(namespaceID)
	if err != nil {
		return nil, err
	}
	return a.GetDocuments(namespaceID, docIDs, atCommit)
}

func (m *MultiplexAdapter) ScanDocuments(namespaceID string, atCommit codec.Hash, batchSize int, onBatch func([]document.Document) error) error {
	a, err := m.route(namespaceID)
	if err != nil {
		return err
	}
	return a.ScanDocuments(namespaceID, atCommit, batchSize, onBatch)
}

func (m *MultiplexAdapter) PutDocument(namespaceID string, doc document.Document) error {
	a, err := m.route(namespaceID)
	if err != nil {
		return err
	}
	return a.PutDocument(namespaceID, doc)
}

func (m *MultiplexAdapter) DeleteDocument(namespaceID string, docID codec.UUID) error {
	a, err := m.route(namespaceID)
	if err != nil {
		return err
	}
	return a.DeleteDocument(namespaceID, docID)
}

func (m *MultiplexAdapter) DiscardPending(namespaceID string) error {
	a, err := m.route(namespaceID)
	if err != nil {
		return err
	}
	return a.DiscardPending(namespaceID)
}

func (m *MultiplexAdapter) CommitTree(namespaceID string, parentTreeHash codec.Hash) (document.DocumentTree, error) {
	a, err := m.route(namespaceID)
	if err != nil {
		return document.DocumentTree{}, err
	}
	return a.CommitTree(namespaceID, parentTreeHash)
}

func (m *MultiplexAdapter) Flush(namespaceID string) error {
	a, err := m.route(namespaceID)
	if err != nil {
		return err
	}
	return a.Flush(namespaceID)
}

// ReadBlob tries each member until one has the blob.
//
// Blobs are content-addressed and the interface carries no namespace to route on, so there is no
// way to know which engine holds one without asking. Trying them in turn is correct because the
// hash identifies the content: whichever engine answers, the bytes are the bytes.
func (m *MultiplexAdapter) ReadBlob(contentHash codec.Hash) ([]byte, error) {
	m.mu.RLock()
	ids := append([]string(nil), m.order...)
	routes := make([]storage.Adapter, 0, len(ids))
	for _, id := range ids {
		if a, ok := m.routes[id]; ok {
			routes = append(routes, a)
		}
	}
	m.mu.RUnlock()

	var firstErr error
	for _, a := range routes {
		b, err := a.ReadBlob(contentHash)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if b != nil {
			return b, nil
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	// Every member answered without error and none had it - which is a miss, not a failure, and
	// is what a single adapter reports the same way.
	return nil, nil
}

// WriteBlob writes to the first registered member.
//
// storage.Adapter gives WriteBlob no namespace to attribute a blob to, so a multiplexer has no
// principled way to choose - and this is the one method of the interface where that is a real
// gap rather than a routing detail. First-member is stable (registration order, not map order)
// and pairs with ReadBlob's try-each, so a blob written here is always findable again through
// the same multiplexer. A caller that needs a blob in a *particular* namespace should write it
// through that namespace's engine directly.
func (m *MultiplexAdapter) WriteBlob(bytes []byte) (codec.Hash, error) {
	a, err := m.first()
	if err != nil {
		return codec.Hash{}, err
	}
	return a.WriteBlob(bytes)
}

// IngestDeltaSegment routes on the segment's own NamespaceID.
//
// The private predecessor of this type sent these to an arbitrary member, which was safe only
// because its one caller never ingested anything: a segment carries the namespace it belongs to,
// and handing it to the wrong engine would fold one namespace's commits into another's history.
func (m *MultiplexAdapter) IngestDeltaSegment(segment storage.DeltaSegmentRef) error {
	a, err := m.route(segment.NamespaceID)
	if err != nil {
		return err
	}
	return a.IngestDeltaSegment(segment)
}
