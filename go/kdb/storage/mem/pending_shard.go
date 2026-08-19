package mem

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// nsPending holds one namespace's staged (not-yet-committed) writes,
// guarded by its own mutex so different namespaces' PutDocument/
// DeleteDocument calls never contend with each other - only the (rare)
// creation of a new namespace's entry takes the shared outer lock.
type nsPending struct {
	mu      sync.Mutex
	puts    map[codec.UUID]document.Document
	deletes map[codec.UUID]struct{}
}

type pendingByNamespace struct {
	mu sync.Mutex
	ns map[string]*nsPending
}

func newPendingByNamespace() *pendingByNamespace {
	return &pendingByNamespace{ns: make(map[string]*nsPending)}
}

func (p *pendingByNamespace) forNamespace(namespaceID string) *nsPending {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.ns[namespaceID]
	if !ok {
		n = &nsPending{
			puts:    make(map[codec.UUID]document.Document),
			deletes: make(map[codec.UUID]struct{}),
		}
		p.ns[namespaceID] = n
	}
	return n
}

func (p *pendingByNamespace) put(namespaceID string, doc document.Document) {
	n := p.forNamespace(namespaceID)
	n.mu.Lock()
	delete(n.deletes, doc.ID)
	n.puts[doc.ID] = doc
	n.mu.Unlock()
}

func (p *pendingByNamespace) delete(namespaceID string, docID codec.UUID) {
	n := p.forNamespace(namespaceID)
	n.mu.Lock()
	delete(n.puts, docID)
	n.deletes[docID] = struct{}{}
	n.mu.Unlock()
}

// takeAndClear atomically returns and clears the namespace's pending
// puts/deletes (used by CommitTree to flush staged writes).
func (p *pendingByNamespace) takeAndClear(namespaceID string) (map[codec.UUID]document.Document, map[codec.UUID]struct{}) {
	n := p.forNamespace(namespaceID)
	n.mu.Lock()
	defer n.mu.Unlock()
	puts := n.puts
	dels := n.deletes
	n.puts = make(map[codec.UUID]document.Document)
	n.deletes = make(map[codec.UUID]struct{})
	return puts, dels
}
