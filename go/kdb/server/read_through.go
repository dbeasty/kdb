package server

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// Read-through for filtered projections (docs/kdb-distributed-self-healing-research.md, Phase 14).
//
// A projection holds only the source documents matching its filter. With read-through, a client
// read of any other document is answered from the source: fetched as of the source commit the
// projection is at - so it is consistent with every document the projection holds - and proved
// against that commit's tree (peersync.RepairSession.FetchDocs). The edge never holds the source's
// tree, only the documents it reads.
//
// What is fetched goes to a bounded LRU "hoard" (after Coda's), kept apart from the projection's
// history: it is never committed, never replicated, and never makes a document part of the
// projection. An entry is good only at the source commit it was read at; when the projection moves
// on, the next read of that document fetches it again.

// DefaultReadThroughCapacity is how many documents a hoard keeps when none is configured.
const DefaultReadThroughCapacity = 1024

// ReadThrough reads source documents a projection does not hold.
type ReadThrough struct {
	// Open connects to the source; it returns the session and the source namespace's name.
	Open func() (*peersync.RepairSession, string, error)
	// Capacity bounds the hoard; 0 means DefaultReadThroughCapacity.
	Capacity int

	mu      sync.Mutex
	session *peersync.RepairSession
	source  string
	lru     *list.List
	entries map[codec.UUID]*list.Element

	fetches, hits atomic.Int64
}

type hoardEntry struct {
	id      codec.UUID
	at      string
	present bool
	body    string
}

// ReadThroughStats reports how a hoard is doing.
type ReadThroughStats struct {
	Fetches, Hits int64
	Held          int
}

// Stats reports fetches from the source, hoard hits, and documents held.
func (r *ReadThrough) Stats() ReadThroughStats {
	r.mu.Lock()
	held := len(r.entries)
	r.mu.Unlock()
	return ReadThroughStats{Fetches: r.fetches.Load(), Hits: r.hits.Load(), Held: held}
}

// Close drops the connection to the source.
func (r *ReadThrough) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.session != nil {
		r.session.Close()
		r.session = nil
	}
}

// ErrReadThroughForbidden is a document the source will not let this node's principal read.
var ErrReadThroughForbidden = errors.New("read-through: the source does not let this node read the document")

// Get returns id as of source commit at: its body and true, or false when that commit does not
// hold it.
func (r *ReadThrough) Get(id codec.UUID, at string) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries, r.lru = map[codec.UUID]*list.Element{}, list.New()
	}
	if el, ok := r.entries[id]; ok {
		e := el.Value.(*hoardEntry)
		if e.at == at {
			r.lru.MoveToFront(el)
			r.hits.Add(1)
			return e.body, e.present, nil
		}
		r.lru.Remove(el)
		delete(r.entries, id)
	}
	doc, err := r.fetchLocked(id, at)
	if err != nil {
		return "", false, err
	}
	if doc.Forbidden {
		return "", false, ErrReadThroughForbidden
	}
	r.entries[id] = r.lru.PushFront(&hoardEntry{id: id, at: at, present: doc.Present, body: doc.Body})
	capacity := r.Capacity
	if capacity <= 0 {
		capacity = DefaultReadThroughCapacity
	}
	for r.lru.Len() > capacity {
		oldest := r.lru.Back()
		r.lru.Remove(oldest)
		delete(r.entries, oldest.Value.(*hoardEntry).id)
	}
	return doc.Body, doc.Present, nil
}

// fetchLocked asks the source, reconnecting once if the held session has gone away.
func (r *ReadThrough) fetchLocked(id codec.UUID, at string) (peersync.ProvenDoc, error) {
	for attempt := 0; ; attempt++ {
		if r.session == nil {
			s, source, err := r.Open()
			if err != nil {
				return peersync.ProvenDoc{}, err
			}
			r.session, r.source = s, source
		}
		r.fetches.Add(1)
		_, docs, err := r.session.FetchDocs(r.source, at, []codec.UUID{id})
		if err == nil {
			return docs[id], nil
		}
		// A refused proof is the source's answer, not a broken connection: never retried.
		if errors.Is(err, peersync.ErrUnproven) || attempt > 0 {
			return peersync.ProvenDoc{}, err
		}
		r.session.Close()
		r.session = nil
	}
}

// ReadDocument is GetDocument for a client's read: when this runtime is a filtered projection
// with read-through and does not hold docID, the source answers, as of the source commit the
// projection is at. Internal callers - which must see only what the projection holds - use
// GetDocument.
func (s *KdbServerRuntime) ReadDocument(namespaceID string, docID codec.UUID) (string, string, bool, error) {
	body, commitHex, found, err := s.GetDocument(namespaceID, docID)
	if err != nil || found || s.ReadThrough == nil {
		return body, commitHex, found, err
	}
	at, err := s.ProjectionTarget().LastSource()
	if err != nil || at == "" {
		// Not yet synced once: there is no source commit to read consistently at.
		return "", commitHex, false, err
	}
	body, present, err := s.ReadThrough.Get(docID, at)
	if err != nil {
		return "", commitHex, false, err
	}
	return body, commitHex, present, nil
}
