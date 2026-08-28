package engine

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// pendingShardCount mirrors doc_shard.go: independent lock domains so
// PutDocument/DeleteDocument for different documents proceed in
// parallel. ServerEngine is constructed per-namespace (see
// NewServerEngine), so unlike mem.pendingByNamespace this needs no
// namespace-keyed outer map.
const pendingShardCount = 64

type pendingShard struct {
	mu      sync.Mutex
	puts    map[codec.UUID]document.Document
	deletes map[codec.UUID]struct{}
}

// shardedPendingStore holds writes staged by PutDocument/DeleteDocument
// but not yet visible via GetDocument/CommitTree - see
// ServerEngine.CommitTree/DiscardPending. A document is staged in at
// most one of puts/deletes at a time: Put always clears any pending
// delete for the same id and vice versa.
type shardedPendingStore struct {
	shards [pendingShardCount]*pendingShard
}

func newShardedPendingStore() *shardedPendingStore {
	s := &shardedPendingStore{}
	for i := range s.shards {
		s.shards[i] = &pendingShard{
			puts:    make(map[codec.UUID]document.Document),
			deletes: make(map[codec.UUID]struct{}),
		}
	}
	return s
}

func (s *shardedPendingStore) shardFor(id codec.UUID) *pendingShard {
	idx := uint64(id.LSB) % uint64(pendingShardCount)
	return s.shards[idx]
}

func (s *shardedPendingStore) Put(doc document.Document) {
	sh := s.shardFor(doc.ID)
	sh.mu.Lock()
	delete(sh.deletes, doc.ID)
	sh.puts[doc.ID] = doc
	sh.mu.Unlock()
}

func (s *shardedPendingStore) Delete(id codec.UUID) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	delete(sh.puts, id)
	sh.deletes[id] = struct{}{}
	sh.mu.Unlock()
}

// TakeAllAndClear atomically (per shard) returns and clears every staged
// put/delete across all shards.
func (s *shardedPendingStore) TakeAllAndClear() ([]document.Document, []codec.UUID) {
	var puts []document.Document
	var deletes []codec.UUID
	for _, sh := range s.shards {
		sh.mu.Lock()
		// Skip shards nothing was staged in. A typical commit touches one
		// document, so 63 of 64 shards are empty - reallocating both maps
		// unconditionally meant 128 fresh map headers per commit to move a
		// single record.
		if len(sh.puts) == 0 && len(sh.deletes) == 0 {
			sh.mu.Unlock()
			continue
		}
		for _, d := range sh.puts {
			puts = append(puts, d)
		}
		for id := range sh.deletes {
			deletes = append(deletes, id)
		}
		sh.puts = make(map[codec.UUID]document.Document)
		sh.deletes = make(map[codec.UUID]struct{})
		sh.mu.Unlock()
	}
	return puts, deletes
}

// DiscardAll clears every staged put/delete without applying them,
// restoring the last-committed visible state.
func (s *shardedPendingStore) DiscardAll() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		if len(sh.puts) > 0 {
			sh.puts = make(map[codec.UUID]document.Document)
		}
		if len(sh.deletes) > 0 {
			sh.deletes = make(map[codec.UUID]struct{})
		}
		sh.mu.Unlock()
	}
}
