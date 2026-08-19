package engine

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// docShardCount is the number of independent lock domains the document
// map is split across. PutDocument/GetDocument/DeleteDocument for
// different documents can now proceed fully in parallel as long as they
// land in different shards, instead of serializing behind one
// namespace-wide mutex - see Phase 2 of docs/benchmarks/phase0-baseline.md.
// A fixed power-of-two keeps shard selection a cheap mask instead of a
// division; 64 is generous relative to typical core counts without
// meaningfully increasing per-engine memory overhead (each shard is just
// a mutex + an empty map header until documents land in it).
const docShardCount = 64

type docShard struct {
	mu   sync.Mutex
	docs map[codec.UUID]document.Document
}

// shardedDocStore replaces a single map[UUID]Document + one mutex with
// docShardCount independently-locked partitions.
type shardedDocStore struct {
	shards [docShardCount]*docShard
}

func newShardedDocStore() *shardedDocStore {
	s := &shardedDocStore{}
	for i := range s.shards {
		s.shards[i] = &docShard{docs: make(map[codec.UUID]document.Document)}
	}
	return s
}

func (s *shardedDocStore) shardFor(id codec.UUID) *docShard {
	// UUIDs are random (codec.RandomUUID), so the low bits of either half
	// are already uniformly distributed - no need for a stronger hash.
	idx := uint64(id.LSB) % uint64(docShardCount)
	return s.shards[idx]
}

func (s *shardedDocStore) Put(doc document.Document) {
	sh := s.shardFor(doc.ID)
	sh.mu.Lock()
	sh.docs[doc.ID] = doc
	sh.mu.Unlock()
}

func (s *shardedDocStore) Get(id codec.UUID) (document.Document, bool) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	d, ok := sh.docs[id]
	return d, ok
}

func (s *shardedDocStore) Delete(id codec.UUID) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	delete(sh.docs, id)
	sh.mu.Unlock()
}

// Snapshot returns a copy of every document across all shards. Used by
// CommitTree/ScanDocuments, which need a consistent-enough full view;
// each shard is locked only for the duration of copying its own
// entries, not the whole snapshot.
func (s *shardedDocStore) Snapshot() []document.Document {
	total := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		total += len(sh.docs)
		sh.mu.Unlock()
	}
	out := make([]document.Document, 0, total)
	for _, sh := range s.shards {
		sh.mu.Lock()
		for _, d := range sh.docs {
			out = append(out, d)
		}
		sh.mu.Unlock()
	}
	return out
}
