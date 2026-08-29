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

// Range streams every document to visit, shard by shard, stopping early when visit returns
// false. Each shard's documents are copied out under its own lock and visited unlocked, so
// visit may be arbitrarily slow (it typically runs a scan batch callback) without stalling
// writers for longer than one shard copy. Consistency is the same as Snapshot's - each shard
// is a point-in-time copy, shards are taken sequentially - but peak memory is one shard's
// documents rather than the whole namespace's.
func (s *shardedDocStore) Range(visit func(document.Document) bool) {
	for _, sh := range s.shards {
		sh.mu.Lock()
		batch := make([]document.Document, 0, len(sh.docs))
		for _, d := range sh.docs {
			batch = append(batch, d)
		}
		sh.mu.Unlock()
		for _, d := range batch {
			if !visit(d) {
				return
			}
		}
	}
}

// Snapshot returns a copy of every document across all shards. Used by
// CommitTree, which needs a consistent-enough full view;
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
