package engine

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// docHashShardCount mirrors doc_shard.go's rationale: independent lock
// domains so lookups/inserts for different content hashes proceed in
// parallel instead of serializing behind one mutex.
const docHashShardCount = 64

type docHashShard struct {
	mu   sync.Mutex
	docs map[codec.Hash]document.Document
}

// shardedDocByHashStore holds every document version ServerEngine has ever
// committed, keyed by content hash rather than doc ID. CommitTree never
// deletes an entry here (unlike the by-ID view a later commit overwrites),
// so a document version referenced by an older committed DocumentTree
// stays retrievable via GetDocument/ScanDocuments at that tree's hash -
// see server_engine.go's treesByHash and the atCommit fix it backs.
type shardedDocByHashStore struct {
	shards [docHashShardCount]*docHashShard
}

func newShardedDocByHashStore() *shardedDocByHashStore {
	s := &shardedDocByHashStore{}
	for i := range s.shards {
		s.shards[i] = &docHashShard{docs: make(map[codec.Hash]document.Document)}
	}
	return s
}

func (s *shardedDocByHashStore) shardFor(h codec.Hash) *docHashShard {
	// Content hashes are SHA256 output, already uniformly distributed - any
	// byte works as a cheap shard selector (mirrors mem/blob_shard.go).
	idx := int(h.Bytes[0]) % docHashShardCount
	return s.shards[idx]
}

func (s *shardedDocByHashStore) Put(h codec.Hash, doc document.Document) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	sh.docs[h] = doc
	sh.mu.Unlock()
}

func (s *shardedDocByHashStore) Get(h codec.Hash) (document.Document, bool) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	d, ok := sh.docs[h]
	return d, ok
}
