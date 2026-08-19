package mem

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// blobShardCount mirrors go/kdb/storage/engine/doc_shard.go's rationale:
// splitting one namespace-wide mutex across independent lock domains so
// WriteBlob/ReadBlob/rememberDocument for different content hashes can
// proceed in parallel instead of always serializing.
const blobShardCount = 64

type blobShard struct {
	mu         sync.Mutex
	blobs      map[codec.Hash][]byte
	docsByBlob map[codec.Hash]document.Document
}

type shardedBlobStore struct {
	shards [blobShardCount]*blobShard
}

func newShardedBlobStore() *shardedBlobStore {
	s := &shardedBlobStore{}
	for i := range s.shards {
		s.shards[i] = &blobShard{
			blobs:      make(map[codec.Hash][]byte),
			docsByBlob: make(map[codec.Hash]document.Document),
		}
	}
	return s
}

func (s *shardedBlobStore) shardFor(h codec.Hash) *blobShard {
	// Content hashes are SHA256 output, already uniformly distributed -
	// any byte works as a cheap shard selector.
	idx := int(h.Bytes[0]) % blobShardCount
	return s.shards[idx]
}

func (s *shardedBlobStore) WriteBlob(h codec.Hash, bytes []byte) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	sh.blobs[h] = bytes
	sh.mu.Unlock()
}

func (s *shardedBlobStore) ReadBlob(h codec.Hash) []byte {
	sh := s.shardFor(h)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	b := sh.blobs[h]
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func (s *shardedBlobStore) RememberDocument(doc document.Document) error {
	h, err := doc.ContentHash()
	if err != nil {
		return err
	}
	sh := s.shardFor(h)
	sh.mu.Lock()
	sh.blobs[h] = []byte(doc.JSON)
	sh.docsByBlob[h] = doc
	sh.mu.Unlock()
	return nil
}

func (s *shardedBlobStore) GetDocByBlob(h codec.Hash) (document.Document, bool) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	d, ok := sh.docsByBlob[h]
	return d, ok
}
