package index

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

type putEvent struct {
	seq   int64
	entry Entry
}

type deleteEvent struct {
	seq      int64
	docID    codec.UUID
	atCommit codec.Hash
}

// MemoryStore is a chronological replay index for tests.
type MemoryStore struct {
	descriptor Descriptor
	log        *eventLog
}

// NewMemoryStore creates an in-memory index bound to a DAG.
func NewMemoryStore(descriptor Descriptor, d *dag.InMemoryCommitDag) *MemoryStore {
	return &MemoryStore{descriptor: descriptor, log: newEventLog(d)}
}

func (s *MemoryStore) Descriptor() Descriptor { return s.descriptor }

func (s *MemoryStore) Put(entry Entry) error {
	s.log.put(entry)
	return nil
}

func (s *MemoryStore) Delete(docID codec.UUID, atCommit codec.Hash) error {
	s.log.delete(docID, atCommit)
	return nil
}

func (s *MemoryStore) BulkLoad(entries []Entry) error {
	s.log.bulkLoad(entries)
	return nil
}

func (s *MemoryStore) Rebuild(entries []Entry) error { return s.BulkLoad(entries) }

func (s *MemoryStore) Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	cutoff, err := s.log.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	return s.log.lookup(key, cutoff), nil
}

func (s *MemoryStore) Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	cutoff, err := s.log.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	return s.log.rangeScan(from, to, cutoff, limit, ascending), nil
}

func (s *MemoryStore) Search(query string, atCommit *codec.Hash, limit int) ([]codec.UUID, error) {
	if s.descriptor.Type != IndexTypeFullText {
		return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeFullText, s.descriptor.Type)
	}
	return nil, fmt.Errorf("fulltext search not implemented")
}

func (s *MemoryStore) NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]RankedResult, error) {
	if s.descriptor.Type != IndexTypeVector {
		return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeVector, s.descriptor.Type)
	}
	return nil, fmt.Errorf("vector search not implemented")
}

func (s *MemoryStore) Clear() error {
	s.log.clear()
	return nil
}

func (s *MemoryStore) IsValid(atCommit codec.Hash) (bool, error) {
	return s.log.hasCommit(atCommit), nil
}

func (s *MemoryStore) Snapshot() ([]byte, error) {
	return s.log.snapshotBytes(), nil
}

func (s *MemoryStore) RestoreSnapshot(data []byte) error {
	return s.log.restoreSnapshotBytes(data)
}

func (s *MemoryStore) requireKeyedType() error {
	if s.descriptor.Type != IndexTypeHash && s.descriptor.Type != IndexTypeBTree {
		return NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeHash, s.descriptor.Type)
	}
	return nil
}

func pruneDoc(buckets map[Key]map[codec.UUID]struct{}, docID codec.UUID) {
	var dead []Key
	for k, ids := range buckets {
		delete(ids, docID)
		if len(ids) == 0 {
			dead = append(dead, k)
		}
	}
	for _, k := range dead {
		delete(buckets, k)
	}
}
